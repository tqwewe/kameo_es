use std::{
    any, collections::HashMap, fmt, future::IntoFuture, marker::PhantomData, sync::Arc,
    time::Instant,
};

use chrono::{DateTime, Utc};
use eventus::{
    server::{eventstore::event_store_client::EventStoreClient, ClientAuthInterceptor},
    ExpectedVersion,
};
use futures::{future::BoxFuture, FutureExt};
use kameo::{
    actor::{ActorID, ActorRef, WeakActorRef},
    error::{ActorStopReason, BoxError, PanicError, SendError},
    mailbox::bounded::BoundedMailbox,
    message::{Context, Message},
    reply::DelegatedReply,
    request::{ForwardMessageSend, MessageSend},
    Actor,
};
use tonic::{service::interceptor::InterceptedService, transport::Channel};
use uuid::Uuid;

use crate::{
    entity_actor::{self, EntityActor},
    error::ExecuteError,
    event_store::{new_event_store, EventStore},
    stream_id::StreamID,
    Apply, CausationMetadata, Command, Entity, Event, Metadata, Transaction, TransactionSender,
};

#[derive(Clone, Debug)]
pub struct CommandService {
    actor_ref: ActorRef<CommandServiceActor>,
    event_store: EventStore,
}

impl CommandService {
    /// Creates a new command service using an event store client connection and default worker count.
    #[inline]
    pub fn new(
        client: EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
    ) -> Self {
        Self::new_with_workers(client, 16)
    }

    /// Creates a new command service using an event store client connection and worker count.
    #[inline]
    pub fn new_with_workers(
        client: EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
        workers: usize,
    ) -> Self {
        let event_store = new_event_store(client, workers);
        let actor_ref = kameo::spawn(CommandServiceActor {
            event_store: event_store.clone(),
            entities: HashMap::new(),
        });

        CommandService {
            actor_ref,
            event_store,
        }
    }

    /// Returns a reference to the inner event store.
    #[inline]
    pub fn event_store(&self) -> &EventStore {
        &self.event_store
    }

    /// Starts a transaction.
    #[inline]
    pub fn transaction(&self) -> Transaction {
        Transaction::new(self.event_store.clone())
    }
}

/// The command service routes commands to spawned entity actors per stream id.
struct CommandServiceActor {
    event_store: EventStore,
    entities: HashMap<StreamID, (ActorID, Box<dyn any::Any + Send + Sync + 'static>)>,
}

impl Actor for CommandServiceActor {
    type Mailbox = BoundedMailbox<Self>;

    async fn on_stop(
        self,
        _actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> Result<(), BoxError> {
        println!("command service actor stopped?? {reason:?}");
        Ok(())
    }

    async fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> Result<Option<ActorStopReason>, BoxError> {
        println!("command service actor errored: {err}");
        Ok(None) // Restart
    }

    async fn on_link_died(
        &mut self,
        _actor_ref: kameo::actor::WeakActorRef<Self>,
        id: ActorID,
        _reason: kameo::error::ActorStopReason,
    ) -> Result<Option<ActorStopReason>, BoxError> {
        self.entities
            .retain(|_, (existing_id, _)| *existing_id != id);
        Ok(None)
    }
}

pub trait ExecuteExt<E, C>
where
    E: Entity + Command<C>,
{
    fn execute<'a>(cmd_service: &'a CommandService, id: E::ID, command: C) -> Execute<'a, E, C>;
}

impl<E, C> ExecuteExt<E, C> for E
where
    E: Entity + Command<C> + Apply + Default + Sync,
    E::Event: Clone,
    E::Error: fmt::Debug + Send + Sync,
    C: Clone + Send + 'static,
{
    fn execute<'a>(cmd_service: &'a CommandService, id: E::ID, command: C) -> Execute<'a, E, C> {
        Execute::new(cmd_service, id, command)
    }
}

#[derive(Debug)]
pub struct Execute<'a, E, C>
where
    E: Entity,
{
    cmd_service: &'a CommandService,
    id: E::ID,
    command: C,
    metadata: Metadata<E::Metadata>,
    expected_version: ExpectedVersion,
    time: DateTime<Utc>,
    executed_at: Instant,
    tx_sender: Option<TransactionSender>,
    phantom: PhantomData<E>,
}

pub enum ExecuteResult<E> {
    /// The command was executed with the resulting events.
    Executed(Vec<AppendedEvent<E>>),
    /// The command was executed, but no new events due to idempotency
    Idempotent,
}

impl<E> ExecuteResult<E> {
    pub fn len(&self) -> usize {
        match self {
            ExecuteResult::Executed(events) => events.len(),
            ExecuteResult::Idempotent => 0,
        }
    }

    pub fn into_iter(self) -> impl Iterator<Item = AppendedEvent<E>> {
        match self {
            ExecuteResult::Executed(events) => events.into_iter(),
            ExecuteResult::Idempotent => vec![].into_iter(),
        }
    }
}

impl<'a, E, C> Execute<'a, E, C>
where
    E: Entity,
{
    fn new(cmd_service: &'a CommandService, id: E::ID, command: C) -> Self {
        Execute {
            cmd_service,
            id,
            command,
            metadata: Metadata::default(),
            expected_version: ExpectedVersion::Any,
            time: Utc::now(),
            executed_at: Instant::now(),
            tx_sender: None,
            phantom: PhantomData,
        }
    }

    pub fn caused_by(mut self, causation_metadata: CausationMetadata) -> Self {
        self.metadata.causation = Some(causation_metadata);
        self
    }

    pub fn caused_by_event<F, N>(self, event: &Event<F, N>) -> Self {
        self.caused_by(CausationMetadata {
            event_id: event.id,
            stream_id: event.stream_id.clone(),
            stream_version: event.stream_version,
            correlation_id: event.metadata.correlation_id,
        })
    }

    pub fn correlation_id(mut self, id: Uuid) -> Self {
        self.metadata.correlation_id = id;
        self
    }

    pub fn metadata(mut self, metadata: E::Metadata) -> Self {
        self.metadata = self.metadata.with_data(metadata);
        self
    }

    pub fn expected_version(mut self, expected: ExpectedVersion) -> Self {
        self.expected_version = expected;
        self
    }

    pub fn current_time(mut self, time: DateTime<Utc>) -> Self {
        self.time = time;
        self
    }

    pub fn transaction(mut self, tx: &Transaction) -> Self {
        self.tx_sender = Some(tx.sender());
        self
    }
}

impl<'a, E, C> IntoFuture for Execute<'a, E, C>
where
    E: Entity + Command<C> + Apply,
    C: Clone + Send + 'static,
{
    type Output = Result<ExecuteResult<E::Event>, ExecuteError<E::Error>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            self.cmd_service
                .actor_ref
                .ask(ExecuteMsg {
                    id: self.id,
                    command: self.command,
                    metadata: self.metadata,
                    expected_version: self.expected_version,
                    time: self.time,
                    executed_at: self.executed_at,
                    tx_sender: self.tx_sender,
                    phantom: PhantomData::<E>,
                })
                .send()
                .await
                .map_err(|err| match err {
                    SendError::ActorNotRunning(_) => ExecuteError::CommandServiceNotRunning,
                    SendError::ActorStopped => ExecuteError::CommandServiceStopped,
                    SendError::MailboxFull(_) => {
                        unreachable!("messages aren't sent to the command service with try_")
                    }
                    SendError::HandlerError(err) => err,
                    SendError::Timeout(_) => {
                        unreachable!("messages aren't sent to the command service with timeouts")
                    }
                })
        }
        .boxed()
    }
}

pub struct AppendedEvent<E> {
    pub event: E,
    pub event_id: u64, // TODO: Make this only available when not using a transaction
    pub stream_version: u64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug)]
struct ExecuteMsg<E, C>
where
    E: Entity,
{
    id: E::ID,
    command: C,
    metadata: Metadata<E::Metadata>,
    expected_version: ExpectedVersion,
    time: DateTime<Utc>,
    executed_at: Instant,
    tx_sender: Option<TransactionSender>,
    phantom: PhantomData<E>,
}

impl<E, C> Message<ExecuteMsg<E, C>> for CommandServiceActor
where
    E: Command<C> + Apply,
    C: Clone + Send + 'static,
{
    type Reply = DelegatedReply<Result<ExecuteResult<E::Event>, ExecuteError<E::Error>>>;

    async fn handle(
        &mut self,
        msg: ExecuteMsg<E, C>,
        mut ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let stream_name = StreamID::new_from_parts(E::name(), &msg.id);
        let entity_ref = match self.entities.get(&stream_name) {
            Some((_, actor_ref)) => actor_ref
                .downcast_ref::<ActorRef<EntityActor<E>>>()
                .cloned()
                .unwrap(),
            None => {
                let entity_ref = kameo::actor::spawn(EntityActor::new(
                    E::default(),
                    stream_name.clone(),
                    self.event_store.clone(),
                ));

                entity_ref.link_child(&ctx.actor_ref()).await;

                self.entities
                    .insert(stream_name, (entity_ref.id(), Box::new(entity_ref.clone())));

                entity_ref
            }
        };

        let (delegated_reply, reply_sender) = ctx.reply_sender();
        match reply_sender {
            Some(tx) => {
                let _ = entity_ref
                    .ask(entity_actor::Execute {
                        id: msg.id,
                        command: msg.command,
                        expected_version: msg.expected_version,
                        metadata: msg.metadata,
                        time: msg.time,
                        executed_at: msg.executed_at,
                        tx_sender: msg.tx_sender,
                    })
                    .forward(tx)
                    .await;
            }
            None => {
                let _ = entity_ref
                    .tell(entity_actor::Execute {
                        id: msg.id,
                        command: msg.command,
                        expected_version: msg.expected_version,
                        metadata: msg.metadata,
                        time: msg.time,
                        executed_at: msg.executed_at,
                        tx_sender: msg.tx_sender,
                    })
                    .send()
                    .await;
            }
        }

        delegated_reply
    }
}

pub struct PrepareStream<E> {
    pub id: Arc<str>,
    pub phantom: PhantomData<E>,
}

impl<E> Message<PrepareStream<E>> for CommandServiceActor
where
    E: Entity + Apply + Default + Send + Sync,
{
    type Reply = ();

    async fn handle(
        &mut self,
        msg: PrepareStream<E>,
        ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let stream_name = StreamID::new_from_parts(E::name(), &msg.id);
        match self.entities.get(&stream_name) {
            Some(_) => {}
            None => {
                let entity_ref = kameo::spawn(EntityActor::new(
                    E::default(),
                    stream_name.clone(),
                    self.event_store.clone(),
                ));

                entity_ref.link_child(&ctx.actor_ref()).await;

                self.entities
                    .insert(stream_name, (entity_ref.id(), Box::new(entity_ref.clone())));
                entity_ref.wait_startup().await;
            }
        };
    }
}
