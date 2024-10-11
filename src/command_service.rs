use std::{any, collections::HashMap, fmt, marker::PhantomData, sync::Arc, time::Instant};

use chrono::{DateTime, Utc};
use eventus::{
    server::{eventstore::event_store_client::EventStoreClient, ClientAuthInterceptor},
    ExpectedVersion,
};
use futures::Future;
use kameo::{
    actor::{ActorID, ActorRef},
    error::{ActorStopReason, BoxError, SendError},
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
    Apply, Command, Entity, Event, Metadata,
};

/// The command service routes commands to spawned entity actors per stream id.
pub struct CommandService {
    event_store: EventStore,
    entities: HashMap<StreamID, (ActorID, Box<dyn any::Any + Send + Sync + 'static>)>,
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
        CommandService {
            event_store: new_event_store(client, workers),
            entities: HashMap::new(),
        }
    }

    /// Returns a reference to the inner event store.
    #[inline]
    pub fn event_store(&self) -> &EventStore {
        &self.event_store
    }
}

impl Actor for CommandService {
    type Mailbox = BoundedMailbox<Self>;

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
    fn execute(
        cmd_service: &ActorRef<CommandService>,
        command: Execute<E, C, E::Metadata>,
    ) -> impl Future<
        Output = Result<
            Vec<AppendedEvent<E::Event>>,
            SendError<Execute<E, C, E::Metadata>, ExecuteError<E::Error>>,
        >,
    >;
}

impl<E, C> ExecuteExt<E, C> for E
where
    E: Entity + Command<C> + Apply + Default + Sync,
    E::Event: Clone,
    E::Error: fmt::Debug + Send + Sync,
    C: Clone + Send + 'static,
{
    async fn execute(
        cmd_service: &ActorRef<CommandService>,
        command: Execute<E, C, E::Metadata>,
    ) -> Result<
        Vec<AppendedEvent<E::Event>>,
        SendError<Execute<E, C, E::Metadata>, ExecuteError<E::Error>>,
    > {
        cmd_service.ask(command).send().await
    }
}

#[derive(Debug)]
pub struct Execute<E, C, M>
where
    E: Entity,
{
    pub id: E::ID,
    pub command: C,
    pub metadata: Metadata<M>,
    pub expected_version: ExpectedVersion,
    pub time: DateTime<Utc>,
    pub executed_at: Instant,
    pub phantom: PhantomData<E>,
}

impl<E, C, M> Execute<E, C, M>
where
    E: Entity,
{
    pub fn new(id: E::ID, command: C) -> Self
    where
        M: Default,
    {
        Execute {
            id,
            command,
            metadata: Metadata::default(),
            expected_version: ExpectedVersion::Any,
            time: Utc::now(),
            executed_at: Instant::now(),
            phantom: PhantomData,
        }
    }

    pub fn caused_by(mut self, event_id: u64, stream_id: StreamID, stream_version: u64) -> Self {
        self.metadata.causation_event_id = Some(event_id);
        self.metadata.causation_stream_id = Some(stream_id);
        self.metadata.causation_stream_version = Some(stream_version);
        self
    }

    pub fn caused_by_event<F, N>(mut self, event: &Event<F, N>) -> Self {
        self.metadata.causation_event_id = Some(event.id);
        self.metadata.causation_stream_id = Some(event.stream_id.clone());
        self.metadata.causation_stream_version = Some(event.stream_version);
        self
    }

    pub fn correlation_id(mut self, id: Uuid) -> Self {
        self.metadata.correlation_id = id;
        self
    }

    pub fn metadata(mut self, metadata: M) -> Self {
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
}

pub struct AppendedEvent<E> {
    pub event: E,
    pub event_id: u64,
    pub stream_version: u64,
    pub timestamp: DateTime<Utc>,
}

impl<E, C> Message<Execute<E, C, E::Metadata>> for CommandService
where
    E: Command<C> + Apply,
    C: Clone + Send + 'static,
{
    type Reply = DelegatedReply<Result<Vec<AppendedEvent<E::Event>>, ExecuteError<E::Error>>>;

    async fn handle(
        &mut self,
        msg: Execute<E, C, E::Metadata>,
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

impl<E> Message<PrepareStream<E>> for CommandService
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
