use std::{any, collections::HashMap, fmt, marker::PhantomData, sync::Arc};

use eventus::{server::eventstore::event_store_client::EventStoreClient, ExpectedVersion};
use futures::Future;
use kameo::{
    actor::{ActorRef, BoundedMailbox},
    error::{ActorStopReason, BoxError, SendError},
    message::{Context, Message},
    reply::DelegatedReply,
    Actor,
};
use tokio::sync::Notify;
use tonic::transport::Channel;

use crate::{
    entity_actor::{self, EntityActor},
    error::ExecuteError,
    event_store::{new_event_store, EventStore},
    stream_id::StreamID,
    Apply, Command, Entity,
};

/// The command service routes commands to spawned entity actors per stream id.
pub struct CommandService {
    event_store: EventStore,
    entities: HashMap<StreamID, (u64, Box<dyn any::Any + Send + Sync + 'static>)>,
}

impl CommandService {
    /// Creates a new command service using an event store client connection and default worker count.
    #[inline]
    pub fn new(client: EventStoreClient<Channel>) -> Self {
        Self::new_with_workers(client, 16)
    }

    /// Creates a new command service using an event store client connection and worker count.
    #[inline]
    pub fn new_with_workers(client: EventStoreClient<Channel>, workers: usize) -> Self {
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
        id: u64,
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
            Vec<E::Event>,
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
    ) -> Result<Vec<E::Event>, SendError<Execute<E, C, E::Metadata>, ExecuteError<E::Error>>> {
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
    pub metadata: M,
    pub expected_version: ExpectedVersion,
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
            metadata: M::default(),
            expected_version: ExpectedVersion::Any,
            phantom: PhantomData,
        }
    }

    pub fn metadata(mut self, metadata: M) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn expected_version(mut self, expected: ExpectedVersion) -> Self {
        self.expected_version = expected;
        self
    }
}

impl<E, C> Message<Execute<E, C, E::Metadata>> for CommandService
where
    E: Command<C> + Apply,
    C: Clone + Send + 'static,
{
    type Reply = DelegatedReply<Result<Vec<E::Event>, ExecuteError<E::Error>>>;

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
                let entity_ref = kameo::actor::spawn_unsync(EntityActor::new(
                    E::default(),
                    stream_name.clone(),
                    self.event_store.clone(),
                    Arc::new(Notify::new()),
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
                let notify = Arc::new(Notify::new());
                let entity_ref = kameo::spawn(EntityActor::new(
                    E::default(),
                    stream_name.clone(),
                    self.event_store.clone(),
                    notify.clone(),
                ));

                entity_ref.link_child(&ctx.actor_ref()).await;

                self.entities
                    .insert(stream_name, (entity_ref.id(), Box::new(entity_ref.clone())));
                notify.notified().await;
            }
        };
    }
}
