pub mod entity_actor;
pub mod error;
pub mod event_store;

use std::{
    any, collections::HashMap, convert::Infallible, fmt, future::Future, marker::PhantomData,
    sync::Arc,
};

use entity_actor::EntityActor;
use error::ExecuteError;
use event_store::EventStore;
use kameo::{
    actor::ActorRef,
    error::SendError,
    message::{Context, Message},
    reply::DelegatedReply,
    Actor,
};
use message_db::stream_name::StreamName;
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

pub trait Entity: Send + 'static {
    type Event: EventType + Serialize + DeserializeOwned + fmt::Debug + Send + Sync;
    type Metadata: Serialize + DeserializeOwned + Default + Unpin + Send + Sync + 'static;

    fn category() -> &'static str;

    fn apply(&mut self, event: Self::Event);

    fn apply_message(
        &mut self,
        message: message_db::message::Message<Self::Event, Self::Metadata>,
    ) {
        self.apply(message.data)
    }
}

pub trait Command<C>: Entity {
    type Error;

    fn handle(&self, cmd: C) -> Result<Vec<Self::Event>, Self::Error>;
}

pub trait EventType {
    fn event_type(&self) -> &'static str;
}

#[derive(Actor)]
pub struct CommandService {
    event_store: EventStore,
    entities: HashMap<StreamName, Box<dyn any::Any + Send + Sync + 'static>>,
}

impl CommandService {
    pub fn new(event_store: EventStore) -> Self {
        CommandService {
            event_store,
            entities: HashMap::new(),
        }
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
            Vec<<E as Entity>::Event>,
            SendError<Execute<E, C, E::Metadata>, ExecuteError<E::Error>>,
        >,
    >;
}

impl<E, C> ExecuteExt<E, C> for E
where
    E: Entity + Command<C> + Default + Sync,
    E::Event: Clone,
    E::Error: fmt::Debug + Send + Sync,
    C: Clone + Send + 'static,
{
    async fn execute(
        cmd_service: &ActorRef<CommandService>,
        command: Execute<E, C, E::Metadata>,
    ) -> Result<
        Vec<<E as Entity>::Event>,
        SendError<Execute<E, C, E::Metadata>, ExecuteError<E::Error>>,
    > {
        cmd_service.send(command).await
    }
}

#[derive(Debug)]
pub struct Execute<E, C, M> {
    pub id: Arc<str>,
    pub command: C,
    pub metadata: M,
    pub expected_version: Option<i64>,
    pub phantom: PhantomData<E>,
}

impl<E, C, M> Execute<E, C, M> {
    pub fn new(id: impl Into<Arc<str>>, command: C) -> Self
    where
        M: Default,
    {
        Execute {
            id: id.into(),
            command,
            metadata: M::default(),
            expected_version: None,
            phantom: PhantomData,
        }
    }

    pub fn metadata(mut self, metadata: M) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn expected_version(mut self, version: i64) -> Self {
        self.expected_version = Some(version);
        self
    }
}

impl<E, C> Message<Execute<E, C, E::Metadata>> for CommandService
where
    E: Command<C> + Default + Sync,
    E::Event: Clone,
    E::Error: fmt::Debug + Send + Sync,
    C: Clone + Send + 'static,
{
    type Reply = DelegatedReply<Result<Vec<<E as Entity>::Event>, ExecuteError<E::Error>>>;

    async fn handle(
        &mut self,
        msg: Execute<E, C, E::Metadata>,
        mut ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let stream_name = StreamName::new_from_parts(E::category(), &msg.id);
        let entity_ref = match self.entities.get(&stream_name) {
            Some(actor_ref) => actor_ref
                .downcast_ref::<ActorRef<EntityActor<E>>>()
                .cloned()
                .unwrap(),
            None => {
                let entity_ref = kameo::spawn(EntityActor::new(
                    E::default(),
                    stream_name.clone(),
                    self.event_store.clone(),
                ));

                self.entities
                    .insert(stream_name, Box::new(entity_ref.clone()));

                entity_ref
            }
        };

        let (delegated_reply, reply_sender) = ctx.reply_sender();
        tokio::spawn(async move {
            let reply = entity_ref
                .send(entity_actor::Execute {
                    id: msg.id.clone(),
                    command: msg.command,
                    expected_version: msg.expected_version,
                    metadata: msg.metadata,
                })
                .await;
            if let Some(reply_sender) = reply_sender {
                let reply = reply.map_err(|err| match err {
                    SendError::ActorNotRunning(_) => ExecuteError::EntityActorNotRunning {
                        category: E::category(),
                        id: msg.id,
                    },
                    SendError::ActorStopped => ExecuteError::EntityActorStopped {
                        category: E::category(),
                        id: msg.id,
                    },
                    SendError::HandlerError(err) => err,
                    SendError::QueriesNotSupported => panic!("the entity actor is never queried"),
                });
                reply_sender.send(reply);
            }
        });

        delegated_reply
    }
}

#[derive(Debug, Error)]
pub enum Error<M = (), E = Infallible> {
    #[error(transparent)]
    Database(#[from] message_db::Error),
    #[error(transparent)]
    SendError(#[from] SendError<M, E>),
}
