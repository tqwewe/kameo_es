use std::{fmt, sync::Arc};

use kameo::{
    actor::ActorRef,
    error::{BoxError, SendError},
    message::{Context, Message},
    Actor,
};
use message_db::{
    database::GetStreamMessagesOpts, message::Message as DbMessage, stream_name::StreamName,
};
use tracing::debug;

use crate::{
    error::ExecuteError,
    event_store::{EventStore, GetStreamMessages, WriteMessages, WriteMessagesError},
    Command, Entity,
};

pub struct EntityActor<E> {
    entity: E,
    stream_name: StreamName,
    event_store: EventStore,
    version: i64,
    conflict_reties: usize,
}

impl<E> EntityActor<E> {
    pub fn new(entity: E, stream_name: StreamName, event_store: EventStore) -> Self {
        EntityActor {
            entity,
            stream_name,
            event_store,
            version: -1,
            conflict_reties: 3,
        }
    }

    async fn resync_with_db(
        &mut self,
    ) -> Result<
        (),
        SendError<
            GetStreamMessages<<E as Entity>::Event, <E as Entity>::Metadata>,
            message_db::Error,
        >,
    >
    where
        E: Entity,
    {
        loop {
            let messages = self
                .event_store
                .send(GetStreamMessages::<E::Event, E::Metadata>::new(
                    self.stream_name.clone(),
                    GetStreamMessagesOpts::builder()
                        .batch_size(1_000)
                        .position(self.version + 1)
                        .build(),
                ))
                .await
                .map_err(SendError::flatten)?;
            let len = messages.len();

            for message in messages {
                assert_eq!(self.version + 1, message.position);
                self.version = message.position;
                self.entity.apply_message(message);
            }

            if len < 1_000 {
                return Ok(());
            }
        }
    }
}

impl<E> Actor for EntityActor<E>
where
    E: Entity,
{
    fn name() -> &'static str {
        "EntityActor"
    }

    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        // Rebuild actor state
        self.resync_with_db().await?;

        Ok(())
    }
}

impl<E> Message<DbMessage<E::Event, E::Metadata>> for EntityActor<E>
where
    E: Entity,
{
    type Reply = ();

    async fn handle(
        &mut self,
        message: DbMessage<E::Event, E::Metadata>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.entity.apply_message(message)
    }
}

pub struct Execute<C, M> {
    pub id: Arc<str>,
    pub command: C,
    pub metadata: M,
    pub expected_version: Option<i64>,
}

impl<E, M> From<WriteMessagesError<M>> for ExecuteError<E> {
    fn from(err: WriteMessagesError<M>) -> Self {
        match err {
            WriteMessagesError::Database(err) => ExecuteError::Database(err),
            WriteMessagesError::IncorrectExpectedVersion {
                category,
                id,
                current,
                expected,
                ..
            } => ExecuteError::IncorrectExpectedVersion {
                category: category.into(),
                id: id.into(),
                current,
                expected,
            },
            WriteMessagesError::SerializeEvent(err) => ExecuteError::SerializeEvent(err),
        }
    }
}

impl<M, E, Me> From<SendError<M, WriteMessagesError<Me>>> for ExecuteError<E> {
    fn from(err: SendError<M, WriteMessagesError<Me>>) -> Self {
        match err {
            SendError::ActorNotRunning(_) => ExecuteError::EventStoreActorNotRunning,
            SendError::ActorStopped => ExecuteError::EventStoreActorStopped,
            SendError::HandlerError(err) => err.into(),
            SendError::QueriesNotSupported => panic!("the event store is never queried"),
        }
    }
}

impl<M, E> From<SendError<M, message_db::Error>> for ExecuteError<E> {
    fn from(err: SendError<M, message_db::Error>) -> Self {
        match err {
            SendError::ActorNotRunning(_) => ExecuteError::EventStoreActorNotRunning,
            SendError::ActorStopped => ExecuteError::EventStoreActorStopped,
            SendError::HandlerError(err) => err.into(),
            SendError::QueriesNotSupported => panic!("the event store is never queried"),
        }
    }
}

impl<E, C> Message<Execute<C, E::Metadata>> for EntityActor<E>
where
    E: Command<C>,
    E::Event: Clone,
    E::Error: fmt::Debug + Send + Sync + 'static,
    C: Clone + Send,
{
    type Reply = Result<Vec<E::Event>, ExecuteError<E::Error>>;

    async fn handle(
        &mut self,
        msg: Execute<C, E::Metadata>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let mut metadata = msg.metadata;
        let mut attempt = 1;
        let events = loop {
            if let Some(expected) = msg.expected_version {
                if self.version != expected {
                    return Err(ExecuteError::IncorrectExpectedVersion {
                        category: E::category().into(),
                        id: msg.id,
                        current: self.version,
                        expected,
                    });
                }
            }

            let events = self
                .entity
                .handle(msg.command.clone())
                .map_err(ExecuteError::Handle)?;
            let res = self
                .event_store
                .send(WriteMessages {
                    stream_name: self.stream_name.clone(),
                    messages: events.clone(),
                    expected_version: Some(self.version),
                    metadata,
                })
                .await
                .map_err(|err| err.flatten());
            match res {
                Ok(_) => {
                    break events;
                }
                Err(SendError::HandlerError(WriteMessagesError::IncorrectExpectedVersion {
                    category,
                    id,
                    current,
                    expected,
                    metadata: m,
                })) => {
                    debug!(%category, %id, %current, %expected, "write conflict");
                    if attempt == self.conflict_reties {
                        return Err(ExecuteError::TooManyConflicts {
                            category: E::category(),
                            id: msg.id,
                        });
                    }

                    self.resync_with_db().await?;

                    metadata = m;
                    attempt += 1;
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        };

        for event in events.clone() {
            self.entity.apply(event);
        }

        self.version += events.len() as i64;

        Ok(events) // TODO: Don't need to clone events if the message is async
    }
}

// pub struct SyncWithDb;

// impl<E> Message<SyncWithDb> for EntityActor<E>
// where
//     E: Entity,
// {
//     type Reply = ();

//     async fn handle(
//         &mut self,
//         msg: SyncWithDb,
//         ctx: Context<'_, Self, Self::Reply>,
//     ) -> Self::Reply {
//         let messages = self
//             .event_store
//             .send(GetStreamMessages::<E::Event, E::Metadata>::new(
//                 self.stream_name.clone(),
//                 GetStreamMessagesOpts::builder()
//                     .position(self.version)
//                     .build(),
//             ))
//             .await?;
//         for message in messages {
//             self.version = message.position;
//             self.entity.apply_message(message);
//         }
//     }
// }
