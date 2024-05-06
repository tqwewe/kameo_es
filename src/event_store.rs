use std::{fmt, marker::PhantomData};

use kameo::{
    actor::{ActorPool, ActorRef},
    message::{Context, Message},
    Actor,
};
use message_db::{
    database::{GetStreamMessagesOpts, MessageStore, WriteMessageOpts},
    stream_name::StreamName,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{Error, EventType};

pub type EventStore = ActorRef<ActorPool<EventStoreWorker>>;

pub fn new_event_store(store: MessageStore, size: usize) -> EventStore {
    kameo::spawn(ActorPool::new(size, move || {
        kameo::spawn(EventStoreWorker {
            store: store.clone(),
        })
    }))
}

#[derive(Actor)]
pub struct EventStoreWorker {
    store: MessageStore,
}

pub struct GetStreamMessages<E, M> {
    stream_name: StreamName,
    opts: GetStreamMessagesOpts<'static>,
    phantom: PhantomData<(E, M)>,
}

impl<E, M> GetStreamMessages<E, M> {
    pub fn new(
        stream_name: StreamName,
        opts: GetStreamMessagesOpts<'static>,
    ) -> GetStreamMessages<E, M> {
        GetStreamMessages {
            stream_name,
            opts,
            phantom: PhantomData,
        }
    }
}

impl<E, M> Message<GetStreamMessages<E, M>> for EventStoreWorker
where
    E: DeserializeOwned + Send + 'static,
    M: DeserializeOwned + Default + Unpin + Send + 'static,
{
    type Reply = Result<Vec<message_db::message::Message<E, M>>, message_db::Error>;

    async fn handle(
        &mut self,
        msg: GetStreamMessages<E, M>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let messages =
            MessageStore::get_stream_messages::<E, M, _>(&self.store, &msg.stream_name, &msg.opts)
                .await?;

        Ok(messages)
    }
}

#[derive(Debug)]
pub struct WriteMessages<E, M> {
    pub stream_name: StreamName,
    pub messages: Vec<E>,
    pub expected_version: Option<i64>,
    pub metadata: M,
}

#[derive(Error)]
pub enum WriteMessagesError<M> {
    #[error(transparent)]
    Database(message_db::Error),
    #[error("expected '{category}-{id}' version {expected} but got {current}")]
    IncorrectExpectedVersion {
        category: String,
        id: String,
        current: i64,
        expected: i64,
        metadata: M,
    },
    #[error(transparent)]
    SerializeEvent(#[from] serde_json::Error),
}

impl<M> fmt::Debug for WriteMessagesError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(arg0) => f.debug_tuple("Database").field(arg0).finish(),
            Self::IncorrectExpectedVersion {
                category,
                id,
                current,
                expected,
                ..
            } => f
                .debug_struct("IncorrectExpectedVersion")
                .field("category", category)
                .field("id", id)
                .field("current", current)
                .field("expected", expected)
                .finish(),
            Self::SerializeEvent(arg0) => f.debug_tuple("SerializeEvent").field(arg0).finish(),
        }
    }
}

impl<M> From<(M, message_db::Error)> for WriteMessagesError<M> {
    fn from((metadata, err): (M, message_db::Error)) -> Self {
        if let message_db::Error::Database(err) = &err {
            if let Some(err) = err.as_database_error() {
                let msg = err.message().trim();
                let prefix = "Wrong expected version: ";
                if msg.starts_with(prefix) {
                    let msg = msg.split_at(prefix.len()).1;
                    let (expected, msg) = msg.split_once(' ').unwrap();
                    let expected = expected.parse().unwrap();
                    let prefix = " (Stream: ";
                    let (_, msg) = msg.split_at(prefix.len() - 1);
                    let (stream_name, msg) = msg.split_once(", Stream Version: ").unwrap();
                    let (category, id) = stream_name.split_once('-').unwrap();
                    let (current, _) = msg.split_once(')').unwrap();
                    let current = current.parse().unwrap();

                    return WriteMessagesError::IncorrectExpectedVersion {
                        category: category.to_string(),
                        id: id.to_string(),
                        current,
                        expected,
                        metadata,
                    };
                }
            }
        }

        WriteMessagesError::Database(err)
    }
}

impl<E, M> Message<WriteMessages<E, M>> for EventStoreWorker
where
    E: EventType + Serialize + Send + 'static,
    M: Serialize + Send + Sync + 'static,
{
    type Reply = Result<i64, WriteMessagesError<M>>;

    async fn handle(
        &mut self,
        msg: WriteMessages<E, M>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let messages: Vec<_> = msg
            .messages
            .iter()
            .enumerate()
            .map(|(i, event)| {
                let opts = match (msg.expected_version, &msg.metadata) {
                    (Some(v), metadata) => WriteMessageOpts::builder()
                        .expected_version(v + i as i64)
                        .metadata(metadata)
                        .build(),
                    (None, metadata) => WriteMessageOpts::builder().metadata(metadata).build(),
                };
                Ok((event.event_type(), serde_json::to_value(event)?, opts))
            })
            .collect::<Result<_, serde_json::Error>>()?;
        let messages_ref: Vec<_> = messages
            .iter()
            .map(|(event_type, data, opts)| (*event_type, data, opts))
            .collect();
        let latest_version =
            MessageStore::write_messages(&self.store, &msg.stream_name, &messages_ref)
                .await
                .map_err(|err| (msg.metadata, err))?;

        Ok(latest_version)
    }
}
