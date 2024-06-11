use std::{fmt, future::Future, marker::PhantomData};

use commitlog::{
    server::eventstore::{
        event_store_client::EventStoreClient, AppendToStreamRequest, GetStreamEventsRequest,
        NewEvent,
    },
    CurrentVersion, Event, ExpectedVersion,
};
use futures::{stream::BoxStream, TryStreamExt};
use kameo::{
    actor::{ActorPool, ActorRef},
    message::{Context, Message},
    Actor,
};
use serde::{de::DeserializeOwned, Serialize};
use tonic::{transport::Channel, Status};

use crate::{stream_id::StreamID, Error, EventType};

pub type EventStore = ActorRef<ActorPool<EventStoreWorker>>;

pub fn new_event_store(client: EventStoreClient<Channel>, size: usize) -> EventStore {
    kameo::spawn(ActorPool::new(size, move || {
        kameo::spawn(EventStoreWorker {
            client: client.clone(),
        })
    }))
}

#[derive(Actor)]
pub struct EventStoreWorker {
    client: EventStoreClient<Channel>,
}

pub struct GetStreamEvents<E, M> {
    stream_id: StreamID,
    stream_version: u64,
    phantom: PhantomData<(E, M)>,
}

impl<E, M> GetStreamEvents<E, M> {
    pub fn new(stream_id: StreamID, stream_version: u64) -> GetStreamEvents<E, M> {
        GetStreamEvents {
            stream_id,
            stream_version,
            phantom: PhantomData,
        }
    }
}

impl<E, M> Message<GetStreamEvents<E, M>> for EventStoreWorker
where
    E: DeserializeOwned + Send + 'static,
    M: DeserializeOwned + Default + Unpin + Send + 'static,
{
    type Reply = Result<BoxStream<'static, Result<Vec<Event<'static>>, Status>>, Status>;

    fn handle(
        &mut self,
        msg: GetStreamEvents<E, M>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> impl Future<Output = Self::Reply> + Send {
        async move {
            let stream = self
                .client
                .get_stream_events(GetStreamEventsRequest {
                    stream_id: msg.stream_id.into_inner(),
                    stream_version: msg.stream_version,
                    batch_size: 1000,
                })
                .await?
                .into_inner()
                .map_ok(|batch| {
                    batch
                        .events
                        .into_iter()
                        .map(|event| Event::<'static>::try_from(event).expect("invalid timestamp"))
                        .collect()
                });

            Ok(Box::pin(stream) as BoxStream<'static, _>)
        }
    }
}

#[derive(Debug)]
pub struct AppendEvents<E, M> {
    pub stream_name: StreamID,
    pub events: Vec<E>,
    pub expected_version: ExpectedVersion,
    pub metadata: M,
}

#[derive(Error)]
pub enum AppendEventsError<M> {
    #[error(transparent)]
    Database(#[from] Status),
    #[error("expected '{category}-{id}' version {expected} but got {current}")]
    IncorrectExpectedVersion {
        category: String,
        id: String,
        current: CurrentVersion,
        expected: ExpectedVersion,
        metadata: M,
    },
    #[error(transparent)]
    SerializeEvent(#[from] serde_json::Error),
}

impl<M> fmt::Debug for AppendEventsError<M> {
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

impl<E, M> Message<AppendEvents<E, M>> for EventStoreWorker
where
    E: EventType + Serialize + Send + 'static,
    M: Serialize + Send + Sync + 'static,
{
    type Reply = Result<(), AppendEventsError<M>>;

    async fn handle(
        &mut self,
        msg: AppendEvents<E, M>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let metadata =
            serde_json::to_vec(&msg.metadata).map_err(AppendEventsError::SerializeEvent)?;
        let events = msg
            .events
            .iter()
            .map(|event| {
                Ok(NewEvent {
                    event_name: event.event_type().to_string(),
                    event_data: serde_json::to_vec(&event)?,
                    metadata: metadata.clone(),
                })
            })
            .collect::<Result<_, serde_json::Error>>()?;
        let req = AppendToStreamRequest {
            stream_id: msg.stream_name.into_inner(),
            expected_version: Some(msg.expected_version.into()),
            events,
        };

        self.client.append_to_stream(req).await?;

        Ok(())
    }
}
