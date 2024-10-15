use std::{fmt, future::Future, io, marker::PhantomData};

use chrono::{DateTime, Utc};
use eventus::{
    server::{
        eventstore::{
            event_store_client::EventStoreClient, AppendToMultipleStreamsRequest,
            AppendToStreamRequest, GetStreamEventsRequest, NewEvent, StreamEvents,
        },
        ClientAuthInterceptor,
    },
    CurrentVersion, Event, ExpectedVersion,
};
use futures::{stream::BoxStream, TryStreamExt};
use kameo::{
    actor::{pool::ActorPool, ActorRef},
    message::{Context, Message},
    Actor,
};
use prost_types::Timestamp;
use serde::{de::DeserializeOwned, Serialize};
use tonic::{service::interceptor::InterceptedService, transport::Channel, Code, Status};

use crate::{stream_id::StreamID, Error, EventType, GenericValue};

pub type EventStore = ActorRef<ActorPool<EventStoreWorker>>;

pub fn new_event_store(
    client: EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
    size: usize,
) -> EventStore {
    kameo::spawn(ActorPool::new(size, move || {
        kameo::spawn(EventStoreWorker {
            client: client.clone(),
        })
    }))
}

#[derive(Actor)]
pub struct EventStoreWorker {
    client: EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
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
    pub stream_id: StreamID,
    pub events: Vec<E>,
    pub expected_version: ExpectedVersion,
    pub metadata: M,
    pub timestamp: DateTime<Utc>,
}

#[derive(Error)]
pub enum AppendEventsError<E, M> {
    #[error(transparent)]
    Database(Status),
    #[error("expected '{stream_id}' version {expected} but got {current}")]
    IncorrectExpectedVersion {
        stream_id: StreamID,
        current: CurrentVersion,
        expected: ExpectedVersion,
        events: E,
        metadata: M,
    },
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error(transparent)]
    SerializeEvent(#[from] ciborium::ser::Error<io::Error>),
}

impl<E, M> fmt::Debug for AppendEventsError<E, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(db) => f.debug_tuple("Database").field(db).finish(),
            Self::IncorrectExpectedVersion {
                stream_id,
                current,
                expected,
                ..
            } => f
                .debug_struct("IncorrectExpectedVersion")
                .field("stream_id", stream_id)
                .field("current", current)
                .field("expected", expected)
                .finish(),
            Self::InvalidTimestamp => f.debug_struct("InvalidTimestamp").finish(),
            Self::SerializeEvent(ev) => f.debug_tuple("SerializeEvent").field(ev).finish(),
        }
    }
}

impl<E, M> Message<AppendEvents<E, M>> for EventStoreWorker
where
    E: EventType + Serialize + Send + Sync + 'static,
    M: Serialize + Send + Sync + 'static,
{
    type Reply = Result<(u64, DateTime<Utc>), AppendEventsError<Vec<E>, M>>;

    async fn handle(
        &mut self,
        msg: AppendEvents<E, M>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let mut metadata = Vec::new();
        ciborium::into_writer(&msg.metadata, &mut metadata)?;
        let events = msg
            .events
            .iter()
            .map(|event| {
                let mut event_data = Vec::new();
                ciborium::into_writer(&event, &mut event_data)?;
                Ok(NewEvent {
                    event_name: event.event_type().to_string(),
                    event_data,
                    metadata: metadata.clone(),
                })
            })
            .collect::<Result<_, ciborium::ser::Error<io::Error>>>()?;
        let req = AppendToStreamRequest {
            stream_id: msg.stream_id.clone().into_inner(),
            expected_version: Some(msg.expected_version.into()),
            events,
            timestamp: Some(Timestamp {
                seconds: msg.timestamp.timestamp(),
                nanos: msg
                    .timestamp
                    .timestamp_subsec_nanos()
                    .try_into()
                    .map_err(|_| AppendEventsError::InvalidTimestamp)?,
            }),
        };

        let res = self.client.append_to_stream(req).await;
        let res = match res {
            Ok(res) => res.into_inner(),
            Err(status) => match status.code() {
                Code::FailedPrecondition => {
                    let current = status.metadata().get("current").unwrap();
                    let current = match current.to_str().unwrap() {
                        "-1" => CurrentVersion::NoStream,
                        s => CurrentVersion::Current(s.parse::<u64>().unwrap()),
                    };

                    let expected = status.metadata().get("expected").unwrap();
                    let expected = match expected.to_str().unwrap() {
                        "any" => ExpectedVersion::Any,
                        "stream_exists" => ExpectedVersion::StreamExists,
                        "no_stream" => ExpectedVersion::NoStream,
                        s => ExpectedVersion::Exact(s.parse::<u64>().unwrap()),
                    };

                    return Err(AppendEventsError::IncorrectExpectedVersion {
                        stream_id: msg.stream_id,
                        current,
                        expected,
                        events: msg.events,
                        metadata: msg.metadata,
                    });
                }
                _ => return Err(AppendEventsError::Database(status)),
            },
        };

        let id = res.first_id;
        let timestamp = res.timestamp.unwrap();

        Ok((
            id,
            DateTime::from_timestamp(timestamp.seconds, timestamp.nanos.try_into().unwrap())
                .unwrap()
                .to_utc(),
        ))
    }
}

#[derive(Debug)]
pub struct AppendTransactionEvents {
    pub stream_events: Vec<AppendEvents<(&'static str, GenericValue), GenericValue>>,
}

impl Message<AppendTransactionEvents> for EventStoreWorker {
    type Reply = Result<
        Vec<(u64, DateTime<Utc>)>,
        AppendEventsError<
            Vec<AppendEvents<(&'static str, GenericValue), GenericValue>>,
            GenericValue,
        >,
    >;

    fn handle(
        &mut self,
        msg: AppendTransactionEvents,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> impl Future<Output = Self::Reply> + Send {
        async move {
            let mut streams = Vec::with_capacity(msg.stream_events.len());
            for batch in &msg.stream_events {
                let mut metadata = Vec::new();
                ciborium::into_writer(&batch.metadata, &mut metadata)?;
                let events = batch
                    .events
                    .iter()
                    .map(|(event_name, event)| {
                        let mut event_data = Vec::new();
                        ciborium::into_writer(&event, &mut event_data)?;
                        Ok(NewEvent {
                            event_name: event_name.to_string(),
                            event_data,
                            metadata: metadata.clone(),
                        })
                    })
                    .collect::<Result<_, ciborium::ser::Error<io::Error>>>()?;

                streams.push(StreamEvents {
                    stream_id: batch.stream_id.clone().into_inner(),
                    expected_version: Some(batch.expected_version.into()),
                    events,
                    timestamp: Some(Timestamp {
                        seconds: batch.timestamp.timestamp(),
                        nanos: batch
                            .timestamp
                            .timestamp_subsec_nanos()
                            .try_into()
                            .map_err(|_| AppendEventsError::InvalidTimestamp)?,
                    }),
                });
            }

            let res = self
                .client
                .append_to_multiple_streams(AppendToMultipleStreamsRequest { streams })
                .await;
            let res = match res {
                Ok(res) => res.into_inner(),
                Err(status) => match status.code() {
                    Code::FailedPrecondition => {
                        let stream_id = StreamID::new(
                            status
                                .metadata()
                                .get("stream_id")
                                .unwrap()
                                .to_str()
                                .unwrap()
                                .to_string(),
                        );

                        let current = status.metadata().get("current").unwrap();
                        let current = match current.to_str().unwrap() {
                            "-1" => CurrentVersion::NoStream,
                            s => CurrentVersion::Current(s.parse::<u64>().unwrap()),
                        };

                        let expected = status.metadata().get("expected").unwrap();
                        let expected = match expected.to_str().unwrap() {
                            "any" => ExpectedVersion::Any,
                            "stream_exists" => ExpectedVersion::StreamExists,
                            "no_stream" => ExpectedVersion::NoStream,
                            s => ExpectedVersion::Exact(s.parse::<u64>().unwrap()),
                        };

                        let (events, metadata) = msg.stream_events.into_iter().fold(
                            (Vec::new(), None),
                            |(mut events, mut metadata), append| {
                                if append.stream_id == stream_id {
                                    metadata = Some(append.metadata);
                                } else {
                                    events.push(append);
                                }
                                (events, metadata)
                            },
                        );

                        return Err(AppendEventsError::IncorrectExpectedVersion {
                            stream_id,
                            current,
                            expected,
                            events,
                            metadata: metadata.unwrap(),
                        });
                    }
                    _ => return Err(AppendEventsError::Database(status)),
                },
            };

            let results = res
                .streams
                .into_iter()
                .map(|res| {
                    let timestamp = res.timestamp.unwrap();
                    (
                        res.first_id,
                        DateTime::from_timestamp(
                            timestamp.seconds,
                            timestamp.nanos.try_into().unwrap(),
                        )
                        .unwrap()
                        .to_utc(),
                    )
                })
                .collect();

            Ok(results)
        }
    }
}
