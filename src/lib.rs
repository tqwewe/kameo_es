pub mod command_service;
mod entity_actor;
pub mod error;
pub mod event_handler;
mod event_store;
pub mod stream_id;
pub mod test_utils;

use std::{convert::Infallible, fmt, ops, str::FromStr, time::Instant};

use chrono::{DateTime, Utc};
use eventus::server::eventstore::{
    event_store_client::EventStoreClient, subscribe_request::StartFrom, EventBatch,
    SubscribeRequest,
};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use kameo::error::SendError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use stream_id::StreamID;
use thiserror::Error;
use tonic::{transport::Channel, Code, Status};

pub trait Entity: Default + Send + 'static {
    type ID: FromStr + fmt::Display + Send;
    type Event: EventType + Clone + Serialize + DeserializeOwned + Send + Sync;
    type Metadata: Serialize + DeserializeOwned + Default + Unpin + Send + Sync + 'static;

    fn name() -> &'static str;
}

pub trait Command<C>: Entity {
    type Error: fmt::Debug + Send + Sync + 'static;

    fn handle(&self, cmd: C, ctx: Context<'_, Self>) -> Result<Vec<Self::Event>, Self::Error>;

    /// Returns true if the command should be ignored due to it being previously handled.
    fn is_idempotent(&self, _cmd: &C, ctx: Context<'_, Self>) -> bool {
        ctx.processed()
    }
}

pub trait Apply
where
    Self: Entity,
{
    fn apply(&mut self, event: Self::Event);
}

pub trait EventType {
    fn event_type(&self) -> &'static str;
}

pub struct Context<'a, E>
where
    E: Entity,
{
    pub metadata: &'a Metadata<E::Metadata>,
    pub last_causation_event_id: Option<u64>,
    pub last_causation_stream_id: Option<&'a StreamID>,
    pub last_causation_stream_version: Option<u64>,
    pub time: DateTime<Utc>,
    pub executed_at: Instant,
}

impl<'a, E> Context<'a, E>
where
    E: Entity,
{
    pub fn processed(&self) -> bool {
        self.last_causation_event_id
            .zip(self.metadata.causation_event_id)
            .map(|(last, current)| last >= current)
            .unwrap_or(false)
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.time + self.executed_at.elapsed()
    }
}

impl<'a, E> Clone for Context<'a, E>
where
    E: Entity,
{
    fn clone(&self) -> Self {
        Context {
            metadata: self.metadata,
            last_causation_event_id: self.last_causation_event_id,
            last_causation_stream_id: self.last_causation_stream_id,
            last_causation_stream_version: self.last_causation_stream_version,
            time: self.time,
            executed_at: self.executed_at,
        }
    }
}

impl<'a, E> Copy for Context<'a, E> where E: Entity {}

impl<'a, E> fmt::Debug for Context<'a, E>
where
    E: Entity,
    E::Metadata: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context")
            .field("metadata", &self.metadata)
            .field("last_causation_event_id", &self.last_causation_event_id)
            .field("last_causation_stream_id", &self.last_causation_stream_id)
            .field(
                "last_causation_stream_version",
                &self.last_causation_stream_version,
            )
            .field("time", &self.time)
            .field("executed_at", &self.executed_at)
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum Error<M = (), E = Infallible> {
    #[error(transparent)]
    Database(#[from] Status),
    #[error(transparent)]
    SendError(#[from] SendError<M, E>),
}

pub async fn subscribe(
    mut client: EventStoreClient<Channel>,
    start_from: StartFrom,
) -> Result<BoxStream<'static, Result<Vec<Event>, Status>>, Status> {
    let stream = client
        .subscribe(SubscribeRequest {
            start_from: Some(start_from),
        })
        .await?
        .into_inner();

    Ok(stream
        .and_then(|EventBatch { events }| async move {
            events
                .into_iter()
                .map(|event| {
                    Event::try_from(event)
                        .map_err(|err| Status::new(Code::Internal, err.to_string()))
                })
                .collect()
        })
        .boxed())
}

#[derive(Debug)]
pub struct Event<E = GenericValue, M = GenericValue> {
    pub id: u64,
    pub stream_id: StreamID,
    pub stream_version: u64,
    pub name: String,
    pub data: E,
    pub metadata: Metadata<M>,
    pub timestamp: DateTime<Utc>,
}

impl Event {
    #[inline]
    pub fn as_entity<E>(
        self,
    ) -> Result<Event<E::Event, E::Metadata>, (Event<(), ()>, rmpv::ext::Error)>
    where
        E: Entity,
    {
        let data = match rmpv::ext::from_value(self.data.0) {
            Ok(data) => data,
            Err(err) => {
                println!("uh oh, no event");
                return Err((
                    Event {
                        id: self.id,
                        stream_id: self.stream_id,
                        stream_version: self.stream_version,
                        name: self.name,
                        data: (),
                        metadata: Metadata {
                            causation_event_id: self.metadata.causation_event_id,
                            causation_stream_id: self.metadata.causation_stream_id,
                            causation_stream_version: self.metadata.causation_stream_version,
                            data: (),
                        },
                        timestamp: self.timestamp,
                    },
                    err,
                ));
            }
        };
        println!("GOT EVENT!!!!");

        let metadata = match self.metadata.cast() {
            Ok(metadata) => metadata,
            Err(err) => {
                return Err((
                    Event {
                        id: self.id,
                        stream_id: self.stream_id,
                        stream_version: self.stream_version,
                        name: self.name,
                        data: (),
                        metadata: Metadata {
                            causation_event_id: None,
                            causation_stream_id: None,
                            causation_stream_version: None,
                            data: (),
                        },
                        timestamp: self.timestamp,
                    },
                    err,
                ))
            }
        };

        Ok(Event {
            id: self.id,
            stream_id: self.stream_id,
            stream_version: self.stream_version,
            name: self.name,
            data,
            metadata,
            timestamp: self.timestamp,
        })
    }
}

impl<E, M> Event<E, M> {
    #[inline]
    pub fn entity_id<Ent>(&self) -> Result<Ent::ID, <Ent::ID as FromStr>::Err>
    where
        Ent: Entity,
    {
        self.stream_id.cardinal_id().parse()
    }
}

#[derive(Debug, Error)]
pub enum TryFromEventusEventError {
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error("failed to deserialize event data: {0}")]
    DeserializeEventData(rmp_serde::decode::Error),
    #[error("failed to deserialize event metadata: {0}")]
    DeserializeEventMetadata(rmp_serde::decode::Error),
}

impl<E, M> TryFrom<eventus::server::eventstore::Event> for Event<E, M>
where
    E: DeserializeOwned,
    M: DeserializeOwned + Default,
{
    type Error = TryFromEventusEventError;

    fn try_from(ev: eventus::server::eventstore::Event) -> Result<Self, Self::Error> {
        Ok(Event {
            id: ev.id,
            stream_id: StreamID::new(ev.stream_id),
            stream_version: ev.stream_version,
            name: ev.event_name,
            data: rmp_serde::from_slice(&ev.event_data)
                .map_err(TryFromEventusEventError::DeserializeEventData)?,
            metadata: rmp_serde::from_slice(&ev.metadata)
                .map_err(TryFromEventusEventError::DeserializeEventMetadata)?,
            timestamp: ev
                .timestamp
                .and_then(|ts| {
                    DateTime::<Utc>::from_timestamp(ts.seconds, ts.nanos.try_into().unwrap_or(0))
                })
                .ok_or(TryFromEventusEventError::InvalidTimestamp)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Metadata<T> {
    #[serde(rename = "ceid", skip_serializing_if = "Option::is_none")]
    pub causation_event_id: Option<u64>,
    #[serde(rename = "csid", skip_serializing_if = "Option::is_none")]
    pub causation_stream_id: Option<StreamID>,
    #[serde(rename = "csv", skip_serializing_if = "Option::is_none")]
    pub causation_stream_version: Option<u64>,
    pub data: T,
}

impl<T> Metadata<T> {
    pub fn with_data<U>(self, data: U) -> Metadata<U> {
        Metadata {
            causation_event_id: self.causation_event_id,
            causation_stream_id: self.causation_stream_id,
            causation_stream_version: self.causation_stream_version,
            data,
        }
    }
}

impl Metadata<GenericValue> {
    pub fn cast<U>(self) -> Result<Metadata<U>, rmpv::ext::Error>
    where
        U: DeserializeOwned + Default,
    {
        Ok(Metadata {
            causation_event_id: self.causation_event_id,
            causation_stream_id: self.causation_stream_id,
            causation_stream_version: self.causation_stream_version,
            data: if matches!(self.data, GenericValue(rmpv::Value::Nil)) {
                U::default()
            } else {
                rmpv::ext::from_value(self.data.0)?
            },
        })
    }
}

impl<T> ops::Deref for Metadata<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> ops::DerefMut for Metadata<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GenericValue(pub rmpv::Value);

impl Default for GenericValue {
    fn default() -> Self {
        GenericValue(rmpv::Value::Nil)
    }
}

impl ops::Deref for GenericValue {
    type Target = rmpv::Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for GenericValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Matches on an event for multiple branches, with each branch being an entity type.
///
/// The passed in event will be converted to Event<E> where E is the Entity for the given branch.
///
/// # Example
///
/// ```
/// match_event! {
///     event,
///     BankAccount => {
///         let id: BankAccount::ID = event.entity_id()?;
///         let data = BankAccount::Event = event.data()?;
///         // ...
///     }
///     else => println!("unknown entity"),
/// }
/// ```
#[macro_export]
macro_rules! match_event {
    // All input is normalized, now transform.
    (@ {
        event=$event:ident;

        $( $ent:ident => $handle:expr, )+

        // Fallback expression used when all select branches have been disabled.
        ; $else:expr

    }) => {{
        let category = $event.stream_id.category();
        $(
            if category == $ent::name() {
                let $event = $event.as_entity::<$ent>();
                {
                    let _res = $handle;
                }
            } else
        )*
        if true {
            $else
        }
    }};

    // ==== Normalize =====

    // These rules match a single `select!` branch and normalize it for
    // processing by the first rule.

    (@ { event=$event:ident; $($t:tt)* } ) => {
        // No `else` branch
        $crate::match_event!(@{ event=$event; $($t)*; {} })
    };
    (@ { event=$event:ident; $($t:tt)* } else => $else:expr $(,)?) => {
        $crate::match_event!(@{ event=$event; $($t)*; $else })
    };
    (@ { event=$event:ident; $($t:tt)* } $ent:ident => $h:block, $($r:tt)* ) => {
        $crate::match_event!(@{ event=$event; $($t)* $ent => $h, } $($r)*)
    };
    (@ { event=$event:ident; $($t:tt)* } $ent:ident => $h:block $($r:tt)* ) => {
        $crate::match_event!(@{ event=$event; $($t)* $ent => $h, } $($r)*)
    };
    (@ { event=$event:ident; $($t:tt)* } $ent:ident => $h:expr ) => {
        $crate::match_event!(@{ event=$event; $($t)* $ent => $h, })
    };
    (@ { event=$event:ident; $($t:tt)* } $ent:ident => $h:expr, $($r:tt)* ) => {
        $crate::match_event!(@{ event=$event; $($t)* $ent => $h, } $($r)*)
    };

    // ===== Entry point =====

    ( $event:ident, else => $else:expr $(,)? ) => {{
        $else
    }};

    ( $event:ident, $ent:ident => $($t:tt)* ) => {
        // Randomly generate a starting point. This makes `select!` a bit more
        // fair and avoids always polling the first future.
        $crate::match_event!(@{ event=$event; } $ent => $($t)*)
    };

    () => {
        compile_error!("select! requires at least one branch.")
    };
}
