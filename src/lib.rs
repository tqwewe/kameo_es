pub mod command_service;
mod entity_actor;
pub mod error;
pub mod event_handler;
mod event_store;
pub mod stream_id;
pub mod test_utils;

use std::{convert::Infallible, fmt, marker::PhantomData, str::FromStr};

use chrono::{DateTime, Utc};
use eventus::server::eventstore::{
    event_store_client::EventStoreClient, subscribe_request::StartFrom, EventBatch,
    InvalidTimestamp, SubscribeRequest,
};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use kameo::error::SendError;
use serde::{de::DeserializeOwned, Serialize};
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

    fn handle(&self, cmd: C) -> Result<Vec<Self::Event>, Self::Error>;
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
) -> Result<BoxStream<'static, Result<Vec<Event<()>>, Status>>, Status> {
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
                        .map_err(|_| Status::new(Code::Internal, "invalid timestamp received"))
                })
                .collect()
        })
        .boxed())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event<E> {
    pub id: u64,
    pub stream_id: StreamID,
    pub stream_version: u64,
    pub event_name: String,
    pub event_data: Vec<u8>,
    pub metadata: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    phantom: PhantomData<E>,
}

impl<E> Event<E> {
    #[inline]
    pub fn as_entity<F>(self) -> Event<F> {
        Event {
            id: self.id,
            stream_id: self.stream_id,
            stream_version: self.stream_version,
            event_name: self.event_name,
            event_data: self.event_data,
            metadata: self.metadata,
            timestamp: self.timestamp,
            phantom: PhantomData,
        }
    }
}

impl<E> Event<E>
where
    E: Entity,
{
    #[inline]
    pub fn entity_id(&self) -> Result<E::ID, <E::ID as FromStr>::Err> {
        self.stream_id.cardinal_id().parse()
    }

    #[inline]
    pub fn data(&self) -> Result<E::Event, rmp_serde::decode::Error> {
        rmp_serde::from_slice(&self.event_data)
    }

    #[inline]
    pub fn metadata(&self) -> Result<E::Metadata, rmp_serde::decode::Error> {
        rmp_serde::from_slice(&self.metadata)
    }
}

impl<E> TryFrom<eventus::server::eventstore::Event> for Event<E> {
    type Error = InvalidTimestamp;

    fn try_from(ev: eventus::server::eventstore::Event) -> Result<Self, Self::Error> {
        Ok(Event {
            id: ev.id,
            stream_id: StreamID::new(ev.stream_id),
            stream_version: ev.stream_version,
            event_name: ev.event_name,
            event_data: ev.event_data,
            metadata: ev.metadata,
            timestamp: ev
                .timestamp
                .and_then(|ts| {
                    DateTime::<Utc>::from_timestamp(ts.seconds, ts.nanos.try_into().unwrap_or(0))
                })
                .ok_or(InvalidTimestamp)?,
            phantom: PhantomData,
        })
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
