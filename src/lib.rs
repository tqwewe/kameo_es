pub mod command_service;
pub mod entity_actor;
pub mod error;
pub mod event_handler;
mod event_store;
pub mod transaction;

use std::{convert::Infallible, io};

use chrono::{DateTime, Utc};
use eventus::server::eventstore::{
    event_store_client::EventStoreClient, subscribe_request::StartFrom, EventBatch,
    SubscribeRequest,
};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use kameo::error::SendError;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tonic::{transport::Channel, Code, Status};

pub use kameo_es_core::*;

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
                    event_from_eventus(event)
                        .map_err(|err| Status::new(Code::Internal, err.to_string()))
                })
                .collect()
        })
        .boxed())
}

#[derive(Debug, Error)]
pub enum TryFromEventusEventError {
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error("failed to deserialize event data: {0}")]
    DeserializeEventData(ciborium::de::Error<io::Error>),
    #[error("failed to deserialize event metadata: {0}")]
    DeserializeEventMetadata(ciborium::de::Error<io::Error>),
}

fn event_from_eventus<E: DeserializeOwned, M: DeserializeOwned + Default>(
    ev: eventus::server::eventstore::Event,
) -> Result<Event<E, M>, TryFromEventusEventError> {
    Ok(Event {
        id: ev.id,
        stream_id: StreamID::new(ev.stream_id),
        stream_version: ev.stream_version,
        name: ev.event_name,
        data: ciborium::from_reader(ev.event_data.as_slice())
            .map_err(TryFromEventusEventError::DeserializeEventData)?,
        metadata: ciborium::from_reader(ev.metadata.as_slice())
            .map_err(TryFromEventusEventError::DeserializeEventMetadata)?,
        timestamp: ev
            .timestamp
            .and_then(|ts| {
                DateTime::<Utc>::from_timestamp(ts.seconds, ts.nanos.try_into().unwrap_or(0))
            })
            .ok_or(TryFromEventusEventError::InvalidTimestamp)?,
    })
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
