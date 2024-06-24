pub mod command_service;
pub mod entity_actor;
pub mod error;
pub mod event_store;
pub mod stream_id;
pub mod test_utils;

use core::fmt;
use std::convert::Infallible;

use kameo::error::SendError;
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use tonic::Status;

pub trait Entity: Default + Send + 'static {
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

// #[derive(Clone, Debug, PartialEq, Eq)]
// pub struct Event<E, M> {
//     pub id: u64,
//     pub stream_id: StreamID,
//     pub stream_version: u64,
//     pub data: E,
//     pub metadata: M,
//     pub timestamp: DateTime<Utc>,
// }

// impl<'a, E, M> TryFrom<eventus::Event<'a>> for Event<E, M>
// where
//     E: DeserializeOwned,
//     M: DeserializeOwned,
// {
//     type Error = serde_json::Error;

//     fn try_from(ev: eventus::Event<'a>) -> Result<Self, Self::Error> {
//         Ok(Event {
//             id: ev.id,
//             stream_id: StreamID::new(ev.stream_id.into_owned()),
//             stream_version: ev.stream_version,
//             data: serde_json::from_slice(&ev.event_data)?,
//             metadata: serde_json::from_slice(&ev.metadata)?,
//             timestamp: ev.timestamp,
//         })
//     }
// }
