pub mod command_service;
pub mod entity_actor;
pub mod error;
pub mod event_store;
pub mod stream_id;

use std::{convert::Infallible, fmt};

use commitlog::Event;
use kameo::error::SendError;
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use tonic::Status;

pub trait Entity: Send + 'static {
    type Event: EventType + Serialize + DeserializeOwned + fmt::Debug + Send + Sync;
    type Metadata: Serialize + DeserializeOwned + Default + Unpin + Send + Sync + 'static;

    fn category() -> &'static str;

    fn apply(&mut self, event: Self::Event);

    fn apply_event(&mut self, event: Event<'static>) {
        self.apply(serde_json::from_slice(&event.event_data).expect("failed to deserialize event"))
    }
}

pub trait Command<C>: Entity {
    type Error;

    fn handle(&self, cmd: C) -> Result<Vec<Self::Event>, Self::Error>;
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
