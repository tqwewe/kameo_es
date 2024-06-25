use std::{borrow::Cow, sync::Arc};

use eventus::{CurrentVersion, ExpectedVersion};
use thiserror::Error;
use tonic::Status;

#[derive(Debug, Error)]
pub enum ExecuteError<E> {
    #[error("{0:?}")]
    Handle(E),
    #[error(transparent)]
    Database(#[from] Status),
    #[error("entity '{category}-{id}' actor not running")]
    EntityActorNotRunning {
        category: &'static str,
        id: Arc<str>,
    },
    #[error("entity '{category}-{id}' actor stopped")]
    EntityActorStopped {
        category: &'static str,
        id: Arc<str>,
    },
    #[error("event store actor not running")]
    EventStoreActorNotRunning,
    #[error("event store actor stopped")]
    EventStoreActorStopped,
    #[error(transparent)]
    SerializeEvent(#[from] rmp_serde::encode::Error),
    #[error("expected '{category}-{id}' version {expected} but got {current}")]
    IncorrectExpectedVersion {
        category: Cow<'static, str>,
        id: String,
        current: CurrentVersion,
        expected: ExpectedVersion,
    },
    #[error("too many write conflicts for stream '{category}-{id}'")]
    TooManyConflicts { category: &'static str, id: String },
}
