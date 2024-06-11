use std::{borrow::Cow, sync::Arc};

use commitlog::{CurrentVersion, ExpectedVersion};
use thiserror::Error;
use tonic::Status;

#[derive(Debug, Error)]
pub enum ExecuteError<E> {
    #[error(transparent)]
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
    SerializeEvent(#[from] serde_json::Error),
    #[error("expected '{category}-{id}' version {expected} but got {current}")]
    IncorrectExpectedVersion {
        category: Cow<'static, str>,
        id: Arc<str>,
        current: CurrentVersion,
        expected: ExpectedVersion,
    },
    #[error("too many write conflicts for stream '{category}-{id}'")]
    TooManyConflicts {
        category: &'static str,
        id: Arc<str>,
    },
}
