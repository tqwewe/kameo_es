use std::{borrow::Cow, io, sync::Arc};

use eventus::{CurrentVersion, ExpectedVersion};
use thiserror::Error;
use tonic::Status;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ExecuteError<E> {
    #[error("{0:?}")]
    Handle(E),
    #[error(
        "entity has existing correlation id {existing} which does not match the one set {new}"
    )]
    CorrelationIDMismatch { existing: Uuid, new: Uuid },
    #[error("entity has no correlation id but contains events")]
    CorrelationIDNotSetOnExistingEntity,
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
    #[error("idempotency violation")]
    IdempotencyViolation,
    #[error(transparent)]
    SerializeEvent(#[from] ciborium::ser::Error<io::Error>),
    #[error("expected '{category}-{id}' version {expected} but got {current}")]
    IncorrectExpectedVersion {
        category: Cow<'static, str>,
        id: String,
        current: CurrentVersion,
        expected: ExpectedVersion,
    },
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error("too many write conflicts for stream '{category}-{id}'")]
    TooManyConflicts { category: &'static str, id: String },
}
