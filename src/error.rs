use std::{io, sync::Arc};

use eventus::{CurrentVersion, ExpectedVersion};
use thiserror::Error;
use tonic::Status;
use uuid::Uuid;

use crate::stream_id::StreamID;

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
    // #[error("idempotency violation")]
    // IdempotencyViolation,
    #[error(transparent)]
    SerializeEvent(ciborium::ser::Error<io::Error>),
    #[error("failed to serialize metadata: {0}")]
    SerializeMetadata(ciborium::ser::Error<io::Error>),
    #[error("expected '{stream_id}' version {expected} but got {current}")]
    IncorrectExpectedVersion {
        stream_id: StreamID,
        current: CurrentVersion,
        expected: ExpectedVersion,
    },
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error("too many write conflicts for stream '{stream_id}'")]
    TooManyConflicts { stream_id: StreamID },
    #[error("transaction aborted and was not completed")]
    TransactionAborted,
}
