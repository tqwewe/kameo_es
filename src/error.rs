use std::{borrow::Cow, sync::Arc};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExecuteError<E> {
    #[error(transparent)]
    Handle(E),
    #[error(transparent)]
    Database(#[from] message_db::Error),
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
        current: i64,
        expected: i64,
    },
    #[error("too many write conflicts for stream '{category}-{id}'")]
    TooManyConflicts {
        category: &'static str,
        id: Arc<str>,
    },
}
