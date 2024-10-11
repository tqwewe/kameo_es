use std::time::Instant;

use chrono::{DateTime, Utc};
use eventus::{CurrentVersion, ExpectedVersion};
use futures::StreamExt;
use kameo::{
    actor::{pool::WorkerMsg, ActorRef},
    error::{BoxError, SendError},
    mailbox::bounded::BoundedMailbox,
    message::{Context, Message},
    request::MessageSend,
    Actor,
};
use tonic::Status;
use tracing::debug;
use uuid::Uuid;

use crate::{
    command_service::AppendedEvent,
    error::ExecuteError,
    event_store::{AppendEvents, AppendEventsError, EventStore, GetStreamEvents},
    stream_id::StreamID,
    Apply, Command, Entity, Metadata,
};

pub struct EntityActor<E> {
    entity: E,
    stream_id: StreamID,
    event_store: EventStore,
    version: CurrentVersion,
    correlation_id: Uuid,
    last_causation_event_id: Option<u64>,
    last_causation_stream_id: Option<StreamID>,
    last_causation_stream_version: Option<u64>,
    conflict_reties: usize,
}

impl<E> EntityActor<E> {
    pub fn new(entity: E, stream_name: StreamID, event_store: EventStore) -> Self {
        EntityActor {
            entity,
            stream_id: stream_name,
            event_store,
            version: CurrentVersion::NoStream,
            correlation_id: Uuid::nil(),
            last_causation_event_id: None,
            last_causation_stream_id: None,
            last_causation_stream_version: None,
            conflict_reties: 3,
        }
    }

    fn apply(&mut self, event: E::Event, stream_version: u64, metadata: Metadata<E::Metadata>)
    where
        E: Entity + Apply,
    {
        assert_eq!(
            match self.version {
                CurrentVersion::Current(version) => version + 1,
                CurrentVersion::NoStream => 0,
            },
            stream_version,
            "expected stream version {} but got {} for stream {}",
            match self.version {
                CurrentVersion::Current(version) => version + 1,
                CurrentVersion::NoStream => 0,
            },
            stream_version,
            self.stream_id,
        );
        let causation_event_id = metadata.causation_event_id;
        let causation_stream_id = metadata.causation_stream_id.clone();
        let causation_stream_version = metadata.causation_stream_version;
        if self.correlation_id.is_nil() {
            assert_eq!(
                self.version,
                CurrentVersion::NoStream,
                "expected correlation id to be nil only if the stream doesn't exist"
            );
            self.correlation_id = metadata.correlation_id;
        }
        self.entity.apply(event, metadata);
        self.version = CurrentVersion::Current(stream_version);
        self.last_causation_event_id = self.last_causation_event_id.or(causation_event_id);
        if let Some(causation_stream_id) = causation_stream_id {
            self.last_causation_stream_id = Some(causation_stream_id);
        }
        self.last_causation_stream_version = self
            .last_causation_stream_version
            .or(causation_stream_version);
    }

    async fn resync_with_db(&mut self) -> Result<(), BoxError>
    where
        E: Entity + Apply,
    {
        let from_version = match self.version {
            CurrentVersion::Current(version) => version + 1,
            CurrentVersion::NoStream => 0,
        };

        let mut stream = self
            .event_store
            .ask(WorkerMsg(GetStreamEvents::<
                <E as Entity>::Event,
                <E as Entity>::Metadata,
            >::new(
                self.stream_id.clone(), from_version
            )))
            .send()
            .await
            .map_err(|err| err.map_msg(|WorkerMsg(msg)| msg))?;

        while let Some(res) = stream.next().await {
            let batch = res?;
            for event in batch {
                assert_eq!(
                    match self.version {
                        CurrentVersion::Current(version) => version + 1,
                        CurrentVersion::NoStream => 0,
                    },
                    event.stream_version,
                    "expected stream version {} but got {} for stream {}",
                    match self.version {
                        CurrentVersion::Current(version) => version + 1,
                        CurrentVersion::NoStream => 0,
                    },
                    event.stream_version,
                    event.stream_id,
                );
                let ent_event = ciborium::from_reader(event.event_data.as_ref())?;
                let metadata: Metadata<E::Metadata> =
                    ciborium::from_reader(event.metadata.as_ref())?;
                self.apply(ent_event, event.stream_version, metadata);
            }
        }

        Ok(())
    }

    fn hydrate_metadata_correlation_id<F>(
        &self,
        metadata: &mut Metadata<E::Metadata>,
    ) -> Result<(), ExecuteError<F>>
    where
        E: Entity,
    {
        match (
            !self.correlation_id.is_nil(),
            !metadata.correlation_id.is_nil(),
        ) {
            (true, true) => {
                // Correlation ID exists, make sure they match
                if self.correlation_id != metadata.correlation_id {
                    return Err(ExecuteError::CorrelationIDMismatch {
                        existing: self.correlation_id,
                        new: metadata.correlation_id,
                    });
                }
            }
            (true, false) => {
                // Correlation ID exists, use the existing one
                metadata.correlation_id = self.correlation_id;
            }
            (false, true) => {
                // Correlation ID doesn't exist, make sure there's no current stream version
                if self.version != CurrentVersion::NoStream {
                    return Err(ExecuteError::CorrelationIDNotSetOnExistingEntity);
                }
            }
            (false, false) => {
                // Correlation ID doesn't exist, and none defined
                metadata.correlation_id = Uuid::new_v4();
            }
        }

        Ok(())
    }
}

impl<E> Actor for EntityActor<E>
where
    E: Entity + Apply,
{
    type Mailbox = BoundedMailbox<Self>;

    fn name() -> &'static str {
        "EntityActor"
    }

    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        self.resync_with_db().await?;
        Ok(())
    }
}

// impl<E> Message<Event<'static>> for EntityActor<E>
// where
//     E: Entity + Apply,
// {
//     type Reply = Result<(), rmp_serde::decode::Error>;

//     async fn handle(
//         &mut self,
//         event: Event<'static>,
//         _ctx: Context<'_, Self, Self::Reply>,
//     ) -> Self::Reply {
//         self.entity.apply(rmp_serde::from_slice(&event.event_data)?);
//         Ok(())
//     }
// }

pub struct Execute<I, C, M> {
    pub id: I,
    pub command: C,
    pub metadata: Metadata<M>,
    pub expected_version: ExpectedVersion,
    pub time: DateTime<Utc>,
    pub executed_at: Instant,
}

impl<E, C> Message<Execute<E::ID, C, E::Metadata>> for EntityActor<E>
where
    E: Entity + Command<C> + Apply,
    C: Clone + Send,
{
    type Reply = Result<Vec<AppendedEvent<E::Event>>, ExecuteError<E::Error>>;

    async fn handle(
        &mut self,
        mut exec: Execute<E::ID, C, E::Metadata>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        // Derive existing correlation ID from if not set, or generate new one
        self.hydrate_metadata_correlation_id(&mut exec.metadata)?;

        let mut attempt = 1;
        let (starting_event_id, timestamp, events) = loop {
            if exec.expected_version.validate(self.version).is_err() {
                return Err(ExecuteError::IncorrectExpectedVersion {
                    category: E::name().into(),
                    id: exec.id.to_string(),
                    current: self.version,
                    expected: exec.expected_version,
                });
            }

            let ctx = crate::Context {
                metadata: &exec.metadata,
                last_causation_event_id: self.last_causation_event_id,
                last_causation_stream_id: self.last_causation_stream_id.as_ref(),
                last_causation_stream_version: self.last_causation_stream_version,
                time: exec.time,
                executed_at: exec.executed_at,
            };
            let is_idempotent = self.entity.is_idempotent(&exec.command, ctx);
            if !is_idempotent {
                return Err(ExecuteError::IdempotencyViolation);
            }
            let events = self
                .entity
                .handle(exec.command.clone(), ctx)
                .map_err(ExecuteError::Handle)?;
            let res = self
                .event_store
                .ask(WorkerMsg(AppendEvents {
                    stream_name: self.stream_id.clone(),
                    events: events.clone(),
                    expected_version: self.version.as_expected_version(),
                    metadata: exec.metadata.clone(),
                    timestamp: exec.time,
                }))
                .send()
                .await
                .map_err(|err| err.map_msg(|msg| msg.0));
            match res {
                Ok((starting_event_id, timestamp)) => {
                    break (starting_event_id, timestamp, events);
                }
                Err(SendError::HandlerError(AppendEventsError::IncorrectExpectedVersion {
                    category,
                    id,
                    current,
                    expected,
                    metadata: m,
                })) => {
                    debug!(%category, %id, %current, %expected, "write conflict");
                    if attempt == self.conflict_reties {
                        return Err(ExecuteError::TooManyConflicts {
                            category: E::name(),
                            id: exec.id.to_string(),
                        });
                    }

                    self.resync_with_db().await.unwrap();

                    exec.metadata = m;
                    attempt += 1;
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        };

        let starting_version = match self.version {
            CurrentVersion::Current(v) => v + 1,
            CurrentVersion::NoStream => 0,
        };
        let mut version = starting_version;

        for event in &events {
            self.apply(event.clone(), version, exec.metadata.clone());
            version += 1;
        }

        Ok(events
            .into_iter()
            .enumerate()
            .map(|(i, event)| AppendedEvent {
                event,
                event_id: starting_event_id + i as u64,
                stream_version: starting_version + i as u64,
                timestamp,
            })
            .collect())
    }
}

impl<E, M> From<AppendEventsError<M>> for ExecuteError<E> {
    fn from(err: AppendEventsError<M>) -> Self {
        match err {
            AppendEventsError::Database(err) => ExecuteError::Database(err),
            AppendEventsError::IncorrectExpectedVersion {
                category,
                id,
                current,
                expected,
                ..
            } => ExecuteError::IncorrectExpectedVersion {
                category: category.into(),
                id: id.into(),
                current,
                expected,
            },
            AppendEventsError::InvalidTimestamp => ExecuteError::InvalidTimestamp,
            AppendEventsError::SerializeEvent(err) => ExecuteError::SerializeEvent(err),
        }
    }
}

impl<M, E, Me> From<SendError<M, AppendEventsError<Me>>> for ExecuteError<E> {
    fn from(err: SendError<M, AppendEventsError<Me>>) -> Self {
        match err {
            SendError::ActorNotRunning(_) => ExecuteError::EventStoreActorNotRunning,
            SendError::ActorStopped => ExecuteError::EventStoreActorStopped,
            SendError::MailboxFull(_) => unreachable!("sending is always awaited"),
            SendError::HandlerError(err) => err.into(),
            SendError::Timeout(_) => unreachable!("no timeouts are used in the event store"),
        }
    }
}

impl<M, E> From<SendError<M, Status>> for ExecuteError<E> {
    fn from(err: SendError<M, Status>) -> Self {
        match err {
            SendError::ActorNotRunning(_) => ExecuteError::EventStoreActorNotRunning,
            SendError::ActorStopped => ExecuteError::EventStoreActorStopped,
            SendError::MailboxFull(_) => unreachable!("sending is always awaited"),
            SendError::HandlerError(err) => err.into(),
            SendError::Timeout(_) => unreachable!("no timeouts are used in the event store"),
        }
    }
}
