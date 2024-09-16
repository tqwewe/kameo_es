use std::{sync::Arc, time::Instant};

use chrono::{DateTime, Utc};
use eventus::{CurrentVersion, ExpectedVersion};
use futures::StreamExt;
use kameo::{
    actor::{ActorRef, WorkerMsg},
    error::{BoxError, SendError},
    mailbox::bounded::BoundedMailbox,
    message::{Context, Message},
    request::MessageSend,
    Actor,
};
use tokio::sync::Notify;
use tonic::Status;
use tracing::debug;

use crate::{
    command_service::AppendedEvent,
    error::ExecuteError,
    event_store::{AppendEvents, AppendEventsError, EventStore, GetStreamEvents},
    stream_id::StreamID,
    Apply, Command, Entity, GenericValue, Metadata,
};

pub struct EntityActor<E> {
    entity: E,
    last_causation_event_id: Option<u64>,
    last_causation_stream_id: Option<StreamID>,
    last_causation_stream_version: Option<u64>,
    stream_id: StreamID,
    event_store: EventStore,
    version: CurrentVersion,
    conflict_reties: usize,
    notify: Arc<Notify>,
}

impl<E> EntityActor<E> {
    pub fn new(
        entity: E,
        stream_name: StreamID,
        event_store: EventStore,
        notify: Arc<Notify>,
    ) -> Self {
        EntityActor {
            entity,
            last_causation_event_id: None,
            last_causation_stream_id: None,
            last_causation_stream_version: None,
            stream_id: stream_name,
            event_store,
            version: CurrentVersion::NoStream,
            conflict_reties: 3,
            notify,
        }
    }

    fn apply(
        &mut self,
        event: E::Event,
        stream_version: u64,
        causation_event_id: Option<u64>,
        causation_stream_id: Option<StreamID>,
        causation_stream_version: Option<u64>,
    ) where
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
        self.entity.apply(event);
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
            .map_err(|err| err.map_msg(|msg| msg.0))?;

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
                let metadata: Metadata<GenericValue> =
                    ciborium::from_reader(event.metadata.as_ref())?;
                self.apply(
                    ent_event,
                    event.stream_version,
                    metadata.causation_event_id,
                    metadata.causation_stream_id,
                    metadata.causation_stream_version,
                );
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
        self.notify.notify_waiters();
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
        msg: Execute<E::ID, C, E::Metadata>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let mut metadata = msg.metadata;
        let causation_event_id = metadata.causation_event_id;
        let causation_stream_id = metadata.causation_stream_id.clone();
        let causation_stream_version = metadata.causation_stream_version;

        let mut attempt = 1;
        let (starting_event_id, timestamp, events) = loop {
            if msg.expected_version.validate(self.version).is_err() {
                return Err(ExecuteError::IncorrectExpectedVersion {
                    category: E::name().into(),
                    id: msg.id.to_string(),
                    current: self.version,
                    expected: msg.expected_version,
                });
            }

            let ctx = crate::Context {
                metadata: &metadata,
                last_causation_event_id: self.last_causation_event_id,
                last_causation_stream_id: self.last_causation_stream_id.as_ref(),
                last_causation_stream_version: self.last_causation_stream_version,
                time: msg.time,
                executed_at: msg.executed_at,
            };
            let is_idempotent = self.entity.is_idempotent(&msg.command, ctx);
            if is_idempotent {
                return Err(ExecuteError::IdempotencyViolation);
            }
            let events = self
                .entity
                .handle(msg.command.clone(), ctx)
                .map_err(ExecuteError::Handle)?;
            let res = self
                .event_store
                .ask(WorkerMsg(AppendEvents {
                    stream_name: self.stream_id.clone(),
                    events: events.clone(),
                    expected_version: self.version.as_expected_version(),
                    metadata,
                    timestamp: msg.time,
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
                            id: msg.id.to_string(),
                        });
                    }

                    self.resync_with_db().await.unwrap();

                    metadata = m;
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
            self.apply(
                event.clone(),
                version,
                causation_event_id,
                causation_stream_id.clone(),
                causation_stream_version,
            );
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
