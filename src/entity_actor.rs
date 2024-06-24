use std::sync::Arc;

use eventus::{CurrentVersion, Event, ExpectedVersion};
use futures::StreamExt;
use kameo::{
    actor::{ActorRef, BoundedMailbox, WorkerMsg},
    error::{BoxError, SendError},
    message::{Context, Message},
    Actor,
};
use tokio::sync::Notify;
use tonic::Status;
use tracing::debug;

use crate::{
    error::ExecuteError,
    event_store::{AppendEvents, AppendEventsError, EventStore, GetStreamEvents},
    stream_id::StreamID,
    Apply, Command, Entity,
};

pub struct EntityActor<E> {
    entity: E,
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
            stream_id: stream_name,
            event_store,
            version: CurrentVersion::NoStream,
            conflict_reties: 3,
            notify,
        }
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
                self.entity.apply(rmp_serde::from_slice(&event.event_data)?);
                self.version = CurrentVersion::Current(event.stream_version);
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

impl<E> Message<Event<'static>> for EntityActor<E>
where
    E: Entity + Apply,
{
    type Reply = Result<(), rmp_serde::decode::Error>;

    async fn handle(
        &mut self,
        event: Event<'static>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.entity.apply(rmp_serde::from_slice(&event.event_data)?);
        Ok(())
    }
}

pub struct Execute<C, M> {
    pub id: Arc<str>,
    pub command: C,
    pub metadata: M,
    pub expected_version: ExpectedVersion,
}

impl<E, C> Message<Execute<C, E::Metadata>> for EntityActor<E>
where
    E: Command<C> + Apply,
    C: Clone + Send,
{
    type Reply = Result<Vec<E::Event>, ExecuteError<E::Error>>;

    async fn handle(
        &mut self,
        msg: Execute<C, E::Metadata>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let mut metadata = msg.metadata;
        let mut attempt = 1;
        let events = loop {
            if msg.expected_version.validate(self.version).is_err() {
                return Err(ExecuteError::IncorrectExpectedVersion {
                    category: E::name().into(),
                    id: msg.id,
                    current: self.version,
                    expected: msg.expected_version,
                });
            }

            let events = self
                .entity
                .handle(msg.command.clone())
                .map_err(ExecuteError::Handle)?;
            let res = self
                .event_store
                .ask(WorkerMsg(AppendEvents {
                    stream_name: self.stream_id.clone(),
                    events: events.clone(),
                    expected_version: self.version.as_expected_version(),
                    metadata,
                }))
                .send()
                .await
                .map_err(|err| err.map_msg(|msg| msg.0));
            match res {
                Ok(_) => {
                    break events;
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
                            id: msg.id,
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

        for event in events.clone() {
            self.entity.apply(event);
        }

        match &mut self.version {
            CurrentVersion::Current(version) => *version += events.len() as u64,
            CurrentVersion::NoStream => {
                self.version = CurrentVersion::Current((events.len() as u64).saturating_sub(1))
            }
        }

        Ok(events)
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
            SendError::QueriesNotSupported => unreachable!("the event store is never queried"),
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
            SendError::QueriesNotSupported => unreachable!("the event store is never queried"),
        }
    }
}
