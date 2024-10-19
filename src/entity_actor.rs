use std::{
    collections::{HashMap, VecDeque},
    time::Instant,
};

use chrono::{DateTime, Utc};
use ciborium::Value;
use eventus::{CurrentVersion, ExpectedVersion};
use futures::StreamExt;
use kameo::{
    actor::{pool::WorkerMsg, ActorRef, WeakActorRef},
    error::{ActorStopReason, BoxError, PanicError, SendError},
    mailbox::unbounded::UnboundedMailbox,
    message::{Context, DynMessage, Message},
    reply::{BoxReplySender, DelegatedReply, ReplySender},
    request::{MessageSend, MessageSendSync},
    Actor,
};
use tokio::sync::oneshot;
use tonic::Status;
use tracing::{debug, error};
use uuid::Uuid;

use crate::{
    command_service::{AppendedEvent, ExecuteResult},
    error::ExecuteError,
    event_store::{AppendEvents, AppendEventsError, EventStore, GetStreamEvents},
    stream_id::StreamID,
    Apply, CausationMetadata, Command, Entity, EventType, GenericValue, Metadata,
    TransactionResult, TransactionSender,
};

pub struct EntityActor<E> {
    entity: E,
    stream_id: StreamID,
    event_store: EventStore,
    version: CurrentVersion,
    correlation_id: Uuid,
    last_causations: HashMap<Uuid, CausationMetadata>,
    // last_causation_event_id: Option<u64>,
    // last_causation_stream_id: Option<StreamID>,
    // last_causation_stream_version: Option<u64>,
    conflict_reties: usize,
    active_tx: Option<TransactionSender>,
    buffered_commands: VecDeque<(Option<BoxReplySender>, Box<dyn DynMessage<Self>>)>,
    temp_entity: Option<E>,
    temp_version: Option<CurrentVersion>,
    temp_correlation_id: Option<Uuid>,
    temp_last_causations: Option<HashMap<Uuid, CausationMetadata>>,
}

impl<E> EntityActor<E> {
    pub fn new(entity: E, stream_name: StreamID, event_store: EventStore) -> Self {
        EntityActor {
            entity,
            stream_id: stream_name,
            event_store,
            version: CurrentVersion::NoStream,
            correlation_id: Uuid::nil(),
            last_causations: HashMap::new(),
            // last_causation_event_id: None,
            // last_causation_stream_id: None,
            // last_causation_stream_version: None,
            conflict_reties: 5,
            active_tx: None,
            buffered_commands: VecDeque::new(),
            temp_entity: None,
            temp_version: None,
            temp_correlation_id: None,
            temp_last_causations: None,
        }
    }

    fn apply(&mut self, event: E::Event, stream_version: u64, metadata: Metadata<E::Metadata>)
    where
        E: Entity + Apply,
    {
        apply_event(
            &self.stream_id,
            &mut self.entity,
            &mut self.version,
            &mut self.correlation_id,
            &mut self.last_causations,
            event,
            stream_version,
            metadata,
        )
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

    async fn abort_transaction(&mut self, actor_ref: &ActorRef<Self>)
    where
        E: Entity + Apply,
    {
        println!("aborting transaction");
        self.temp_version = None;
        self.temp_entity = None;
        self.temp_correlation_id = None;
        self.temp_last_causations = None;
        self.active_tx = None;
        while let Some((reply_sender, exec)) = self.buffered_commands.pop_front() {
            println!("handling buffered command...");
            if let Some(err) = exec.handle_dyn(self, actor_ref.clone(), reply_sender).await {
                std::panic::resume_unwind(Box::new(err));
            }
            println!("done handling buffered command.");
        }
    }

    fn execute<C>(
        &mut self,
        id: &E::ID,
        command: C,
        metadata: &Metadata<E::Metadata>,
        expected_version: ExpectedVersion,
        time: DateTime<Utc>,
        executed_at: Instant,
    ) -> Result<ActorExecutionResult<E::Event>, ExecuteError<E::Error>>
    where
        E: Entity + Command<C> + Apply,
        C: Clone + Send,
    {
        if expected_version.validate(self.version).is_err() {
            return Err(ExecuteError::IncorrectExpectedVersion {
                stream_id: StreamID::new_from_parts(E::name(), id),
                current: self.version,
                expected: expected_version,
            });
        }

        let ctx = crate::Context {
            metadata: &metadata,
            last_causation: metadata
                .causation
                .as_ref()
                .and_then(|causation| self.last_causations.get(&causation.correlation_id)),
            time,
            executed_at,
        };
        let is_idempotent = self.entity.is_idempotent(&command, ctx);
        if !is_idempotent {
            return Ok(ActorExecutionResult::Idempotent);
        }

        let events = self
            .temp_entity
            .as_ref()
            .unwrap_or(&self.entity)
            .handle(command, ctx)
            .map_err(ExecuteError::Handle)?;
        if events.is_empty() {
            return Ok(ActorExecutionResult::Events(vec![]));
        }

        match self
            .active_tx
            .as_ref()
            .and_then(|active_tx| active_tx.try_clone())
        {
            Some(tx_sender) => {
                let generic_events: Vec<_> = events
                    .iter()
                    .map(|event| {
                        let generic_event =
                            GenericValue(Value::serialized(event).map_err(|err| {
                                ExecuteError::SerializeEvent(match err {
                                    ciborium::value::Error::Custom(msg) => {
                                        ciborium::ser::Error::Value(msg)
                                    }
                                })
                            })?);
                        Ok((event.event_type(), generic_event))
                    })
                    .collect::<Result<_, ExecuteError<E::Error>>>()?;
                let generic_metadata =
                    GenericValue(Value::serialized(&metadata).map_err(|err| {
                        ExecuteError::SerializeMetadata(match err {
                            ciborium::value::Error::Custom(msg) => ciborium::ser::Error::Value(msg),
                        })
                    })?);
                let append = AppendEvents {
                    stream_id: self.stream_id.clone(),
                    events: generic_events,
                    expected_version: self
                        .temp_version
                        .unwrap_or(self.version)
                        .as_expected_version(),
                    metadata: generic_metadata,
                    timestamp: time,
                };
                let reply_receiver = tx_sender
                    .send_events(self.stream_id.clone(), append)
                    .map_err(|_| ExecuteError::TransactionAborted)?;

                let starting_version = match self.version {
                    CurrentVersion::Current(v) => v + 1,
                    CurrentVersion::NoStream => 0,
                };
                let appended_events = events
                    .into_iter()
                    .enumerate()
                    .map(|(i, event)| AppendedEvent {
                        event,
                        event_id: 0,
                        stream_version: starting_version + i as u64,
                        timestamp: Utc::now(),
                    })
                    .collect();

                Ok(ActorExecutionResult::Transaction {
                    events: appended_events,
                    reply_receiver,
                })
            }
            None => Ok(ActorExecutionResult::Events(events)),
        }
    }
}

enum ActorExecutionResult<E> {
    Events(Vec<E>),
    Idempotent,
    Transaction {
        events: Vec<AppendedEvent<E>>,
        reply_receiver: oneshot::Receiver<TransactionResult>,
    },
}

impl<E> Actor for EntityActor<E>
where
    E: Entity + Apply,
{
    type Mailbox = UnboundedMailbox<Self>;

    fn name() -> &'static str {
        "EntityActor"
    }

    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        if let Err(err) = self.resync_with_db().await {
            error!("resync error: {err}");
        }
        Ok(())
    }

    async fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _err: PanicError,
    ) -> Result<Option<ActorStopReason>, BoxError> {
        Ok(None) // Restart
    }

    async fn on_stop(
        self,
        _actor_ref: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), BoxError> {
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
    pub tx_sender: Option<TransactionSender>,
}

impl<E, C> Message<Execute<E::ID, C, E::Metadata>> for EntityActor<E>
where
    E: Entity + Command<C> + Apply + Clone,
    C: Clone + Send + Sync + 'static,
{
    type Reply = DelegatedReply<Result<ExecuteResult<E::Event>, ExecuteError<E::Error>>>;

    async fn handle(
        &mut self,
        mut exec: Execute<E::ID, C, E::Metadata>,
        mut ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        println!("handle...");
        let (delegated_reply, reply_sender) = ctx.reply_sender();

        match (
            &self.active_tx,
            exec.tx_sender
                .as_ref()
                .and_then(|tx_sender| tx_sender.try_clone()),
        ) {
            (_, Some(TransactionSender::Retry(_))) => {
                // Its a retry, we'll handle this now
                println!("handling retry");
                self.active_tx = exec.tx_sender.take();
            }
            (Some(active_tx), Some(tx_sender)) if active_tx.same_transaction(&tx_sender) => {
                // Apply pending transaction events
            }
            (Some(_), Some(_) | None) => {
                println!("buffering event");
                self.buffered_commands
                    .push_back((reply_sender.map(ReplySender::boxed), Box::new(exec)));
                return delegated_reply;
            }
            (None, tx_sender) => {
                // if let Some(tx_sender) = &tx_sender {
                //     if tx_sender.is_closed() {
                //         if let Some(reply_sender) = reply_sender {
                //             let _ = reply_sender.send(Ok(ExecuteResult::Executed(vec![])));
                //         }
                //         return delegated_reply;
                //     }
                // }
                self.active_tx = tx_sender;
            }
        }

        // Derive existing correlation ID from if not set, or generate new one
        if let Err(err) = self.hydrate_metadata_correlation_id(&mut exec.metadata) {
            if let Some(reply_sender) = reply_sender {
                let _ = reply_sender.send(Err(err));
            }
            dbg!("return");
            return delegated_reply;
        }

        let mut attempt = 1;
        loop {
            println!("loop");
            match self.execute(
                &exec.id,
                exec.command.clone(),
                &exec.metadata,
                exec.expected_version,
                exec.time,
                exec.executed_at,
            ) {
                Ok(ActorExecutionResult::Events(events)) => {
                    if events.is_empty() {
                        if let Some(reply_sender) = reply_sender {
                            reply_sender.send(Ok(ExecuteResult::Executed(vec![])));
                        }
                        dbg!("return");
                        return delegated_reply;
                    }
                    let res = self
                        .event_store
                        .ask(WorkerMsg(AppendEvents {
                            stream_id: self.stream_id.clone(),
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
                            let starting_version = match self.version {
                                CurrentVersion::Current(v) => v + 1,
                                CurrentVersion::NoStream => 0,
                            };
                            let mut version = starting_version;

                            for event in &events {
                                self.apply(event.clone(), version, exec.metadata.clone());
                                version += 1;
                            }

                            let appended_events = events
                                .into_iter()
                                .enumerate()
                                .map(|(i, event)| AppendedEvent {
                                    event,
                                    event_id: starting_event_id + i as u64,
                                    stream_version: starting_version + i as u64,
                                    timestamp,
                                })
                                .collect();

                            if let Some(reply_sender) = reply_sender {
                                reply_sender.send(Ok(ExecuteResult::Executed(appended_events)));
                            }

                            dbg!("return");
                            return delegated_reply;
                        }
                        Err(SendError::HandlerError(
                            AppendEventsError::IncorrectExpectedVersion {
                                stream_id,
                                current,
                                expected,
                                metadata: m,
                                ..
                            },
                        )) => {
                            debug!(%stream_id, %current, %expected, "write conflict");
                            if attempt == self.conflict_reties {
                                if let Some(reply_sender) = reply_sender {
                                    reply_sender
                                        .send(Err(ExecuteError::TooManyConflicts { stream_id }));
                                }
                                return delegated_reply;
                            }

                            self.resync_with_db().await.unwrap();

                            exec.metadata = m;
                            attempt += 1;
                            exec.tx_sender = None;
                        }
                        Err(err) => {
                            if let Some(reply_sender) = reply_sender {
                                reply_sender.send(Err(err.into()));
                            }
                            dbg!("return");
                            return delegated_reply;
                        }
                    }
                }
                Ok(ActorExecutionResult::Idempotent) => {
                    if let Some(reply_sender) = reply_sender {
                        reply_sender.send(Ok(ExecuteResult::Idempotent));
                    }
                    return delegated_reply;
                }
                Ok(ActorExecutionResult::Transaction {
                    events,
                    reply_receiver,
                }) => {
                    // Reply to actor request
                    let events = match reply_sender {
                        Some(reply_sender) => {
                            let inner_events: Vec<_> = events
                                .iter()
                                .map(|appended| appended.event.clone())
                                .collect();
                            reply_sender.send(Ok(ExecuteResult::Executed(events)));
                            inner_events
                        }
                        None => events.into_iter().map(|appended| appended.event).collect(),
                    };

                    // Apply to temporary state
                    let temp_entity = self.temp_entity.get_or_insert_with(|| self.entity.clone());
                    let temp_version = self.temp_version.get_or_insert(self.version);
                    let temp_correlation_id =
                        self.temp_correlation_id.get_or_insert(self.correlation_id);
                    let temp_last_causations = self
                        .temp_last_causations
                        .get_or_insert_with(|| self.last_causations.clone());

                    let starting_version = match temp_version {
                        CurrentVersion::Current(v) => *v + 1,
                        CurrentVersion::NoStream => 0,
                    };
                    let mut version = starting_version;

                    for event in events.clone() {
                        apply_event(
                            &self.stream_id,
                            temp_entity,
                            temp_version,
                            temp_correlation_id,
                            temp_last_causations,
                            event,
                            version,
                            exec.metadata.clone(),
                        );
                        version += 1;
                    }

                    let actor_ref = ctx.actor_ref();
                    tokio::spawn(async move {
                        println!("=== INSIDE TOKIO ACTOR SPAWN ===");
                        // Wait for the transaction to be comitted
                        // println!("waiting for transaction reply...");
                        match reply_receiver.await {
                            Ok(tx_res) => match tx_res {
                                TransactionResult::Ok => {
                                    println!("telling transaction comitted");
                                    let _ = actor_ref.tell(TransactionComitted).send().await;
                                }
                                TransactionResult::WriteConflict { tx_sender } => {
                                    exec.tx_sender = Some(tx_sender);
                                    println!("telling transaction write conflict");
                                    let _ = actor_ref
                                        .tell(TransactionWriteConflict { exec })
                                        .send()
                                        .await;
                                }
                            },
                            Err(_) => {
                                println!("got err tx reply");
                                let _ = actor_ref.tell(TransactionAborted).send().await;
                            }
                        }
                        println!("=== OUTSIDE TOKIO ACTOR SPAWN ===");
                    });

                    dbg!("return");
                    return delegated_reply;
                }
                Err(err) => {
                    if let Some(reply_sender) = reply_sender {
                        reply_sender.send(Err(err));
                    }
                    dbg!("return");
                    return delegated_reply;
                }
            }
        }
    }
}

struct TransactionComitted;

impl<E> Message<TransactionComitted> for EntityActor<E>
where
    E: Entity + Apply,
{
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: TransactionComitted,
        ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        if self.active_tx.is_some() {
            self.version = self.temp_version.take().unwrap();
            self.entity = self.temp_entity.take().unwrap();
            self.correlation_id = self.temp_correlation_id.take().unwrap();
            self.last_causations = self.temp_last_causations.take().unwrap();
            self.active_tx = None;
        }

        debug!(
            "transaction comitted, executing {} buffered commands",
            self.buffered_commands.len()
        );

        println!("handling buffered commands");
        while let Some((reply_sender, exec)) = self.buffered_commands.pop_front() {
            dbg!(reply_sender.is_some());
            if let Some(err) = exec.handle_dyn(self, ctx.actor_ref(), reply_sender).await {
                std::panic::resume_unwind(Box::new(err));
            }
        }
    }
}

struct TransactionWriteConflict<I, C, M> {
    exec: Execute<I, C, M>,
}

impl<E, C> Message<TransactionWriteConflict<E::ID, C, E::Metadata>> for EntityActor<E>
where
    E: Entity + Command<C> + Apply + Clone,
    C: Clone + Send + Sync + 'static,
{
    type Reply = Result<(), SendError<Execute<E::ID, C, E::Metadata>, ExecuteError<E::Error>>>;

    async fn handle(
        &mut self,
        msg: TransactionWriteConflict<E::ID, C, E::Metadata>,
        ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        println!("write conflict");
        self.resync_with_db().await.unwrap();
        ctx.actor_ref().tell(msg.exec).send_sync()
    }
}

struct TransactionAborted;

impl<E> Message<TransactionAborted> for EntityActor<E>
where
    E: Entity + Apply,
{
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: TransactionAborted,
        ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        println!("transaction aborted");
        self.abort_transaction(&ctx.actor_ref()).await
    }
}

impl<E, Ev, M> From<AppendEventsError<Ev, M>> for ExecuteError<E> {
    fn from(err: AppendEventsError<Ev, M>) -> Self {
        match err {
            AppendEventsError::Database(err) => ExecuteError::Database(err),
            AppendEventsError::IncorrectExpectedVersion {
                stream_id,
                current,
                expected,
                ..
            } => ExecuteError::IncorrectExpectedVersion {
                stream_id,
                current,
                expected,
            },
            AppendEventsError::InvalidTimestamp => ExecuteError::InvalidTimestamp,
            AppendEventsError::SerializeEvent(err) => ExecuteError::SerializeEvent(err),
        }
    }
}

impl<M, E, Ev, Me> From<SendError<M, AppendEventsError<Ev, Me>>> for ExecuteError<E> {
    fn from(err: SendError<M, AppendEventsError<Ev, Me>>) -> Self {
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

#[inline]
fn apply_event<E>(
    stream_id: &StreamID,
    entity: &mut E,
    current_version: &mut CurrentVersion,
    current_correlation_id: &mut Uuid,
    last_causations: &mut HashMap<Uuid, CausationMetadata>,
    event: E::Event,
    stream_version: u64,
    metadata: Metadata<E::Metadata>,
) where
    E: Entity + Apply,
{
    assert_eq!(
        match current_version {
            CurrentVersion::Current(version) => *version + 1,
            CurrentVersion::NoStream => 0,
        },
        stream_version,
        "expected stream version {} but got {} for stream {}",
        match current_version {
            CurrentVersion::Current(version) => *version + 1,
            CurrentVersion::NoStream => 0,
        },
        stream_version,
        stream_id,
    );
    let causation = metadata.causation.clone();
    if current_correlation_id.is_nil() {
        assert_eq!(
            *current_version,
            CurrentVersion::NoStream,
            "expected correlation id to be nil only if the stream doesn't exist"
        );
        *current_correlation_id = metadata.correlation_id;
    }
    entity.apply(event, metadata);
    *current_version = CurrentVersion::Current(stream_version);

    if let Some(causation) = causation {
        last_causations.insert(causation.correlation_id, causation);
    }
}
