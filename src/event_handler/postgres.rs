use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    fmt,
    marker::PhantomData,
    ops,
    sync::Arc,
};

use futures::{Future, FutureExt};
use kameo::{
    actor::{ActorID, ActorRef, WeakActorRef},
    error::{ActorStopReason, BoxError, PanicError},
    mailbox::{bounded::BoundedMailbox, unbounded::UnboundedMailbox},
    message::{Context, Message},
    messages,
    reply::DelegatedReply,
    request::{ForwardMessageSend, MessageSend},
    Actor,
};
use sqlx::{prelude::FromRow, PgPool, Postgres};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::Event;

use super::{CompositeEventHandler, EventHandler, EventHandlerError, EventProcessor};

pub struct Transaction {
    pub(crate) tx: sqlx::Transaction<'static, Postgres>,
    pub(crate) dirty: bool,
}

impl Transaction {
    pub fn new(tx: sqlx::Transaction<'static, Postgres>) -> Self {
        Transaction { tx, dirty: false }
    }
}

impl ops::Deref for Transaction {
    type Target = sqlx::Transaction<'static, Postgres>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl ops::DerefMut for Transaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.tx
    }
}

pub struct Coordinator<E: 'static, H: Send + 'static> {
    pool: PgPool,
    checkpoints_table: Arc<str>,
    projection_id: Arc<str>,
    handler: H,
    correlations: HashMap<Uuid, ActorRef<CorrelationWorker<E, H>>>,
    global_flush_semaphore: Arc<Semaphore>,
    flush_interval: u64,
    global_offset_actor: ActorRef<GlobalOffsetActor>,
}

impl<E: 'static, H: Send + 'static> Coordinator<E, H> {
    pub fn new(
        pool: PgPool,
        checkpoints_table: impl Into<Arc<str>>,
        projection_id: impl Into<Arc<str>>,
        handler: H,
        concurrency: usize,
    ) -> Self {
        let checkpoints_table = checkpoints_table.into();
        let projection_id = projection_id.into();
        Coordinator {
            pool: pool.clone(),
            checkpoints_table: checkpoints_table.clone(),
            projection_id: projection_id.clone(),
            handler,
            correlations: HashMap::new(),
            global_flush_semaphore: Arc::new(Semaphore::new(concurrency)),
            flush_interval: 100,
            global_offset_actor: kameo::spawn(GlobalOffsetActor::new(
                pool,
                checkpoints_table,
                projection_id,
            )),
        }
    }

    /// The number of ignored events to process before performing a flush.
    pub fn flush_interval(mut self, num_ignored_events: u64) -> Self {
        self.flush_interval = num_ignored_events;
        self
    }

    /// Gets a reference to the inner handler.
    pub fn handler(&mut self) -> &mut H {
        &mut self.handler
    }
}

impl<E, H> EventProcessor<E, H> for ActorRef<Coordinator<E, H>>
where
    E: 'static,
    H: EventHandler<Transaction>
        + CompositeEventHandler<E, Transaction, PostgresEventProcessorError>
        + Clone
        + Send
        + 'static,
    H::Error: fmt::Debug + Unpin + Sync,
{
    type Context = Transaction;
    type Error = PostgresEventProcessorError;

    async fn start_from(&self) -> Result<u64, Self::Error> {
        // Request the global offset from Coordinator
        let reply =
            self.ask(GetGlobalOffset).send().await.map_err(|_| {
                PostgresEventProcessorError::UnexpectedLastEventId { expected: None }
            })?;

        Ok(reply.map(|n| n + 1).unwrap_or(0))
    }

    async fn process_event(
        &mut self,
        event: Event,
    ) -> Result<(), EventHandlerError<Self::Error, <H as EventHandler<Self::Context>>::Error>> {
        self.tell(HandleEvent(event)).send().await.unwrap();
        Ok(())
    }
}

impl<E: 'static, H: Send + 'static> Actor for Coordinator<E, H> {
    type Mailbox = BoundedMailbox<Self>;

    async fn on_link_died(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        id: ActorID,
        _reason: ActorStopReason,
    ) -> Result<Option<ActorStopReason>, BoxError> {
        self.correlations.retain(|_, worker| worker.id() != id);
        Ok(None)
    }
}

struct HandleEvent(Event);

impl<E, H> Message<HandleEvent> for Coordinator<E, H>
where
    E: 'static,
    H: EventHandler<Transaction>
        + CompositeEventHandler<E, Transaction, PostgresEventProcessorError>
        + Clone
        + Send
        + 'static,
    H::Error: fmt::Debug + Unpin + Sync,
{
    type Reply = DelegatedReply<
        Result<
            (),
            EventHandlerError<PostgresEventProcessorError, <H as EventHandler<Transaction>>::Error>,
        >,
    >;

    fn handle(
        &mut self,
        HandleEvent(event): HandleEvent,
        mut ctx: Context<'_, Self, Self::Reply>,
    ) -> impl Future<Output = Self::Reply> + Send {
        async move {
            if event.metadata.correlation_id.is_nil() {
                panic!("nil correlation id");
            }

            let permit = self
                .global_flush_semaphore
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let entry = self.correlations.entry(event.metadata.correlation_id);
            let correlation_worker_ref;
            match entry {
                Entry::Vacant(vacancy) => {
                    let worker_ref = kameo::spawn(CorrelationWorker {
                        correlation_id: event.metadata.correlation_id,
                        pool: self.pool.clone(),
                        checkpoints_table: self.checkpoints_table.clone(),
                        projection_id: self.projection_id.clone(),
                        handler: self.handler.clone(),
                        last_handled_event: None,
                        flush_interval: self.flush_interval,
                        global_offset_actor: self.global_offset_actor.clone(),
                        phantom: PhantomData,
                    });
                    worker_ref.link_child(&ctx.actor_ref()).await;

                    correlation_worker_ref = vacancy.insert(worker_ref);
                }
                Entry::Occupied(occupied) => {
                    correlation_worker_ref = occupied.into_mut();
                }
            }

            self.global_offset_actor
                .tell(StartProcessing {
                    event_id: event.id,
                    permit,
                })
                .send()
                .await
                .unwrap();

            let (delegated_reply, reply_sender) = ctx.reply_sender();
            match reply_sender {
                Some(reply_sender) => {
                    correlation_worker_ref
                        .ask(HandleEvent(event))
                        .forward(reply_sender)
                        .await
                        .unwrap();
                }
                None => {
                    correlation_worker_ref
                        .tell(HandleEvent(event))
                        .send()
                        .await
                        .unwrap();
                }
            }

            delegated_reply
        }
    }
}

struct CorrelationWorker<E, H> {
    correlation_id: Uuid,
    pool: PgPool,
    checkpoints_table: Arc<str>,
    projection_id: Arc<str>,
    handler: H,
    last_handled_event: Option<u64>,
    flush_interval: u64,
    global_offset_actor: ActorRef<GlobalOffsetActor>,
    phantom: PhantomData<fn() -> E>,
}

impl<E: 'static, H: Send + 'static> Actor for CorrelationWorker<E, H> {
    type Mailbox = UnboundedMailbox<Self>;

    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        #[derive(FromRow)]
        struct CheckpointRow {
            last_event_id: i64,
        }

        let checkpoint: Option<CheckpointRow> = sqlx::query_as(&format!(
            "
                SELECT last_event_id FROM {}
                WHERE projection_id = $1 AND correlation_id = $2
            ",
            self.checkpoints_table
        ))
        .bind(self.projection_id.as_ref())
        .bind(self.correlation_id)
        .fetch_optional(&self.pool)
        .await?;

        self.last_handled_event = checkpoint.map(|cp| cp.last_event_id as u64);

        Ok(())
    }

    async fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> Result<Option<ActorStopReason>, BoxError> {
        error!("{err}");
        Ok(None)
    }
}

impl<E: 'static, H: Send + 'static> Message<GetGlobalOffset> for Coordinator<E, H> {
    type Reply = DelegatedReply<Option<u64>>;

    async fn handle(
        &mut self,
        msg: GetGlobalOffset,
        mut ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let (delegated_reply, reply_sender) = ctx.reply_sender();
        if let Some(reply_sender) = reply_sender {
            self.global_offset_actor
                .ask(msg)
                .forward(reply_sender)
                .await
                .unwrap();
        }

        delegated_reply
    }
}

impl<E, H> Message<HandleEvent> for CorrelationWorker<E, H>
where
    E: 'static,
    H: EventHandler<Transaction>
        + CompositeEventHandler<E, Transaction, PostgresEventProcessorError>
        + Send
        + 'static,
    H::Error: fmt::Debug + Sync,
{
    type Reply = Result<
        (),
        EventHandlerError<PostgresEventProcessorError, <H as EventHandler<Transaction>>::Error>,
    >;

    fn handle(
        &mut self,
        HandleEvent(event): HandleEvent,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> impl Future<Output = Self::Reply> + Send {
        async move {
            let mut tx = Transaction::new(self.pool.begin().await?);
            let event_id = event.id;
            let res = self.handler.composite_handle(&mut tx, event).await;
            if res.is_err() {
                tx.tx.rollback().await?;
                return res.map_err(|e| e.into());
            }

            let Transaction { mut tx, dirty } = tx;

            let events_since_last_flush = self
                .last_handled_event
                .map(|last| event_id - last)
                .unwrap_or(u64::MAX);
            if dirty || events_since_last_flush > self.flush_interval {
                match self.last_handled_event {
                    Some(last_handled_event) => {
                        let res = sqlx::query(&format!(
                            "
                                UPDATE {} SET last_event_id = $1
                                WHERE projection_id = $2 AND correlation_id = $3
                            ",
                            self.checkpoints_table
                        ))
                        .bind(event_id as i64)
                        .bind(self.projection_id.as_ref())
                        .bind(self.correlation_id)
                        .execute(&mut *tx)
                        .await?;
                        if res.rows_affected() == 0 {
                            return Err(EventHandlerError::Processor(
                                PostgresEventProcessorError::UnexpectedLastEventId {
                                    expected: Some(last_handled_event),
                                },
                            ));
                        }
                    }
                    None => {
                        let res = sqlx::query(
                                &format!(
                                    "INSERT INTO {} (projection_id, correlation_id, last_event_id) VALUES ($1, $2, $3)",
                                    self.checkpoints_table
                                ),
                            )
                            .bind(self.projection_id.as_ref())
                            .bind(self.correlation_id)
                            .bind(event_id as i64)
                            .execute(&mut *tx)
                            .await;

                        match res {
                            Ok(_) => {}
                            Err(sqlx::Error::Database(db_err))
                                if db_err.code().as_deref() == Some("23505") =>
                            {
                                // 23505 is the error code for unique violations (e.g., primary key conflicts)
                                return Err(EventHandlerError::Processor(
                                    PostgresEventProcessorError::UnexpectedLastEventId {
                                        expected: None,
                                    },
                                ));
                            }
                            Err(err) => return Err(err.into()),
                        }
                    }
                }

                tx.commit().await?;
                self.last_handled_event = Some(event_id);
            } else {
                debug!("ignoring event");
            }

            // Notify the GlobalOffsetActor about the new event id.
            self.global_offset_actor
                .tell(FinishProcessing { event_id })
                .send()
                .await
                .map_err(|_| {
                    EventHandlerError::Processor(PostgresEventProcessorError::Postgres(
                        sqlx::Error::RowNotFound, // Replace with appropriate error
                    ))
                })?;

            Ok(())
        }
    }
}

// The GlobalOffsetWorker actor
struct GlobalOffsetActor {
    pool: PgPool,
    checkpoints_table: Arc<str>,
    projection_id: Arc<str>,
    processing_event_ids: VecDeque<(u64, OwnedSemaphorePermit)>,
    processed_event_ids: HashSet<u64>,
    global_offset: Option<u64>,
}

impl Actor for GlobalOffsetActor {
    type Mailbox = UnboundedMailbox<Self>;

    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        // Fetch the persisted global_offset
        let global_offset_row: Option<(i64,)> = sqlx::query_as(&format!(
            "SELECT last_event_id FROM {} WHERE projection_id = $1 AND correlation_id = $2",
            self.checkpoints_table,
        ))
        .bind(self.projection_id.as_ref())
        .bind(Uuid::nil())
        .fetch_optional(&self.pool)
        .await?;

        self.global_offset = global_offset_row.map(|(n,)| n as u64);

        Ok(())
    }

    async fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> Result<Option<ActorStopReason>, BoxError> {
        error!("GlobalOffsetActor panicked: {}", err);
        Ok(None)
    }
}

#[messages]
impl GlobalOffsetActor {
    fn new(pool: PgPool, checkpoints_table: Arc<str>, projection_id: Arc<str>) -> Self {
        Self {
            pool,
            checkpoints_table,
            projection_id,
            processing_event_ids: VecDeque::new(),
            processed_event_ids: HashSet::new(),
            global_offset: None,
        }
    }

    #[message]
    async fn start_processing(&mut self, event_id: u64, permit: OwnedSemaphorePermit) {
        self.processing_event_ids.push_back((event_id, permit));
    }

    #[message]
    async fn finish_processing(&mut self, event_id: u64) -> Result<(), sqlx::Error> {
        self.processed_event_ids.insert(event_id);

        let old_global_offset = self.global_offset;
        loop {
            if let Some((front_event_id, _)) = self.processing_event_ids.front() {
                if self.processed_event_ids.remove(&front_event_id) {
                    self.global_offset = Some(*front_event_id);
                    self.processing_event_ids.pop_front();
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if old_global_offset != self.global_offset {
            self.persist_global_offset().await?;
        }

        Ok(())
    }

    #[message]
    fn get_global_offset(&self) -> Option<u64> {
        self.global_offset
    }

    // Persist the global offset to the database
    async fn persist_global_offset(&self) -> Result<(), sqlx::Error> {
        let Some(global_offset) = self.global_offset else {
            return Ok(());
        };

        sqlx::query(
            "INSERT INTO checkpoints (projection_id, correlation_id, last_event_id) 
             VALUES ($1, $2, $3)
             ON CONFLICT (projection_id, correlation_id) 
             DO UPDATE SET last_event_id = EXCLUDED.last_event_id",
        )
        .bind(self.projection_id.as_ref())
        .bind(Uuid::nil())
        .bind(global_offset as i64)
        .execute(&self.pool)
        .await?;

        info!("persisted global_offset: {global_offset}");

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum PostgresEventProcessorError {
    #[error("unexpected last event id, expected {expected:?}")]
    UnexpectedLastEventId { expected: Option<u64> },
    #[error(transparent)]
    Postgres(#[from] sqlx::Error),
}

impl<H> From<sqlx::Error> for EventHandlerError<PostgresEventProcessorError, H> {
    fn from(err: sqlx::Error) -> Self {
        EventHandlerError::Processor(PostgresEventProcessorError::Postgres(err))
    }
}
