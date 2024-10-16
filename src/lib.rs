pub mod command_service;
mod entity_actor;
pub mod error;
pub mod event_handler;
mod event_store;
pub mod stream_id;
pub mod test_utils;

use std::{collections::HashMap, convert::Infallible, fmt, io, ops, str::FromStr, time::Instant};

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use ciborium::Value;
use event_store::{AppendEvents, AppendEventsError, AppendTransactionEvents, EventStore};
use eventus::server::eventstore::{
    event_store_client::EventStoreClient, subscribe_request::StartFrom, EventBatch,
    SubscribeRequest,
};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use kameo::{actor::pool::WorkerMsg, error::SendError, request::MessageSend};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use stream_id::StreamID;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tonic::{transport::Channel, Code, Status};
use tracing::debug;
use uuid::Uuid;

pub trait Entity: Default + Send + 'static {
    type ID: FromStr + fmt::Display + Send + Sync;
    type Event: EventType + Clone + Serialize + DeserializeOwned + Send + Sync;
    type Metadata: Serialize + DeserializeOwned + Clone + Default + Unpin + Send + Sync + 'static;

    fn name() -> &'static str;
}

pub trait Command<C>: Entity {
    type Error: fmt::Debug + Send + Sync + 'static;

    fn handle(&self, cmd: C, ctx: Context<'_, Self>) -> Result<Vec<Self::Event>, Self::Error>;

    /// Checks if the command can be processed idempotently.
    ///
    /// # Returns
    ///
    /// * `true` if the operation can be processed idempotently, meaning no side effects or duplicates will occur.
    /// * `false` if processing the operation would cause an idempotency violation, and it should be avoided.
    fn is_idempotent(&self, _cmd: &C, ctx: Context<'_, Self>) -> bool {
        !ctx.processed()
    }
}

pub trait Apply
where
    Self: Entity,
{
    fn apply(&mut self, event: Self::Event, metadata: Metadata<Self::Metadata>);
}

pub trait EventType {
    fn event_type(&self) -> &'static str;
}

pub struct Context<'a, E>
where
    E: Entity,
{
    pub metadata: &'a Metadata<E::Metadata>,
    pub last_causation: Option<&'a CausationMetadata>,
    pub time: DateTime<Utc>,
    pub executed_at: Instant,
}

impl<'a, E> Context<'a, E>
where
    E: Entity,
{
    pub fn processed(&self) -> bool {
        self.last_causation
            .zip(self.metadata.causation.as_ref())
            .map(|(last, current)| last.event_id >= current.event_id)
            .unwrap_or(false)
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.time + self.executed_at.elapsed()
    }
}

impl<'a, E> Clone for Context<'a, E>
where
    E: Entity,
{
    fn clone(&self) -> Self {
        Context {
            metadata: self.metadata,
            last_causation: self.last_causation,
            time: self.time,
            executed_at: self.executed_at,
        }
    }
}

impl<'a, E> Copy for Context<'a, E> where E: Entity {}

impl<'a, E> fmt::Debug for Context<'a, E>
where
    E: Entity,
    E::Metadata: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context")
            .field("metadata", &self.metadata)
            .field("last_causation", &self.last_causation)
            .field("time", &self.time)
            .field("executed_at", &self.executed_at)
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum Error<M = (), E = Infallible> {
    #[error(transparent)]
    Database(#[from] Status),
    #[error(transparent)]
    SendError(#[from] SendError<M, E>),
}

pub async fn subscribe(
    mut client: EventStoreClient<Channel>,
    start_from: StartFrom,
) -> Result<BoxStream<'static, Result<Vec<Event>, Status>>, Status> {
    let stream = client
        .subscribe(SubscribeRequest {
            start_from: Some(start_from),
        })
        .await?
        .into_inner();

    Ok(stream
        .and_then(|EventBatch { events }| async move {
            events
                .into_iter()
                .map(|event| {
                    Event::try_from(event)
                        .map_err(|err| Status::new(Code::Internal, err.to_string()))
                })
                .collect()
        })
        .boxed())
}

#[derive(Debug)]
pub struct Event<E = GenericValue, M = GenericValue> {
    pub id: u64,
    pub stream_id: StreamID,
    pub stream_version: u64,
    pub name: String,
    pub data: E,
    pub metadata: Metadata<M>,
    pub timestamp: DateTime<Utc>,
}

impl Event {
    #[inline]
    pub fn as_entity<E>(
        self,
    ) -> Result<Event<E::Event, E::Metadata>, (Event, ciborium::value::Error)>
    where
        E: Entity,
    {
        let data = match self.data.0.deserialized() {
            Ok(data) => data,
            Err(err) => {
                return Err((self, err));
            }
        };

        let metadata = match self.metadata.cast() {
            Ok(metadata) => metadata,
            Err(CastMetadataError { err, metadata }) => {
                return Err((
                    Event {
                        id: self.id,
                        stream_id: self.stream_id,
                        stream_version: self.stream_version,
                        name: self.name,
                        data: self.data,
                        metadata,
                        timestamp: self.timestamp,
                    },
                    err,
                ))
            }
        };

        Ok(Event {
            id: self.id,
            stream_id: self.stream_id,
            stream_version: self.stream_version,
            name: self.name,
            data,
            metadata,
            timestamp: self.timestamp,
        })
    }
}

impl<E, M> Event<E, M> {
    #[inline]
    pub fn entity_id<Ent>(&self) -> Result<Ent::ID, <Ent::ID as FromStr>::Err>
    where
        Ent: Entity,
    {
        self.stream_id.cardinal_id().parse()
    }
}

#[derive(Debug, Error)]
pub enum TryFromEventusEventError {
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error("failed to deserialize event data: {0}")]
    DeserializeEventData(ciborium::de::Error<io::Error>),
    #[error("failed to deserialize event metadata: {0}")]
    DeserializeEventMetadata(ciborium::de::Error<io::Error>),
}

impl<E, M> TryFrom<eventus::server::eventstore::Event> for Event<E, M>
where
    E: DeserializeOwned,
    M: DeserializeOwned + Default,
{
    type Error = TryFromEventusEventError;

    fn try_from(ev: eventus::server::eventstore::Event) -> Result<Self, Self::Error> {
        Ok(Event {
            id: ev.id,
            stream_id: StreamID::new(ev.stream_id),
            stream_version: ev.stream_version,
            name: ev.event_name,
            data: ciborium::from_reader(ev.event_data.as_slice())
                .map_err(TryFromEventusEventError::DeserializeEventData)?,
            metadata: ciborium::from_reader(ev.metadata.as_slice())
                .map_err(TryFromEventusEventError::DeserializeEventMetadata)?,
            timestamp: ev
                .timestamp
                .and_then(|ts| {
                    DateTime::<Utc>::from_timestamp(ts.seconds, ts.nanos.try_into().unwrap_or(0))
                })
                .ok_or(TryFromEventusEventError::InvalidTimestamp)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Metadata<T> {
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub causation: Option<CausationMetadata>,
    #[serde(rename = "cid")]
    pub correlation_id: Uuid,
    pub data: T,
}

impl<T> Metadata<T> {
    pub fn with_data<U>(self, data: U) -> Metadata<U> {
        Metadata {
            causation: self.causation,
            correlation_id: self.correlation_id,
            data,
        }
    }
}

impl Metadata<GenericValue> {
    pub fn cast<U>(self) -> Result<Metadata<U>, CastMetadataError>
    where
        U: DeserializeOwned + Default,
    {
        let data = if matches!(self.data, GenericValue(Value::Null)) {
            U::default()
        } else {
            match self.data.0.deserialized() {
                Ok(data) => data,
                Err(err) => {
                    return Err(CastMetadataError {
                        err,
                        metadata: self,
                    })
                }
            }
        };
        Ok(Metadata {
            causation: self.causation,
            correlation_id: self.correlation_id,
            data,
        })
    }
}

impl<T> ops::Deref for Metadata<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> ops::DerefMut for Metadata<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CausationMetadata {
    #[serde(rename = "ceid")]
    pub event_id: u64,
    #[serde(rename = "csid")]
    pub stream_id: StreamID,
    #[serde(rename = "csv")]
    pub stream_version: u64,
    #[serde(rename = "ccid")]
    pub correlation_id: Uuid,
}

#[derive(Debug)]
pub struct CastMetadataError {
    pub err: ciborium::value::Error,
    pub metadata: Metadata<GenericValue>,
}

impl fmt::Display for CastMetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to cast metadata: {}", self.err)
    }
}

impl std::error::Error for CastMetadataError {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GenericValue(pub Value);

impl Default for GenericValue {
    fn default() -> Self {
        GenericValue(Value::Null)
    }
}

impl ops::Deref for GenericValue {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for GenericValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct Transaction {
    event_store: EventStore,
    receiver: mpsc::UnboundedReceiver<TransactionMessage>,
    sender: mpsc::UnboundedSender<TransactionMessage>,
}

impl Transaction {
    pub(crate) fn new(event_store: EventStore) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Transaction {
            event_store,
            receiver,
            sender,
        }
    }

    pub async fn commit(mut self) -> anyhow::Result<()> {
        self.receiver.close();

        let mut reply_senders = HashMap::new();
        let mut stream_events = Vec::new();
        while let Some(TransactionMessage {
            stream_id,
            reply_sender,
            append,
        }) = self.receiver.recv().await
        {
            reply_senders.insert(stream_id, reply_sender);
            if !append.events.is_empty() {
                stream_events.push(append);
            }
        }

        if stream_events.is_empty() {
            return Ok(());
        }

        let mut attempt = 1;
        loop {
            let res = self
                .event_store
                .ask(WorkerMsg(AppendTransactionEvents { stream_events }))
                .send()
                .await;
            match res {
                Ok(_) => {
                    for (_, reply_sender) in reply_senders {
                        let _ = reply_sender.send(TransactionResult::Ok);
                    }
                    return Ok(());
                }
                Err(SendError::HandlerError(AppendEventsError::IncorrectExpectedVersion {
                    stream_id,
                    current,
                    expected,
                    events,
                    ..
                })) => {
                    debug!(%stream_id, %current, %expected, "write conflict");
                    if attempt == 5 {
                        return Err(anyhow!("too many conflict retries"));
                    }

                    stream_events = events;
                    attempt += 1;

                    let reply_sender = reply_senders.remove(&stream_id).unwrap();
                    let (retry_sender, retry_receiver) = oneshot::channel();
                    let _ = reply_sender.send(TransactionResult::WriteConflict {
                        tx_sender: TransactionSender::Retry(retry_sender),
                    });
                    let tx_msg = retry_receiver.await?;
                    reply_senders.insert(tx_msg.stream_id, tx_msg.reply_sender);
                    if !tx_msg.append.events.is_empty() {
                        stream_events.push(tx_msg.append);
                    }
                    if stream_events.is_empty() {
                        return Ok(());
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    pub(crate) fn sender(&self) -> TransactionSender {
        TransactionSender::Initial(self.sender.clone())
    }
}

#[derive(Debug)]
pub(crate) struct TransactionMessage {
    stream_id: StreamID,
    reply_sender: oneshot::Sender<TransactionResult>,
    append: AppendEvents<(&'static str, GenericValue), GenericValue>,
}

#[derive(Debug)]
pub(crate) enum TransactionSender {
    Initial(mpsc::UnboundedSender<TransactionMessage>),
    Retry(oneshot::Sender<TransactionMessage>),
}

impl TransactionSender {
    pub(crate) fn send_events(
        self,
        stream_id: StreamID,
        append: AppendEvents<(&'static str, GenericValue), GenericValue>,
    ) -> anyhow::Result<oneshot::Receiver<TransactionResult>> {
        let (reply_sender, reply_receiver) = oneshot::channel();
        match self {
            TransactionSender::Initial(sender) => {
                sender.send(TransactionMessage {
                    stream_id,
                    reply_sender,
                    append,
                })?;
            }
            TransactionSender::Retry(sender) => {
                sender
                    .send(TransactionMessage {
                        stream_id,
                        reply_sender,
                        append,
                    })
                    .map_err(|_| anyhow!("failed to send to retry transaction sender"))?;
            }
        }

        Ok(reply_receiver)
    }
}

#[derive(Debug)]
pub(crate) enum TransactionResult {
    Ok,
    WriteConflict { tx_sender: TransactionSender },
}

/// Matches on an event for multiple branches, with each branch being an entity type.
///
/// The passed in event will be converted to Event<E> where E is the Entity for the given branch.
///
/// # Example
///
/// ```
/// match_event! {
///     event,
///     BankAccount => {
///         let id: BankAccount::ID = event.entity_id()?;
///         let data = BankAccount::Event = event.data()?;
///         // ...
///     }
///     else => println!("unknown entity"),
/// }
/// ```
#[macro_export]
macro_rules! match_event {
    // All input is normalized, now transform.
    (@ {
        event=$event:ident;

        $( $ent:ident => $handle:expr, )+

        // Fallback expression used when all select branches have been disabled.
        ; $else:expr

    }) => {{
        let category = $event.stream_id.category();
        $(
            if category == $ent::name() {
                let $event = $event.as_entity::<$ent>();
                {
                    let _res = $handle;
                }
            } else
        )*
        if true {
            $else
        }
    }};

    // ==== Normalize =====

    // These rules match a single `select!` branch and normalize it for
    // processing by the first rule.

    (@ { event=$event:ident; $($t:tt)* } ) => {
        // No `else` branch
        $crate::match_event!(@{ event=$event; $($t)*; {} })
    };
    (@ { event=$event:ident; $($t:tt)* } else => $else:expr $(,)?) => {
        $crate::match_event!(@{ event=$event; $($t)*; $else })
    };
    (@ { event=$event:ident; $($t:tt)* } $ent:ident => $h:block, $($r:tt)* ) => {
        $crate::match_event!(@{ event=$event; $($t)* $ent => $h, } $($r)*)
    };
    (@ { event=$event:ident; $($t:tt)* } $ent:ident => $h:block $($r:tt)* ) => {
        $crate::match_event!(@{ event=$event; $($t)* $ent => $h, } $($r)*)
    };
    (@ { event=$event:ident; $($t:tt)* } $ent:ident => $h:expr ) => {
        $crate::match_event!(@{ event=$event; $($t)* $ent => $h, })
    };
    (@ { event=$event:ident; $($t:tt)* } $ent:ident => $h:expr, $($r:tt)* ) => {
        $crate::match_event!(@{ event=$event; $($t)* $ent => $h, } $($r)*)
    };

    // ===== Entry point =====

    ( $event:ident, else => $else:expr $(,)? ) => {{
        $else
    }};

    ( $event:ident, $ent:ident => $($t:tt)* ) => {
        // Randomly generate a starting point. This makes `select!` a bit more
        // fair and avoids always polling the first future.
        $crate::match_event!(@{ event=$event; } $ent => $($t)*)
    };

    () => {
        compile_error!("select! requires at least one branch.")
    };
}
