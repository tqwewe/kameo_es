mod stream_id;
pub mod test_utils;

pub use kameo_es_macros::EventType;
pub use stream_id::StreamID;

use std::{fmt, ops, str::FromStr, time::Instant};

use chrono::{DateTime, Utc};
use ciborium::Value;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
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
