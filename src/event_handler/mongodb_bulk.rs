use std::{
    mem,
    sync::Arc,
    time::{Duration, Instant},
};

use mongodb::{
    bson::doc,
    error::{ErrorKind, WriteError, WriteFailure},
    options::{
        DeleteManyModel, DeleteOneModel, InsertOneModel, ReplaceOneModel, UpdateManyModel,
        UpdateOneModel, WriteModel,
    },
    Client, ClientSession, Collection,
};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task::JoinHandle};
use tracing::info;

use crate::Event;

use super::{
    mongodb::MongoEventProcessorError, CompositeEventHandler, EventHandler, EventHandlerError,
    EventProcessor,
};

#[derive(Debug)]
pub struct Models {
    models: Vec<WriteModel>,
}

impl Models {
    pub fn new() -> Self {
        Models { models: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Models {
            models: Vec::with_capacity(capacity),
        }
    }

    pub fn insert_one(&mut self, model: InsertOneModel) {
        self.models.push(WriteModel::InsertOne(model));
    }

    pub fn update_one(&mut self, model: UpdateOneModel) {
        self.models.push(WriteModel::UpdateOne(model));
    }

    pub fn update_many(&mut self, model: UpdateManyModel) {
        self.models.push(WriteModel::UpdateMany(model));
    }

    pub fn replace_one(&mut self, model: ReplaceOneModel) {
        self.models.push(WriteModel::ReplaceOne(model));
    }

    pub fn delete_one(&mut self, model: DeleteOneModel) {
        self.models.push(WriteModel::DeleteOne(model));
    }

    pub fn delete_many(&mut self, model: DeleteManyModel) {
        self.models.push(WriteModel::DeleteMany(model));
    }

    pub fn len(&self) -> usize {
        self.models.len()
    }
}

pub struct MongoDBBulkEventProcessor<H> {
    client: Client,
    checkpoints_collection: Collection<Checkpoint>,
    projection_id: String,
    handler: H,
    models: Models,
    last_handled_event: Option<u64>,
    last_flushed: Arc<Mutex<(Instant, Option<u64>)>>,
    flush_interval_time: Duration,
    flush_interval_models: usize,
    flush_interval_events: u64,
}

impl<H> MongoDBBulkEventProcessor<H> {
    pub async fn new(
        client: Client,
        checkpoints_collection: Collection<Checkpoint>,
        projection_id: impl Into<String>,
        handler: H,
    ) -> anyhow::Result<Self> {
        let projection_id = projection_id.into();

        let last_handled_event = checkpoints_collection
            .find_one(doc! { "_id": &projection_id })
            .await?
            .map(|checkpoint: Checkpoint| checkpoint.last_event_id as u64);

        Ok(MongoDBBulkEventProcessor {
            client,
            checkpoints_collection,
            projection_id,
            handler,
            models: Models::with_capacity(500),
            last_handled_event,
            last_flushed: Arc::new(Mutex::new((Instant::now(), last_handled_event))),
            flush_interval_time: Duration::from_secs(10),
            flush_interval_models: 100_000,
            flush_interval_events: 2_000,
        })
    }

    /// The number of seconds since last flush before attempting to flush again.
    pub fn flush_interval_time(mut self, period: Duration) -> Self {
        self.flush_interval_time = period;
        self
    }

    /// The number of models queued before attempting to flush.
    pub fn flush_interval_models(mut self, models_count: usize) -> Self {
        self.flush_interval_models = models_count;
        self
    }

    /// The number of events since the last flush before attempting to flush again.
    pub fn flush_interval_events(mut self, events_count: u64) -> Self {
        self.flush_interval_events = events_count;
        self
    }

    /// Gets a reference to the inner handler.
    pub fn handler(&mut self) -> &mut H {
        &mut self.handler
    }

    pub fn last_handled_event(&self) -> Option<u64> {
        self.last_handled_event
    }

    pub fn should_flush(&self) -> Option<FlushReason> {
        if self.models.len() >= self.flush_interval_models {
            return Some(FlushReason::ModelsInterval);
        }
        let last_flushed = self.last_flushed.try_lock().ok()?;
        if last_flushed.1 == self.last_handled_event {
            return None;
        }
        let exceeded_flush_interval_events = match (last_flushed.1, self.last_handled_event) {
            (_, None) => false,
            (None, Some(last_handled_event)) => last_handled_event >= self.flush_interval_events,
            (Some(last_flushed_event), Some(last_handled_event)) => {
                last_handled_event - last_flushed_event >= self.flush_interval_events
            }
        };
        if exceeded_flush_interval_events {
            dbg!(last_flushed.1, self.last_handled_event);
            return Some(FlushReason::EventsInterval);
        }
        if last_flushed.0.elapsed() >= self.flush_interval_time {
            return Some(FlushReason::TimeInterval);
        }

        None
    }

    pub async fn flush(&mut self) -> Option<JoinHandle<Result<(), MongoEventProcessorError>>> {
        if let Some(reason) = self.should_flush() {
            return self.force_flush(reason).await;
        }

        None
    }

    pub async fn force_flush(
        &mut self,
        reason: FlushReason,
    ) -> Option<JoinHandle<Result<(), MongoEventProcessorError>>> {
        let Some(last_handled_event) = self.last_handled_event else {
            return None;
        };

        info!("flushing at event {last_handled_event} because {reason:?}");

        let mut last_flushed = self.last_flushed.clone().lock_owned().await;
        let client = self.client.clone();
        let checkpoints_collection = self.checkpoints_collection.clone();
        let projection_id = self.projection_id.clone();
        let models = mem::take(&mut self.models.models);

        let handle = tokio::spawn(async move {
            let mut session = client.start_session().await?;
            session.start_transaction().await?;

            let res = Self::force_flush_inner(
                client,
                checkpoints_collection,
                &mut session,
                last_flushed.1,
                projection_id,
                last_handled_event,
                models,
            )
            .await;
            match res {
                Ok(()) => {
                    session.commit_transaction().await?;
                }
                Err(err) => {
                    let _ = session.abort_transaction().await;
                    return Err(err);
                }
            }

            last_flushed.0 = Instant::now();
            last_flushed.1 = Some(last_handled_event);

            Ok(())
        });

        Some(handle)
    }

    async fn force_flush_inner(
        client: Client,
        checkpoints_collection: Collection<Checkpoint>,
        session: &mut ClientSession,
        last_flushed_event: Option<u64>,
        projection_id: String,
        last_handled_event: u64,
        models: Vec<WriteModel>,
    ) -> Result<(), MongoEventProcessorError> {
        match last_flushed_event {
            Some(last_flushed_event) => {
                let res = checkpoints_collection
                    .update_one(
                        doc! { "_id": projection_id, "last_event_id": last_flushed_event as i64 },
                        doc! { "$set": { "last_event_id": last_handled_event as i64 } },
                    )
                    .session(&mut *session)
                    .await?;
                if res.matched_count == 0 {
                    return Err(MongoEventProcessorError::UnexpectedLastEventId {
                        expected: Some(last_flushed_event),
                    });
                }
            }
            None => {
                let res = checkpoints_collection
                    .insert_one(Checkpoint {
                        id: projection_id,
                        last_event_id: last_handled_event,
                    })
                    .session(&mut *session)
                    .await;
                match res {
                    Ok(_) => {}
                    Err(err) => match err.kind.as_ref() {
                        ErrorKind::Write(WriteFailure::WriteError(WriteError { code, .. }))
                            if *code == 11000 =>
                        {
                            return Err(MongoEventProcessorError::UnexpectedLastEventId {
                                expected: None,
                            });
                        }
                        _ => return Err(err.into()),
                    },
                }
            }
        }

        if !models.is_empty() {
            client.bulk_write(models).session(&mut *session).await?;
        }

        Ok(())
    }
}

impl<E, H> EventProcessor<E, H> for MongoDBBulkEventProcessor<H>
where
    H: EventHandler<Models> + CompositeEventHandler<E, Models, MongoEventProcessorError>,
{
    type Context = Models;
    type Error = MongoEventProcessorError;

    async fn start_from(&self) -> Result<u64, Self::Error> {
        Ok(self.last_handled_event.map(|n| n + 1).unwrap_or(0))
    }

    async fn process_event(
        &mut self,
        event: Event,
    ) -> Result<(), EventHandlerError<Self::Error, <H as EventHandler<Self::Context>>::Error>> {
        let event_id = event.id;
        self.handler
            .composite_handle(&mut self.models, event)
            .await?;
        self.last_handled_event = Some(event_id);
        self.flush().await;

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Checkpoint {
    #[serde(rename = "_id")]
    pub id: String,
    pub last_event_id: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum FlushReason {
    TimeInterval,
    EventsInterval,
    ModelsInterval,
    Manual,
}

pub trait FlushHandler {
    fn flush(&mut self);
}
