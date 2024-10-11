use std::sync::Arc;

use polodb_core::{
    bson::{bson, doc},
    options::UpdateOptions,
    CollectionT, Database, Transaction,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::Event;

use super::{CompositeEventHandler, EventHandler, EventHandlerError, EventProcessor};

pub struct PoloDBEventProcessor<H> {
    db: Arc<Database>,
    projection_id: String,
    handler: H,
    last_handled_event: Option<u64>,
}

impl<H> PoloDBEventProcessor<H> {
    pub fn new(
        db: Arc<Database>,
        projection_id: impl Into<String>,
        handler: H,
    ) -> anyhow::Result<Self> {
        let projection_id = projection_id.into();

        let last_handled_event = db
            .collection("checkpoints")
            .find_one(doc! { "_id": &projection_id })?
            .map(|checkpoint: Checkpoint| checkpoint.last_event_id as u64);

        Ok(PoloDBEventProcessor {
            db,
            projection_id,
            handler,
            last_handled_event,
        })
    }

    /// Gets a reference to the inner handler.
    pub fn handler(&mut self) -> &mut H {
        &mut self.handler
    }
}

impl<E, H> EventProcessor<E, H> for PoloDBEventProcessor<H>
where
    H: EventHandler<Transaction> + CompositeEventHandler<E, Transaction, PoloDBEventProcessorError>,
{
    type Context = Transaction;
    type Error = PoloDBEventProcessorError;

    async fn start_from(&self) -> Result<u64, Self::Error> {
        Ok(self.last_handled_event.map(|n| n + 1).unwrap_or(0))
    }

    async fn process_event(
        &mut self,
        event: Event,
    ) -> Result<(), EventHandlerError<Self::Error, <H as EventHandler<Self::Context>>::Error>> {
        let event_id = event.id;

        let mut tx = self.db.start_transaction()?;

        let res = self.handler.composite_handle(&mut tx, event).await;
        if res.is_err() {
            let abort_res = tx.rollback();
            res?;
            abort_res?;
        }

        let expected_last_event_id = match self.last_handled_event {
            Some(last) => bson!({ "$eq": last as i64 }),
            None => bson!({ "$exists": false }),
        };
        let update_result = tx.collection::<Checkpoint>("checkpoints")
                .update_one_with_options(
                    doc! { "_id": &self.projection_id, "last_event_id": &expected_last_event_id },
                    doc! { "$set": { "last_event_id": event_id as i64 }, "$setOnInsert": { "_id": &self.projection_id } },
                    UpdateOptions::builder().upsert(true).build()
                );
        match update_result {
            Ok(update_result) => {
                if update_result.modified_count == 0 && event_id > 0 {
                    tx.rollback()?;
                    return Err(EventHandlerError::Processor(
                        PoloDBEventProcessorError::UnexpectedLastEventId {
                            expected: self.last_handled_event,
                        },
                    ));
                }
            }
            Err(err) => return Err(err.into()),
        }

        tx.commit()?;

        self.last_handled_event = Some(event_id);

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Checkpoint {
    #[serde(rename = "_id")]
    pub id: String,
    pub last_event_id: u64,
}

#[derive(Debug, Error)]
pub enum PoloDBEventProcessorError {
    #[error("unexpected last event id, expected {expected:?}")]
    UnexpectedLastEventId { expected: Option<u64> },
    #[error(transparent)]
    PoloDB(#[from] polodb_core::Error),
}

impl<H> From<polodb_core::Error> for EventHandlerError<PoloDBEventProcessorError, H> {
    fn from(err: polodb_core::Error) -> Self {
        EventHandlerError::Processor(PoloDBEventProcessorError::PoloDB(err))
    }
}
