use std::{collections::HashMap, convert::Infallible};

use eventus::server::eventstore::{
    event_store_client::EventStoreClient, subscribe_request::StartFrom,
};
use kameo_es::{
    event_handler::{start_event_handler, Acknowledgement, EventHandler, EventHandlerBehaviour},
    Entity, Event, EventType,
};
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = EventStoreClient::connect("http://[::1]:9220").await?;
    start_event_handler::<(MyEntity,), _>(client, EventKindCounter::default()).await?;

    Ok(())
}

// === Entity & Events ===

#[derive(Default)]
pub struct MyEntity;

impl Entity for MyEntity {
    type ID = String;
    type Event = MyEntityEvent;
    type Metadata = ();

    fn name() -> &'static str {
        "my_entity"
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum MyEntityEvent {
    Foo {},
    Bar {},
    Baz {},
}

impl EventType for MyEntityEvent {
    fn event_type(&self) -> &'static str {
        match self {
            MyEntityEvent::Foo {} => "Foo",
            MyEntityEvent::Bar {} => "Bar",
            MyEntityEvent::Baz {} => "Baz",
        }
    }
}

// === Event Handler ===

#[derive(Default)]
pub struct EventKindCounter {
    events: HashMap<String, u32>,
}

impl EventHandlerBehaviour for EventKindCounter {
    type Error = Infallible;

    async fn start_from(&self) -> Result<StartFrom, Self::Error> {
        // This function might query a database for the last processed event id,
        // or simply return StartFrom::SubscriberId to load from the last acknowledged event.
        //
        // In this example, we'll just start from 0 every time, since we're projecting only in-memory.
        Ok(StartFrom::EventId(0))
    }

    async fn fallback(&mut self, _event: Event<()>) -> Result<Acknowledgement, Self::Error> {
        Ok(Acknowledgement::Manual)
    }
}

impl EventHandler<MyEntity> for EventKindCounter {
    async fn handle(&mut self, event: Event<MyEntity>) -> Result<Acknowledgement, Self::Error> {
        // Increment the counter for this event name
        *self.events.entry(event.event_name).or_default() += 1;

        // We'll print the event counts for debugging purposes
        let output = self
            .events
            .iter()
            .map(|(event_name, count)| format!("{event_name} = {count}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!("{output}");

        // Indicate that we're handling acknowledgements manually
        Ok(Acknowledgement::Manual)
    }
}
