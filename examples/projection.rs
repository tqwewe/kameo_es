use std::collections::HashMap;

use eventus::server::{eventstore::event_store_client::EventStoreClient, ClientAuthInterceptor};
use kameo_es::{
    event_handler::{
        in_memory::InMemoryEventProcessor, EntityEventHandler, EventHandler,
        EventHandlerStreamBuilder,
    },
    Entity, Event, EventType,
};
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let channel = Channel::builder("http://[::1]:9220".parse()?)
        .connect()
        .await?;
    let mut client =
        EventStoreClient::with_interceptor(channel, ClientAuthInterceptor::new("localhost")?);

    <(MyEntity,)>::event_handler_stream(
        &mut client,
        InMemoryEventProcessor::new(EventKindCounter::default()),
    )
    .await?
    .run()
    .await?;

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

impl EventHandler<()> for EventKindCounter {
    type Error = anyhow::Error;
}

impl EntityEventHandler<MyEntity, ()> for EventKindCounter {
    async fn handle(
        &mut self,
        _ctx: &mut (),
        _id: String,
        event: Event<MyEntityEvent, ()>,
    ) -> Result<(), Self::Error> {
        // Increment the counter for this event name
        *self.events.entry(event.name).or_default() += 1;

        // We'll print the event counts for debugging purposes
        let output = self
            .events
            .iter()
            .map(|(event_name, count)| format!("{event_name} = {count}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!("{output}");

        Ok(())
    }
}
