use std::convert::Infallible;

use crate::{
    event_handler::{CompositeEventHandler, EventHandler, EventHandlerError, EventProcessor},
    Event,
};

pub struct InMemoryEventProcessor<H> {
    handler: H,
}

impl<H> InMemoryEventProcessor<H> {
    pub fn new(handler: H) -> Self {
        InMemoryEventProcessor { handler }
    }
}

impl<E, H> EventProcessor<E, H> for InMemoryEventProcessor<H>
where
    H: EventHandler<()> + CompositeEventHandler<E, (), Infallible>,
{
    type Context = ();
    type Error = Infallible;

    async fn start_from(&self) -> Result<u64, Self::Error> {
        Ok(0)
    }

    async fn process_event(
        &mut self,
        event: Event,
    ) -> Result<(), EventHandlerError<Self::Error, <H as EventHandler<()>>::Error>> {
        self.handler.composite_handle(&mut (), event).await
    }
}
