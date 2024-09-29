pub mod file;
pub mod in_memory;
#[cfg(feature = "mongodb")]
pub mod mongodb;

use std::{marker::PhantomData, pin, task};

use eventus::server::{
    eventstore::{
        event_store_client::EventStoreClient, subscribe_request::StartFrom, EventBatch,
        SubscribeRequest,
    },
    ClientAuthInterceptor,
};
use futures::{ready, Future, Stream, StreamExt, TryStreamExt};
use thiserror::Error;
use tokio_util::sync::ReusableBoxFuture;
use tonic::{
    service::interceptor::InterceptedService, transport::Channel, Code, Status, Streaming,
};
use tracing::info;

use crate::{Entity, Event};

pub trait EventProcessor<E, H>
where
    Self: Send,
    H: EventHandler<Self::Context>,
{
    type Context: Send;
    type Error: Send;

    /// Which event to start streaming from.
    fn start_from(&self) -> impl Future<Output = Result<u64, Self::Error>>;

    /// Processes an event, which should internally call the event handler.
    fn process_event(
        &mut self,
        event: Event,
    ) -> impl Future<Output = Result<(), EventHandlerError<Self::Error, H::Error>>> + Send;
}

/// An event handler.
pub trait EventHandler<C>: Send {
    type Error: Send;

    /// Handles an event, typically as a fallback when no entities were matched.
    fn handle(
        &mut self,
        _ctx: &mut C,
        _event: Event,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move { Ok(()) }
    }
}

/// An event handler for an entity.
pub trait EntityEventHandler<E, C>: EventHandler<C>
where
    E: Entity,
{
    /// Handles an event for an entity.
    fn handle(
        &mut self,
        ctx: &mut C,
        id: E::ID,
        event: Event<E::Event, E::Metadata>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A trait for handling events based on a tuple of entities, where each entity is checked against the event category
/// in order until a match is found, which will then be handled using the `EntityEventHandler`.
pub trait CompositeEventHandler<E, C, PE>
where
    Self: EventHandler<C> + Sized,
{
    /// Handles an event, determining which entity it belongs to, falling back to the `EventHandler` implementation.
    fn composite_handle(
        &mut self,
        ctx: &mut C,
        event: Event,
    ) -> impl Future<Output = Result<(), EventHandlerError<PE, Self::Error>>> + Send;
}

/// A helper trait for creating an event handler stream.
pub trait EventHandlerStreamBuilder: Sized + 'static {
    fn event_handler_stream<P, H>(
        client: &mut EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
        processor: P,
    ) -> impl Future<
        Output = Result<EventHandlerStream<Self, P, H>, EventHandlerError<P::Error, H::Error>>,
    >
    where
        P: EventProcessor<Self, H> + 'static,
        H: EventHandler<P::Context> + 'static;
}

impl<E: 'static> EventHandlerStreamBuilder for E {
    async fn event_handler_stream<P, H>(
        client: &mut EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
        processor: P,
    ) -> Result<EventHandlerStream<Self, P, H>, EventHandlerError<P::Error, H::Error>>
    where
        P: EventProcessor<Self, H> + 'static,
        H: EventHandler<P::Context> + 'static,
    {
        EventHandlerStream::new(client, processor).await
    }
}

/// An error which occurs when handling an event.
#[derive(Debug, Error)]
pub enum EventHandlerError<P, H> {
    #[error("failed to deserialize event '{event}' for entity '{entity}': {err}")]
    DeserializeEvent {
        entity: &'static str,
        event: String,
        err: ciborium::value::Error,
    },
    #[error(transparent)]
    Grpc(#[from] Status),
    #[error("failed to parse entity id: {0}")]
    ParseID(String),
    #[error("{0}")]
    Processor(P),
    #[error("{0}")]
    Handler(H),
}

/// A stream which processes events using an `EventProcessor`.
pub struct EventHandlerStream<E, P, H>
where
    P: EventProcessor<E, H>,
    H: EventHandler<P::Context>,
{
    next_fut: ReusableBoxFuture<
        'static,
        (
            P,
            Streaming<EventBatch>,
            Option<Result<(), EventHandlerError<P::Error, H::Error>>>,
        ),
    >,
    phantom: PhantomData<(E, H)>,
}

impl<E, P, H> EventHandlerStream<E, P, H>
where
    E: 'static,
    P: EventProcessor<E, H> + 'static,
    H: EventHandler<P::Context> + 'static,
{
    async fn new(
        client: &mut EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
        processor: P,
    ) -> Result<Self, EventHandlerError<P::Error, H::Error>> {
        let stream = client
            .subscribe(SubscribeRequest {
                start_from: Some(StartFrom::EventId(
                    processor
                        .start_from()
                        .await
                        .map_err(EventHandlerError::Processor)?,
                )),
            })
            .await?
            .into_inner();

        Ok(EventHandlerStream {
            next_fut: ReusableBoxFuture::new(event_handler_stream_next(processor, stream)),
            phantom: PhantomData,
        })
    }

    pub async fn run(self) -> Result<(), EventHandlerError<P::Error, H::Error>>
    where
        E: Unpin + 'static,
        P: EventProcessor<E, H> + Unpin + 'static,
        H: EventHandler<P::Context> + Unpin + 'static,
    {
        self.try_collect().await
    }
}

impl<E, P, H> Stream for EventHandlerStream<E, P, H>
where
    E: Unpin + 'static,
    P: EventProcessor<E, H> + Unpin + 'static,
    H: EventHandler<P::Context> + Unpin + 'static,
{
    type Item = Result<(), EventHandlerError<P::Error, H::Error>>;

    fn poll_next(
        self: pin::Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let (processor, stream, res) = ready!(this.next_fut.poll(cx));
        this.next_fut
            .set(event_handler_stream_next(processor, stream));
        cx.waker().wake_by_ref();

        task::Poll::Ready(res)
    }
}

async fn event_handler_stream_next<E, P, H>(
    mut processor: P,
    mut stream: Streaming<EventBatch>,
) -> (
    P,
    Streaming<EventBatch>,
    Option<Result<(), EventHandlerError<P::Error, H::Error>>>,
)
where
    P: EventProcessor<E, H>,
    H: EventHandler<P::Context>,
{
    match stream.next().await {
        Some(Ok(EventBatch { events })) => {
            for event in events {
                let Ok(event) = Event::try_from(event) else {
                    return (
                        processor,
                        stream,
                        Some(Err(EventHandlerError::Grpc(Status::new(
                            Code::Internal,
                            "invalid timestamp received",
                        )))),
                    );
                };
                info!(
                    "{} {:<32} {:>6} > {}",
                    event.id, event.stream_id, event.stream_version, event.name
                );
                if let Err(err) = processor.process_event(event).await {
                    return (processor, stream, Some(Err(err)));
                }
            }

            (processor, stream, Some(Ok(())))
        }
        Some(Err(status)) => (
            processor,
            stream,
            Some(Err(EventHandlerError::Grpc(status))),
        ),
        None => (processor, stream, None),
    }
}

macro_rules! impl_composite_event_handler {
    (
        $( ( $( $ent:ident ),* ), )+
    ) => {
        $(
            impl_composite_event_handler!( $( $ent ),* );
        )+
    };
    ( $( $( $ent:ident ),+ )? ) => {
        impl<H, C, PE $( , $( $ent ),+ )?> CompositeEventHandler<( $( $( $ent, )+ )? ), C, PE> for H
        where
            H: EventHandler<C> + Sized,
            C: Send,
            PE: Send,
            $( $(
                H: EntityEventHandler<$ent, C>,
                $ent: Entity,
            )+ )?
        {
            async fn composite_handle(
                &mut self,
                ctx: &mut C,
                event: Event,
            ) -> Result<(), EventHandlerError<PE, Self::Error>> {
                $(
                    let category = event.stream_id.category();
                    $(
                        if category == $ent::name() {
                            EntityEventHandler::<$ent, C>::handle(
                                self,
                                ctx,
                                event.entity_id::<$ent>().map_err(|_| {
                                    EventHandlerError::ParseID(event.stream_id.cardinal_id().to_string())
                                })?,
                                event.as_entity::<$ent>().map_err(|(event, err)| {
                                    EventHandlerError::DeserializeEvent {
                                        entity: $ent::name(),
                                        event: event.name,
                                        err,
                                    }
                                })?,
                            )
                            .await
                            .map_err(EventHandlerError::Handler)
                        } else
                    )+
                )?

                {
                    EventHandler::handle(self, ctx, event)
                        .await
                        .map_err(EventHandlerError::Handler)
                }
            }
        }
    };
}

impl_composite_event_handler![
    (),
    (E1),
    (E1, E2),
    (E1, E2, E3),
    (E1, E2, E3, E4),
    (E1, E2, E3, E4, E5),
    (E1, E2, E3, E4, E5, E6),
    (E1, E2, E3, E4, E5, E6, E7),
    (E1, E2, E3, E4, E5, E6, E7, E8),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16),
];
