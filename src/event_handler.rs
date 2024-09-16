use std::{
    cell::OnceCell,
    marker::PhantomData,
    pin,
    task::{self, ready},
    time::Duration,
};

use eventus::server::{
    eventstore::{
        event_store_client::EventStoreClient, subscribe_request::StartFrom, AcknowledgeRequest,
        EventBatch, SubscribeRequest,
    },
    ClientAuthInterceptor,
};
use futures::{Future, Stream, StreamExt};
use kameo::{
    actor::{ActorRef, WeakActorRef},
    error::{ActorStopReason, BoxError, PanicError, SendError},
    mailbox::bounded::BoundedMailbox,
    messages,
    request::MessageSend,
    Actor,
};
use thiserror::Error;
use tokio_util::sync::ReusableBoxFuture;
use tonic::{
    service::interceptor::InterceptedService, transport::Channel, Code, Status, Streaming,
};

use crate::{Entity, Event};

pub enum Acknowledgement {
    /// Saves the last handled event id in eventus.
    Eventus { subscriber_id: String },
    /// The last handled event id is stored manually.
    Manual,
}

pub trait EventHandlerBehaviour: Send {
    type Error: Send;

    /// Where to start streaming events from.
    fn start_from(&self) -> impl Future<Output = Result<StartFrom, Self::Error>> + Send;

    /// Fallback when no entities were matched.
    fn fallback(
        &mut self,
        event: Event,
    ) -> impl Future<Output = Result<Acknowledgement, Self::Error>> + Send;

    /// How often to sync the last handled event id with eventus.
    ///
    /// Defaults to every 2 seconds.
    fn acknowledge_eventus_interval() -> Duration {
        Duration::from_secs(2)
    }
}

pub trait EventHandler<E>: EventHandlerBehaviour
where
    E: Entity,
{
    fn handle(
        &mut self,
        id: E::ID,
        event: Event<E::Event, E::Metadata>,
    ) -> impl Future<Output = Result<Acknowledgement, Self::Error>> + Send;
}

pub trait CompositeEventHandler<E>: EventHandlerBehaviour {
    fn handle(
        &mut self,
        event: Event,
    ) -> impl Future<Output = Result<Acknowledgement, EventHandlerError<Self::Error>>> + Send;
}

impl<T> CompositeEventHandler<()> for T
where
    T: EventHandlerBehaviour,
{
    async fn handle(
        &mut self,
        event: Event,
    ) -> Result<Acknowledgement, EventHandlerError<Self::Error>> {
        self.fallback(event)
            .await
            .map_err(EventHandlerError::Handler)
    }
}

impl<T, E1> CompositeEventHandler<(E1,)> for T
where
    T: EventHandlerBehaviour + EventHandler<E1>,
    E1: Entity,
{
    async fn handle(
        &mut self,
        event: Event,
    ) -> Result<Acknowledgement, EventHandlerError<Self::Error>> {
        if event.stream_id.category() == E1::name() {
            return EventHandler::<E1>::handle(
                self,
                event.entity_id::<E1>().map_err(|_| {
                    EventHandlerError::ParseID(event.stream_id.cardinal_id().to_string())
                })?,
                event.as_entity::<E1>().map_err(|(event, err)| {
                    EventHandlerError::DeserializeEvent {
                        entity: E1::name(),
                        event: event.name,
                        err,
                    }
                })?,
            )
            .await
            .map_err(EventHandlerError::Handler);
        }

        self.fallback(event)
            .await
            .map_err(EventHandlerError::Handler)
    }
}

impl<T, E1, E2> CompositeEventHandler<(E1, E2)> for T
where
    T: EventHandlerBehaviour,
    T: EventHandler<E1>,
    E1: Entity,
    T: EventHandler<E2>,
    E2: Entity,
{
    async fn handle(
        &mut self,
        event: Event,
    ) -> Result<Acknowledgement, EventHandlerError<Self::Error>> {
        if event.stream_id.category() == E1::name() {
            return EventHandler::<E1>::handle(
                self,
                event.entity_id::<E1>().map_err(|_| {
                    EventHandlerError::ParseID(event.stream_id.cardinal_id().to_string())
                })?,
                event.as_entity::<E1>().map_err(|(event, err)| {
                    EventHandlerError::DeserializeEvent {
                        entity: E1::name(),
                        event: event.name,
                        err,
                    }
                })?,
            )
            .await
            .map_err(EventHandlerError::Handler);
        } else if event.stream_id.category() == E2::name() {
            return EventHandler::<E2>::handle(
                self,
                event.entity_id::<E2>().map_err(|_| {
                    EventHandlerError::ParseID(event.stream_id.cardinal_id().to_string())
                })?,
                event.as_entity::<E2>().map_err(|(event, err)| {
                    EventHandlerError::DeserializeEvent {
                        entity: E2::name(),
                        event: event.name,
                        err,
                    }
                })?,
            )
            .await
            .map_err(EventHandlerError::Handler);
        }

        self.fallback(event)
            .await
            .map_err(EventHandlerError::Handler)
    }
}

impl<T, E1, E2, E3> CompositeEventHandler<(E1, E2, E3)> for T
where
    T: EventHandlerBehaviour,
    T: EventHandler<E1>,
    E1: Entity,
    T: EventHandler<E2>,
    E2: Entity,
    T: EventHandler<E3>,
    E3: Entity,
{
    async fn handle(
        &mut self,
        event: Event,
    ) -> Result<Acknowledgement, EventHandlerError<Self::Error>> {
        if event.stream_id.category() == E1::name() {
            return EventHandler::<E1>::handle(
                self,
                event.entity_id::<E1>().map_err(|_| {
                    EventHandlerError::ParseID(event.stream_id.cardinal_id().to_string())
                })?,
                event.as_entity::<E1>().map_err(|(event, err)| {
                    EventHandlerError::DeserializeEvent {
                        entity: E1::name(),
                        event: event.name,
                        err,
                    }
                })?,
            )
            .await
            .map_err(EventHandlerError::Handler);
        } else if event.stream_id.category() == E2::name() {
            return EventHandler::<E2>::handle(
                self,
                event.entity_id::<E2>().map_err(|_| {
                    EventHandlerError::ParseID(event.stream_id.cardinal_id().to_string())
                })?,
                event.as_entity::<E2>().map_err(|(event, err)| {
                    EventHandlerError::DeserializeEvent {
                        entity: E2::name(),
                        event: event.name,
                        err,
                    }
                })?,
            )
            .await
            .map_err(EventHandlerError::Handler);
        } else if event.stream_id.category() == E3::name() {
            return EventHandler::<E3>::handle(
                self,
                event.entity_id::<E3>().map_err(|_| {
                    EventHandlerError::ParseID(event.stream_id.cardinal_id().to_string())
                })?,
                event.as_entity::<E3>().map_err(|(event, err)| {
                    EventHandlerError::DeserializeEvent {
                        entity: E3::name(),
                        event: event.name,
                        err,
                    }
                })?,
            )
            .await
            .map_err(EventHandlerError::Handler);
        }

        self.fallback(event)
            .await
            .map_err(EventHandlerError::Handler)
    }
}

#[derive(Debug, Error)]
pub enum EventHandlerError<E> {
    #[error(transparent)]
    AcknowledgeFailed(#[from] SendError<Acknowledge>),
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
    Handler(E),
}

pub struct EventHandlerStream<E, T>
where
    T: EventHandlerBehaviour + CompositeEventHandler<E>,
{
    pub client: EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
    pub state: T,
    acknowledger: OnceCell<ActorRef<Acknowledger>>,
    stream: Streaming<EventBatch>,
    phantom: PhantomData<E>,
}

impl<E, T> EventHandlerStream<E, T>
where
    T: EventHandlerBehaviour + CompositeEventHandler<E>,
{
    pub async fn new(
        mut client: EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
        state: T,
    ) -> Result<Self, EventHandlerError<T::Error>> {
        let acknowledger = OnceCell::new();

        let stream = client
            .subscribe(SubscribeRequest {
                start_from: Some(
                    state
                        .start_from()
                        .await
                        .map_err(EventHandlerError::Handler)?,
                ),
            })
            .await?
            .into_inner();

        Ok(EventHandlerStream {
            client,
            state,
            acknowledger,
            stream,
            phantom: PhantomData,
        })
    }

    pub async fn next(&mut self) -> Option<Result<EventHandlerBatch<'_, E, T>, Status>> {
        Some(
            self.stream
                .next()
                .await?
                .map(|batch| EventHandlerBatch { batch, state: self }),
        )
    }
}

pub struct EventHandlerBatch<'a, E, T>
where
    T: EventHandlerBehaviour + CompositeEventHandler<E>,
{
    batch: EventBatch,
    state: &'a mut EventHandlerStream<E, T>,
}

impl<'a, E, T> EventHandlerBatch<'a, E, T>
where
    T: EventHandlerBehaviour + CompositeEventHandler<E>,
{
    pub async fn process(self) -> Result<(), EventHandlerError<T::Error>> {
        let EventBatch { events } = self.batch;
        for event in events {
            let event = Event::try_from(event).map_err(|_| {
                EventHandlerError::Grpc(Status::new(Code::Internal, "invalid timestamp received"))
            })?;
            let event_id = event.id;
            let ack = self.state.state.handle(event).await?;
            match ack {
                Acknowledgement::Eventus { subscriber_id } => {
                    let client = self.state.client.clone();
                    self.state
                        .acknowledger
                        .get_or_init(move || {
                            kameo::spawn(Acknowledger {
                                client,
                                subscriber_id,
                                last_event_id: 0,
                                dirty: false,
                            })
                        })
                        .tell(Acknowledge { event_id })
                        .send()
                        .await?;
                }
                Acknowledgement::Manual => {}
            }
        }

        Ok(())
    }
}

pub struct EventHandlerStreamOld<E, T>
where
    T: EventHandlerBehaviour + CompositeEventHandler<E>,
{
    fut: Option<
        ReusableBoxFuture<
            'static,
            Option<Result<EventHandlerStreamState<T>, EventHandlerError<T::Error>>>,
        >,
    >,
    phantom: PhantomData<E>,
}

impl<E, T> EventHandlerStreamOld<E, T>
where
    T: EventHandlerBehaviour + CompositeEventHandler<E> + 'static,
    E: Unpin + 'static,
{
    pub async fn new(
        mut client: EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
        state: T,
    ) -> Result<Self, EventHandlerError<T::Error>> {
        let acknowledger = OnceCell::new();

        let stream = client
            .subscribe(SubscribeRequest {
                start_from: Some(
                    state
                        .start_from()
                        .await
                        .map_err(EventHandlerError::Handler)?,
                ),
            })
            .await?
            .into_inner();

        Ok(EventHandlerStreamOld {
            fut: Some(ReusableBoxFuture::new(handle_next_event(
                EventHandlerStreamState {
                    client,
                    state,
                    acknowledger,
                    stream,
                },
            ))),
            phantom: PhantomData,
        })
    }
}

impl<E, T> Stream for EventHandlerStreamOld<E, T>
where
    T: EventHandlerBehaviour + CompositeEventHandler<E> + 'static,
    E: Unpin + 'static,
{
    type Item = Result<(), EventHandlerError<T::Error>>;

    fn poll_next(
        self: pin::Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match &mut this.fut {
            Some(fut) => match ready!(fut.poll(cx)) {
                Some(Ok(state)) => {
                    fut.set(handle_next_event(state));
                    return task::Poll::Ready(Some(Ok(())));
                }
                Some(Err(err)) => {
                    this.fut = None;
                    return task::Poll::Ready(Some(Err(err)));
                }
                None => {
                    return task::Poll::Ready(None);
                }
            },
            None => return task::Poll::Ready(None),
        }
    }
}

pub struct EventHandlerStreamState<T> {
    client: EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
    state: T,
    acknowledger: OnceCell<ActorRef<Acknowledger>>,
    stream: Streaming<EventBatch>,
}

async fn handle_next_event<E, T>(
    mut state: EventHandlerStreamState<T>,
) -> Option<Result<EventHandlerStreamState<T>, EventHandlerError<T::Error>>>
where
    T: EventHandlerBehaviour + CompositeEventHandler<E>,
{
    let res = state.stream.next().await?;
    Some(handle_next_event_inner(res, state).await)
}

async fn handle_next_event_inner<E, T>(
    res: Result<EventBatch, Status>,
    mut state: EventHandlerStreamState<T>,
) -> Result<EventHandlerStreamState<T>, EventHandlerError<T::Error>>
where
    T: EventHandlerBehaviour + CompositeEventHandler<E>,
{
    let EventBatch { events } = res.map_err(EventHandlerError::Grpc)?;
    for event in events {
        let event = Event::try_from(event).map_err(|_| {
            EventHandlerError::Grpc(Status::new(Code::Internal, "invalid timestamp received"))
        })?;
        let event_id = event.id;
        let ack = state.state.handle(event).await?;
        match ack {
            Acknowledgement::Eventus { subscriber_id } => {
                let client = state.client.clone();
                state
                    .acknowledger
                    .get_or_init(move || {
                        kameo::spawn(Acknowledger {
                            client,
                            subscriber_id,
                            last_event_id: 0,
                            dirty: false,
                        })
                    })
                    .tell(Acknowledge { event_id })
                    .send()
                    .await?;
            }
            Acknowledgement::Manual => {}
        }
    }

    Ok(state)
}

pub async fn start_event_handler<E, T>(
    mut client: EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
    mut state: T,
) -> Result<(), EventHandlerError<T::Error>>
where
    T: EventHandlerBehaviour + CompositeEventHandler<E>,
{
    let acknowledger = OnceCell::new();

    let mut stream = client
        .subscribe(SubscribeRequest {
            start_from: Some(
                state
                    .start_from()
                    .await
                    .map_err(EventHandlerError::Handler)?,
            ),
        })
        .await?
        .into_inner();
    while let Some(res) = stream.next().await {
        let EventBatch { events } = res.map_err(EventHandlerError::Grpc)?;
        for event in events {
            let event = Event::try_from(event).map_err(|_| {
                EventHandlerError::Grpc(Status::new(Code::Internal, "invalid timestamp received"))
            })?;
            let event_id = event.id;
            let ack = state.handle(event).await?;
            match ack {
                Acknowledgement::Eventus { subscriber_id } => {
                    let client = client.clone();
                    acknowledger
                        .get_or_init(move || {
                            kameo::spawn(Acknowledger {
                                client,
                                subscriber_id,
                                last_event_id: 0,
                                dirty: false,
                            })
                        })
                        .tell(Acknowledge { event_id })
                        .send()
                        .await?;
                }
                Acknowledgement::Manual => {}
            }
        }
    }

    Ok(())
}

struct Acknowledger {
    client: EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
    subscriber_id: String,
    last_event_id: u64,
    dirty: bool,
}

impl Actor for Acknowledger {
    type Mailbox = BoundedMailbox<Self>;

    async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let res = actor_ref.tell(Flush).send().await;
                if let Err(SendError::ActorNotRunning(_)) = res {
                    return;
                }
            }
        });

        Ok(())
    }

    async fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _err: PanicError,
    ) -> Result<Option<ActorStopReason>, BoxError> {
        Ok(None)
    }
}

#[messages]
impl Acknowledger {
    #[message(derive(Clone))]
    pub fn acknowledge(&mut self, event_id: u64) {
        self.last_event_id = event_id;
        self.dirty = true;
    }

    #[message]
    async fn flush(&mut self) -> Result<(), Status> {
        if self.dirty {
            self.client
                .acknowledge(AcknowledgeRequest {
                    subscriber_id: self.subscriber_id.clone(),
                    last_event_id: self.last_event_id,
                })
                .await?;
            self.dirty = false;
        }
        Ok(())
    }
}
