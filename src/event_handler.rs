use std::{cell::OnceCell, time::Duration};

use eventus::server::{
    eventstore::{
        event_store_client::EventStoreClient, subscribe_request::StartFrom, AcknowledgeRequest,
        EventBatch, SubscribeRequest,
    },
    ClientAuthInterceptor,
};
use futures::{Future, StreamExt};
use kameo::{
    actor::{ActorRef, BoundedMailbox, WeakActorRef},
    error::{ActorStopReason, BoxError, PanicError, SendError},
    messages, Actor,
};
use thiserror::Error;
use tonic::{service::interceptor::InterceptedService, transport::Channel, Code, Status};

use crate::{Entity, Event};

pub enum Acknowledgement {
    /// Saves the last handled event id in eventus.
    Eventus { subscriber_id: String },
    /// The last handled event id is stored manually.
    Manual,
}

pub trait EventHandlerBehaviour {
    type Error;

    /// Where to start streaming events from.
    fn start_from(&self) -> impl Future<Output = Result<StartFrom, Self::Error>>;

    /// Fallback when no entities were matched.
    fn fallback(
        &mut self,
        event: Event<()>,
    ) -> impl Future<Output = Result<Acknowledgement, Self::Error>>;

    /// How often to sync the last handled event id with eventus.
    ///
    /// Defaults to every 2 seconds.
    fn acknowledge_eventus_interval() -> Duration {
        Duration::from_secs(2)
    }
}

pub trait EventHandler<E>: EventHandlerBehaviour {
    fn handle(
        &mut self,
        event: Event<E>,
    ) -> impl Future<Output = Result<Acknowledgement, Self::Error>>;
}

impl<T> EventHandler<()> for T
where
    T: EventHandlerBehaviour,
{
    async fn handle(&mut self, event: Event<()>) -> Result<Acknowledgement, Self::Error> {
        self.fallback(event).await
    }
}

impl<T, E1> EventHandler<(E1,)> for T
where
    T: EventHandlerBehaviour + EventHandler<E1>,
    E1: Entity,
{
    async fn handle(&mut self, event: Event<(E1,)>) -> Result<Acknowledgement, Self::Error> {
        if event.stream_id.category() == E1::name() {
            return EventHandler::<E1>::handle(self, event.as_entity()).await;
        }

        self.fallback(event.as_entity()).await
    }
}

impl<T, E1, E2> EventHandler<(E1, E2)> for T
where
    T: EventHandlerBehaviour,
    T: EventHandler<E1>,
    E1: Entity,
    T: EventHandler<E2>,
    E2: Entity,
{
    async fn handle(&mut self, event: Event<(E1, E2)>) -> Result<Acknowledgement, Self::Error> {
        if event.stream_id.category() == E1::name() {
            return EventHandler::<E1>::handle(self, event.as_entity()).await;
        } else if event.stream_id.category() == E2::name() {
            return EventHandler::<E2>::handle(self, event.as_entity()).await;
        }

        self.fallback(event.as_entity()).await
    }
}

impl<T, E1, E2, E3> EventHandler<(E1, E2, E3)> for T
where
    T: EventHandlerBehaviour,
    T: EventHandler<E1>,
    E1: Entity,
    T: EventHandler<E2>,
    E2: Entity,
    T: EventHandler<E3>,
    E3: Entity,
{
    async fn handle(&mut self, event: Event<(E1, E2, E3)>) -> Result<Acknowledgement, Self::Error> {
        if event.stream_id.category() == E1::name() {
            return EventHandler::<E1>::handle(self, event.as_entity()).await;
        } else if event.stream_id.category() == E2::name() {
            return EventHandler::<E2>::handle(self, event.as_entity()).await;
        } else if event.stream_id.category() == E3::name() {
            return EventHandler::<E3>::handle(self, event.as_entity()).await;
        }

        self.fallback(event.as_entity()).await
    }
}

#[derive(Clone, Debug, Error)]
pub enum EventHandlerError<E> {
    #[error(transparent)]
    AcknowledgeFailed(#[from] SendError<Acknowledge>),
    #[error(transparent)]
    Grpc(#[from] Status),
    #[error("{0}")]
    Handler(E),
}

pub async fn start_event_handler<E, T>(
    mut client: EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
    mut state: T,
) -> Result<(), EventHandlerError<T::Error>>
where
    T: EventHandlerBehaviour + EventHandler<E>,
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
            let ack = state
                .handle(event)
                .await
                .map_err(EventHandlerError::Handler)?;
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
