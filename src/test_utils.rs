use std::fmt;

use crate::{Apply, Command, Entity};

pub trait GivenEntity: Entity + Apply {
    fn given(events: impl Into<Vec<Self::Event>>) -> Given<Self>;
}

impl<E> GivenEntity for E
where
    E: Entity + Apply,
{
    fn given(events: impl Into<Vec<Self::Event>>) -> Given<Self> {
        Given {
            entity: Self::default(),
            events: events.into(),
        }
    }
}

pub struct Given<E>
where
    E: Entity,
{
    entity: E,
    events: Vec<E::Event>,
}

impl<E> Given<E>
where
    E: Entity + Apply,
{
    pub fn when<C>(mut self, cmd: C) -> When<E, C>
    where
        E: Entity + Command<C> + Apply,
    {
        for event in self.events {
            self.entity.apply(event);
        }

        When {
            entity: self.entity,
            cmd,
        }
    }
}

pub struct When<E, C>
where
    E: Entity + Command<C>,
{
    entity: E,
    cmd: C,
}

impl<E, C> When<E, C>
where
    E: Entity + Command<C>,
{
    pub fn then(self, events: impl Into<Vec<E::Event>>) -> Given<E>
    where
        E::Event: fmt::Debug + PartialEq<E::Event>,
    {
        let true_events = self
            .entity
            .handle(self.cmd)
            .expect("expected command to succeed");
        let expected_events: Vec<_> = events.into();
        assert_eq!(&true_events, &expected_events, "wrong events returned");

        Given {
            entity: self.entity,
            events: true_events,
        }
    }

    pub fn then_error(self, err: <E as Command<C>>::Error) -> Given<E>
    where
        E::Event: fmt::Debug,
        E::Error: fmt::Debug + PartialEq<E::Error>,
    {
        let true_err = self
            .entity
            .handle(self.cmd)
            .expect_err("expected command to return an error");
        assert_eq!(true_err, err);

        Given {
            entity: self.entity,
            events: Vec::new(),
        }
    }

    pub fn and_then<F>(self, f: F) -> Given<E>
    where
        F: FnOnce(Vec<E::Event>),
    {
        let events = self
            .entity
            .handle(self.cmd)
            .expect("expected command to succeed");
        f(events.clone());

        Given {
            entity: self.entity,
            events,
        }
    }
}
