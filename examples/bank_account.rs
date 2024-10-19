use std::{mem, sync::Arc, time::Duration};

use anyhow::{bail, Context as _};
use eventus::server::{
    eventstore::{event_store_client::EventStoreClient, GetEventsRequest},
    ClientAuthInterceptor,
};
use futures::TryStreamExt;
use kameo_es::{
    command_service::{CommandService, Execute, ExecuteExt},
    Apply, Command, Context, Entity, Event, EventType, GenericValue, Metadata,
};

use serde::{Deserialize, Serialize};
use tokio::sync::Barrier;
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let channel = Channel::builder("http://[::1]:9220".parse()?)
        .connect()
        .await?;
    let mut client =
        EventStoreClient::with_interceptor(channel, ClientAuthInterceptor::new("localhost")?);

    let mut events = client
        .get_events(GetEventsRequest {
            start_event_id: 0,
            batch_size: 1024 * 64,
            limit: None,
        })
        .await?
        .into_inner();
    while let Some(batch) = events.try_next().await? {
        for event in batch.events {
            let event = Event::<GenericValue, GenericValue>::try_from(event).unwrap();
            // dbg!(event);
        }
    }

    let cmd_service = CommandService::new(client);

    for _ in 0..2000 {
        // BankAccount::execute(&cmd_service, "abc".to_string(), Deposit { amount: 10_000 }).await?;

        let tx = cmd_service.transaction();

        BankAccount::execute(&cmd_service, "abc".to_string(), Deposit { amount: 1_000 })
            .transaction(&tx)
            .await?;

        BankAccount::execute(&cmd_service, "abc".to_string(), Deposit { amount: 2_500 })
            .transaction(&tx)
            .await?;
        // let barrier = Arc::new(Barrier::new(2));
        // tokio::spawn({
        //     let barrier = barrier.clone();
        //     let cmd_service = cmd_service.clone();
        //     async move {
        //         BankAccount::execute(&cmd_service, "abc".to_string(), Deposit { amount: 7_000 })
        //             .await
        //             .unwrap();
        //         barrier.wait().await;
        //     }
        // });
        mem::drop(tx);
        // tokio::time::sleep(Duration::from_millis(10)).await;

        let tx = cmd_service.transaction();

        BankAccount::execute(&cmd_service, "abc".to_string(), Deposit { amount: 2_500 })
            .transaction(&tx)
            .await?;
        // BankAccount::execute(&cmd_service, "abc".to_string(), Deposit { amount: 2_500 })
        //     .transaction(&tx)
        //     .await?;

        // BankAccount::execute(&cmd_service, "def".to_string(), Deposit { amount: 2_500 })
        //     .transaction(&tx)
        //     .await?;

        tx.commit().await.context("commit transaction")?;
    }

    // barrier.wait().await;

    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct BankAccount {
    balance: i64,
}

impl Entity for BankAccount {
    type ID = String;
    type Event = BankAccountEvent;
    type Metadata = ();

    fn name() -> &'static str {
        "BankAccount"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BankAccountEvent {
    MoneyWithdrawn { amount: u32 },
    MoneyDeposited { amount: u32 },
}

impl Apply for BankAccount {
    fn apply(&mut self, event: Self::Event, _metadata: Metadata<()>) {
        match event {
            BankAccountEvent::MoneyWithdrawn { amount } => self.balance -= amount as i64,
            BankAccountEvent::MoneyDeposited { amount } => self.balance += amount as i64,
        }
    }
}

impl EventType for BankAccountEvent {
    fn event_type(&self) -> &'static str {
        match self {
            BankAccountEvent::MoneyWithdrawn { .. } => "MoneyWithdrawn",
            BankAccountEvent::MoneyDeposited { .. } => "MoneyDeposited",
        }
    }
}

#[derive(Clone)]
pub struct Withdraw {
    pub amount: u32,
}

impl Command<Withdraw> for BankAccount {
    type Error = anyhow::Error;

    fn handle(
        &self,
        cmd: Withdraw,
        _ctx: Context<'_, Self>,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        if self.balance < cmd.amount as i64 {
            bail!("insufficient balance");
        }

        Ok(vec![BankAccountEvent::MoneyWithdrawn {
            amount: cmd.amount,
        }])
    }
}

#[derive(Clone)]
pub struct Deposit {
    pub amount: u32,
}

impl Command<Deposit> for BankAccount {
    type Error = anyhow::Error;

    fn handle(
        &self,
        cmd: Deposit,
        _ctx: Context<'_, Self>,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        Ok(vec![BankAccountEvent::MoneyDeposited {
            amount: cmd.amount,
        }])
    }
}
