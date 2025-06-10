use std::time::Duration;

use anyhow::bail;
use eventus::server::{eventstore::event_store_client::EventStoreClient, ClientAuthInterceptor};
use futures::FutureExt;
use kameo_es::{
    command_service::{CommandService, ExecuteExt},
    transaction::{Transaction, TransactionOutcome},
    Apply, Command, Context, Entity, EventType, Metadata,
};

use serde::{Deserialize, Serialize};
use tonic::transport::Channel;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let channel = Channel::builder("http://[::1]:9220".parse()?)
        .connect()
        .await?;
    let client =
        EventStoreClient::with_interceptor(channel, ClientAuthInterceptor::new("localhost")?);

    // let mut events = client
    //     .get_events(GetEventsRequest {
    //         start_event_id: 0,
    //         batch_size: 1024 * 64,
    //         limit: None,
    //     })
    //     .await?
    //     .into_inner();
    // while let Some(batch) = events.try_next().await? {
    //     for event in batch.events {
    //         let event = Event::<GenericValue, GenericValue>::try_from(event).unwrap();
    //         // dbg!(event);
    //     }
    // }

    let mut cmd_service = CommandService::new(client);

    for _ in 0..2 {
        // BankAccount::execute(&cmd_service, "abc".to_string(), Deposit { amount: 10_000 }).await?;

        // let barrier = Arc::new(Barrier::new(2));

        tokio::spawn({
            let cmd_service = cmd_service.clone();
            // let barrier = barrier.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                BankAccount::execute(&cmd_service, "abc".to_string(), Deposit { amount: 5_000 })
                    .await
                    .unwrap();
                // barrier.wait().await;
            }
        });

        // abc - 1,000
        // abc - 2,000
        // xyz - 3,000
        // abc - 5,000

        // abc - 1,000
        // abc - 2,000
        // xyz - 3,000
        // abc - 5,000

        cmd_service
            .transaction::<(), ()>(|mut tx: Transaction<'_>| {
                // let barrier = barrier.clone();
                async move {
                    BankAccount::execute(&mut tx, "abc".to_string(), Deposit { amount: 1_000 })
                        .await?;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    BankAccount::execute(&mut tx, "abc".to_string(), Deposit { amount: 2_000 })
                        .await?;
                    BankAccount::execute(&mut tx, "xyz".to_string(), Deposit { amount: 3_000 })
                        .await?;

                    // BankAccount::execute(&mut *tx, "abc".to_string(), Deposit { amount: 2_500 }).await;
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
                    // mem::drop(tx);
                    // tokio::time::sleep(Duration::from_millis(10)).await;

                    // let tx = cmd_service.transaction();

                    // BankAccount::execute(&cmd_service, "abc".to_string(), Deposit { amount: 2_500 })
                    //     .transaction(&tx)
                    //     .await?;
                    // BankAccount::execute(&cmd_service, "abc".to_string(), Deposit { amount: 2_500 })
                    //     .transaction(&tx)
                    //     .await?;

                    // BankAccount::execute(&cmd_service, "def".to_string(), Deposit { amount: 2_500 })
                    //     .transaction(&tx)
                    //     .await?;

                    // tx.commit().await.context("commit transaction")?;
                    Ok(TransactionOutcome::Commit(()))
                }
                .boxed()
            })
            .await?;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

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

#[derive(Clone, Debug, EventType, Serialize, Deserialize)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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
