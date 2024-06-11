use std::{
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};

use commitlog::server::eventstore::event_store_client::EventStoreClient;
use kameo_es::{
    command_service::{CommandService, Execute, ExecuteExt, PrepareStream},
    event_store::new_event_store,
};
use serde_json::json;
use tracing_subscriber::EnvFilter;

use bank_account::{BankAccount, Deposit};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("kameo=warn".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    // let message_store = MessageStore::connect(
    //     "postgresql://postgres:postgres@localhost:5433/message_store?options=-c%20search_path%3Dmessage_store",
    // )
    // .await?;
    let client = EventStoreClient::connect("http://[::1]:50051").await?;
    let event_store = new_event_store(client, 8);
    // 8 = 31501 ms
    // 16 = 31238 ms
    // 32 = 31079 ms
    // 64 = 31465 ms

    // LOL with my own event store, I got 6376 ms at 32 workers!

    // 4,938 ms
    // 4,460 ms
    // 5,212 ms
    // 6,059 ms
    // 7,194 ms
    // 8,435 ms
    //

    // 3,189 ms
    // 4,001 ms
    // 4,754 ms
    // 5,876 ms
    // 6,762 ms
    // 7,550 ms
    // RESTARTED SERVER
    // 8,073 ms
    // 9,134 ms

    // 3,432
    // 3,134
    // 3,484
    // 3,357
    // 3,244
    // 3,310
    // 3,790
    // 7,031
    // 3,346
    // 3

    let cmd_service = kameo::spawn(CommandService::new(event_store));
    let start = Instant::now();
    cmd_service
        .ask(PrepareStream {
            id: "def".into(),
            phantom: PhantomData::<BankAccount>,
        })
        .send()
        .await?;
    println!("{} ms", start.elapsed().as_millis());

    // loop {
    //     let start = Instant::now();
    //     for _ in 0..10_000 {
    //         let res = BankAccount::execute(
    //             &cmd_service,
    //             Execute::new("def", Deposit { amount: 10_000 }).metadata(json!({ "hi": "there" })),
    //         )
    //         .await;
    //         if let Err(err) = res {
    //             println!("{err}");
    //         }
    //     }
    //     println!("{} ms", start.elapsed().as_millis());
    //     // tokio::time::sleep(Duration::from_millis(5000)).await;
    // }

    Ok(())
}
