use std::time::Instant;

use kameo_es::{event_store::new_event_store, CommandService, Execute, ExecuteExt};
use message_db::database::MessageStore;
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

    let message_store = MessageStore::connect(
        "postgresql://postgres:postgres@localhost:5433/message_store?options=-c%20search_path%3Dmessage_store",
    )
    .await?;
    let event_store = new_event_store(message_store, 32);
    // 8 = 31501 ms
    // 16 = 31238 ms
    // 32 = 31079 ms
    // 64 = 31465 ms

    let cmd_service = kameo::spawn(CommandService::new(event_store));

    let start = Instant::now();
    for _ in 0..20_000 {
        let res = BankAccount::execute(
            &cmd_service,
            Execute::new("123", Deposit { amount: 10_000 }).metadata(json!({ "hi": "there" })),
        )
        .await;
        if let Err(err) = res {
            println!("{err}");
        }
        // tokio::time::sleep(Duration::from_millis(1000)).await;
    }
    println!("{} ms", start.elapsed().as_millis());

    Ok(())
}
