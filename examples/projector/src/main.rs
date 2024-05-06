use bank_account::{BankAccount, BankAccountEvent};
use futures::StreamExt;
use kameo_es::Entity;
use message_db::database::{MessageStore, SubscribeToCategoryOpts};
use serde_json::Value;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("kameo=trace".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    let  message_store = MessageStore::connect(
        "postgresql://postgres:postgres@localhost:5433/message_store?options=-c%20search_path%3Dmessage_store",
    )
    .await?;

    let mut stream = MessageStore::subscribe_to_category::<BankAccountEvent, Value, _>(
        &message_store,
        BankAccount::category(),
        &SubscribeToCategoryOpts::builder()
            .identifier("testo")
            .position_update_interval(1)
            .build(),
    )
    .await?;
    while let Some(res) = stream.next().await {
        let messages = res?;
        for message in messages {
            println!("{}: {}", message.position, message.msg_type);
        }
    }

    Ok(())
}
