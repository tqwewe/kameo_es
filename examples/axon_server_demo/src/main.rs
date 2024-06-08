use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use dendrite::{
    axon_server::{
        command::command_service_client::CommandServiceClient,
        common::MetaDataValue,
        event::{event_store_client::EventStoreClient, Event},
    },
    axon_utils::{
        command_worker, wait_for_server, AggregateRegistry, SerializedObject, TheAggregateRegistry,
    },
};
use synapse_client::apis::{
    aggregate_api::{self, read_aggregate_events},
    configuration::Configuration,
    events_api::publish_event,
};
use tokio_postgres::{Error, NoTls};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("kameo=warn".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    // let (client, connection) = tokio_postgres::connect(
    //     "host=localhost user=postgres password=mysecretpassword",
    //     NoTls,
    // )
    // .await?;

    // tokio::spawn(async move {
    //     if let Err(e) = connection.await {
    //         eprintln!("connection error: {}", e);
    //     }
    // });

    // loop {
    //     let start = Instant::now();
    //     let rows = client.query("SELECT $1::TEXT", &[&"hello world"]).await?;
    //     println!("{}", start.elapsed().as_millis());
    //     tokio::time::sleep(Duration::from_millis(200)).await;
    // }

    // let mut configuration = Configuration::new();

    // publish_event(, , , , , , , , )

    // loop {
    //     // configuration.base_path = "http://0.0.0.0:8124".to_string();
    //     let start = Instant::now();
    //     let foo = read_aggregate_events(&configuration, "default", "123").await?;
    //     println!("{}", start.elapsed().as_millis());
    //     tokio::time::sleep(Duration::from_millis(200)).await;
    // }

    // tokio::spawn(async move {
    //    let cmd_client = CommandServiceClient::connect("http://0.0.0.0:8124").await?;
    //    cmd_client.dispatch()
    // });

    // let axon_server_handle = wait_for_server("0.0.0.0", 8124, "API").await?;
    // let mut aggregate_registry = TheAggregateRegistry { handlers: HashMap::new() };
    // AggregateHandl
    // aggregate_registry.register()
    // command_worker(axon_server_handle, , )

    let mut client = EventStoreClient::connect("http://0.0.0.0:8124").await?;
    client
        .append_event(tokio_stream::iter(vec![Event {
            message_identifier: Uuid::new_v4().to_string(),
            aggregate_identifier: "123".to_string(),
            aggregate_sequence_number: 2,
            aggregate_type: "bank_account".to_string(),
            timestamp: 1714999388185,
            payload: Some(SerializedObject {
                r#type: "MoneyDeposited".to_string(),
                revision: "v1".to_string(),
                data: serde_json::to_vec(&serde_json::json!({
                    "amount": 200,
                }))?,
            }),
            meta_data: HashMap::from_iter([(
                "my_key".to_string(),
                MetaDataValue {
                    data: Some(
                        dendrite::axon_server::common::meta_data_value::Data::TextValue(
                            "howdy".to_string(),
                        ),
                    ),
                },
            )]),
            snapshot: false,
        }]))
        .await?;
    println!("all g!");

    Ok(())
}
