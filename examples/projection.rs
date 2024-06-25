use eventus::server::eventstore::{
    event_store_client::EventStoreClient, subscribe_request::StartFrom, AcknowledgeRequest,
};
use futures::StreamExt;
use kameo_es::subscribe;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = EventStoreClient::connect("http://[::1]:9220").await?;

    let subscriber_id = "example_projection".to_string();
    let mut stream = subscribe(
        client.clone(),
        StartFrom::SubscriberId(subscriber_id.clone()),
    )
    .await?;
    while let Some(batch) = stream.next().await {
        for event in batch? {
            // Handle the event
            println!("{} - {}", event.id, event.event_name);

            // Acknowledge the event, updating the subscriber last event
            //
            // Acknowledging events could be done after the batch of events, or even
            // deferred and throttled for better performance using a separate tokio task
            // and tokio channels, but for simplicity sake here we just acknowledge every
            // event one by one.
            client
                .acknowledge(AcknowledgeRequest {
                    subscriber_id: subscriber_id.clone(),
                    last_event_id: event.id,
                })
                .await?;
        }
    }

    Ok(())
}
