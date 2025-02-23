use std::time::Duration;
use log::error;
use crate::config::Config;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use crate::enricher::EnrichedRecord;

pub fn initialise_producer(config: &Config) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", config.kafka_addresses.join(","))
        .set("message.timeout.ms", "5000")
        .set("linger.ms", "5")
        .set("batch.size", "16384")
        .set("acks", "1")
        .create()
        .expect("Producer creation error")
}


pub async fn send_to_kafka(producer: &FutureProducer, config: &Config, payload: EnrichedRecord) {
    let kafka_topic = config.output_topic.as_str();

    let json_record = tokio::task::spawn_blocking(move || serde_json::to_string(&payload).unwrap())
        .await
        .unwrap();

    let produce_future = producer.send(
        FutureRecord::<(), String>::to(kafka_topic).payload(&json_record),
        Duration::from_secs(0),
    );

    match produce_future.await {
        Ok(..) => {
        }
        Err((e, _)) => error!("Error: {:?}", e),
    }
}
