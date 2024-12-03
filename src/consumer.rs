use log::{error, info};
use futures::stream::TryStreamExt;
use log::__private_api::Value;
use rdkafka::error::KafkaError;
use rdkafka::{ClientConfig, Message};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, MessageStream};
use crate::app_metrics::KAFKA_LATENCY;
use crate::config::Config;

pub fn initialise_consumer(config: &Config) -> StreamConsumer {
    let binding = config.kafka_addresses.join(",");
    let brokers = binding.as_str();
    let group_id = "enrichment-consumer-1";

    ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        .create()
        .expect("Consumer creation failed")
}

pub async fn consume_and_print(config: &Config, consumer: StreamConsumer) {
    consumer
        .subscribe(&[&config.kafka_topic])
        .expect("Can't subscribe to specified topics");

    let consumer_stream = consumer.stream().try_for_each(|borrowed_message| {
        async move {
            let owned_message = borrowed_message.detach();
            tokio::spawn(async move {
                let timer = KAFKA_LATENCY.start_timer();
                if let Some(payload) = owned_message.payload() {
                    match std::str::from_utf8(payload) {
                        Ok(text) => {
                            info!("Received message: {}", text);
                        },
                        Err(e) => {
                            error!("Error decoding message payload: {}", e);
                        },
                    }
                } else {
                    info!("Received message with empty payload");
                }
                timer.observe_duration();
            });
            Ok::<(), KafkaError>(())
        }
    });

    info!("Starting event loop");
    consumer_stream.await.expect("Stream processing failed.");
    info!("Stream processing terminated");
}