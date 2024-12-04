use log::info;
use futures::stream::TryStreamExt;
use rdkafka::error::KafkaError;
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::producer::FutureProducer;
use crate::app_metrics::KAFKA_LATENCY;
use crate::config::Config;
use crate::enricher::{EnrichedRecord, RustBertModels};
use crate::producer;

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
        .set_log_level(RDKafkaLogLevel::Error)
        .create()
        .expect("Consumer creation failed")
}

pub async fn consume_records(config: &Config, consumer: StreamConsumer, producer: FutureProducer, models: &RustBertModels<'_>) {
    consumer
        .subscribe(&[&config.kafka_topic])
        .expect("Can't subscribe to specified topics");

    let consumer_stream = consumer.stream().try_for_each(|borrowed_message| {
        let producer = producer.clone();
        async move {
            let owned_message = borrowed_message.detach();
            let timer = KAFKA_LATENCY.start_timer();
            let enriched_record = EnrichedRecord::enrich_record(owned_message, models).unwrap();
            producer::send_to_kafka(&producer, config, enriched_record).await;
            timer.observe_duration();
            Ok::<(), KafkaError>(())
        }
    });

    info!("Starting event loop");
    consumer_stream.await.expect("Stream processing failed.");
    info!("Stream processing terminated");
}