use lazy_static::lazy_static;
use log::info;
use prometheus::core::Number;
use prometheus::register_histogram;
use prometheus::{self, Histogram};

lazy_static! {
    pub static ref KAFKA_LATENCY: Histogram = register_histogram!(
        "records_process_duration_seconds",
        "Time taken to process records from Kafka",
        vec![0.1, 1.0, 5.0]
    )
    .unwrap();
}

pub async fn gather_metrics(prev_count: &f64, prev_time: &f64) -> (f64, f64) {
    let counter = KAFKA_LATENCY.get_sample_count().into_f64();
    let timer = KAFKA_LATENCY.get_sample_sum();
    let avg_latency = (timer - prev_time) / (counter - prev_count) * 1000.0;
    let tps = (counter - prev_count) / 10.0;
    info!(
        "{:<14} {:<10} {:<4} {:<10.1} {:<2} {:<2.2}ms",
        "Total Records:", counter as u64, "TPS:", tps, "Avg Latency:", avg_latency
    );
    (counter, timer)
}
