mod config;
mod app_metrics;
mod consumer;
mod enricher;
mod producer;

use std::time::Duration;
use dotenv::dotenv;
use log::info;
use tokio::time;
use crate::app_metrics::gather_metrics;
use crate::config::Config;
use crate::enricher::RustBertModels;

#[tokio::main]
async fn run(models: RustBertModels) {
    dotenv().ok();
    env_logger::init();
    let config = Config::from_env();
    info!("Running with config: {:#?}", &config);
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(10));
        let mut prev_count = 0.0;
        let mut prev_time = 0.0;
        loop {
            interval.tick().await;
            (prev_count, prev_time) = gather_metrics(&prev_count, &prev_time).await;
        }
    });
    let consumer = consumer::initialise_consumer(&config);
    let producer = producer::initialise_producer(&config);
    consumer::consume_records(&config, consumer, producer, &models).await;
}

fn main() {
    let models = RustBertModels::new();
    run(models);
}