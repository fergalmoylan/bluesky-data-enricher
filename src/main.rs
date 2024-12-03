mod config;
mod app_metrics;
mod consumer;

use std::time::Duration;
use dotenv::dotenv;
use log::{info, warn};
use tokio::time;
use crate::app_metrics::gather_metrics;
use crate::config::Config;

#[tokio::main]
async fn main() {
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
    consumer::consume_and_print(&config, consumer).await;
}