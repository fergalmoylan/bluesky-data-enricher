use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub kafka_addresses: Vec<String>,
    pub kafka_topic: String,
}

impl Config {
    pub fn from_env() -> Self {
        dotenv::dotenv().ok();

        let kafka_addresses = env::var("KAFKA_ADDRESSES")
            .expect("KAFKA_ADDRESSES is not set")
            .split(',')
            .map(String::from)
            .collect();

        let kafka_topic = env::var("KAFKA_TOPIC").expect("KAFKA_TOPIC is not set");

        Config {
            kafka_addresses,
            kafka_topic,
        }
    }
}