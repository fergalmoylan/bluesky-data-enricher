use std::io::{Error, ErrorKind};
use log::error;
use rdkafka::Message;
use rdkafka::message::OwnedMessage;
use rust_bert::pipelines::keywords_extraction::{Keyword, KeywordExtractionModel};
use rust_bert::RustBertError;
use serde::{Deserialize, Serialize};
use vader_sentimental::SentimentIntensityAnalyzer;

pub(crate) struct RustBertModels<'a> {
    keyword_model: KeywordExtractionModel<'a>,
}

impl RustBertModels<'_> {
    pub fn new() -> Self {
        let keyword_model = KeywordExtractionModel::new(Default::default())
            .expect("Failed to create KeywordExtractionModel");
        Self {
            keyword_model
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct EnrichedRecord {
    created_at: String,
    text: String,
    languages: Vec<String>,
    hashtags: Vec<String>,
    urls: Vec<String>,
    hostnames: Vec<String>,
    sentiment: Option<Vec<String>>,
    keywords: Option<Vec<String>>,
}

impl EnrichedRecord {

    fn calculate_keywords(text: &str, model: &KeywordExtractionModel) -> Option<Vec<String>> {
        let input = [text];
        let output: Result<Vec<Vec<Keyword>>, RustBertError> = model.predict(&input);
        match output {
            Ok(keywords) => {
                if let Some(inner_vec) = keywords.first() {
                    Some(
                        inner_vec
                            .iter()
                            .filter(|keyword| keyword.score >= 0.3999)
                            .map(|keyword| keyword.text.clone())
                            .collect()
                    )
                } else {
                    Some(Vec::new())
                }
            }
            Err(_) => Some(Vec::new())
        }
    }

    fn calculate_sentiment(text: &str, model: SentimentIntensityAnalyzer) -> Option<Vec<String>> {
        let input = text;
        let output = model.polarity_scores(input);
        let sentiment = if output.compound >= 0.05 {
            "Positive"
        } else if output.compound <= -0.05 {
            "Negative"
        } else {
            "Neutral"
        };

        Some(vec![sentiment.to_string()])
    }

    pub fn enrich_record(record: OwnedMessage, models: &RustBertModels) -> Result<EnrichedRecord, Error> {
        let keyword_model = &models.keyword_model;
        let sentiment_analyzer = SentimentIntensityAnalyzer::new();
        if let Some(payload) = record.payload() {
            match serde_json::from_slice::<EnrichedRecord>(payload) {
                Ok(mut record) => {
                    if record.languages.contains(&String::from("English")) {
                        let text = record.text.clone();
                        record.sentiment = Self::calculate_sentiment(&text, sentiment_analyzer);
                        record.keywords = Self::calculate_keywords(text.as_str(), keyword_model);
                        match serde_json::to_vec(&record) {
                            Ok(_) => {
                                Ok(record)
                            },
                            Err(e) => {
                                error!("Error serializing record: {}", e);
                                Err(Error::from(e))
                            },
                        }
                    } else {
                        record.sentiment = Some(Vec::new());
                        record.keywords = Some(Vec::new());
                        Ok(record)
                    }
                },
                Err(e) => {
                    error!("Error deserializing record: {}", e);
                    Err(Error::from(e))
                },
            }
        } else {
            error!("Record missing payload");
            Err(Error::from(ErrorKind::InvalidInput))
        }
    }
}
