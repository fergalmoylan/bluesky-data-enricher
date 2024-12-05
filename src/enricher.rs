use std::io::{Error, ErrorKind};
use log::{error, info};
use rdkafka::Message;
use rdkafka::message::OwnedMessage;
use rust_bert::pipelines::keywords_extraction::{Keyword, KeywordExtractionModel};
use rust_bert::pipelines::ner::NERModel;
use rust_bert::pipelines::sentiment::{SentimentModel, SentimentPolarity};
use rust_bert::RustBertError;
use serde::{Deserialize, Serialize};

pub(crate) struct RustBertModels<'a> {
    sentiment_model: SentimentModel,
    ner_model: NERModel,
    keyword_model: KeywordExtractionModel<'a>,
}

impl RustBertModels<'_> {
    pub fn new() -> Self {
        let sentiment_model = SentimentModel::new(Default::default())
            .expect("Failed to create SentimentModel");
        let ner_model = NERModel::new(Default::default())
            .expect("Failed to create NERModel");
        let keyword_model = KeywordExtractionModel::new(Default::default())
            .expect("Failed to create KeywordExtractionModel");
        Self {
            sentiment_model,
            ner_model,
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
    sentiment: Option<String>,
    named_entities: Option<Vec<String>>,
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

    fn calculate_ner(text: &str, model: &NERModel) -> Option<Vec<String>> {
        let input = [text];
        let output = model.predict(&input);

        if let Some(entities) = output.first() {
            Some(
                entities
                    .iter()
                    .map(|entity| entity.word.clone())
                    .collect(),
            )
        } else {
            error!("No NER data available.");
            None
        }
    }

    fn calculate_sentiment(text: &str, model: &SentimentModel) -> Option<String> {
        let input = [text];
        let output = model.predict(input);

        if let Some(sentiment) = output.first() {
            match sentiment.polarity {
                SentimentPolarity::Positive => Some("Positive".to_string()),
                SentimentPolarity::Negative => Some("Negative".to_string()),
            }
        } else {
            error!("No sentiment data available.");
            None
        }
    }

    pub fn enrich_record(record: OwnedMessage, models: &RustBertModels) -> Result<EnrichedRecord, Error> {
        let sentiment_model = &models.sentiment_model;
        let ner_model = &models.ner_model;
        let keyword_model = &models.keyword_model;
        if let Some(payload) = record.payload() {
            match serde_json::from_slice::<EnrichedRecord>(payload) {
                Ok(mut record) => {
                    if record.languages.contains(&String::from("English")) {
                        let text = record.text.clone();
                        record.sentiment = Self::calculate_sentiment(&text, sentiment_model);
                        //record.named_entities = Self::calculate_ner(&text, ner_model);
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
