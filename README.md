# bluesky-data-enricher

This application reads from a Kafka topic, enriches the record, and writes the enriched record to a Kafka topic.

The record is enriched in two ways:
1. Rust Bert's keyword extraction model
   - This collects the top keyword's from the record's text field and adds them to a vector
2. Vadar_Sentimental sentiment analysis
   - This performs sentimental analysis on the record's text field and determines whether the sentiment of the text is positive, negative, or neutral

Only records with English text are enriched.