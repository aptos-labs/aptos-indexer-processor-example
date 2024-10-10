


#[cfg(test)]
mod tests {
    use aptos_indexer_processor_sdk::aptos_indexer_transaction_stream::TransactionStreamConfig;
    use aptos_indexer_test_transactions::IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_;
    use diesel::{Connection};
    use testing_framework::new_test_context::{SdkTestContext, PostgresTestDatabase, TestDatabase};
    use aptos_indexer_processor_example::schema::events::dsl::*;
    use aptos_indexer_processor_example::db::queryable_models::Event;
    use diesel::{pg::PgConnection, RunQueryDsl};
    use url::Url;
    use aptos_indexer_processor_example::config::indexer_processor_config::{DbConfig, IndexerProcessorConfig};
    use aptos_indexer_processor_example::config::processor_config::ProcessorConfig;
    use aptos_indexer_processor_example::processors::events::events_processor::EventsProcessor;
    use aptos_indexer_processor_sdk::traits::processor_trait::ProcessorTrait;

    #[tokio::test]
    async fn test_run() {
        let imported_txns = [IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_];

        // Create an instance of PostgresTestDatabase
        let mut db = PostgresTestDatabase::new();
        // Initialize the test context with user-defined container setup
        let test_context = SdkTestContext::new(&imported_txns, db)
            .await
            .unwrap();

        // processor_config;
        let processor_config = ProcessorConfig::EventsProcessor;

        // let postgres_config = PostgresConfig {
        //     connection_string: db_url.to_string(),
        //     db_pool_size: 100,
        // };

        println!("DB URL: {}", test_context.database.get_db_url());

        let db_config = DbConfig {
            postgres_connection_string: test_context.database.get_db_url(),
            db_pool_size: 100,
        };

        let transaction_stream_config = TransactionStreamConfig {
            indexer_grpc_data_service_address: Url::parse("http://localhost:51254")
                .expect("Could not parse database url"),
            starting_version: Some(1255836496), // dynamically pass the starting version
            request_ending_version: Some(1255836496), // dynamically pass the ending version
            auth_token: "".to_string(),
            request_name_header: "sdk testing".to_string(),
            indexer_grpc_http2_ping_interval_secs: 30,
            indexer_grpc_http2_ping_timeout_secs: 10,
            indexer_grpc_reconnection_timeout_secs: 10,
            indexer_grpc_response_item_timeout_secs: 60,
        };

        let indexer_processor_config = IndexerProcessorConfig {
            processor_config,
            transaction_stream_config,
            db_config,
        };

        let events_processor = EventsProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create EventsProcessor");



        // Run the processor and define custom validation logic
        test_context
            .run(
                &events_processor,
                |db_url| {
                    // User establishes the connection and runs the query

                    let mut conn = PgConnection::establish(&db_url).expect("Failed to establish DB connection");

                    // Custom validation logic
                    let events_result = events.load::<Event>(&mut conn);
                    let all_events = events_result.expect("Failed to load events");

                    assert_eq!(all_events.len(), 5, "Expected 5 events");
                    Ok(())
                },
            )
            .await
            .unwrap();
    }

}