use super::{events_extractor::EventsExtractor, events_storer::EventsStorer};
use crate::{
    common::processor_status_saver::get_processor_status_saver,
    utils::{
        chain_id::check_or_update_chain_id,
        database::{new_db_pool, run_migrations},
        starting_version::get_starting_version,
    },
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::{TransactionStream, TransactionStreamConfig},
    builder::ProcessorBuilder,
    common_steps::{
        TransactionStreamStep, VersionTrackerStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
    },
    config::indexer_processor_config::IndexerProcessorConfig,
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep},
};
use async_trait::async_trait;
use tracing::info;

pub struct EventsProcessor;

#[async_trait]
impl ProcessorTrait for EventsProcessor {
    fn name(&self) -> &'static str {
        "events_processor"
    }

    async fn run_processor(&self, config: IndexerProcessorConfig) -> Result<()> {
        // Get a connection pool
        let db_pool = new_db_pool(
            &config.db_config.postgres_connection_string,
            Some(config.db_config.db_pool_size),
        )
        .await
        .expect("Failed to create connection pool");

        // Run migrations
        run_migrations(
            config.db_config.postgres_connection_string.clone(),
            db_pool.clone(),
        )
        .await;

        // Merge the starting version from config and the latest processed version from the DB
        let starting_version = get_starting_version(&config, db_pool.clone()).await?;

        // Check and update the ledger chain id to ensure we're indexing the correct chain
        let grpc_chain_id = TransactionStream::new(config.transaction_stream_config.clone())
            .await?
            .get_chain_id()
            .await?;
        check_or_update_chain_id(grpc_chain_id as i64, db_pool.clone()).await?;

        // Define processor steps
        let transaction_stream_config = config.transaction_stream_config.clone();
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(starting_version),
            ..transaction_stream_config
        })
        .await?;
        let events_extractor = EventsExtractor {};
        let events_storer = EventsStorer::new(db_pool.clone());
        let version_tracker = VersionTrackerStep::new(
            get_processor_status_saver(db_pool.clone(), config.clone()),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );

        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(events_extractor.into_runnable_step(), 10)
        .connect_to(events_storer.into_runnable_step(), 10)
        .connect_to(version_tracker.into_runnable_step(), 10)
        .end_and_return_output_receiver(10);

        // (Optional) Parse the results
        loop {
            match buffer_receiver.recv().await {
                Ok(txn_context) => {
                    if txn_context.data.is_empty() {
                        continue;
                    }
                    info!(
                        "Finished processing events from versions [{:?}, {:?}]",
                        txn_context.metadata.start_version, txn_context.metadata.end_version,
                    );
                }
                Err(_) => {
                    info!("Channel is closed");
                    return Ok(());
                }
            }
        }
    }
}
