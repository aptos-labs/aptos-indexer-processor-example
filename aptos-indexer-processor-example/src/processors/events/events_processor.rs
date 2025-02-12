use super::{events_extractor::EventsExtractor, events_storer::EventsStorer};
use crate::{
    common::config::{PostgresConfig, RunnablePostgresConfig},
    db::common::models::events_models::EventModel,
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    builder::ProcessorBuilder,
    common_steps::{
        TransactionStreamStep, VersionTrackerStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
    },
    config::indexer_processor_config::{DbConfig, IndexerProcessorConfig},
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep},
};
use async_trait::async_trait;
use downcast::Any;
use tracing::info;

pub struct EventsProcessor;

impl EventsProcessor {}

#[async_trait]
impl ProcessorTrait for EventsProcessor {
    fn name(&self) -> &'static str {
        "events_processor"
    }

    async fn run_processor<D>(&self, config: IndexerProcessorConfig<D>) -> Result<()>
    where
        D: DbConfig + Send + Sync + 'static,
    {
        // Get the raw postgres storage config
        let postgres_config = PostgresConfig::downcast_from_config::<D>(config.clone());

        // Convert the postgres config to a runnable config. This will initialize the connection pool.
        let runnable_postgres_config: RunnablePostgresConfig =
            postgres_config.into_runnable_config().await?;

        // Run any additional setup, like running db migrations
        runnable_postgres_config.setup().await?;

        // Get the connection pool for any steps that require it
        let db_pool = runnable_postgres_config.get_db_pool();

        // TODO: Move this to server framework since we should always do this check to protect against chain id mismatch
        config.check_or_update_chain_id().await?;

        // Define processor steps
        let transaction_stream = TransactionStreamStep::new(config.clone()).await?;
        let events_extractor = EventsExtractor {};
        let events_storer = EventsStorer::new(db_pool.clone());
        let version_tracker: VersionTrackerStep<_, RunnablePostgresConfig, D> =
            VersionTrackerStep::new(
                runnable_postgres_config,
                DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
                self.name(),
                config.clone(),
            );

        let runnable_version_tracker = version_tracker.into_runnable_step();

        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(events_extractor.into_runnable_step(), 10)
        .connect_to(events_storer.into_runnable_step(), 10)
        .connect_to(runnable_version_tracker, 10)
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
