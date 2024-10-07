use crate::{
    config::indexer_processor_config::DbConfig,
    db::common::models::processor_status::ProcessorStatus,
    schema::processor_status,
    utils::database::{execute_with_better_error, new_db_pool, ArcDbPool},
};
use anyhow::{Context, Result};
use aptos_indexer_processor_sdk::{
    traits::{NamedStep, PollableAsyncRunType, PollableAsyncStep, Processable},
    types::transaction_context::TransactionContext,
    utils::{errors::ProcessorError, time::parse_timestamp},
};
use async_trait::async_trait;
use diesel::{upsert::excluded, ExpressionMethods};

pub const UPDATE_PROCESSOR_STATUS_SECS: u64 = 1;

/// This step tracks the latest version processed by the processor and stores the version in DB.
/// It assumes that the step inputs are ordered by starting_version.
/// To guarantee the order by starting_version, you should connect `OrderByStartingVersionStep` before this step.
pub struct LatestVersionProcessedTracker<T>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
{
    conn_pool: ArcDbPool,
    tracker_name: String,
    // Last successful batch of sequentially processed transactions. Includes metadata to write to storage.
    last_success_batch: Option<TransactionContext<T>>,
}

impl<T> LatestVersionProcessedTracker<T>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
{
    pub async fn new(db_config: DbConfig, tracker_name: String) -> Result<Self> {
        let conn_pool = new_db_pool(
            &db_config.postgres_connection_string,
            Some(db_config.db_pool_size),
        )
        .await
        .context("Failed to create connection pool")?;
        Ok(Self {
            conn_pool,
            tracker_name,
            last_success_batch: None,
        })
    }

    async fn save_processor_status(&mut self) -> Result<(), ProcessorError> {
        // Update the processor status
        if let Some(last_success_batch) = self.last_success_batch.as_ref() {
            let end_timestamp = last_success_batch
                .metadata
                .end_transaction_timestamp
                .as_ref()
                .map(|t| parse_timestamp(t, last_success_batch.metadata.end_version as i64))
                .map(|t| t.naive_utc());
            let status = ProcessorStatus {
                processor: self.tracker_name.clone(),
                last_success_version: last_success_batch.metadata.end_version as i64,
                last_transaction_timestamp: end_timestamp,
            };
            execute_with_better_error(
                self.conn_pool.clone(),
                diesel::insert_into(processor_status::table)
                    .values(&status)
                    .on_conflict(processor_status::processor)
                    .do_update()
                    .set((
                        processor_status::last_success_version
                            .eq(excluded(processor_status::last_success_version)),
                        processor_status::last_updated.eq(excluded(processor_status::last_updated)),
                        processor_status::last_transaction_timestamp
                            .eq(excluded(processor_status::last_transaction_timestamp)),
                    )),
                Some(" WHERE processor_status.last_success_version <= EXCLUDED.last_success_version "),
            ).await.map_err(|e| ProcessorError::DBStoreError {
                message: format!("Failed to update processor status: {}", e),
                query: None,
            })?;
        }
        Ok(())
    }
}

#[async_trait]
impl<T> Processable for LatestVersionProcessedTracker<T>
where
    Self: Sized + Send + 'static,
    T: Clone + Send + 'static,
{
    type Input = T;
    type Output = T;
    type RunType = PollableAsyncRunType;

    async fn process(
        &mut self,
        current_batch: TransactionContext<T>,
    ) -> Result<Option<TransactionContext<T>>, ProcessorError> {
        // If there's a gap in version, return an error
        if let Some(last_success_batch) = self.last_success_batch.as_ref() {
            if last_success_batch.metadata.end_version + 1 != current_batch.metadata.start_version {
                return Err(ProcessorError::ProcessError {
                    message: format!(
                        "Gap detected starting from version: {}",
                        current_batch.metadata.start_version
                    ),
                });
            }
        }

        // Update the last success batch
        self.last_success_batch = Some(current_batch.clone());

        // Pass through
        Ok(Some(current_batch))
    }

    async fn cleanup(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        // If processing or polling ends, save the last successful batch to the database.
        self.save_processor_status().await?;
        Ok(None)
    }
}

#[async_trait]
impl<T: Clone + Send + 'static> PollableAsyncStep for LatestVersionProcessedTracker<T>
where
    Self: Sized + Send + Sync + 'static,
    T: Send + 'static,
{
    fn poll_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(UPDATE_PROCESSOR_STATUS_SECS)
    }

    async fn poll(&mut self) -> Result<Option<Vec<TransactionContext<T>>>, ProcessorError> {
        // TODO: Add metrics for gap count
        self.save_processor_status().await?;
        // Nothing should be returned
        Ok(None)
    }
}

impl<T> NamedStep for LatestVersionProcessedTracker<T>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
{
    fn name(&self) -> String {
        format!(
            "LatestVersionProcessedTracker: {}",
            std::any::type_name::<T>()
        )
    }
}
