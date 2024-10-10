use crate::{
    db::common::models::processor_status::ProcessorStatus,
    schema::processor_status,
    utils::database::{execute_with_better_error, ArcDbPool},
};
use anyhow::Result;
use aptos_indexer_processor_sdk::common_steps::ProcessorStatusSaver;
use aptos_indexer_processor_sdk::{
    types::transaction_context::TransactionContext,
    utils::{errors::ProcessorError, time::parse_timestamp},
};
use async_trait::async_trait;
use diesel::{upsert::excluded, ExpressionMethods};

pub struct DefaultProcessorStatusSaver {
    pub conn_pool: ArcDbPool,
}

#[async_trait]
impl ProcessorStatusSaver for DefaultProcessorStatusSaver {
    async fn save_processor_status(
        &self,
        tracker_name: &str,
        last_success_batch: &TransactionContext<()>,
    ) -> Result<(), ProcessorError> {
        let end_timestamp = last_success_batch
            .metadata
            .end_transaction_timestamp
            .as_ref()
            .map(|t| parse_timestamp(t, last_success_batch.metadata.end_version as i64))
            .map(|t| t.naive_utc());

        let status = ProcessorStatus {
            processor: tracker_name.to_string(),
            last_success_version: last_success_batch.metadata.end_version as i64,
            last_transaction_timestamp: end_timestamp,
        };

        // Save regular processor status to the database
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
        )
        .await?;

        Ok(())
    }
}
