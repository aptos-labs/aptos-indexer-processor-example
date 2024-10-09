use crate::{
    config::indexer_processor_config::IndexerProcessorConfig,
    db::common::models::{
        backfill_processor_status::BackfillProcessorStatus, processor_status::ProcessorStatus,
    },
    schema::{backfill_processor_status, processor_status},
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

pub enum ProcessorStatusSaverEnum {
    Default(DefaultProcessorStatusSaver),
    Backfill(BackfillProcessorStatusSaver),
}

#[async_trait]
impl ProcessorStatusSaver for ProcessorStatusSaverEnum {
    async fn save_processor_status(
        &self,
        last_success_batch: &TransactionContext<()>,
    ) -> Result<(), ProcessorError> {
        match self {
            ProcessorStatusSaverEnum::Default(saver) => {
                saver.save_processor_status(last_success_batch).await
            }
            ProcessorStatusSaverEnum::Backfill(saver) => {
                saver.save_processor_status(last_success_batch).await
            }
        }
    }
}

pub fn get_processor_status_saver(
    conn_pool: ArcDbPool,
    config: IndexerProcessorConfig,
) -> ProcessorStatusSaverEnum {
    if let Some(backfill_config) = config.backfill_config {
        let txn_stream_cfg = config.transaction_stream_config;
        let backfill_start_version = txn_stream_cfg.starting_version;
        let backfill_end_version = txn_stream_cfg.request_ending_version;
        let backfill_alias = backfill_config.backfill_alias.clone();
        ProcessorStatusSaverEnum::Backfill(BackfillProcessorStatusSaver {
            conn_pool,
            backfill_alias,
            backfill_start_version,
            backfill_end_version,
        })
    } else {
        let processor_type = config.processor_config.name().to_string();
        ProcessorStatusSaverEnum::Default(DefaultProcessorStatusSaver {
            conn_pool,
            processor_type,
        })
    }
}

pub struct DefaultProcessorStatusSaver {
    pub conn_pool: ArcDbPool,
    pub processor_type: String,
}

#[async_trait]
impl ProcessorStatusSaver for DefaultProcessorStatusSaver {
    async fn save_processor_status(
        &self,
        last_success_batch: &TransactionContext<()>,
    ) -> Result<(), ProcessorError> {
        let end_timestamp = last_success_batch
            .metadata
            .end_transaction_timestamp
            .as_ref()
            .map(|t| parse_timestamp(t, last_success_batch.metadata.end_version as i64))
            .map(|t| t.naive_utc());

        let status = ProcessorStatus {
            processor: self.processor_type.clone(),
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
pub struct BackfillProcessorStatusSaver {
    pub conn_pool: ArcDbPool,
    pub backfill_alias: String,
    pub backfill_start_version: Option<u64>,
    pub backfill_end_version: Option<u64>,
}

#[async_trait]
impl ProcessorStatusSaver for BackfillProcessorStatusSaver {
    async fn save_processor_status(
        &self,
        last_success_batch: &TransactionContext<()>,
    ) -> Result<(), ProcessorError> {
        let end_timestamp = last_success_batch
            .metadata
            .end_transaction_timestamp
            .as_ref()
            .map(|t| parse_timestamp(t, last_success_batch.metadata.end_version as i64))
            .map(|t| t.naive_utc());
        let status = BackfillProcessorStatus {
            backfill_alias: self.backfill_alias.clone(),
            last_success_version: last_success_batch.metadata.end_version as i64,
            last_transaction_timestamp: end_timestamp,
            backfill_start_version: self.backfill_start_version.unwrap_or(0) as i64,
            backfill_end_version: self
                .backfill_end_version
                .unwrap_or(last_success_batch.metadata.end_version)
                as i64,
        };
        execute_with_better_error(
            self.conn_pool.clone(),
            diesel::insert_into(backfill_processor_status::table)
                .values(&status)
                .on_conflict(backfill_processor_status::backfill_alias)
                .do_update()
                .set((
                    backfill_processor_status::last_success_version.eq(excluded(backfill_processor_status::last_success_version)),
                    backfill_processor_status::last_updated.eq(excluded(backfill_processor_status::last_updated)),
                    backfill_processor_status::last_transaction_timestamp.eq(excluded(backfill_processor_status::last_transaction_timestamp)),
                    backfill_processor_status::backfill_start_version.eq(excluded(backfill_processor_status::backfill_start_version)),
                    backfill_processor_status::backfill_end_version.eq(excluded(backfill_processor_status::backfill_end_version)),
                )),
            Some(" WHERE backfill_processor_status.last_success_version <= EXCLUDED.last_success_version "),
        ).await?;
        Ok(())
    }
}
