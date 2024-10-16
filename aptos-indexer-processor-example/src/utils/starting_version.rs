use std::cmp::max;

use super::database::ArcDbPool;
use crate::{
    config::indexer_processor_config::IndexerProcessorConfig,
    db::common::models::{
        backfill_processor_status::{BackfillProcessorStatusQuery, BackfillStatus},
        processor_status::ProcessorStatusQuery,
    },
};
use anyhow::{Context, Result};

/// Get the appropriate starting version for the processor.
///
/// If it is a regular processor, this will return the higher of the checkpointed version,
/// or `staring_version` from the config, or 0 if not set.
///
/// If this is a backfill processor and threre is an in-progress backfill, this will return
/// the checkpointed version + 1.
///
/// If this is a backfill processor and there is not an in-progress backfill (i.e., no checkpoint or
/// backfill status is COMPLETE), this will return `starting_version` from the config, or 0 if not set.
pub async fn get_starting_version(
    indexer_processor_config: &IndexerProcessorConfig,
    conn_pool: ArcDbPool,
) -> Result<u64> {
    // Check if there's a checkpoint in the approrpiate processor status table.
    let latest_processed_version =
        get_latest_processed_version_from_db(indexer_processor_config, conn_pool)
            .await
            .context("Failed to get latest processed version from DB")?;

    // If nothing checkpointed, return the `starting_version` from the config, or 0 if not set.
    Ok(latest_processed_version.unwrap_or(
        indexer_processor_config
            .transaction_stream_config
            .starting_version
            .unwrap_or(0),
    ))
}

async fn get_latest_processed_version_from_db(
    indexer_processor_config: &IndexerProcessorConfig,
    conn_pool: ArcDbPool,
) -> Result<Option<u64>> {
    let mut conn = conn_pool.get().await?;

    if let Some(backfill_config) = &indexer_processor_config.backfill_config {
        let backfill_status = BackfillProcessorStatusQuery::get_by_processor(
            &backfill_config.backfill_alias,
            &mut conn,
        )
        .await?;

        // Return None if there is no checkpoint or if the backfill is old (complete).
        // Otherwise, return the checkpointed version + 1.
        return Ok(
            backfill_status.and_then(|status| match status.backfill_status {
                BackfillStatus::InProgress => Some(status.last_success_version as u64 + 1),
                // If status is Complete, this is the start of a new backfill job.
                BackfillStatus::Complete => None,
            }),
        );
    }

    let status = ProcessorStatusQuery::get_by_processor(
        indexer_processor_config.processor_config.name(),
        &mut conn,
    )
    .await?;

    // Return None if there is no checkpoint. Otherwise,
    // return the higher of the checkpointed version + 1 the `starting_version`.
    Ok(status.map(|status| {
        std::cmp::max(
            status.last_success_version as u64 + 1,
            indexer_processor_config
                .transaction_stream_config
                .starting_version
                .unwrap_or(0),
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::indexer_processor_config::{BackfillConfig, DbConfig, IndexerProcessorConfig},
        db::common::models::{
            backfill_processor_status::{BackfillProcessorStatus, BackfillStatus},
            processor_status::ProcessorStatus,
        },
    };
    use aptos_indexer_processor_sdk::aptos_indexer_transaction_stream::TransactionStreamConfig;
    use chrono::Utc;

    #[tokio::test]
    async fn test_get_starting_version_no_checkpoint() {
        let indexer_processor_config = IndexerProcessorConfig {
            db_config: DbConfig {
                postgres_connection_string: "test".to_string(),
                db_pool_size: 1,
            },
            transaction_stream_config: TransactionStreamConfig {
                indexer_grpc_data_service_address: "test_url".parse().unwrap(),
                starting_version: None,
                request_ending_version: None,
                auth_token: "test".to_string(),
                request_name_header: "test".to_string(),
            },
            processor_config: Default::default(),
            backfill_config: None,
        };
        let conn_pool = ArcDbPool::new(Default::default());

        let starting_version = get_starting_version(&indexer_processor_config, conn_pool)
            .await
            .unwrap();
        assert_eq!(starting_version, 0);
    }
}
