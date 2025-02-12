use crate::db::common::models::processor_status::ProcessorStatus as PostgresProcessorStatus;
use crate::{
    schema::processor_status,
    utils::database::{execute_with_better_error, new_db_pool, run_migrations, ArcDbPool},
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    config::indexer_processor_config::{DbConfig, IndexerProcessorConfig, RunnableDbConfig},
    config::processor_status_saver::ProcessorStatus,
    config::processor_status_saver::ProcessorStatusSaver,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::upsert::excluded;
use diesel::ExpressionMethods;
use downcast::Any;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    pub postgres_connection_string: String,
    // Size of the pool for writes/reads to the DB. Limits maximum number of queries in flight
    #[serde(default = "PostgresConfig::default_db_pool_size")]
    pub db_pool_size: u32,
}

impl PostgresConfig {
    pub const fn default_db_pool_size() -> u32 {
        150
    }

    pub fn downcast_from_config<D: DbConfig>(config: IndexerProcessorConfig<D>) -> PostgresConfig {
        let postgres_config = config.db_config;
        postgres_config
            .as_any()
            .downcast_ref::<PostgresConfig>()
            .unwrap()
            .clone()
    }
}

#[async_trait]
impl DbConfig for PostgresConfig {
    type Runnable = RunnablePostgresConfig;

    async fn into_runnable_config(self) -> Result<Self::Runnable> {
        let db_pool = new_db_pool(&self.postgres_connection_string, Some(self.db_pool_size))
            .await
            .expect("Failed to create connection pool");
        Ok(RunnablePostgresConfig {
            db_pool,
            postgres_connection_string: self.postgres_connection_string,
        })
    }
}

pub struct RunnablePostgresConfig {
    pub db_pool: ArcDbPool,
    pub postgres_connection_string: String,
}

impl RunnablePostgresConfig {
    pub async fn setup(&self) -> Result<()> {
        // Run migrations
        run_migrations(
            self.postgres_connection_string.clone(),
            self.db_pool.clone(),
        )
        .await;
        Ok(())
    }

    pub fn get_db_pool(&self) -> ArcDbPool {
        self.db_pool.clone()
    }
}

impl RunnableDbConfig for RunnablePostgresConfig {}

#[async_trait]
impl ProcessorStatusSaver for RunnablePostgresConfig {
    async fn save_processor_status(
        &self,
        processor_status: ProcessorStatus,
    ) -> Result<(), ProcessorError> {
        let status = PostgresProcessorStatus {
            processor: processor_status.processor_id.to_string(),
            last_success_version: processor_status.last_success_version as i64,
            last_transaction_timestamp: processor_status.last_transaction_timestamp,
        };

        // Save regular processor status to the database
        execute_with_better_error(
            self.db_pool.clone(),
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

    async fn save_chain_id(&self, _chain_id: u64) -> Result<(), ProcessorError> {
        // TODO: Implement
        Ok(())
    }

    async fn get_chain_id(&self) -> Result<Option<u64>, ProcessorError> {
        // TODO: Implement
        Ok(None)
    }

    async fn get_processor_status(
        &self,
        _processor_name: &str,
    ) -> Result<Option<ProcessorStatus>, ProcessorError> {
        // TODO: Implement
        Ok(None)
    }
}
