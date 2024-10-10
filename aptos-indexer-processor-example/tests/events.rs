use diesel::{Identifiable, Insertable, Queryable};
use field_count::FieldCount;
// use processor::schema::{events, fungible_asset_activities, token_activities_v2};
use serde::{Deserialize, Serialize};
use aptos_indexer_processor_example::schema::events;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = events)]
pub struct Event {
    pub sequence_number: i64,
    pub creation_number: i64,
    pub account_address: String,
    pub transaction_version: i64,
    pub transaction_block_height: i64,
    pub type_: String,
    pub data: serde_json::Value,
    pub inserted_at: chrono::NaiveDateTime,
    pub event_index: i64,
    pub indexed_type: String,
}
