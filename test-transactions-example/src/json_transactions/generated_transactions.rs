// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]
#![allow(unused_variables)]

pub const IMPORTED_MAINNET_TXNS_685_USER_TXN_ED25519: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/json_transactions/imported_mainnet_txns/685_user_txn_ed25519.json"
));
pub const ALL_IMPORTED_MAINNET_TXNS: &[&[u8]] = &[IMPORTED_MAINNET_TXNS_685_USER_TXN_ED25519];

pub const IMPORTED_TESTNET_TXNS_1_GENESIS: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/json_transactions/imported_testnet_txns/1_genesis.json"
));
pub const ALL_IMPORTED_TESTNET_TXNS: &[&[u8]] = &[IMPORTED_TESTNET_TXNS_1_GENESIS];
pub const ALL_SCRIPTED_TRANSACTIONS: &[&[u8]] = &[];

pub fn get_transaction_name(const_data: &[u8]) -> Option<&'static str> {
    match const_data {
        _ => None,
    }
}
