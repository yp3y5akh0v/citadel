//! The integration tests, built as one binary: each file directly under `tests/` links
//! its own copy of the crate and its dependencies. `fips`, `cipher_selector_retirement`
//! and `key_backup` stay separate because the FIPS CI job builds only them.

mod audit_log;
mod audit_torture;
mod authenticated_slot_requirement;
mod backup;
mod btree_reference;
mod btree_torture;
mod compaction;
mod create_guards;
mod diff_engine;
mod diff_torture;
mod encryption_roundtrip;
mod inspection_surface;
mod integrity;
mod key_rotation;
mod kv_torture;
mod merkle;
mod merkle_sync;
mod merkle_torture;
mod multi_table_sync;
mod named_table_merkle;
mod named_table_reader;
mod named_tables;
mod patch_torture;
mod peer_to_peer;
mod public_api;
mod region_store_concurrency;
mod sync_patch;
mod sync_session;
mod sync_torture;
mod table_enumeration;
mod transactions;
