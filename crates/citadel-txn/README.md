# citadeldb-txn

Transaction manager for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine. Implements MVCC with snapshot isolation, shadow paging commit protocol, and the pending-free allocator.

This crate is part of the Citadel workspace. Depend on the main [`citadeldb`](https://crates.io/crates/citadeldb) crate instead.

`WriteTxn::read_snapshot()` captures pending data and catalog changes in a
`StatementReadTxn`. The snapshot remains stable through later writes, savepoint
restores, commit, or abort, and can outlive the writer.

`ReadTxn::view()` and `StatementReadTxn::view()` expose the data-only `ReadView`
API. `ReadView::cache_generation()` returns `Some` for committed snapshots and
`None` for pending snapshots. Pending views must not read or populate shared
caches keyed by a committed generation.

If a commit's final sync fails in `SyncMode::Full`, its durability is uncertain.
Further write transactions return `Error::ReopenRequired` until the database is
reopened and recovery selects a valid commit. Transaction ID exhaustion
returns `Error::TxnIdExhausted` instead of reusing IDs.

The optional `parallel` feature enables parallel page encryption for commits
with at least 128 dirty pages when the current Rayon pool has multiple workers.
Smaller commits and single-worker pools encrypt on the caller thread. Parallel
batches contain up to 256 pages (about 2 MiB); serial batches use at most 64 pages.
The page format, authentication, and commit protocol are unchanged.
Parallel encryption can reduce large-commit latency at the cost of additional
worker CPU. Rayon's current pool, or `RAYON_NUM_THREADS` for its default pool,
controls the worker count. The feature is disabled by default.

## License

Apache-2.0
