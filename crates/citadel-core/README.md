# citadeldb-core

Core types, error definitions, and constants for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine.

This crate is part of the Citadel workspace. Depend on the main [`citadeldb`](https://crates.io/crates/citadeldb) crate instead.

`Error::PageIdExhausted` and `Error::TxnIdExhausted` report exhausted page and
transaction ID spaces. Allocations and writes return errors instead of wrapping
or reusing those IDs.

`Error::ReopenRequired` means an earlier commit's durability is uncertain after
a failed final sync. Reopen the database before starting another write
transaction so recovery can select a valid commit.

## License

Apache-2.0
