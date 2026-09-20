# citadeldb-core

Core types, error definitions, and constants for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine.

This crate is part of the Citadel workspace. Depend on the main [`citadeldb`](https://crates.io/crates/citadeldb) crate instead.

`Error::PageIdExhausted` reports that no usable page ID is available for an
allocation. Code that exhaustively matches `Error` must handle this variant.

## License

Apache-2.0
