# citadeldb-page

Page format, serialization, and checksum for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine. Defines the B+ tree page structure including leaf pages, branch pages, and overflow pages.

This crate is part of the Citadel workspace. Depend on the main [`citadeldb`](https://crates.io/crates/citadeldb) crate instead.

`overflow::write_chain` and `write_chain_with_cancel` return `Result<PageId>`;
their allocation callbacks must also return `Result<PageId>`. Propagate allocation
failures, including `Error::PageIdExhausted`, to the transaction owner.

## License

Apache-2.0
