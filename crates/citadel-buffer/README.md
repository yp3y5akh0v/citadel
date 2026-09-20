# citadeldb-buffer

Buffer pool with SIEVE eviction for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine. Manages the encrypt/decrypt pipeline between in-memory pages and on-disk storage.

This crate is part of the Citadel workspace. Depend on the main [`citadeldb`](https://crates.io/crates/citadeldb) crate instead.

`PageAllocator::allocate` and `allocate_nonzero` return `Result<PageId>`.
`BTree::new`, `btree::cow_page`, and `btree::propagate_cow_up` also return `Result`
and propagate allocation failures, including `Error::PageIdExhausted`.

`Cursor::current_ref_lazy` returns `Result<Option<LeafCell>>`. `Ok(None)` means
the cursor is invalid; page-loading failures return `Err`. Callers must propagate
these errors rather than treat them as end of iteration.

## License

Apache-2.0
