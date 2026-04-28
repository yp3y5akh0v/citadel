# citadeldb-sql

SQL engine for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine. Supports CREATE/DROP TABLE, SELECT with JOINs, subqueries, CTEs, window functions, INSERT (with UPSERT / `ON CONFLICT`), UPDATE, DELETE, RETURNING (with `OLD`/`NEW` row aliases), generated columns (`STORED` + `VIRTUAL`), indexes, aggregates, constraints, transactions with savepoints, prepared statements, and multi-statement scripts.

This crate is part of the Citadel workspace. Depend on the main [`citadeldb`](https://crates.io/crates/citadeldb) crate instead.

## License

MIT OR Apache-2.0
