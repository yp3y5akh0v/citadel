# citadeldb-sql

SQL engine for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine. Supports CREATE/DROP TABLE (incl. `STRICT`), ALTER TABLE, TRUNCATE, SELECT with JOINs (INNER, LEFT, RIGHT, CROSS, FULL OUTER, LATERAL), subqueries, CTEs (recursive + WITH-DML), window functions, INSERT (with UPSERT / `ON CONFLICT`), UPDATE, DELETE, RETURNING (with `OLD`/`NEW` row aliases), generated columns (`STORED` + `VIRTUAL`), partial indexes (`CREATE INDEX … WHERE`), `COLLATE` (BINARY/NOCASE/RTRIM) at column/expression/ORDER BY/index level, foreign keys with full referential actions (CASCADE / SET NULL / SET DEFAULT / RESTRICT / NO ACTION), aggregates, constraints, transactions with savepoints, prepared statements, and multi-statement scripts.

This crate is part of the Citadel workspace. Depend on the main [`citadeldb`](https://crates.io/crates/citadeldb) crate instead.

## License

MIT OR Apache-2.0
