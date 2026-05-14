# citadeldb-sql

SQL engine for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine. Supports CREATE/DROP TABLE (incl. `STRICT`), ALTER TABLE, TRUNCATE, SELECT with JOINs (INNER, LEFT, RIGHT, CROSS, FULL OUTER, LATERAL), subqueries, CTEs (recursive + WITH-DML), window functions, INSERT (with UPSERT / `ON CONFLICT`), UPDATE, DELETE, RETURNING (with `OLD`/`NEW` row aliases), generated columns (`STORED` + `VIRTUAL`), partial indexes (`CREATE INDEX … WHERE`), GIN inverted indexes (`CREATE INDEX … USING gin`) for JSONB `@>` containment queries, `COLLATE` (BINARY/NOCASE/RTRIM) at column/expression/ORDER BY/index level, JSON / JSONB types with 12 PG operators (`->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`, `#-`, `@?`, `@@`), `JSON_TABLE` / `JSON_EXISTS` / `JSON_VALUE` / `JSON_QUERY` with SQL/JSON predicate path language, 16 JSON scalar functions, 4 JSON/JSONB aggregates (`json_agg`, `jsonb_agg`, `json_object_agg`, `jsonb_object_agg`), and 5 set-returning JSON functions, foreign keys with full referential actions (CASCADE / SET NULL / SET DEFAULT / RESTRICT / NO ACTION), aggregates, constraints, transactions with savepoints, prepared statements, and multi-statement scripts. Large values (TEXT / BLOB / JSON / JSONB) transparently spill to overflow page chains.

This crate is part of the Citadel workspace. Depend on the main [`citadeldb`](https://crates.io/crates/citadeldb) crate instead.

## License

MIT OR Apache-2.0
