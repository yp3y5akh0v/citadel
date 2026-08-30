# citadeldb-sql-json-path

Vendored SQL/JSON Path implementation for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine. Parses and evaluates `$.path.expressions` used by Citadel's JSON functions and JSONB operators, including the SQL:2023 conversion methods `.bigint()`, `.decimal()`, `.integer()`, `.number()`, `.string()`, `.boolean()`, `.date()`, `.time()`, `.time_tz()`, `.timestamp()`, and `.timestamp_tz()`.

Forked from [sql-json-path](https://github.com/risingwavelabs/sql-json-path) v0.1.1 (RisingWave Labs, Apache-2.0) with backend slimming, PostgreSQL-compatible numeric and datetime semantics, session-time-zone support, and bug fixes. Direct users can bind `_tz` evaluation to a stable context with `JsonPath::with_session_tz` and `JsonPath::with_session_date`. See `NOTICE` for attribution.

This crate is part of the Citadel workspace. Depend on the main [`citadeldb`](https://crates.io/crates/citadeldb) crate instead.

## License

Apache-2.0
