# citadeldb-sql-json-path

Vendored SQL/JSON Path implementation for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine. Parses and evaluates `$.path.expressions` used by Citadel's JSON functions and JSONB operators.

Forked from [sql-json-path](https://github.com/risingwavelabs/sql-json-path) v0.1.1 (RisingWave Labs, Apache-2.0) with backend slimming, datetime/timezone support, additional jsonpath methods, and bug fixes. See `NOTICE` for attribution.

This crate is part of the Citadel workspace. Depend on the main [`citadeldb`](https://crates.io/crates/citadeldb) crate instead.

## License

Apache-2.0
