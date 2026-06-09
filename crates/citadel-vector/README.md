# citadeldb-vector

Vector search for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded
database engine. Provides the `VECTOR(N)` SQL type, distance operators (`<->` L2, `<#>` inner
product, `<=>` cosine), and a [PRISM](https://github.com/yp3y5akh0v/prism)-backed filtered approximate-nearest-neighbor index that
reads through Citadel's encrypted page storage, so the index is encrypted at rest like every
other page.

This crate is part of the Citadel workspace. For SQL vector queries depend on the main
[`citadeldb`](https://crates.io/crates/citadeldb) crate; for agent memory see
[`citadeldb-mem`](https://crates.io/crates/citadeldb-mem).

## License

MIT OR Apache-2.0
