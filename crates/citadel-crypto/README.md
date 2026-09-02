# citadeldb-crypto

Cryptographic primitives for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine. Includes AES-256-CTR encryption, HMAC-SHA256 authentication, Argon2id key derivation, and AES Key Wrap.

Key files and key backups accept the authenticated legacy cipher identifier `1` as
AES-256-CTR. New files use the canonical identifier `0`.

This crate is part of the Citadel workspace. Depend on the main [`citadeldb`](https://crates.io/crates/citadeldb) crate instead.

## License

Apache-2.0
