# citadeldb-crypto

Cryptographic primitives for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine. Includes AES-256-CTR encryption, HMAC-SHA256 authentication, Argon2id key derivation, and AES Key Wrap.

Historical cipher ID 1 is authenticated and interpreted as AES-256-CTR because that is what released storage wrote. Any different at-rest cipher requires its own format identifier.

This crate is part of the Citadel workspace. Depend on the main [`citadeldb`](https://crates.io/crates/citadeldb) crate instead.

## License

Apache-2.0
