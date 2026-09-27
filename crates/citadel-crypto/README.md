# citadeldb-crypto

Cryptographic primitives for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine. Includes AES-256-CTR encryption, HMAC-SHA256 authentication, Argon2id key derivation, and AES Key Wrap.

Key derivation accepts Argon2id time costs from 1 through 16 and
PBKDF2-HMAC-SHA256 iteration counts from 600,000 through 10,000,000. These limits
apply to creating and opening key files and key backups.

Key files and key backups accept the authenticated legacy cipher identifier `1` as
AES-256-CTR. New files use the canonical identifier `0`.

Page and blob CTR paths use only AES's forward block operation for both
encryption and decryption, avoiding unused inverse key schedules. These paths
still authenticate before decrypting, with unchanged ciphertext formats and
cipher identifiers. AES Key Wrap retains the full AES cipher for wrapping and
unwrapping.

This crate is part of the Citadel workspace. Depend on the main [`citadeldb`](https://crates.io/crates/citadeldb) crate instead.

## License

Apache-2.0
