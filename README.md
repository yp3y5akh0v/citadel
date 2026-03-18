<p align="center">
  <img src=".github/logo.png" alt="Citadel" width="128">
</p>

<h1 align="center">Citadel</h1>

<p align="center">An encrypted-first embedded database engine written in Rust.<br>Every page is encrypted at rest — encryption is not an afterthought, it's the foundation.</p>

## Features

- **Encrypted at rest** — AES-256-CTR or ChaCha20, HMAC-SHA256 per page, verified before decryption
- **Full SQL engine** — CREATE/DROP TABLE, SELECT with JOINs, subqueries, aggregates, indexes
- **ACID transactions** — Copy-on-Write B+ tree with shadow paging, no WAL. Snapshot isolation with concurrent readers
- **P2P sync** — Merkle tree diffing over Noise-encrypted transport with PSK authentication
- **Interactive CLI** — SQL shell with tab completion, syntax highlighting, 22 dot-commands
- **Key hierarchy** — Passphrase → Argon2id → Master Key → AES-KW → REK → HKDF → DEK + MAC
- **FIPS mode** — PBKDF2-HMAC-SHA256 + AES-256-CTR for compliance environments
- **Audit logging** — HMAC-SHA256 chained tamper-evident log
- **Hot backup** — Consistent snapshots via MVCC without blocking writes
- **Large values** — Overflow pages handle values up to any size transparently
- **Cross-platform** — Windows, Linux, macOS, and more. C FFI (35 functions) and WebAssembly bindings included
- **2,100+ tests** — Unit, integration, and torture tests across all crates

## Quick Start

### As a Library

```rust
use citadel::{DatabaseBuilder, Argon2Profile};
use citadel_sql::Connection;

// Create a new encrypted database
let db = DatabaseBuilder::new("my.db")
    .passphrase(b"secret")
    .argon2_profile(Argon2Profile::Interactive)
    .create()?;

// SQL
let mut conn = Connection::open(&db)?;
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")?;
conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
let result = conn.query("SELECT * FROM users;")?;

// Key-value (default table)
let mut wtx = db.begin_write()?;
wtx.insert(b"key", b"value")?;
wtx.commit()?;

let mut rtx = db.begin_read();
assert_eq!(rtx.get(b"key")?.unwrap(), b"value");

// Named tables
let mut wtx = db.begin_write()?;
wtx.create_table(b"sessions")?;
wtx.table_insert(b"sessions", b"token-abc", b"user-42")?;
wtx.commit()?;

// In-memory database (no file I/O, useful for testing and WASM)
let mem_db = DatabaseBuilder::new("")
    .passphrase(b"secret")
    .create_in_memory()?;
```

### CLI

```bash
# Create and open a database
citadel --create my.db

# Run SQL interactively
citadel> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
citadel> INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
citadel> SELECT * FROM users;
┌────┬───────┐
│ id │ name  │
├────┼───────┤
│  1 │ Alice │
│  2 │ Bob   │
└────┴───────┘

# Database operations
citadel> .backup mydb.bak          # Hot backup (non-blocking)
citadel> .verify                   # Integrity check
citadel> .stats                    # Tree depth, pages, Merkle root
citadel> .audit verify             # Verify HMAC chain
citadel> .rekey                    # Change passphrase
citadel> .compact clean.db         # Compact to new file
citadel> .dump users               # Export as SQL statements

# P2P encrypted sync
citadel> .keygen                   # Generate a 256-bit sync key
citadel> .listen 4248 <KEY>        # Terminal A: wait for peer
citadel> .sync 127.0.0.1:4248 <KEY> # Terminal B: push tables
```

## SQL Support

**Statements**: CREATE/DROP TABLE, CREATE/DROP INDEX, INSERT, SELECT, UPDATE, DELETE, BEGIN/COMMIT/ROLLBACK, EXPLAIN

**Types**: INTEGER, REAL, TEXT, BLOB, BOOLEAN

**Expressions**: JOINs (INNER, LEFT, RIGHT, CROSS), subqueries (scalar, IN, EXISTS), CASE, BETWEEN, LIKE, DISTINCT, GROUP BY/HAVING, ORDER BY, LIMIT/OFFSET

**Functions**: COUNT, SUM, AVG, MIN, MAX, LENGTH, UPPER, LOWER, SUBSTR, ABS, ROUND, COALESCE, NULLIF, CAST

**Parameters**: Positional binding with `$1, $2, ...` and LRU statement cache

## Security

**No plaintext on disk.** Every page is encrypted before writing and authenticated before reading. There is no "unencrypted mode" — the database file is always opaque.

**Separate key file.** Encryption keys live in `{dbname}.citadel-keys`, not inside the database. The passphrase never touches disk — it derives a master key in memory via Argon2id (or PBKDF2 in FIPS mode).

**Key backup.** Export an encrypted key backup with a separate recovery passphrase. If the original passphrase is lost, restore access without re-encrypting the database.

**Passphrase change.** Re-wraps the root encryption key with the new passphrase. No page re-encryption needed — instant regardless of database size.

**Encrypted sync.** P2P sync uses the Noise protocol (`NNpsk0_25519_ChaChaPoly_BLAKE2s`). Both peers must share a 256-bit pre-shared key. Each session generates ephemeral Curve25519 keys for forward secrecy. No data is sent in plaintext.

## Architecture

```
┌─────────────┬───────────────┬───────────────┐
│ citadel-cli  │  citadel-ffi  │ citadel-wasm  │  CLI, C FFI, WebAssembly
├─────────────┴───────────────┴───────────────┤
│                 citadel-sql                  │  SQL parser, planner, executor
├─────────────────────────────────────────────┤
│                  citadel                     │  Database API, builder, sync
├──────────────┬──────────────┬───────────────┤
│  citadel-txn │ citadel-sync │ citadel-crypto│  Transactions, replication, keys
├──────────────┼──────────────┴───────────────┤
│citadel-buffer│         citadel-page         │  Buffer pool (SIEVE), page codec
├──────────────┴──────────────────────────────┤
│              citadel-io                      │  File I/O, fsync, io_uring
├─────────────────────────────────────────────┤
│              citadel-core                    │  Types, errors, constants
└─────────────────────────────────────────────┘
```

### Page Layout (8208 bytes on disk)

```
┌──────────┬────────────────────┬──────────┐
│  IV 16B  │  Ciphertext 8160B  │  MAC 32B │
└──────────┴────────────────────┴──────────┘
```

Every page gets a fresh random IV. HMAC is verified before decryption to prevent ciphertext manipulation.

### Commit Protocol

Shadow paging with a god byte — a single byte controls which of two commit slots is active. Commits are atomic without a write-ahead log:

1. Write dirty pages to new locations (CoW)
2. Compute Merkle hashes bottom-up
3. Update inactive commit slot
4. Flip god byte (single-byte atomic write)

## Language Bindings

### C / C++

The `citadel-ffi` crate builds as a static or dynamic library with an auto-generated `citadel.h` header (cbindgen). All functions are panic-safe.

```c
#include "citadel.h"

CitadelDb *db = NULL;
citadel_create("my.db", "secret", 6, &db);

// Key-value
CitadelWriteTxn *wtx = NULL;
citadel_write_begin(db, &wtx);
citadel_write_put(wtx, (const uint8_t*)"key", 3, (const uint8_t*)"val", 3);
citadel_write_commit(wtx);

// SQL
CitadelSqlConn *conn = NULL;
citadel_sql_open(db, &conn);
CitadelSqlResult *result = NULL;
citadel_sql_execute(conn, "SELECT * FROM users;", &result);

citadel_close(db);
```

### WebAssembly

The `citadel-wasm` crate provides a JavaScript API for browser and Node.js environments.

```js
import { CitadelDb } from "citadel-wasm";

const db = new CitadelDb("secret");
db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);");
db.execute("INSERT INTO t (id, name) VALUES (1, 'Alice');");

const result = db.query("SELECT * FROM t;");
// { columns: ["id", "name"], rows: [[1, "Alice"]] }

db.put(new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6]));
```

Build with `wasm-pack build crates/citadel-wasm --target web`.

## Building

Requires Rust 1.75 or later.

```bash
git clone https://github.com/yp3y5akh0v/citadel.git
cd citadel
cargo build --release
```

### Feature Flags

| Flag | Description |
|------|-------------|
| `audit-log` | Tamper-evident HMAC-chained audit log (default: on) |
| `fips` | FIPS 140-3 mode: PBKDF2 + AES-256-CTR only |
| `io-uring` | Linux async I/O via io_uring |

## License

MIT OR Apache-2.0
