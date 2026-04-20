<p align="center">
  <img src="https://raw.githubusercontent.com/yp3y5akh0v/citadel/master/.github/banner.png" alt="Citadel" width="600">
</p>

<h1 align="center">Citadel</h1>

<p align="center">Encrypted-first embedded database engine that outperforms unencrypted SQLite.</p>

<p align="center">
  <a href="https://crates.io/crates/citadeldb"><img src="https://img.shields.io/crates/v/citadeldb" alt="crates.io"></a>
  <a href="https://www.npmjs.com/package/@citadeldb/wasm"><img src="https://img.shields.io/npm/v/@citadeldb/wasm" alt="npm"></a>
  <a href="https://github.com/yp3y5akh0v/citadel/actions/workflows/ci.yml"><img src="https://github.com/yp3y5akh0v/citadel/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/yp3y5akh0v/citadel#license"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue" alt="License"></a>
</p>

Every page is encrypted and authenticated before it hits disk. The database file is always opaque. Beats unencrypted SQLite in all 23 benchmarks with equal cache budgets.

## Features

- **Encrypted at rest** - AES-256-CTR + HMAC-SHA256 per page, verified before decryption
- **SQL** - JOINs, subqueries, CTEs (recursive), UNION/INTERSECT/EXCEPT, window functions, views, aggregates, indexes, constraints, ALTER TABLE, prepared statements
- **ACID** - Copy-on-Write B+ tree, shadow paging, no WAL. Snapshot isolation with concurrent readers
- **P2P sync** - Merkle-based table diffing over Noise-encrypted channels with PSK auth
- **CLI** - SQL shell with tab completion, syntax highlighting, dot-commands (.backup, .verify, .rekey, .sync, .dump, ...)
- **3-tier key hierarchy** - Passphrase -> Argon2id -> Master Key -> AES-KW -> REK -> HKDF -> DEK + MAC
- **FIPS 140-3** - PBKDF2-HMAC-SHA256 + AES-256-CTR when compliance requires it
- **Audit log** - HMAC-SHA256 chained, tamper-evident
- **Hot backup** - Consistent snapshots via MVCC, no write blocking
- **Overflow pages** - Large values handled transparently, no size limits
- **Cross-platform** - Windows, Linux, macOS. C FFI (37 functions), WebAssembly bindings
- **2,670+ tests** - Unit, integration, torture tests across 10 crates

## Benchmarks

Single-threaded on 100K rows, schema `(id INTEGER PK, name TEXT, age INTEGER)`. Ratio = SQLite / Citadel time.

```
Benchmark          Citadel        SQLite         Ratio
------------------------------------------------------
correlated_in      9.50 ms        1.84 s         194x
count              550 ns         62.3 us        113x
correlated_scalar  590 us         18.1 ms        31x
cte                1.67 ms        14.6 ms        8.7x
group_by           2.80 ms        21.0 ms        7.5x
point              2.01 us        14.7 us        7.3x
savepoint_nested   620 us         3.09 ms        5.0x
sort               1.60 ms        6.37 ms        4.0x
filter             1.05 ms        2.75 ms        2.6x
view_point         4.99 us        12.4 us        2.5x
view_filter        764 us         1.79 ms        2.3x
savepoint_rollback 4.20 ms        9.30 ms        2.2x
recursive_cte      156 us         264 us         1.7x
correlated_exists  4.02 ms        6.55 ms        1.63x
window_rank        149 ms         238 ms         1.6x
distinct           2.43 ms        3.80 ms        1.56x
insert             124 us         188 us         1.5x
sum                1.44 ms        1.89 ms        1.31x
insert_select      853 us         1.13 ms        1.3x
scan               10.83 ms       13.59 ms       1.25x
window_agg         67.7 ms        75.3 ms        1.11x
update             26.9 us        29.5 us        1.10x
delete             178 us         165 us         0.93x
join               348 us         253 us         0.73x
union              1.01 ms        252 us         0.25x
savepoint_create   3.66 us        685 ns         0.19x
```

### Native DATE / TIMESTAMP (Citadel only, SQLite has no native type)

```
Benchmark          Citadel
-------------------------------
date_sort          1.23 ms
date_arith         1.79 ms
date_range_scan    1.86 ms
date_groupby       16.8 ms
date_extract       21.3 ms
```

<details>
<summary>Methodology</summary>

- **correlated_in** - `SELECT COUNT(*) FROM t WHERE id IN (SELECT id FROM ref_table WHERE ref_table.val = t.age)`
- **count** - `SELECT COUNT(*) FROM t`
- **correlated_scalar** - `SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.a_id = a.id) FROM a`
- **point** - `SELECT * FROM t WHERE id = 50000`
- **date_arith** - `SELECT COUNT(*) FROM events WHERE ts + INTERVAL '1 day' > TIMESTAMP '2024-06-01 00:00:00'`
- **group_by** - `SELECT age, COUNT(*) FROM t GROUP BY age`
- **cte** - `WITH filtered AS (SELECT ... WHERE age < 50) SELECT age, COUNT(*) FROM filtered GROUP BY age`
- **view_point** - `SELECT * FROM v WHERE id = 50000`
- **savepoint_rollback** - `BEGIN; INSERT 1K rows; SAVEPOINT sp; INSERT 10K rows; ROLLBACK TO sp; COMMIT`
- **date_groupby** - `SELECT DATE_TRUNC('month', ts), COUNT(*) FROM events GROUP BY 1`
- **date_sort** - `SELECT id FROM events ORDER BY ts LIMIT 100`
- **view_filter** - `SELECT * FROM v WHERE age = 42`
- **filter** - `SELECT * FROM t WHERE age = 42`
- **window_agg** - `SUM(age) OVER (ORDER BY id ROWS 50 PRECEDING), MIN(age) OVER ...`
- **sort** - `SELECT * FROM t ORDER BY age LIMIT 10`
- **window_rank** - `ROW_NUMBER() OVER (PARTITION BY age ORDER BY id), RANK() OVER ...`
- **savepoint_nested** - 10 nested savepoints each with 100-row INSERT, alternating RELEASE/ROLLBACK TO
- **date_range_scan** - `SELECT COUNT(*) FROM events WHERE d BETWEEN DATE '2024-02-01' AND DATE '2024-03-31'`
- **insert** - 100 rows in one transaction
- **scan** - `SELECT * FROM t`
- **insert_select** - `INSERT INTO sink SELECT id, val FROM a`
- **sum** - `SELECT SUM(age) FROM t`
- **delete** - insert 100 rows then delete them
- **join** - `SELECT a.id, b.data FROM a INNER JOIN b ON a.id = b.a_id`
- **distinct** - `SELECT DISTINCT age FROM t`
- **correlated_exists** - `SELECT COUNT(*) FROM t WHERE EXISTS (SELECT 1 FROM ref_table WHERE ref_table.id = t.id)`
- **update** - `UPDATE t SET age = age + 1 WHERE id BETWEEN 10000 AND 10099`
- **date_extract** - `SELECT AVG(EXTRACT(HOUR FROM ts)) FROM events`
- **union** - `SELECT id, val FROM a UNION ALL SELECT id, data FROM b`
- **recursive_cte** - `WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 1000) SELECT SUM(x) FROM seq`

SQLite config: `journal_mode=OFF, synchronous=OFF, cache_size=8192` (~32 MB).
Citadel config: `SyncMode::Off, cache_size=4096` (~32 MB).
Both run with durability disabled to measure pure engine overhead, not disk I/O.

Reproduce with `cargo bench -p citadeldb-sql --bench h2h_bench`

</details>

## Quick Start

### Library

```rust
use citadel::DatabaseBuilder;
use citadel_sql::Connection;

let db = DatabaseBuilder::new("my.db")
    .passphrase(b"secret")
    .create()?;

let mut conn = Connection::open(&db)?;
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")?;
conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
let result = conn.query("SELECT * FROM users;")?;

// Key-value API
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

// In-memory (no file I/O - useful for testing and WASM)
let mem_db = DatabaseBuilder::new("")
    .passphrase(b"secret")
    .create_in_memory()?;
```

### CLI

```bash
citadel --create my.db

citadel> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
citadel> INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
citadel> SELECT * FROM users;
+----+-------+
| id | name  |
+----+-------+
|  1 | Alice |
|  2 | Bob   |
+----+-------+

citadel> .backup mydb.bak
citadel> .verify
citadel> .stats
citadel> .audit verify
citadel> .rekey
citadel> .compact clean.db
citadel> .dump users

# P2P sync
citadel> .keygen
citadel> .listen 4248 <KEY>              # Terminal A
citadel> .sync 127.0.0.1:4248 <KEY>      # Terminal B
```

## SQL

**Statements** - CREATE/DROP TABLE, ALTER TABLE (ADD/DROP/RENAME COLUMN, RENAME TABLE), CREATE/DROP INDEX, CREATE/DROP VIEW, INSERT (VALUES, SELECT), SELECT, UPDATE, DELETE, BEGIN/COMMIT/ROLLBACK, SAVEPOINT/RELEASE/ROLLBACK TO, EXPLAIN

**Constraints** - PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT, CHECK (column + table level), FOREIGN KEY (RESTRICT/NO ACTION)

**Types** - INTEGER, REAL, TEXT, BLOB, BOOLEAN, DATE, TIME, TIMESTAMP (WITH TIME ZONE), INTERVAL

**Clauses** - JOINs (INNER, LEFT, RIGHT, CROSS), subqueries (scalar, IN, EXISTS, correlated), CTEs (WITH / WITH RECURSIVE), UNION/INTERSECT/EXCEPT [ALL], CASE, BETWEEN, LIKE, DISTINCT, GROUP BY/HAVING, ORDER BY, LIMIT/OFFSET

**Window functions** - ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, SUM/COUNT/AVG/MIN/MAX OVER with PARTITION BY, ORDER BY, ROWS/RANGE frames

**Views** - CREATE/DROP VIEW, OR REPLACE, IF NOT EXISTS/IF EXISTS, column aliases, nested views

**Functions** - COUNT, SUM, AVG, MIN, MAX, LENGTH, UPPER, LOWER, SUBSTR, ABS, ROUND, COALESCE, NULLIF, CAST, TYPEOF, HEX, TRIM, REPLACE, RANDOM, IIF, GLOB

**Date/Time Functions** - NOW, CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME, LOCALTIMESTAMP, LOCALTIME, CLOCK_TIMESTAMP, EXTRACT, DATE_PART, DATE_TRUNC, DATE_BIN, AGE, MAKE_DATE, MAKE_TIME, MAKE_TIMESTAMP, MAKE_INTERVAL, JUSTIFY_DAYS, JUSTIFY_HOURS, JUSTIFY_INTERVAL, ISFINITE, DATE, TIME, DATETIME, STRFTIME, JULIANDAY, UNIXEPOCH, TIMEDIFF, AT TIME ZONE. Supports `INTERVAL '1 year 2 months'`, `DATE '2024-01-15'`, `TIMESTAMP '2024-01-15 12:30:00Z'`, `infinity`/`-infinity` sentinels, BC dates, full IANA zone parsing (jiff), PG-normalized INTERVAL comparison.

**Prepared statements** - `$1, $2, ...` positional parameters with LRU statement cache

## Security

**No plaintext on disk.** Every page is encrypted before writing and authenticated before reading.

**Separate key file.** Encryption keys live in `{dbname}.citadel-keys`, not inside the database. The passphrase derives a master key in memory via Argon2id (or PBKDF2 in FIPS mode) and never touches disk.

**Key backup.** Export an encrypted key backup with a separate recovery passphrase. Restore access without re-encrypting the entire database.

**Instant rekey.** Changing the passphrase re-wraps the root encryption key. No page re-encryption - instant regardless of database size.

**Encrypted sync.** Noise protocol (`NNpsk0_25519_ChaChaPoly_BLAKE2s`) with a 256-bit pre-shared key. Ephemeral Curve25519 keys per session for forward secrecy.

## Architecture

```
+-------------+---------------+---------------+
| citadel-cli | citadel-ffi   | citadel-wasm  |  CLI, C FFI, WebAssembly
+-------------+---------------+---------------+
|                 citadel-sql                  |  SQL parser, planner, executor
+---------------------------------------------+
|                  citadel                     |  Database API, builder, sync
+--------------+--------------+---------------+
|  citadel-txn | citadel-sync | citadel-crypto|  Transactions, replication, keys
+--------------+--------------+---------------+
|citadel-buffer|         citadel-page         |  Buffer pool (SIEVE), page codec
+--------------+------------------------------+
|              citadel-io                      |  File I/O, fsync, io_uring
+---------------------------------------------+
|              citadel-core                    |  Types, errors, constants
+---------------------------------------------+
```

### Page Layout (8,208 bytes)

```
+----------+--------------------+----------+
|  IV 16B  |  Ciphertext 8160B  |  MAC 32B |
+----------+--------------------+----------+
```

Fresh random IV per page. HMAC verified before decryption.

### Commit Protocol

Shadow paging with a god byte - one byte selects the active commit slot. Atomic commits without WAL:

1. Write dirty pages to new locations (CoW)
2. Compute Merkle hashes bottom-up
3. Update the inactive commit slot
4. Flip the god byte

## Language Bindings

### C / C++

Static or dynamic library with auto-generated `citadel.h` (cbindgen). All 37 functions are panic-safe.

```c
#include "citadel.h"

CitadelDb *db = NULL;
citadel_create("my.db", "secret", 6, &db);

CitadelWriteTxn *wtx = NULL;
citadel_write_begin(db, &wtx);
citadel_write_put(wtx, (const uint8_t*)"key", 3, (const uint8_t*)"val", 3);
citadel_write_commit(wtx);

CitadelSqlConn *conn = NULL;
citadel_sql_open(db, &conn);
CitadelSqlResult *result = NULL;
citadel_sql_execute(conn, "SELECT * FROM users;", &result);

citadel_close(db);
```

### WebAssembly

```js
import { CitadelDb } from "@citadeldb/wasm";

const db = new CitadelDb("secret");
db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);");
db.execute("INSERT INTO t (id, name) VALUES (1, 'Alice');");

const result = db.query("SELECT * FROM t;");
// { columns: ["id", "name"], rows: [[1, "Alice"]] }

db.put(new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6]));
```

Build: `wasm-pack build crates/citadel-wasm --target web`

## Building

Rust 1.75+.

```bash
git clone https://github.com/yp3y5akh0v/citadel.git
cd citadel
cargo build --release
```

### Feature Flags

| Flag | Description |
|------|-------------|
| `audit-log` | HMAC-chained tamper-evident audit log (default: on) |
| `fips` | FIPS 140-3: PBKDF2 + AES-256-CTR only |
| `io-uring` | Linux io_uring async I/O |

## License

[MIT](LICENSE-MIT) OR [Apache-2.0](LICENSE-APACHE)
