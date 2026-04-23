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

Every page is encrypted and authenticated before it hits disk. The database file is always opaque. Beats unencrypted SQLite in all 26 head-to-head benchmarks with equal cache budgets.

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
- **2,700+ tests** - Unit, integration, torture tests across 10 crates

## Benchmarks

Single-threaded on 100K rows, schema `(id INTEGER PK, name TEXT, age INTEGER)`. Ratio = SQLite / Citadel time.

```
Benchmark          Citadel        SQLite         Ratio
------------------------------------------------------
correlated_in      6.20 ms        1.94 s         312x
count              146 ns         21.4 us        147x
correlated_scalar  298 us         18.5 ms        62x
point              808 ns         12.4 us        15.3x
group_by           1.20 ms        10.5 ms        8.75x
cte                1.09 ms        6.23 ms        5.74x
view_point         3.01 us        12.9 us        4.28x
filter             726 us         1.73 ms        2.38x
sort               1.09 ms        2.59 ms        2.37x
view_filter        737 us         1.74 ms        2.36x
window_agg         32.1 ms        74.0 ms        2.31x
savepoint_create   343 ns         716 ns         2.09x
window_rank        60.8 ms        124 ms         2.04x
insert_select      627 us         1.15 ms        1.83x
correlated_exists  3.94 ms        6.74 ms        1.71x
distinct           2.39 ms        3.97 ms        1.66x
update             17.9 us        27.95 us       1.56x
delete             55.8 us        78.5 us        1.41x
scan               6.13 ms        7.90 ms        1.29x
recursive_cte      104 us         130 us         1.26x
sum                1.48 ms        1.85 ms        1.25x
union              117 us         147 us         1.25x
savepoint_nested   304 us         367 us         1.21x
insert             44.0 us        52.6 us        1.19x
join               87.0 us        100 us         1.15x
savepoint_rollback 2.12 ms        2.23 ms        1.05x
```

### Native DATE / TIMESTAMP (Citadel only, SQLite has no native type)

```
Benchmark          Citadel
-------------------------------
date_arith         1.64 ms
date_range_scan    1.66 ms
date_sort          3.63 ms
date_groupby       9.11 ms
date_extract       12.88 ms
```

<details>
<summary>Methodology</summary>

H2H benchmarks (sorted by ratio, highest first):

- **correlated_in** - `SELECT COUNT(*) FROM t WHERE id IN (SELECT id FROM ref_table WHERE ref_table.val = t.age)`
- **count** - `SELECT COUNT(*) FROM t`
- **correlated_scalar** - `SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.a_id = a.id) FROM a`
- **point** - `SELECT * FROM t WHERE id = 50000`
- **group_by** - `SELECT age, COUNT(*) FROM t GROUP BY age`
- **cte** - `WITH filtered AS (SELECT ... WHERE age < 50) SELECT age, COUNT(*) FROM filtered GROUP BY age`
- **view_point** - `SELECT * FROM v WHERE id = 50000`
- **filter** - `SELECT * FROM t WHERE age = 42`
- **sort** - `SELECT * FROM t ORDER BY age LIMIT 10`
- **view_filter** - `SELECT * FROM v WHERE age = 42`
- **window_agg** - `SUM(age) OVER (ORDER BY id ROWS 50 PRECEDING), MIN(age) OVER ...`
- **savepoint_create** - `BEGIN; SAVEPOINT sp; RELEASE sp; COMMIT`
- **window_rank** - `ROW_NUMBER() OVER (PARTITION BY age ORDER BY id), RANK() OVER ...`
- **insert_select** - `INSERT INTO sink SELECT id, val FROM a`
- **correlated_exists** - `SELECT COUNT(*) FROM t WHERE EXISTS (SELECT 1 FROM ref_table WHERE ref_table.id = t.id)`
- **distinct** - `SELECT DISTINCT age FROM t`
- **update** - `UPDATE t SET age = age + 1 WHERE id BETWEEN 10000 AND 10099`
- **delete** - insert 100 rows then delete them
- **scan** - `SELECT * FROM t`
- **recursive_cte** - `WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 1000) SELECT SUM(x) FROM seq`
- **sum** - `SELECT SUM(age) FROM t`
- **union** - `SELECT id, val FROM a UNION ALL SELECT id, data FROM b`
- **savepoint_nested** - 10 nested savepoints each with 100-row INSERT, alternating RELEASE/ROLLBACK TO
- **insert** - 100 rows in one transaction
- **join** - `SELECT a.id, b.data FROM a INNER JOIN b ON a.id = b.a_id`
- **savepoint_rollback** - `BEGIN; INSERT 1K rows; SAVEPOINT sp; INSERT 10K rows; ROLLBACK TO sp; COMMIT`

Date benchmarks (Citadel-only, sorted by duration):

- **date_arith** - `SELECT COUNT(*) FROM events WHERE ts + INTERVAL '1 day' > TIMESTAMP '2024-06-01 00:00:00'`
- **date_range_scan** - `SELECT COUNT(*) FROM events WHERE d BETWEEN DATE '2024-02-01' AND DATE '2024-03-31'`
- **date_sort** - `SELECT id FROM events ORDER BY ts LIMIT 100`
- **date_groupby** - `SELECT DATE_TRUNC('month', ts), COUNT(*) FROM events GROUP BY 1`
- **date_extract** - `SELECT AVG(EXTRACT(HOUR FROM ts)) FROM events`

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

**Statements** - CREATE/DROP TABLE, ALTER TABLE (ADD/DROP/RENAME COLUMN, RENAME TABLE), CREATE/DROP INDEX, CREATE/DROP VIEW, INSERT (VALUES, SELECT), SELECT, UPDATE, DELETE, BEGIN/COMMIT/ROLLBACK, SAVEPOINT/RELEASE/ROLLBACK TO, SET TIME ZONE, EXPLAIN

**Constraints** - PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT, CHECK (column + table level), FOREIGN KEY (RESTRICT/NO ACTION)

**Types** - INTEGER, REAL, TEXT, BLOB, BOOLEAN, DATE, TIME, TIMESTAMP (WITH TIME ZONE), INTERVAL

**Clauses** - JOINs (INNER, LEFT, RIGHT, CROSS), subqueries (scalar, IN, EXISTS, correlated), CTEs (WITH / WITH RECURSIVE), UNION/INTERSECT/EXCEPT [ALL], CASE, BETWEEN, LIKE, DISTINCT, GROUP BY/HAVING, ORDER BY, LIMIT/OFFSET

**Window functions** - ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, SUM/COUNT/AVG/MIN/MAX OVER with PARTITION BY, ORDER BY, ROWS/RANGE frames

**Views** - CREATE/DROP VIEW, OR REPLACE, IF NOT EXISTS/IF EXISTS, column aliases, nested views

**Functions** - COUNT, SUM, AVG, MIN, MAX, LENGTH, UPPER, LOWER, SUBSTR/SUBSTRING, TRIM/LTRIM/RTRIM, REPLACE, INSTR, CONCAT, HEX, ABS, ROUND, CEIL/CEILING, FLOOR, SIGN, SQRT, RANDOM, COALESCE, NULLIF, CAST, TYPEOF, IIF

**Date/Time Functions** - NOW, CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME, LOCALTIMESTAMP, LOCALTIME, CLOCK_TIMESTAMP, EXTRACT, DATE_PART, DATE_TRUNC, DATE_BIN, AGE, MAKE_DATE, MAKE_TIME, MAKE_TIMESTAMP, MAKE_INTERVAL, JUSTIFY_DAYS, JUSTIFY_HOURS, JUSTIFY_INTERVAL, ISFINITE, DATE, TIME, DATETIME, STRFTIME, JULIANDAY, UNIXEPOCH, TIMEDIFF, AT TIME ZONE. Supports `INTERVAL '1 year 2 months'`, `DATE '2024-01-15'`, `TIMESTAMP '2024-01-15 12:30:00Z'`, `infinity`/`-infinity` sentinels, BC dates, full IANA zone parsing (jiff), PG-normalized INTERVAL comparison.

**Prepared statements** - `$1, $2, ...` positional parameters with LRU statement cache

**Multi-statement scripts** - `Connection::execute_script(sql)` runs `;`-separated statements in one call, returning per-statement outcomes with partial-success preserved. WASM: `db.run(sql)` returns `[{type, ...}, ...]`.

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
