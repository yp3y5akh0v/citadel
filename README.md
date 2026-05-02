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

Every page is encrypted and authenticated before it hits disk. The database file is always opaque. Wins all 42 head-to-head benchmarks against unencrypted SQLite at equal cache budgets.

## Features

- **Encrypted at rest** - AES-256-CTR + HMAC-SHA256 per page, verified before decryption
- **SQL** - JOINs, subqueries, CTEs (recursive + WITH-DML), UNION/INTERSECT/EXCEPT, window functions, views, aggregates, indexes (partial), constraints, generated columns (STORED + VIRTUAL), ALTER TABLE, TRUNCATE, UPSERT (`ON CONFLICT`), RETURNING (with `OLD/NEW`), full FK actions (CASCADE / SET NULL / SET DEFAULT / RESTRICT), prepared statements
- **ACID** - Copy-on-Write B+ tree, shadow paging, no WAL. Snapshot isolation with concurrent readers
- **P2P sync** - Merkle-based table diffing over Noise-encrypted channels with PSK auth
- **CLI** - SQL shell with tab completion, syntax highlighting, dot-commands (.backup, .verify, .rekey, .sync, .dump, ...)
- **3-tier key hierarchy** - Passphrase -> Argon2id -> Master Key -> AES-KW -> REK -> HKDF -> DEK + MAC
- **FIPS 140-3** - PBKDF2-HMAC-SHA256 + AES-256-CTR when compliance requires it
- **Audit log** - HMAC-SHA256 chained, tamper-evident
- **Hot backup** - Consistent snapshots via MVCC, no write blocking
- **Overflow pages** - Large values handled transparently, no size limits
- **Cross-platform** - Windows, Linux, macOS. C FFI (37 functions), WebAssembly bindings
- **3,200+ tests** - Unit, integration, torture tests across 10 crates

## Benchmarks

Single-threaded on 100K rows, schema `(id INTEGER PK, name TEXT, age INTEGER)`. Ratio = SQLite / Citadel time.

```
Benchmark              Citadel        SQLite         Ratio
----------------------------------------------------------
correlated_in          5.82 ms        1.86 s         320x
count                  154 ns         21.3 us        139x
correlated_scalar      288 us         18.72 ms       65x
point                  769 ns         12.4 us        16.1x
group_by               1.27 ms        9.99 ms        7.89x
cte                    1.17 ms        5.99 ms        5.10x
partial_index_point    2.42 us        12.31 us       5.08x
view_point             2.81 us        12.4 us        4.41x
truncate               17.6 us        57.4 us        3.26x
insert_returning       59.3 us        166 us         2.80x
upsert_returning       60.1 us        166 us         2.77x
filter                 747 us         1.79 ms        2.40x
view_filter            757 us         1.71 ms        2.27x
window_agg             32.1 ms        72.1 ms        2.25x
savepoint_create       314 ns         701 ns         2.23x
sort                   1.21 ms        2.63 ms        2.17x
upsert_counter         24.7 us        52.0 us        2.11x
window_rank            57.6 ms        121 ms         2.10x
delete_returning       88.1 us        164 us         1.86x
upsert_dedup           18.1 us        31.5 us        1.75x
savepoint_nested       214 us         344 us         1.61x
delete                 44.2 us        70.9 us        1.60x
correlated_exists      4.34 ms        6.74 ms        1.55x
update                 18.0 us        27.6 us        1.54x
insert_select          718 us         1.09 ms        1.52x
with_dml               73.4 us        107 us         1.46x
distinct               2.66 ms        3.85 ms        1.45x
savepoint_rollback     1.50 ms        2.15 ms        1.44x
insert                 37.0 us        49.9 us        1.35x
update_returning       111 us         145 us         1.31x
scan                   6.28 ms        7.77 ms        1.24x
insert_gen_virtual     44.5 us        53.9 us        1.21x
sum                    1.58 ms        1.89 ms        1.20x
insert_gen_stored      47.5 us        55.5 us        1.17x
upsert_all_new         43.0 us        49.9 us        1.16x
select_gen_virtual     15.6 us        17.6 us        1.13x
recursive_cte          103 us         116 us         1.12x
update_gen_propagate   39.7 us        44.5 us        1.12x
upsert_mixed           51.4 us        57.2 us        1.11x
union                  125 us         136 us         1.09x
fk_cascade             84.9 us        88.2 us        1.04x
join                   88.6 us        90.6 us        1.02x
```

### Native DATE / TIMESTAMP (Citadel only, SQLite has no native type)

```
Benchmark          Citadel
-------------------------------
date_sort          1.33 ms
date_range_scan    1.70 ms
date_arith         1.74 ms
date_groupby       9.50 ms
date_extract       12.63 ms
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
- **partial_index_point** - `SELECT * FROM t WHERE email = ? AND deleted_at IS NULL`
- **view_point** - `SELECT * FROM v WHERE id = 50000`
- **truncate** - `TRUNCATE TABLE t`
- **insert_returning** - `INSERT INTO t (id, val) VALUES (...) RETURNING id, val`
- **upsert_returning** - `INSERT ... ON CONFLICT (id) DO UPDATE SET c = c + 1 RETURNING c`
- **filter** - `SELECT * FROM t WHERE age = 42`
- **view_filter** - `SELECT * FROM v WHERE age = 42`
- **window_agg** - `SUM(age) OVER (ORDER BY id ROWS 50 PRECEDING)`
- **savepoint_create** - `BEGIN; SAVEPOINT sp; RELEASE sp; COMMIT`
- **sort** - `SELECT * FROM t ORDER BY age LIMIT 10`
- **upsert_counter** - `INSERT ... ON CONFLICT (id) DO UPDATE SET c = c + 1`
- **window_rank** - `ROW_NUMBER() OVER (PARTITION BY age ORDER BY id)`
- **delete_returning** - `DELETE ... WHERE id = ? RETURNING id, val`
- **upsert_dedup** - `INSERT ... ON CONFLICT (id) DO NOTHING`
- **savepoint_nested** - `BEGIN; SAVEPOINT sp1; ... ; RELEASE/ROLLBACK TO sp1; COMMIT`
- **delete** - `DELETE FROM t WHERE id = ?`
- **correlated_exists** - `SELECT COUNT(*) FROM t WHERE EXISTS (SELECT 1 FROM ref_table WHERE ref_table.id = t.id)`
- **update** - `UPDATE t SET age = age + 1 WHERE id BETWEEN 10000 AND 10099`
- **insert_select** - `INSERT INTO sink SELECT id, val FROM a`
- **with_dml** - `WITH d AS (DELETE FROM src RETURNING *) INSERT INTO archive SELECT * FROM d`
- **distinct** - `SELECT DISTINCT age FROM t`
- **savepoint_rollback** - `BEGIN; INSERT 1K rows; SAVEPOINT sp; INSERT 10K rows; ROLLBACK TO sp; COMMIT`
- **insert** - `INSERT INTO t (id, val) VALUES (?, ?)`
- **update_returning** - `UPDATE t SET c = c + ? WHERE id = ? RETURNING c`
- **scan** - `SELECT * FROM t`
- **insert_gen_virtual** - `INSERT INTO t (id, a, b) VALUES (?, ?, ?)`
- **sum** - `SELECT SUM(age) FROM t`
- **insert_gen_stored** - `INSERT INTO t (id, a, b) VALUES (?, ?, ?)`
- **upsert_all_new** - `INSERT ... ON CONFLICT (id) DO NOTHING`
- **select_gen_virtual** - `SELECT id, s FROM t WHERE s > ?`
- **recursive_cte** - `WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 1000) SELECT SUM(x) FROM seq`
- **update_gen_propagate** - `UPDATE t SET a = a + ? WHERE id = ?`
- **upsert_mixed** - `INSERT ... ON CONFLICT (id) DO UPDATE SET c = c + 1`
- **union** - `SELECT id, val FROM a UNION ALL SELECT id, data FROM b`
- **fk_cascade** - `DELETE FROM A WHERE id = ?` cascading through `A→B→C→D→E`
- **join** - `SELECT a.id, b.data FROM a INNER JOIN b ON a.id = b.a_id`

Date benchmarks (Citadel-only, sorted by duration):

- **date_sort** - `SELECT id FROM events ORDER BY ts LIMIT 100`
- **date_range_scan** - `SELECT COUNT(*) FROM events WHERE d BETWEEN DATE '2024-02-01' AND DATE '2024-03-31'`
- **date_arith** - `SELECT COUNT(*) FROM events WHERE ts + INTERVAL '1 day' > TIMESTAMP '2024-06-01 00:00:00'`
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

**Statements** - CREATE/DROP TABLE, ALTER TABLE (ADD/DROP/RENAME COLUMN, RENAME TABLE), CREATE/DROP INDEX (incl. partial `WHERE`), CREATE/DROP VIEW, INSERT (VALUES, SELECT, ON CONFLICT DO NOTHING/DO UPDATE, ON CONSTRAINT), SELECT, UPDATE, DELETE, TRUNCATE TABLE, RETURNING (with `OLD`/`NEW`), BEGIN/COMMIT/ROLLBACK, SAVEPOINT/RELEASE/ROLLBACK TO, SET TIME ZONE, EXPLAIN

**Constraints** - PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT, CHECK (column + table level), FOREIGN KEY with full referential actions (`ON DELETE` / `ON UPDATE` `CASCADE` / `SET NULL` / `SET DEFAULT` / `RESTRICT` / `NO ACTION`), GENERATED ALWAYS AS (...) STORED|VIRTUAL

**Types** - INTEGER, REAL, TEXT, BLOB, BOOLEAN, DATE, TIME, TIMESTAMP (WITH TIME ZONE), INTERVAL

**Clauses** - JOINs (INNER, LEFT, RIGHT, CROSS), subqueries (scalar, IN, EXISTS, correlated), CTEs (`WITH` / `WITH RECURSIVE` / WITH-DML: `WITH x AS (INSERT/UPDATE/DELETE … [RETURNING *]) SELECT …`), UNION/INTERSECT/EXCEPT [ALL], CASE, BETWEEN, LIKE, DISTINCT, GROUP BY/HAVING, ORDER BY, LIMIT/OFFSET

**Window functions** - ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, SUM/COUNT/AVG/MIN/MAX OVER with PARTITION BY, ORDER BY, ROWS/RANGE frames

**Views** - CREATE/DROP VIEW, OR REPLACE, IF NOT EXISTS/IF EXISTS, column aliases, nested views

**Functions** - COUNT, SUM, AVG, MIN, MAX, LENGTH, UPPER, LOWER, SUBSTR/SUBSTRING, TRIM/LTRIM/RTRIM, REPLACE, INSTR, CONCAT, HEX, ABS, ROUND, CEIL/CEILING, FLOOR, SIGN, SQRT, RANDOM, COALESCE, NULLIF, CAST, TYPEOF, IIF

**Date/Time Functions** - NOW, CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME, LOCALTIMESTAMP, LOCALTIME, CLOCK_TIMESTAMP, EXTRACT, DATE_PART, DATE_TRUNC, DATE_BIN, AGE, MAKE_DATE, MAKE_TIME, MAKE_TIMESTAMP, MAKE_INTERVAL, JUSTIFY_DAYS, JUSTIFY_HOURS, JUSTIFY_INTERVAL, ISFINITE, DATE, TIME, DATETIME, STRFTIME, JULIANDAY, UNIXEPOCH, TIMEDIFF, AT TIME ZONE. Supports `INTERVAL '1 year 2 months'`, `DATE '2024-01-15'`, `TIMESTAMP '2024-01-15 12:30:00Z'`, `infinity`/`-infinity` sentinels, BC dates, full IANA zone parsing (jiff), PG-normalized INTERVAL comparison.

**Prepared statements** - `$1, $2, ...` positional parameters with LRU statement cache

**Multi-statement scripts** - `Connection::execute_script(sql)` runs `;`-separated statements in one call, returning per-statement outcomes with partial-success preserved. WASM: `db.run(sql)` returns `[{type, ...}, ...]`.

**UPSERT** - `INSERT ... ON CONFLICT (cols) DO NOTHING` / `DO UPDATE SET col = excluded.col ... WHERE ...` and `ON CONFLICT ON CONSTRAINT idx_name`. `excluded.*` refers to the proposed row; bare `col` refers to the existing row. Single-descent storage primitive: on the canonical `DO UPDATE SET counter = counter + 1` pattern, Citadel is ~2× faster than SQLite.

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
