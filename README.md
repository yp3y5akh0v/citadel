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

Every page is encrypted and authenticated before it hits disk. The database file is always opaque. Wins all 50 head-to-head benchmarks against unencrypted SQLite at equal cache budgets.

## Features

- **Encrypted at rest** - AES-256-CTR + HMAC-SHA256 per page, verified before decryption
- **SQL** - JOINs (INNER, LEFT, RIGHT, CROSS, FULL OUTER, LATERAL), subqueries, CTEs (recursive + WITH-DML), UNION/INTERSECT/EXCEPT, window functions, views, aggregates, indexes (partial, COLLATE, GIN, FTS), constraints, generated columns (STORED + VIRTUAL), STRICT tables, COLLATE (BINARY/NOCASE/RTRIM), JSON/JSONB types with 12 PG operators (`->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`, `#-`, `@?`, `@@`), `JSON_TABLE` / `JSON_EXISTS` / `JSON_VALUE` / `JSON_QUERY` + SQL/JSON predicate path language, 16 JSON scalar functions + 4 JSONB aggregates, set-returning JSON functions (`jsonb_array_elements`, `jsonb_each`, `jsonb_object_keys`), full-text search (FTS) with `tsvector`/`tsquery` types, `@@` match operator, ranking (`ts_rank`, `ts_rank_cd`), inverted FTS indexes (`CREATE INDEX … USING fts`), system catalog (`information_schema.*`, `pg_timezone_names`, `pg_timezone_abbrevs`), ALTER TABLE, TRUNCATE, UPSERT (`ON CONFLICT`), RETURNING (with `OLD/NEW`), full FK actions (CASCADE / SET NULL / SET DEFAULT / RESTRICT), prepared statements with snapshot-tagged plan caching
- **ACID** - Copy-on-Write B+ tree, shadow paging, no WAL. Snapshot isolation with concurrent readers
- **P2P sync** - Merkle-based table diffing over Noise-encrypted channels with PSK auth
- **CLI** - SQL shell with tab completion, syntax highlighting, dot-commands (.backup, .verify, .rekey, .sync, .dump, ...)
- **3-tier key hierarchy** - Passphrase -> Argon2id -> Master Key -> AES-KW -> REK -> HKDF -> DEK + MAC
- **FIPS 140-3** - PBKDF2-HMAC-SHA256 + AES-256-CTR when compliance requires it
- **Audit log** - HMAC-SHA256 chained, tamper-evident
- **Hot backup** - Consistent snapshots via MVCC, no write blocking
- **Overflow pages** - Large values handled transparently, no size limits
- **Cross-platform** - Windows, Linux, macOS. C FFI (37 functions), WebAssembly bindings
- **4,000+ tests** - Unit, integration, torture tests across 11 crates

## Benchmarks

Single-threaded on 100K rows, schema `(id INTEGER PK, name TEXT, age INTEGER)`. Ratio = SQLite / Citadel time.

```
Benchmark              Citadel        SQLite         Ratio
----------------------------------------------------------
full_outer_join        61.9 us        22.4 ms        362x
correlated_in          5.95 ms        1.89 s         318x
count                  148 ns         21.3 us        144x
correlated_scalar      300 us         20.2 ms        67x
point                  930 ns         12.7 us        14x
fts_rank               4.91 ms        40.2 ms        8.2x
group_by               1.35 ms        9.79 ms        7.2x
cte                    1.24 ms        5.78 ms        4.7x
union                  28.9 us        136 us         4.7x
view_point             3.08 us        12.8 us        4.2x
truncate               18.9 us        58.3 us        3.1x
partial_index_point    4.59 us        13.1 us        2.85x
upsert_returning       60.8 us        167 us         2.75x
insert_returning       63.7 us        167 us         2.62x
fts_match              3.02 ms        7.37 ms        2.44x
window_agg             33.2 ms        77.5 ms        2.33x
jsonb_contains         11.7 ms        27.1 ms        2.32x
fts_phrase             4.29 ms        9.17 ms        2.14x
savepoint_create       329 ns         696 ns         2.12x
sort                   1.29 ms        2.53 ms        1.96x
view_filter            877 us         1.71 ms        1.95x
upsert_counter         27.5 us        53.1 us        1.93x
delete_returning       90.7 us        175 us         1.93x
filter                 943 us         1.80 ms        1.91x
insert_select          613 us         1.12 ms        1.83x
json_extract           17.2 ms        31.0 ms        1.80x
join                   50.5 us        89.2 us        1.77x
window_rank            68.1 ms        119.5 ms       1.76x
delete                 44.9 us        71.0 us        1.58x
recursive_cte          75.7 us        117.9 us       1.56x
update                 18.0 us        27.8 us        1.54x
savepoint_nested       236 us         361 us         1.53x
upsert_dedup           21.3 us        32.4 us        1.52x
correlated_exists      4.64 ms        6.61 ms        1.43x
with_dml               76.9 us        108 us         1.40x
distinct               2.83 ms        3.80 ms        1.34x
fk_cascade_delete_only 59.7 us        77.4 us        1.30x
update_returning       113 us         146 us         1.29x
insert                 39.2 us        50.5 us        1.29x
savepoint_rollback     1.75 ms        2.20 ms        1.26x
sort_nocase            2.53 ms        3.02 ms        1.19x
insert_gen_virtual     47.0 us        54.5 us        1.16x
sum                    1.60 ms        1.83 ms        1.14x
insert_gen_stored      50.0 us        56.7 us        1.13x
upsert_all_new         45.0 us        51.0 us        1.13x
update_gen_propagate   42.8 us        47.5 us        1.11x
upsert_mixed           52.3 us        57.6 us        1.10x
scan                   7.31 ms        7.69 ms        1.05x
select_gen_virtual     17.0 us        17.7 us        1.04x
fk_cascade             86.5 us        89.4 us        1.03x
```

50 head-to-head benchmarks. Citadel wins all 50. Geometric mean speedup: ~2.8x.

### Citadel-only (no direct SQLite equivalent)

```
Benchmark           Citadel
-------------------------------
date_extract        13.6 ms
date_groupby        9.54 ms
json_table          8.07 ms
lateral             2.65 ms
date_arith          1.74 ms
date_range_scan     1.71 ms
date_sort           1.46 ms
```

### Index speedups (citadel-internal)

```
Benchmark              Without index    With index     Speedup
---------------------------------------------------------------
json_gin               11.2 ms          36.4 us        308x
fts_index              1.29 s           3.14 ms        412x
```

<details>
<summary>Methodology</summary>

H2H benchmarks:

- **correlated_in** - `SELECT COUNT(*) FROM t WHERE id IN (SELECT id FROM ref_table WHERE ref_table.val = t.age)`
- **full_outer_join** - `SELECT a.id, b.data FROM a FULL OUTER JOIN b ON a.id = b.a_id`
- **count** - `SELECT COUNT(*) FROM t`
- **correlated_scalar** - `SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.a_id = a.id) FROM a`
- **point** - `SELECT * FROM t WHERE id = 50000`
- **group_by** - `SELECT age, COUNT(*) FROM t GROUP BY age`
- **partial_index_point** - `SELECT * FROM t WHERE email = ? AND deleted_at IS NULL`
- **cte** - `WITH filtered AS (SELECT ... WHERE age < 50) SELECT age, COUNT(*) FROM filtered GROUP BY age`
- **view_point** - `SELECT * FROM v WHERE id = 50000`
- **truncate** - `TRUNCATE TABLE t`
- **insert_returning** - `INSERT INTO t (id, val) VALUES (...) RETURNING id, val`
- **upsert_returning** - `INSERT ... ON CONFLICT (id) DO UPDATE SET c = c + 1 RETURNING c`
- **view_filter** - `SELECT * FROM v WHERE age = 42`
- **filter** - `SELECT * FROM t WHERE age = 42`
- **window_agg** - `SUM(age) OVER (ORDER BY id ROWS 50 PRECEDING)`
- **jsonb_contains** - `SELECT id FROM users WHERE data @> '{"role":"admin"}'::jsonb`
- **savepoint_create** - `BEGIN; SAVEPOINT sp; RELEASE sp; COMMIT`
- **sort** - `SELECT * FROM t ORDER BY age LIMIT 10`
- **upsert_counter** - `INSERT ... ON CONFLICT (id) DO UPDATE SET c = c + 1`
- **window_rank** - `ROW_NUMBER() OVER (PARTITION BY age ORDER BY id)`
- **delete_returning** - `DELETE ... WHERE id = ? RETURNING id, val`
- **upsert_dedup** - `INSERT ... ON CONFLICT (id) DO NOTHING`
- **json_extract** - `SELECT data ->> 'name' FROM users`
- **delete** - `DELETE FROM t WHERE id = ?`
- **update** - `UPDATE t SET age = age + 1 WHERE id BETWEEN 10000 AND 10099`
- **correlated_exists** - `SELECT COUNT(*) FROM t WHERE EXISTS (SELECT 1 FROM ref_table WHERE ref_table.id = t.id)`
- **savepoint_nested** - `BEGIN; SAVEPOINT sp1; ... ; RELEASE/ROLLBACK TO sp1; COMMIT`
- **with_dml** - `WITH d AS (DELETE FROM src RETURNING *) INSERT INTO archive SELECT * FROM d`
- **distinct** - `SELECT DISTINCT age FROM t`
- **insert_select** - `INSERT INTO sink SELECT id, val FROM a`
- **savepoint_rollback** - `BEGIN; INSERT 1K rows; SAVEPOINT sp; INSERT 10K rows; ROLLBACK TO sp; COMMIT`
- **update_returning** - `UPDATE t SET c = c + ? WHERE id = ? RETURNING c`
- **insert** - `INSERT INTO t (id, val) VALUES (?, ?)`
- **scan** - `SELECT * FROM t`
- **sort_nocase** - `SELECT name FROM t ORDER BY name COLLATE NOCASE LIMIT 10`
- **sum** - `SELECT SUM(age) FROM t`
- **insert_gen_virtual** - `INSERT INTO t (id, a, b) VALUES (?, ?, ?)`
- **union** - `SELECT id, val FROM a UNION ALL SELECT id, data FROM b`
- **select_gen_virtual** - `SELECT id, s FROM t WHERE s > ?`
- **update_gen_propagate** - `UPDATE t SET a = a + ? WHERE id = ?`
- **upsert_mixed** - `INSERT ... ON CONFLICT (id) DO UPDATE SET c = c + 1`
- **upsert_all_new** - `INSERT ... ON CONFLICT (id) DO NOTHING`
- **recursive_cte** - `WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 1000) SELECT SUM(x) FROM seq`
- **insert_gen_stored** - `INSERT INTO t (id, a, b) VALUES (?, ?, ?)`
- **fk_cascade** - `DELETE FROM parent WHERE id = ?`
- **fk_cascade_delete_only** - `DELETE FROM parent WHERE id = ?` without index on child
- **join** - `SELECT a.id, b.data FROM a INNER JOIN b ON a.id = b.a_id`
- **fts_match** - `SELECT id FROM docs WHERE body @@ to_tsquery('rust & database')`
- **fts_phrase** - `SELECT id FROM docs WHERE body @@ phraseto_tsquery('rust database')`
- **fts_rank** - `SELECT id, ts_rank(body, to_tsquery('rust & database')) FROM docs WHERE body @@ ... ORDER BY r DESC LIMIT 10`

Citadel-only benchmarks:

- **date_extract** - `SELECT AVG(EXTRACT(HOUR FROM ts)) FROM events`
- **date_groupby** - `SELECT DATE_TRUNC('month', ts), COUNT(*) FROM events GROUP BY 1`
- **json_table** - `SELECT a, b, c FROM JSON_TABLE(j, '$[*]' COLUMNS (a INT PATH '$.a', b TEXT PATH '$.b', c INT PATH '$.c'))`
- **lateral** - `SELECT c.id, p.name FROM c, LATERAL (SELECT name FROM p WHERE p.cat_id = c.id ORDER BY price DESC LIMIT 1) p`
- **date_range_scan** - `SELECT COUNT(*) FROM events WHERE d BETWEEN DATE '2024-02-01' AND DATE '2024-03-31'`
- **date_arith** - `SELECT COUNT(*) FROM events WHERE ts + INTERVAL '1 day' > TIMESTAMP '2024-06-01 00:00:00'`
- **date_sort** - `SELECT id FROM events ORDER BY ts LIMIT 100`
- **json_gin** - `SELECT id FROM users WHERE data @> '{"role":"admin"}'::jsonb` with vs without `CREATE INDEX ... USING gin (data)`
- **fts_index** - `SELECT id FROM docs WHERE body @@ to_tsquery('...')` with vs without `CREATE INDEX … USING fts (body)` (`body` is a `TSVECTOR` column)

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

**Full-text search** - `tsvector` / `tsquery` types, `to_tsvector` / `to_tsquery` / `plainto_tsquery` / `phraseto_tsquery` / `websearch_to_tsquery` builders, `@@` match operator, `ts_rank` / `ts_rank_cd` ranking with weighted positions (A/B/C/D), prefix matching (`term:*`), phrase distance (`<N>`), inverted indexes via `CREATE INDEX … USING fts` for ~400× speedup over sequential scan

**System catalog** - `information_schema.tables`, `information_schema.columns`, `information_schema.key_column_usage`, `information_schema.table_constraints`, `pg_timezone_names`, `pg_timezone_abbrevs` (virtual tables, queryable)

**Prepared statements** - `$1, $2, ...` positional parameters with LRU statement cache plus snapshot-tagged plan caching for joins and compound queries (cache invalidates only on commit, never per-call)

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
