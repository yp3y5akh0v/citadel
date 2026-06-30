<p align="center">
  <img src="https://raw.githubusercontent.com/yp3y5akh0v/citadel/HEAD/.github/banner.png" alt="Citadel" width="600">
</p>

<h1 align="center">Citadel</h1>

<p align="center">Encrypted-first embedded SQL database with a built-in memory engine.</p>

<p align="center">
  <a href="https://crates.io/crates/citadeldb"><img src="https://badgen.net/crates/v/citadeldb" alt="crates.io"></a>
  <a href="https://www.npmjs.com/package/@citadeldb/wasm"><img src="https://img.shields.io/npm/v/@citadeldb/wasm" alt="npm"></a>
  <a href="https://pypi.org/project/citadeldb/"><img src="https://img.shields.io/pypi/v/citadeldb" alt="PyPI"></a>
  <a href="https://github.com/yp3y5akh0v/citadel/actions/workflows/ci.yml"><img src="https://github.com/yp3y5akh0v/citadel/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/yp3y5akh0v/citadel/blob/HEAD/crates/citadel-membench/RESULTS.md"><img src="https://img.shields.io/badge/LoCoMo%20(gpt--4o--mini%2Fgemini--flash)-85.5%2F90.6%25-success" alt="LoCoMo 85.5% (gpt-4o-mini) / 90.6% (gemini-3.5-flash) readers"></a>
  <a href="https://github.com/yp3y5akh0v/citadel/blob/HEAD/crates/citadel-membench/RESULTS.md"><img src="https://img.shields.io/badge/LongMemEval%20oracle%20(gpt--4o)-90.6%25-success" alt="LongMemEval oracle 90.6% (gpt-4o reader)"></a>
  <a href="https://github.com/yp3y5akh0v/citadel#license"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue" alt="License"></a>
</p>

Citadel is an embedded SQL database that encrypts and authenticates every page with AES-256-CTR and HMAC-SHA256 before it is written, so the database file is always opaque. The same encrypted pages hold SQL tables and a zero-LLM memory engine that recalls over encrypted regions. The tables below report its results against unencrypted SQLite across 54 head-to-head benchmarks and on the LoCoMo and LongMemEval long-term-memory benchmarks.

**LoCoMo** - `gpt-4o-mini` reader and judge (the field's standard setup):

| Memory system | Score | Memory built with |
|---|---|---|
| **Citadel** | **85.5%** | **no LLM** - raw turns |
| Full context (no retrieval) | 72.9% | - |
| Mem0 (graph) | 68.4% | LLM facts + graph |
| Mem0 | 66.9% | LLM fact-extraction |
| Zep / Graphiti | 66.0% | LLM knowledge graph |
| LangMem | 58.1% | LLM-managed |
| OpenAI memory | 52.9% | LLM-managed |

Competitor scores as published in the Mem0 paper ([arXiv 2504.19413](https://arxiv.org/abs/2504.19413)), at the same `gpt-4o-mini` reader and judge.

**LongMemEval** ([arXiv 2410.10813](https://arxiv.org/abs/2410.10813)) oracle split, official CoT prompt and `gpt-4o-2024-08-06` judge:

| Reader | Overall | Task-averaged |
|---|---|---|
| gpt-4o | 90.6% | 89.3% |
| gpt-4o-mini | 82.2% | 83.0% |

Oracle = retrieval-complete (the evidence sessions are in context), so this measures the reader ceiling on Citadel's retrieved memory. The gpt-4o reader exceeds the LongMemEval paper's own gpt-4o oracle score (0.870). Protocol and per-question audit in [citadel-membench](https://github.com/yp3y5akh0v/citadel/blob/HEAD/crates/citadel-membench/RESULTS.md).

## Encrypted memory engine

The same encrypted pages that hold SQL tables also hold memory. Three crates make up
the memory engine:

- **[citadeldb-vector](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-vector)** - a `VECTOR(N)` SQL type, distance operators (`<->` L2, `<#>` inner, `<=>` cosine), and a [PRISM](https://github.com/yp3y5akh0v/prism)-backed filtered ANN index that reads through the encrypted page store.
- **[citadeldb-mem](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-mem)** - the memory engine (regions, atoms, edges) with hybrid recall and **cryptographic forgetting**: an atom or region is erased by destroying its key, at whole-store, per-region, and per-atom granularity.
- **[citadeldb-mcp](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-mcp)** - a Model Context Protocol server exposing a Citadel memory region (encrypted by default) to any MCP client (Claude Desktop, IDEs) as recall/remember/link/evolve/forget/verify tools.

### Zero-LLM memory path

citadeldb-mem uses no LLM at ingest or retrieval: it stores raw conversation content
and recalls with embeddings, BM25 keyword matching, and a cross-encoder reranker.
Remembering costs zero tokens, recall is deterministic, and the conversation is never
sent to an LLM to build or search the memory. The score above uses a `gpt-4o-mini` reader and judge; with a
`gemini-3.5-flash` reader the same encrypted retrieval scores 90.6% (mean of 3 runs). Protocol,
per-question audit, and a comparison with published systems are in
[citadel-membench](https://github.com/yp3y5akh0v/citadel/blob/HEAD/crates/citadel-membench/RESULTS.md).

## Agent runtime

- **[citadeldb-ai](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-ai)** - an autonomous agent runtime (ReAct + Reflexion, tool registry, budget caps, pluggable LLM backends) that uses citadeldb-mem for persistence.

## Features

- **Encrypted at rest** - AES-256-CTR + HMAC-SHA256 per page, verified before decryption
- **SQL** - JOINs, subqueries, CTEs (recursive + WITH-DML), UNION/INTERSECT/EXCEPT, window functions, views, materialized views, triggers, TEMP tables, generated columns (STORED + VIRTUAL), constraints, full FK actions, UPSERT, RETURNING, JSON/JSONB (14 Postgres operators + SQL/JSON path language), full-text search, prepared statements with plan caching, and a queryable system catalog. Full list under [SQL](#sql)
- **ACID** - Copy-on-Write B+ tree, shadow paging, no WAL. Snapshot isolation with concurrent readers
- **P2P sync** - Merkle-based table diffing over Noise-encrypted channels with PSK auth
- **CLI** - SQL shell with tab completion, syntax highlighting, dot-commands (.backup, .verify, .rekey, .sync, .dump, ...)
- **3-tier key hierarchy** - Passphrase -> Argon2id -> Master Key -> AES-KW -> REK -> HKDF -> DEK + MAC
- **Cryptographic forgetting** - Erase data by destroying its key, not by overwriting: whole-store, and per-region / per-atom via [citadeldb-mem](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-mem). A forgotten region or atom is unrecoverable
- **FIPS 140-3** - PBKDF2-HMAC-SHA256 + AES-256-CTR when compliance requires it
- **Audit log** - HMAC-SHA256 chained, tamper-evident
- **Hot backup** - Consistent snapshots via MVCC, no write blocking
- **Overflow pages** - Large values handled transparently, no size limits
- **Cross-platform** - Windows, Linux, macOS. Python, C FFI (37 functions), and WebAssembly bindings
- **5,200+ tests** - Unit, integration, torture tests across 20 crates

## Benchmarks

Single-threaded, durability off (pure engine overhead). Most benchmarks run on 100K rows of `(id INTEGER PK, name TEXT, age INTEGER)`; per-benchmark queries and schemas are in Methodology. Ratio = SQLite / Citadel time (higher is faster).

```
Benchmark              Citadel        SQLite         Ratio
----------------------------------------------------------
correlated_in          6.52 ms        1.97 s         302x
full_outer_join        70.6 us        20.6 ms        292x
correlated_scalar      324 us         19.2 ms        59x
count                  605 ns         21.0 us        35x
point                  1.12 us        12.5 us        11x
fts_rank               4.85 ms        41.8 ms        8.6x
group_by               1.38 ms        10.3 ms        7.5x
union                  27.6 us        148 us         5.3x
cte                    1.30 ms        6.10 ms        4.7x
jsonb_contains         5.63 ms        26.2 ms        4.6x
view_point             3.29 us        12.3 us        3.7x
truncate               20.6 us        56.7 us        2.75x
window_agg             28.8 ms        76.1 ms        2.65x
fts_match              2.87 ms        7.54 ms        2.63x
upsert_dedup           12.4 us        32.3 us        2.61x
json_extract           12.2 ms        31.3 ms        2.57x
partial_index_point    4.78 us        12.2 us        2.54x
insert_returning       70.9 us        172 us         2.42x
fts_phrase             4.04 ms        9.05 ms        2.24x
upsert_returning       79.2 us        174 us         2.19x
window_rank            60.6 ms        127 ms         2.09x
savepoint_create       345 ns         716 ns         2.08x
sort                   1.34 ms        2.67 ms        1.99x
filter                 973 us         1.87 ms        1.92x
view_filter            980 us         1.81 ms        1.85x
scan                   5.03 ms        9.33 ms        1.85x
savepoint_nested       188 us         348 us         1.85x
savepoint_rollback     1.25 ms        2.26 ms        1.80x
insert_select          553 us         936 us         1.69x
join                   59.6 us        95.3 us        1.60x
update                 18.6 us        29.2 us        1.56x
insert                 33.1 us        51.3 us        1.55x
upsert_all_new         32.5 us        50.2 us        1.55x
upsert_counter         36.3 us        55.0 us        1.51x
wide_proj_full         4.69 ms        7.06 ms        1.51x
wide_proj_pk           315 us         462 us         1.46x
delete_returning       120 us         172 us         1.44x
recursive_cte          86.7 us        123 us         1.42x
delete                 52.0 us        73.5 us        1.41x
correlated_exists      5.02 ms        6.87 ms        1.37x
distinct               2.84 ms        3.86 ms        1.36x
fk_cascade_delete_only 59.8 us        77.5 us        1.30x
with_dml               82.0 us        105 us         1.28x
wide_proj_3col         943 us         1.20 ms        1.27x
sum                    1.55 ms        1.93 ms        1.24x
wide_proj_2col         510 us         623 us         1.22x
sort_nocase            2.72 ms        3.30 ms        1.21x
insert_gen_virtual     45.8 us        54.2 us        1.19x
upsert_mixed           50.7 us        57.8 us        1.14x
select_gen_virtual     15.9 us        17.8 us        1.12x
insert_gen_stored      49.8 us        55.3 us        1.11x
fk_cascade             80.7 us        87.5 us        1.09x
update_gen_propagate   43.9 us        45.5 us        1.03x
update_returning       146 us         148 us         1.01x
```

54 head-to-head benchmarks. Citadel is faster on all 54. Geometric mean speedup: ~2.6x.

### Citadel-only (no direct SQLite equivalent)

```
Benchmark           Citadel
-------------------------------
date_groupby        19.2 ms
date_extract        14.4 ms
json_table          9.46 ms
lateral             2.76 ms
date_range_scan     1.80 ms
date_arith          1.73 ms
date_sort           1.43 ms
```

### Index speedups (citadel-internal)

```
Benchmark              Without index    With index     Speedup
---------------------------------------------------------------
json_gin               5.63 ms          36.9 us        153x
fts_index              1.35 s           2.85 ms        475x
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
- **window_agg** - `SELECT SUM(age) OVER (ORDER BY id ROWS 50 PRECEDING) FROM t`
- **jsonb_contains** - `SELECT id FROM users WHERE data @> '{"role":"admin"}'::jsonb`
- **savepoint_create** - `BEGIN; SAVEPOINT sp; RELEASE sp; COMMIT`
- **sort** - `SELECT * FROM t ORDER BY age LIMIT 10`
- **upsert_counter** - `INSERT ... ON CONFLICT (id) DO UPDATE SET c = c + 1`
- **window_rank** - `SELECT ROW_NUMBER() OVER (PARTITION BY age ORDER BY id) FROM t`
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
- **wide_proj_pk** - `SELECT id FROM wide` (24-column table: 3 INT keys, 8 INT, 12 TEXT; 10K rows)
- **wide_proj_2col** - `SELECT id, k1 FROM wide`
- **wide_proj_3col** - `SELECT id, k1, t1 FROM wide`
- **wide_proj_full** - `SELECT * FROM wide`
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
- **fk_cascade_delete_only** - `DELETE FROM parent WHERE id = ?` (no index on child)
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

Index speedups (same query, with vs without the index):

- **json_gin** - `SELECT id FROM users WHERE data @> '{"role":"admin"}'::jsonb`; index `CREATE INDEX ... USING gin (data)`
- **fts_index** - `SELECT id FROM docs WHERE body @@ to_tsquery(...)`; index `CREATE INDEX ... USING fts (body)` (`body` is a `TSVECTOR` column)

SQLite config: `journal_mode=OFF, synchronous=OFF, cache_size=8192` (~32 MB).
Citadel config: `SyncMode::Off, cache_size=4096` (~32 MB).

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

**Statements** - CREATE/DROP TABLE (incl. `TEMP`), ALTER TABLE (ADD/DROP/RENAME COLUMN, RENAME TABLE, DISABLE/ENABLE TRIGGER), CREATE/DROP INDEX (incl. partial `WHERE`, expression keys, `CONCURRENTLY`), CREATE/DROP VIEW, CREATE/DROP MATERIALIZED VIEW (with `REFRESH [CONCURRENTLY]`), CREATE/DROP TRIGGER (BEFORE/AFTER/INSTEAD OF, FOR EACH ROW/STATEMENT, `REFERENCING NEW/OLD TABLE`, `WHEN`, `UPDATE OF cols`), INSERT (VALUES, SELECT, ON CONFLICT DO NOTHING/DO UPDATE, ON CONSTRAINT), SELECT, UPDATE, DELETE, TRUNCATE TABLE, RETURNING (with `OLD`/`NEW`), BEGIN [READ ONLY | READ WRITE]/COMMIT/ROLLBACK, SAVEPOINT/RELEASE/ROLLBACK TO, SET TIME ZONE, EXPLAIN, REFRESH MATERIALIZED VIEW

**Constraints** - PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT, CHECK (column + table level), FOREIGN KEY with full referential actions (`ON DELETE` / `ON UPDATE` `CASCADE` / `SET NULL` / `SET DEFAULT` / `RESTRICT` / `NO ACTION`), GENERATED ALWAYS AS (...) STORED|VIRTUAL

**Types** - INTEGER, REAL, TEXT, BLOB, BOOLEAN, DATE, TIME, TIMESTAMP (WITH TIME ZONE), INTERVAL, JSON, JSONB, TSVECTOR, TSQUERY, ARRAY

**Clauses** - JOINs (INNER, LEFT, RIGHT, CROSS, FULL OUTER, LATERAL), subqueries (scalar, IN, EXISTS, correlated), CTEs (`WITH` / `WITH RECURSIVE` / WITH-DML: `WITH x AS (INSERT/UPDATE/DELETE ... [RETURNING *]) SELECT ...`), UNION/INTERSECT/EXCEPT [ALL], CASE, BETWEEN, LIKE, DISTINCT, `ANY` / `ALL` (subquery + array forms), GROUP BY/HAVING, ORDER BY, LIMIT/OFFSET

**Window functions** - ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, SUM/COUNT/AVG/MIN/MAX OVER with PARTITION BY, ORDER BY, ROWS/RANGE frames

**Views** - CREATE/DROP VIEW, OR REPLACE, IF NOT EXISTS/IF EXISTS, column aliases, nested views

**Materialized views** - `CREATE MATERIALIZED VIEW [IF NOT EXISTS] name AS SELECT ...`, `REFRESH MATERIALIZED VIEW [CONCURRENTLY] name` (`CONCURRENTLY` does a diff-merge - DELETE removed rows, UPDATE changed rows, INSERT new rows - instead of TRUNCATE+repopulate), `DROP MATERIALIZED VIEW [CASCADE]`, full backing-table semantics (indexes, joins, planner sees a real table), `pg_matviews` introspection

**Triggers** - `CREATE TRIGGER name {BEFORE|AFTER|INSTEAD OF} {INSERT|UPDATE [OF cols]|DELETE} ON table FOR EACH {ROW|STATEMENT} [REFERENCING NEW TABLE AS new_t OLD TABLE AS old_t] [WHEN (expr)] BEGIN ... END`. INSTEAD OF triggers make views writable. Transition tables work as virtual tables in trigger bodies. `ALTER TABLE ... DISABLE/ENABLE TRIGGER [name|ALL]`. PG-faithful name-order firing. Introspection via `information_schema.triggers` and `SHOW TRIGGERS [ON table]`.

**TEMP tables** - `CREATE TEMP TABLE ...` lives in a per-connection in-memory database, dropped on disconnect. Full DDL/DML/index/constraint/trigger parity with persistent tables.

**Functions** - COUNT, SUM, AVG, MIN, MAX, LENGTH, UPPER, LOWER, SUBSTR/SUBSTRING, TRIM/LTRIM/RTRIM, REPLACE, INSTR, CONCAT, HEX, ABS, ROUND, CEIL/CEILING, FLOOR, SIGN, SQRT, RANDOM, COALESCE, NULLIF, CAST, TYPEOF, IIF

**Date/Time Functions** - NOW, CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME, LOCALTIMESTAMP, LOCALTIME, CLOCK_TIMESTAMP, EXTRACT, DATE_PART, DATE_TRUNC, DATE_BIN, AGE, MAKE_DATE, MAKE_TIME, MAKE_TIMESTAMP, MAKE_INTERVAL, JUSTIFY_DAYS, JUSTIFY_HOURS, JUSTIFY_INTERVAL, ISFINITE, DATE, TIME, DATETIME, STRFTIME, JULIANDAY, UNIXEPOCH, TIMEDIFF, AT TIME ZONE. Supports `INTERVAL '1 year 2 months'`, `DATE '2024-01-15'`, `TIMESTAMP '2024-01-15 12:30:00Z'`, `infinity`/`-infinity` sentinels, BC dates, full IANA zone parsing (jiff), PG-normalized INTERVAL comparison.

**Full-text search** - `tsvector` / `tsquery` types, `to_tsvector` / `to_tsquery` / `plainto_tsquery` / `phraseto_tsquery` / `websearch_to_tsquery` builders, `@@` match operator, `ts_rank` / `ts_rank_cd` ranking with weighted positions (A/B/C/D), prefix matching (`term:*`), phrase distance (`<N>`), inverted indexes via `CREATE INDEX ... USING fts` for ~475x speedup over sequential scan

**System catalog** - `information_schema.tables`, `information_schema.columns`, `information_schema.key_column_usage`, `information_schema.table_constraints`, `information_schema.triggers`, `pg_timezone_names`, `pg_timezone_abbrevs`, `pg_matviews` (virtual tables, queryable). `SHOW TRIGGERS [ON table]` and `SHOW MATERIALIZED VIEWS` shorthands for the corresponding catalog queries.

**Prepared statements** - `$1, $2, ...` positional parameters with LRU statement cache plus snapshot-tagged plan caching for joins and compound queries (cache invalidates only on commit, never per-call)

**Multi-statement scripts** - `Connection::execute_script(sql)` runs `;`-separated statements in one call, returning per-statement outcomes with partial-success preserved. WASM: `db.run(sql)` returns `[{type, ...}, ...]`.

**UPSERT** - `INSERT ... ON CONFLICT (cols) DO NOTHING` / `DO UPDATE SET col = excluded.col ... WHERE ...` and `ON CONFLICT ON CONSTRAINT idx_name`. `excluded.*` refers to the proposed row; bare `col` refers to the existing row. Single-descent storage primitive: on the canonical `DO UPDATE SET counter = counter + 1` pattern, Citadel is ~1.5x faster than SQLite.

## Security

**No plaintext on disk.** Every page is encrypted before writing and authenticated before reading.

**Separate key file.** Encryption keys live in `{dbname}.citadel-keys`, not inside the database. The passphrase derives a master key in memory via Argon2id (or PBKDF2 in FIPS mode) and never touches disk.

**Key backup.** Export an encrypted key backup with a separate recovery passphrase. Restore access without re-encrypting the entire database.

**Instant rekey.** Changing the passphrase re-wraps the root encryption key. No page re-encryption - instant regardless of database size.

**Encrypted sync.** Noise protocol (`NNpsk0_25519_ChaChaPoly_BLAKE2s`) with a 256-bit pre-shared key. Ephemeral Curve25519 keys per session for forward secrecy.

## Architecture

```
Agent layer:
+---------------------------------------------+
|                 citadel-ai                  |  Agent runtime (ReAct + Reflexion)
+---------------------------------------------+

Memory layer:
+---------------------------------------------+
|                 citadel-mcp                 |  MCP server: memory tools for any MCP client
+---------------------------------------------+
|                 citadel-mem                 |  Memory engine: regions, atoms, recall, erasure
+---------------------------------------------+
|                citadel-vector               |  VECTOR(N) type + PRISM filtered ANN index
+---------------------------------------------+

Encrypted database engine:
+----------------------+----------------------+
|      citadel-cli     |    citadel-python    |  CLI, Python wheel
+----------------------+----------------------+
|      citadel-ffi     |     citadel-wasm     |  C FFI, WebAssembly
+----------------------+----------------------+
|                 citadel-sql                 |  SQL parser, planner, executor
+---------------------------------------------+
|                   citadel                   |  Database API, builder, sync
+-------------+--------------+----------------+
| citadel-txn | citadel-sync | citadel-crypto |  Transactions, replication, keys
+-------------+--------------+----------------+
|       citadel-buffer       |  citadel-page  |  Buffer pool (SIEVE), page codec
+----------------------------+----------------+
|                 citadel-io                  |  File I/O, fsync, io_uring
+---------------------------------------------+
|                citadel-core                 |  Types, errors, constants
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

### Python

One importable wheel with the full engine (SQL, vectors, memory, agent runtime) and bundled type stubs.

```
pip install citadeldb
```

```python
import citadeldb

db = citadeldb.connect("my.db", key="secret", create=True)
db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
db.execute("INSERT INTO t VALUES (1, 'Alice')")
db.query("SELECT * FROM t").to_dicts()
# [{'id': 1, 'name': 'Alice'}]
```

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

[MIT](https://github.com/yp3y5akh0v/citadel/blob/HEAD/LICENSE-MIT) OR [Apache-2.0](https://github.com/yp3y5akh0v/citadel/blob/HEAD/LICENSE-APACHE)
