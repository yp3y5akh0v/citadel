<p align="center">
  <img src="https://raw.githubusercontent.com/yp3y5akh0v/citadel/HEAD/.github/banner.png" alt="Citadel" width="600">
</p>

<p align="center">
  <a href="https://crates.io/crates/citadeldb"><img src="https://badgen.net/crates/v/citadeldb" alt="crates.io"></a>
  <a href="https://www.npmjs.com/package/@citadeldb/wasm"><img src="https://img.shields.io/npm/v/@citadeldb/wasm" alt="npm"></a>
  <a href="https://pypi.org/project/citadeldb/"><img src="https://img.shields.io/pypi/v/citadeldb?label=pypi%20citadeldb" alt="PyPI citadeldb"></a>
  <a href="https://pypi.org/project/citadeldb-mcp/"><img src="https://img.shields.io/pypi/v/citadeldb-mcp?label=pypi%20citadeldb-mcp" alt="PyPI citadeldb-mcp"></a>
  <a href="https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-mcp"><img src="https://img.shields.io/badge/MCP-dev.citadeldb%2Fmcp-blue" alt="MCP registry: dev.citadeldb/mcp"></a>
  <br>
  <a href="https://github.com/yp3y5akh0v/citadel/actions/workflows/ci.yml"><img src="https://github.com/yp3y5akh0v/citadel/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/yp3y5akh0v/citadel/blob/HEAD/crates/citadel-membench/RESULTS.md"><img src="https://img.shields.io/badge/LoCoMo%20(gpt--4o--mini)-87.2%25-success" alt="LoCoMo 87.2% (gpt-4o-mini, mean of 3 runs)"></a>
  <a href="https://github.com/yp3y5akh0v/citadel/blob/HEAD/crates/citadel-membench/RESULTS.md"><img src="https://img.shields.io/badge/LongMemEval--S%20(gpt--4o)-86.2%25-success" alt="LongMemEval-S 86.2% (gpt-4o reader)"></a>
  <a href="https://github.com/yp3y5akh0v/citadel#license"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License"></a>
</p>

## Quick Start

```bash
pip install citadeldb
```

```python
import citadeldb

db = citadeldb.connect("memory.cdl", key="your-passphrase", region_keys=True)
mem = db.memory()
mem.create_encrypted_region("chat", citadeldb.MockEmbedder(dim=64))

mem.remember("chat", {"kind": "fact", "text": "Alice's cat is named Mochi"})
berlin = mem.remember("chat", {"kind": "fact", "text": "Alice lives in Berlin"})

for hit in mem.recall("chat", text="where does Alice live?", k=2):
    print(f"{hit.score:.3f}  {hit.text}")
# 0.850  Alice lives in Berlin
# 0.200  Alice's cat is named Mochi

# Forgetting destroys the atom's key, so the ciphertext is unrecoverable.
receipt = mem.forget("chat", [berlin])
print(receipt.cryptographic_erasure, receipt.algorithm)
# True AES-256-KW(RFC3394)
```

`MockEmbedder` needs no download and is enough to try the API. For real recall
quality use `CandleEmbedder` with a local e5-large, which is the benchmark setup.

### Memory (Rust)

Uses the `citadeldb` and `citadeldb-mem` crates (enable `citadeldb-mem`'s `candle-embed` feature). `e5_large` loads the recommended local embedder, and adding a `CrossEncoder` reranker gives the best recall (the benchmark config). Other presets (`bge_large`, `bge_small`, ...) or a custom `Embedder` work too.

```rust
use std::sync::Arc;
use citadel::DatabaseBuilder;
use citadel_mem::{AtomInput, CandleEmbedder, CrossEncoder, MemoryEngine, RecallQuery, RerankStrategy};

// Encrypted store (per-atom keys enable cryptographic forgetting)
let db = DatabaseBuilder::new("memory.db")
    .passphrase(b"secret")
    .enable_region_keys(true)
    .create()?;
let mem = MemoryEngine::open(Arc::new(db))?;

// Local embedder (e5-large) + cross-encoder reranker = the best-recall setup
let embedder = Arc::new(CandleEmbedder::e5_large("/path/to/e5-large")?);
mem.create_encrypted_region("chat", embedder)?;
mem.set_reranker(
    Arc::new(CrossEncoder::ms_marco_minilm_l6("/path/to/ms-marco-minilm")?),
    RerankStrategy::default(),
);

// Remember raw turns (no LLM)
mem.remember("chat", AtomInput::new("fact", "Alice's cat is named Mochi"))?;
let berlin = mem.remember("chat", AtomInput::new("fact", "Alice lives in Berlin"))?;

// Recall by relevance
for hit in mem.recall("chat", RecallQuery::by_text("where does Alice live?", 5))? {
    println!("{:.3}  {}", hit.score, hit.text);
}

// Cryptographic forgetting: destroy the atom's key
mem.forget_atom("chat", berlin)?;
```

### SQL and key-value

Uses the `citadeldb` and `citadeldb-sql` crates - or try SQL with no install in the [live playground](https://citadeldb.dev/demo/).

```rust
use citadel::DatabaseBuilder;
use citadel_sql::Connection;

let db = DatabaseBuilder::new("my.db")
    .passphrase(b"secret")
    .create()?;

let conn = Connection::open(&db)?;
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
citadel> .upgrade
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

### Agent frameworks

Each package implements that framework's own storage interface, so existing code keeps
working and only the constructor changes. Deleting through any of them destroys the
record's key, not just its row, and search is ranked recall rather than a `LIKE`.

| Framework | Package | Implements |
|---|---|---|
| [LangGraph](packaging/citadeldb-langgraph) | [`citadeldb-langgraph`](https://pypi.org/project/citadeldb-langgraph/) | `BaseStore` |
| [CrewAI](packaging/citadeldb-crewai) | [`citadeldb-crewai`](https://pypi.org/project/citadeldb-crewai/) | `StorageBackend` |
| [OpenAI Agents SDK](packaging/citadeldb-openai-agents) | [`citadeldb-openai-agents`](https://pypi.org/project/citadeldb-openai-agents/) | `Session` |
| [Google ADK](packaging/citadeldb-google-adk) | [`citadeldb-google-adk`](https://pypi.org/project/citadeldb-google-adk/) | `BaseMemoryService` |
| [LlamaIndex](packaging/citadeldb-llamaindex) | [`citadeldb-llamaindex`](https://pypi.org/project/citadeldb-llamaindex/) | `BasePydanticVectorStore` |
| [LangChain](packaging/citadeldb-langchain) | [`citadeldb-langchain`](https://pypi.org/project/citadeldb-langchain/) | `VectorStore`, `BaseChatMessageHistory` |
| [Haystack](packaging/citadeldb-haystack) | [`citadeldb-haystack`](https://pypi.org/project/citadeldb-haystack/) | `DocumentStore` |
| [Microsoft Agent Framework](packaging/citadeldb-ms-agent-framework) | [`citadeldb-ms-agent-framework`](https://pypi.org/project/citadeldb-ms-agent-framework/) | `HistoryProvider`, `ContextProvider` |
| [Strands Agents](packaging/citadeldb-strands-agents) | [`citadeldb-strands-agents`](https://pypi.org/project/citadeldb-strands-agents/) | `SessionRepository` |

```bash
pip install citadeldb-langgraph
```

```python
from citadeldb_langgraph import CitadelStore

store = CitadelStore("agent.cdl", key="your-passphrase")
store.put(("users", "alice"), "prefs", {"theme": "dark"})
store.search(("users",))                     # every namespace under users/
store.forget_namespace(("users", "alice"))   # cryptographic erasure, returns a count
```

One database serves every adapter on the thread that opened it, so a graph's long-term
store and its session transcripts can share one encrypted file. See [`packaging/`](packaging/) for each
package's own README.

### MCP

Serve an encrypted memory region to Claude Desktop or any MCP client. `citadeldb-mcp` is
published to PyPI and listed in the official [MCP registry](https://registry.modelcontextprotocol.io/v0/servers?search=dev.citadeldb/mcp)
as `dev.citadeldb/mcp`. Run it with no install via `uvx citadeldb-mcp`, or
`pip install citadeldb-mcp` / `cargo install citadeldb-mcp`, then add it to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "citadel": {
      "command": "citadeldb-mcp",
      "args": ["--db", "memory.cdl", "--embedder", "e5-large", "--reranker", "ms-marco-minilm"],
      "env": { "CITADEL_KEY": "your-passphrase" }
    }
  }
}
```

For the best recall (the benchmark config), `pull e5-large` + `pull ms-marco-minilm` first,
then use `--embedder e5-large --reranker ms-marco-minilm`. Omit both for instant keyword-only recall.

## Memory benchmarks

Citadel is scored on the LoCoMo and LongMemEval long-term-memory benchmarks. Execution speed against unencrypted SQLite across 58 head-to-head benchmarks is under [Speed benchmarks](#speed-benchmarks).

**LoCoMo** - `gpt-4o-mini` reader and judge (the 2025 paper-comparison protocol), mean of 3 runs:

| Metric | Score |
|---|---|
| Overall | 87.2% +/- 0.3 |
| Full context at the same reader (no retrieval) | 72.9% |

Retrieval is identical across the three runs; the spread is reader and judge
nondeterminism. A manual audit estimates that ~6.4% of LoCoMo answer keys are erroneous,
so raw accuracy should be interpreted with that annotation noise in mind.

Memory is built with no LLM - raw turns only, indexed and recalled deterministically.

**LongMemEval_S** ([arXiv 2410.10813](https://arxiv.org/abs/2410.10813)) full-haystack split (~40-50 sessions/question), gpt-4o reader, official CoT prompt and `gpt-4o-2024-08-06` judge:

| Metric | Score |
|---|---|
| Overall | 86.2% |
| Task-averaged | 86.8% |
| Abstention | 80.0% |

Full-haystack stresses retrieval against distractors (not the oracle reader ceiling). Protocol and per-type results in [citadel-membench](https://github.com/yp3y5akh0v/citadel/blob/HEAD/crates/citadel-membench/RESULTS.md).

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
sent to an LLM to build or search the memory. The readers and judges above are separate
LLMs - gpt-4o-mini for LoCoMo, gpt-4o for LongMemEval. Protocol and a comparison with
published systems are in
[citadel-membench](https://github.com/yp3y5akh0v/citadel/blob/HEAD/crates/citadel-membench/RESULTS.md).

## Agent runtime

- **[citadeldb-llm](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-llm)** - the provider-neutral LLM client layer (Claude, OpenAI, Ollama, Gemini) behind one factory, with canonical request hashing and a non-secret client request identity.
- **[citadeldb-ai](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-ai)** - an autonomous agent runtime (ReAct + Reflexion, tool registry, budget caps, pluggable LLM backends) that uses citadeldb-mem for persistence.

## Features

- **Encrypted at rest** - AES-256-CTR + HMAC-SHA256 per page, verified before decryption
- **SQL** - JOINs, subqueries, CTEs (recursive + WITH-DML), UNION/INTERSECT/EXCEPT, window functions, views, materialized views, triggers, TEMP tables, generated columns (STORED + VIRTUAL), constraints, full FK actions, UPSERT, RETURNING, JSON/JSONB (14 Postgres operators + SQL/JSON path language), full-text search, prepared statements with plan caching, and a queryable system catalog. Full list under [SQL](#sql)
- **ACID** - Copy-on-Write B+ tree, shadow paging, no WAL. Snapshot isolation with concurrent readers
- **Authenticated commit slots** - the commit metadata (table roots, catalog) carries its own HMAC; older files migrate one-way via `.upgrade`
- **P2P sync** - Merkle-based table diffing over Noise-encrypted channels with PSK auth
- **CLI** - SQL shell with tab completion, syntax highlighting, 27 dot-commands (.backup, .verify, .upgrade, .rekey, .sync, .dump, ...)
- **3-tier key hierarchy** - Passphrase -> Argon2id -> Master Key -> AES-KW -> REK -> HKDF -> DEK + MAC
- **Cryptographic forgetting** - Erase data by destroying its key, not by overwriting: whole-store, and per-region / per-atom via [citadeldb-mem](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-mem). A forgotten region or atom is unrecoverable
- **FIPS 140-3** - PBKDF2-HMAC-SHA256 + AES-256-CTR when compliance requires it
- **Audit log** - HMAC-SHA256 chained, tamper-evident
- **Hot backup** - Consistent snapshots via MVCC, no write blocking
- **Overflow pages** - Large values handled transparently, no size limits
- **Cross-platform** - Windows, Linux, macOS. Python, C FFI (37 functions), and WebAssembly bindings
- **5,200+ tests** - Unit, integration, torture tests across 21 crates

## Speed benchmarks

Single-threaded, durability off (pure engine overhead). Most benchmarks run on 100K rows of `(id INTEGER PK, name TEXT, age INTEGER)`; per-benchmark queries and schemas are in Methodology. Ratio = SQLite / Citadel time (higher is faster). Two-run medians.

### Execution speed

Every iteration computes its result: writes, and reads whose parameters rotate per iteration or whose shape re-executes against the storage engine.

```
Benchmark              Citadel        SQLite         Ratio
----------------------------------------------------------
correlated_scalar      12.8 us        19.8 ms        1,549x
full_outer_join        14.1 us        21.8 ms        1,540x
view_filter            21.6 us        1.83 ms        85x
filter                 23.2 us        1.84 ms        80x
join_param             1.55 us        34.8 us        22x
join                   14.2 us        97.7 us        6.89x
union                  28 us          150 us         5.35x
delete_returning       48.8 us        171 us         3.50x
update_returning       46.6 us        150 us         3.23x
insert_returning       61.1 us        174 us         2.84x
truncate               20.8 us        58.7 us        2.83x
fts_match              2.91 ms        8.03 ms        2.76x
json_extract           12.2 ms        32.7 ms        2.68x
sort_paginate_pk       5.62 us        14.7 us        2.61x
upsert_returning       67.2 us        175 us         2.61x
window_agg             29.5 ms        76.5 ms        2.59x
upsert_dedup           13 us          32.8 us        2.52x
fts_phrase             4.19 ms        9.73 ms        2.32x
savepoint_create       349 ns         748 ns         2.14x
window_rank            63.4 ms        130 ms         2.05x
insert_select          543 us         1.1 ms         2.03x
delete                 35 us          69.9 us        2.00x
scan                   4.97 ms        9.54 ms        1.92x
savepoint_rollback     1.28 ms        2.28 ms        1.78x
wide_proj_2col         501 us         842 us         1.68x
upsert_mixed           35.5 us        59.1 us        1.66x
savepoint_nested       197 us         326 us         1.66x
wide_proj_full         4.59 ms        7.53 ms        1.64x
update                 17.9 us        28.3 us        1.58x
wide_proj_pk           319 us         480 us         1.51x
upsert_counter         35.8 us        53.7 us        1.50x
insert                 35.4 us        51.9 us        1.47x
upsert_all_new         35.6 us        51.4 us        1.44x
covered_count          257 us         359 us         1.40x
with_dml               80.5 us        107 us         1.34x
fk_cascade_delete_only 63.5 us        80.7 us        1.27x
insert_gen_virtual     48.5 us        55 us          1.13x
wide_proj_3col         1.11 ms        1.23 ms        1.11x
insert_gen_stored      51.3 us        56.2 us        1.10x
covered_range          67.7 us        74.4 us        1.10x
fk_cascade             80.7 us        87.3 us        1.08x
update_gen_propagate   44.6 us        45.2 us        1.01x
```

42 execution benchmarks. Citadel is faster on all 42. Geometric mean speedup: ~3.4x.

### Memoized repeat-reads

Deterministic read-only statements re-executed with identical parameters against unchanged data are served from a generation-keyed result cache. Any commit invalidates the cache, and the first execution after a write recomputes at execution speed. SQLite has no result cache and re-executes every query.

```
Benchmark              Citadel        SQLite         Ratio
----------------------------------------------------------
correlated_in          103 ns         1.97 s         19,208,388x
fts_rank               219 ns         42.5 ms        194,338x
correlated_exists      102 ns         6.89 ms        67,712x
jsonb_contains         1.09 us        27.7 ms        25,273x
sort_nocase            213 ns         3.31 ms        15,532x
cte                    668 ns         6.13 ms        9,179x
sort                   312 ns         2.76 ms        8,853x
group_by               1.27 us        10.7 ms        8,411x
sum                    468 ns         1.97 ms        4,214x
distinct               1.11 us        4.08 ms        3,675x
recursive_cte          105 ns         122 us         1,165x
partial_index_point    103 ns         12.6 us        122x
view_point             121 ns         12.7 us        105x
point                  121 ns         12.5 us        104x
count                  457 ns         21.6 us        47x
select_gen_virtual     1.05 us        18.1 us        17x
```

16 memoized benchmarks. Geometric mean speedup: ~3,700x.

### Citadel-only (no direct SQLite equivalent)

Fixed-parameter reads; every benchmark except `json_table` is served from the result cache on repeat execution.

```
Benchmark           Citadel
-------------------------------
json_table          9.25 ms
lateral             1.46 us
date_sort           1.10 us
date_extract        473 ns
date_groupby        242 ns
date_range_scan     102 ns
date_arith          100 ns
```

### Index speedups (citadel-internal)

Rotating probes; both arms measure execution speed.

```
Benchmark              Without index    With index     Speedup
---------------------------------------------------------------
json_gin               4.70 ms          3.49 us        1,347x
fts_index              1.37 s           2.98 ms        461x
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
- **covered_range** - `SELECT age, id FROM t WHERE age = ?` on an indexed column, parameter rotating per iteration
- **covered_count** - `SELECT COUNT(*) FROM t WHERE age >= ?` on an indexed column, parameter rotating per iteration
- **sort_paginate_pk** - `SELECT id, name FROM t WHERE id > ? ORDER BY id LIMIT 20`, parameter advancing per iteration
- **join_param** - `SELECT a.val, b.data FROM a JOIN b ON b.a_id = a.id WHERE a.id = ?`, parameter rotating per iteration
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

**Full-text search** - `tsvector` / `tsquery` types, `to_tsvector` / `to_tsquery` / `plainto_tsquery` / `phraseto_tsquery` / `websearch_to_tsquery` builders, `@@` match operator, `ts_rank` / `ts_rank_cd` ranking with weighted positions (A/B/C/D), prefix matching (`term:*`), phrase distance (`<N>`), inverted indexes via `CREATE INDEX ... USING fts` for ~461x speedup over sequential scan

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
|                 citadel-llm                 |  LLM client layer: Claude, OpenAI, Ollama, Gemini
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
|     citadel-cli      |    citadel-python    |  CLI, Python wheel
+----------------------+----------------------+
|     citadel-ffi      |     citadel-wasm     |  C FFI, WebAssembly
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

### Integrity Boundary

What the at-rest integrity machinery does and does not guarantee against an attacker with file access:

- **Per-page HMAC** binds `(epoch, page_id, IV, ciphertext)`. Any modification of a page's bytes is detected before decryption. It does **not** bind the commit generation: a page image validly written in the past for the same `(page_id, epoch)` verifies forever.
- **Commit slots** are keyed-MAC'd (truncated HMAC-SHA256 over the whole slot) when the named-table entries fit the authenticated layout; files written by pre-1.13 versions carry only a keyless checksum over part of the slot and are still accepted, so slot authentication is corruption detection and a tampering bar, not a hard guarantee - an attacker can re-encode a slot in the legacy format.
- **Rollback to an older genuine state** (an earlier file snapshot, or an old slot plus its old pages) passes every check by construction and cannot be detected from the file alone. Deployments that need freshness must keep an external anchor - e.g. record the latest commit's `txn_id` and Merkle root outside the attacker's reach and compare after opening.

## Language Bindings

### C / C++

Static or dynamic library with auto-generated `citadel.h` (cbindgen). All 37 functions are panic-safe.

```c
#include "citadel.h"

CitadelDb *db = NULL;
citadel_create("my.db", (const uint8_t*)"secret", 6, NULL, &db);

CitadelWriteTxn *wtx = NULL;
citadel_write_begin(db, &wtx);
citadel_write_put(wtx, (const uint8_t*)"key", 3, (const uint8_t*)"val", 3, NULL);
citadel_write_commit(wtx);

CitadelSqlConn *conn = NULL;
citadel_sql_open(db, &conn);
CitadelSqlResult *result = NULL;
citadel_sql_execute(conn, "SELECT * FROM users;", &result);

citadel_close(db);
```

### WebAssembly

Install with `npm install @citadeldb/wasm`.

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

Rust 1.88+.

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

[Apache-2.0](https://github.com/yp3y5akh0v/citadel/blob/HEAD/LICENSE-APACHE)
