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

<p align="center"><a href="crates/citadel-membench/RESULTS.md">Historical memory results and configurations</a></p>

## Quick Start

For semantic memory through MCP, install [uv](https://docs.astral.sh/uv/) and pull the
local embedder and optional cross-encoder reranker:

```console
uvx citadeldb-mcp pull e5-large
uvx citadeldb-mcp pull ms-marco-minilm
```

Set `CITADEL_KEY` to your vault passphrase (`export CITADEL_KEY="your-passphrase"`
on macOS/Linux or `$env:CITADEL_KEY = "your-passphrase"` in PowerShell), then start:

```console
uvx citadeldb-mcp --db memory.cdl --embedder e5-large --reranker ms-marco-minilm
```

The server communicates over stdio. See [MCP](#mcp) for client configuration.
Model downloads do not need a vault key; serving does.

### Memory (Python)

Install the published package with `pip install citadeldb`. See the
[Python source-build and semantic-memory guide](python/README.md).
Embedders implement `embed_with_cancel(texts, cancel_token)` and check cancellation
between bounded batches. Local Candle models require the `candle-embed` build feature.

### Memory (Rust)

Uses `citadeldb` and `citadeldb-mem` with the `candle-embed` feature. This example
loads e5-large and a local cross-encoder reranker. Other presets or a custom
`Embedder` are supported.

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
    println!("{:.3}  {}", hit.relevance.expect("ranked recall"), hit.text);
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

### Citadel Studio

A native desktop client for Windows, macOS, and Linux. Open encrypted vaults,
browse tables and memory, run SQL with EXPLAIN and ANALYZE, and inspect vectors
and integrity results.

See the [Studio guide](crates/citadel-studio/README.md) for screenshots and build
instructions. [Download Citadel Studio for Windows, macOS, or Linux](https://citadeldb.dev/download/#studio).

### Agent frameworks

The adapters implement framework-specific storage, session, and retrieval interfaces.
Each requires an explicit embedder. See the package README for setup, search behavior,
and supported filters.

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

One database serves every adapter on the thread that opened it, so a graph's long-term
store and its session transcripts can share one encrypted file. See [`packaging/`](packaging/) for each
package's own README.

### MCP

Serve an encrypted memory region to Claude Desktop or any MCP client. `citadeldb-mcp` is
published to PyPI and listed in the official [MCP registry](https://registry.modelcontextprotocol.io/v0/servers?search=dev.citadeldb/mcp)
as `dev.citadeldb/mcp`. Run it without installing through `uvx`.

For the recommended semantic-recall setup, pull the embedder and cross-encoder reranker once:

```console
uvx citadeldb-mcp pull e5-large
uvx citadeldb-mcp pull ms-marco-minilm
```

The pull commands do not need a vault key. Before starting the server, set `CITADEL_KEY`
to the vault passphrase: use `export CITADEL_KEY="your-passphrase"` on macOS/Linux or
`$env:CITADEL_KEY = "your-passphrase"` in PowerShell. Then run:

```console
uvx citadeldb-mcp --db memory.cdl --embedder e5-large --reranker ms-marco-minilm
```

`--db`, `--embedder`, and `CITADEL_KEY` are required when serving. The reranker is optional,
but `e5-large` with `ms-marco-minilm` is the configuration used for the memory benchmarks.

To install the executable instead, run `pip install citadeldb-mcp` or
`cargo install citadeldb-mcp`. Pull the same models with `citadeldb-mcp pull e5-large` and
`citadeldb-mcp pull ms-marco-minilm`, then add it to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "citadel": {
      "command": "citadeldb-mcp",
      "args": [
        "--db", "/absolute/path/to/memory.cdl",
        "--embedder", "e5-large",
        "--reranker", "ms-marco-minilm"
      ],
      "env": { "CITADEL_KEY": "your-passphrase" }
    }
  }
}
```

## Historical memory benchmarks

Recorded LoCoMo and LongMemEval results are summarized below; their [configurations and limitations](crates/citadel-membench/RESULTS.md) predate the current memory-engine changes. SQL comparisons with unencrypted SQLite across 59 cases are under [Speed benchmarks](#speed-benchmarks).

**LoCoMo** - `gpt-4o-mini` reader and judge with the harness's prompts, mean of 3 runs measured August 18, 2026:

| Metric | Score |
|---|---|
| Overall | 87.2% +/- 0.3 |
| Full context, no retrieval (reported in the Mem0 paper, not rerun here) | 72.9% |

Retrieval is identical across the three runs; the spread is reader and judge
nondeterminism. A manual audit estimates that ~6.4% of LoCoMo answer keys are erroneous,
so raw accuracy should be interpreted with that annotation noise in mind.

Memory is built with no LLM - raw turns enriched with supplied photo captions and image-search text, indexed and recalled deterministically.

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

citadeldb-mem stores raw conversation content without a summarizer LLM. Recall uses
embeddings, BM25 keyword matching, and an optional reranker. Local embedding and
reranking backends keep this processing on-device; custom backends determine their
own network use and costs. The benchmark readers and judges are separate LLMs -
gpt-4o-mini for LoCoMo, gpt-4o for LongMemEval. The protocol and results are in
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
- **Citadel Studio** - Native desktop client for SQL, stored memory, vector inspection, and vault diagnostics
- **3-tier key hierarchy** - Passphrase -> Argon2id -> Master Key -> AES-KW -> REK -> HKDF -> DEK + MAC
- **Cryptographic forgetting** - Whole-store and per-region / per-atom key erasure via [citadeldb-mem](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-mem). Pre-erasure backups, copied keys, and exported plaintext are outside that erasure
- **FIPS-oriented at-rest profile** - PBKDF2-HMAC-SHA256 + AES-256-CTR for database storage; not a claim of whole-product validation
- **Audit log** - HMAC-SHA256 chained within files and across retained v2 generations; retained-history verification detects record edits and broken retained links, but there is no external anti-rollback anchor
- **Hot backup** - Consistent snapshots via MVCC, no write blocking
- **Overflow pages** - Large values handled transparently, up to 1 GiB per value
- **Cross-platform** - Windows, Linux, macOS. Python, C FFI, and WebAssembly bindings
- **Thousands of tests** - Unit, integration, and torture tests across the workspace

## Speed benchmarks

Measured on September 13 and 20, 2026 (UTC) on an Intel Core i9-12900HX, Windows 11 Pro, Rust 1.98.0, and SQLite 3.51.3. Runs use one fixed logical processor, with durability disabled and both caches configured for 4,096 pages (about 32 MiB). Most cases use 100K rows; schemas and operations vary as listed below.

Each time is the arithmetic mean of two or four per-run sample medians, with 30 samples per run. Ratios use unrounded SQLite time / Citadel time: above 1 means Citadel is faster, below 1 means Citadel is slower. For example, 0.5x means Citadel takes twice as long as SQLite.

Ten execution comparisons were refreshed on September 20: eight write/scan cases at `2bb8516f` and two window cases at `74aa7020`. Other rows retain September 13 measurements at `6b41d0c3` or `ea8827d7`. Each row pairs Citadel and SQLite from the same cohort. This is a combined snapshot, not a full-suite run at the latest revision. [Source revisions, run settings, medians, 95% intervals, and drift](site/data/sql-benchmarks.json) identify every row.

### Execution speed

37 comparisons of writes and reads that execute each iteration, including rotating-parameter queries. Fixture resets are excluded unless the case description says otherwise.

```
Benchmark                     Citadel        SQLite         Ratio
----------------------------------------------------------------------
join_param                    2.74 us        54.1 us        19.8x
fts_rank_first_execution      6.86 ms        64.5 ms        9.4x
insert_returning              80.8 us        351 us         4.34x
update_returning              56.9 us        209 us         3.67x
window_agg                    29.3 ms        97.4 ms        3.33x
upsert_returning              135 us         392 us         2.91x
sort_paginate_pk              9.13 us        26.1 us        2.86x
delete_returning              97.2 us        269 us         2.76x
fts_phrase                    5.66 ms        14.6 ms        2.58x
window_rank                   63.9 ms        161 ms         2.51x
fts_match                     4.94 ms        12.1 ms        2.44x
json_extract                  22.6 ms        49 ms          2.17x
scan                          6.31 ms        13.2 ms        2.09x
insert                        25.5 us        51.8 us        2.03x
insert_gen_virtual            36.4 us        66.4 us        1.82x
wide_proj_full                6.83 ms        12.1 ms        1.77x
insert_gen_stored             37.3 us        65.7 us        1.76x
upsert_all_new                36.2 us        63.5 us        1.76x
wide_proj_pk                  416 us         728 us         1.75x
truncate                      57.6 us        101 us         1.75x
upsert_dedup                  31.9 us        53.7 us        1.68x
savepoint_rollback            2.08 ms        3.22 ms        1.55x
delete                        75.1 us        116 us         1.54x
wide_proj_2col                651 us         998 us         1.53x
covered_count                 377 us         561 us         1.49x
wide_proj_3col                1.28 ms        1.89 ms        1.47x
savepoint_nested              232 us         322 us         1.39x
update                        33.4 us        42.5 us        1.27x
insert_select                 171 us         214 us         1.25x
with_dml                      122 us         147 us         1.21x
fk_cascade_delete_only        52.4 us        63.2 us        1.2x
upsert_mixed                  48.6 us        57.7 us        1.19x
fk_cascade                    122 us         144 us         1.18x
upsert_counter                63.7 us        74.5 us        1.17x
savepoint_create              916 ns         1.07 us        1.16x
covered_range                 105 us         119 us         1.14x
update_gen_propagate          59.4 us        66.1 us        1.11x
```

### Cached repeat reads

22 comparisons of identical reads against unchanged data. Citadel reuses cached results; `union` reuses projected branch rows and reconstructs UNION ALL output. SQLite executes the query again. These timings do not represent the first query after a write.

```
Benchmark                     Citadel        SQLite         Ratio
----------------------------------------------------------------------
correlated_in                 268 ns         2.86 s         10700000x
fts_rank                      534 ns         63.8 ms        120000x
correlated_exists             267 ns         10.1 ms        37900x
jsonb_contains                1.81 us        40.5 ms        22400x
sort_nocase                   446 ns         4.71 ms        10600x
cte                           1.46 us        9.29 ms        6350x
group_by                      2.62 us        15.9 ms        6060x
sort                          674 ns         4.03 ms        5990x
sum                           851 ns         2.88 ms        3390x
distinct                      1.82 us        5.94 ms        3260x
full_outer_join               25.7 us        31 ms          1210x
correlated_scalar             24.1 us        28.7 ms        1190x
recursive_cte                 267 ns         175 us         654x
partial_index_point           269 ns         22.6 us        84.1x
view_point                    300 ns         22.8 us        75.9x
point                         302 ns         22.6 us        74.9x
filter                        38.6 us        2.74 ms        70.9x
view_filter                   38.6 us        2.65 ms        68.8x
count                         855 ns         37.4 us        43.7x
select_gen_virtual            2.21 us        34.5 us        15.6x
join                          25.3 us        151 us         5.95x
union                         50.6 us        230 us         4.54x
```

### Citadel-only

No SQLite comparison is reported for these seven cases. `json_table` executes each iteration; the other six measure cached repeat reads.

```
Benchmark                     Citadel        SQLite         Ratio
----------------------------------------------------------------------
json_table                    7.42 ms        -              -
lateral                       2.63 us        -              -
date_sort                     1.81 us        -              -
date_extract                  848 ns         -              -
date_groupby                  576 ns         -              -
date_arith                    260 ns         -              -
date_range_scan               257 ns         -              -
```

### Index comparisons

The same query within Citadel, with and without its index. Ratios are unindexed / indexed time. `json_gin` rotates unique JSON-id probes; `fts_index` repeats a fixed query on a TEXT column. Both execute each iteration.

```
Benchmark                     Without index  With index     Ratio
----------------------------------------------------------------------
json_gin                      8.26 ms        5.77 us        1430x
fts_index                     1.95 s         4.76 ms        409x
```

<details>
<summary>Methodology</summary>

Exact queries, schemas, input sizes, and timed boundaries are in the
[H2H implementations](crates/citadel-sql/benches/h2h/). Shared database settings and
result collection are in [common.rs](crates/citadel-sql/benches/h2h/common.rs).

- SQLite uses `page_size=8192, journal_mode=MEMORY, synchronous=OFF, cache_size=4096`.
  Citadel uses `SyncMode::Off` and `cache_size=4096`; its 8,208-byte stored pages
  contain an 8,160-byte decrypted body. Cache entry counts match, not exact byte use.
  These runs do not measure durable commit latency.
- Result rows, including RETURNING output, are fully collected. Most read cases
  reuse a prepared statement. Dataset creation is outside the timer.
- `insert_select` includes creating the destination table and copying 1K rows
  into it, each as a separate autocommit statement. Dropping it is excluded.
- `fts_rank_first_execution` uses a fresh prepared statement each iteration;
  preparation and disposal are excluded. It is not a disk-cold I/O measurement.
  `fts_rank` reuses the prepared result. Citadel TS_RANK and SQLite BM25 are
  different ranking algorithms.
- `fk_cascade` includes inserting one parent and 100 children, committing, then
  deleting the parent. `fk_cascade_delete_only` times only the cascading delete.
- `savepoint_create` includes BEGIN, SAVEPOINT, RELEASE, and COMMIT.
  `savepoint_nested` creates ten nested savepoints with 100 inserts at each level,
  rolls back to the sixth, releases the remaining savepoints, and commits.
  `savepoint_rollback` inserts 1K rows before a savepoint and 10K after it,
  rolls back the latter, and commits.
- Criterion uses 30 samples per arm. September 13 cohorts use a 1-second warmup
  and a 2-second measurement target; September 20 cohorts use 3 and 8 seconds.
  Slow cases run longer to complete all samples. Runs are serial on logical
  processor 0 in reference/candidate/candidate/reference order. The September 20
  `update`, `update_gen_propagate`, `window_agg`, and `window_rank` cohorts also
  run in reverse order, giving four candidate runs; other rows use two. Every
  candidate run and its matching SQLite control contributes to the displayed mean.
- Per-run 95% median intervals and drift are retained in the data. Intervals are
  not pooled, and ratios do not establish a universal speedup or measure the
  change from a previous release.

For example, run the September 20 UPDATE cohort at its recorded revision:

```sh
cargo bench --locked -p citadeldb-sql --bench h2h_bench -- \
  '^(update|update_gen_propagate)/(citadel|sqlite)/$' \
  --sample-size 30 --warm-up-time 3 --measurement-time 8 --noplot
```

For a source comparison, build and preserve both source snapshots first, then run
the cohort's recorded case filter and run order, with no concurrent builds and
fixed CPU affinity. The command above runs the current checkout; reproducing a
published row requires its recorded revision and cohort. Exact Criterion IDs,
executable hashes, and per-run results are in
[sql-benchmarks.json](site/data/sql-benchmarks.json).

</details>

## SQL

**Statements** - CREATE/DROP TABLE (incl. `TEMP`), ALTER TABLE (ADD/DROP/RENAME COLUMN, RENAME TABLE, DISABLE/ENABLE TRIGGER), CREATE/DROP INDEX (incl. partial `WHERE`, expression keys, `CONCURRENTLY`), CREATE/DROP VIEW, CREATE/DROP MATERIALIZED VIEW (with `REFRESH [CONCURRENTLY]`), CREATE/DROP TRIGGER (BEFORE/AFTER/INSTEAD OF, FOR EACH ROW/STATEMENT, `REFERENCING NEW/OLD TABLE`, `WHEN`, `UPDATE OF cols`), INSERT (VALUES, SELECT, ON CONFLICT DO NOTHING/DO UPDATE, ON CONSTRAINT), SELECT, UPDATE, DELETE, TRUNCATE TABLE, RETURNING (with `OLD`/`NEW`), BEGIN [READ ONLY | READ WRITE]/COMMIT/ROLLBACK, SAVEPOINT/RELEASE/ROLLBACK TO, SET [LOCAL] TIME ZONE, EXPLAIN, REFRESH MATERIALIZED VIEW

**Constraints** - PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT, CHECK (column + table level), FOREIGN KEY with full referential actions (`ON DELETE` / `ON UPDATE` `CASCADE` / `SET NULL` / `SET DEFAULT` / `RESTRICT` / `NO ACTION`), GENERATED ALWAYS AS (...) STORED|VIRTUAL

**Collations** - `BINARY`, `NOCASE` (ASCII case-insensitive), and `RTRIM` (ignores trailing spaces). Text primary keys use their declared collation; foreign keys use the referenced columns' collation. Column index keys inherit their column's collation unless overridden with `COLLATE`.

**Types** - INTEGER, REAL, TEXT, BLOB, BOOLEAN, DATE, TIME, TIMESTAMP (WITH TIME ZONE), INTERVAL, JSON, JSONB, TSVECTOR, TSQUERY, ARRAY

**JSON / JSONB** - Postgres operators plus SQL/JSON path functions and the SQL:2023 item methods `.bigint()`, `.decimal()`, `.integer()`, `.number()`, `.string()`, `.boolean()`, `.date()`, `.time()`, `.time_tz()`, `.timestamp()`, and `.timestamp_tz()`. Time-zone-dependent evaluation uses the connection's transactional `SET [LOCAL] TIME ZONE` context.

**Clauses** - JOINs (INNER, LEFT, RIGHT, CROSS, FULL OUTER, LATERAL), subqueries (scalar, IN, EXISTS, correlated), CTEs (`WITH` / `WITH RECURSIVE` / WITH-DML: `WITH x AS (INSERT/UPDATE/DELETE ... [RETURNING *]) SELECT ...`), UNION/INTERSECT/EXCEPT [ALL], CASE, BETWEEN, LIKE, DISTINCT, `ANY` / `ALL` (subquery + array forms), GROUP BY/HAVING, ORDER BY, LIMIT/OFFSET

**Window functions** - ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, SUM/COUNT/AVG/MIN/MAX OVER with PARTITION BY, ORDER BY, ROWS/RANGE frames

**Views** - CREATE/DROP VIEW, OR REPLACE, IF NOT EXISTS/IF EXISTS, column aliases, nested views

**Materialized views** - `CREATE MATERIALIZED VIEW [IF NOT EXISTS] name AS SELECT ...`, `REFRESH MATERIALIZED VIEW [CONCURRENTLY] name` (`CONCURRENTLY` does a diff-merge - DELETE removed rows, UPDATE changed rows, INSERT new rows - instead of TRUNCATE+repopulate), `DROP MATERIALIZED VIEW [CASCADE]`, full backing-table semantics (indexes, joins, planner sees a real table), `pg_matviews` introspection

**Triggers** - `CREATE TRIGGER name {BEFORE|AFTER|INSTEAD OF} {INSERT|UPDATE [OF cols]|DELETE} ON table FOR EACH {ROW|STATEMENT} [REFERENCING NEW TABLE AS new_t OLD TABLE AS old_t] [WHEN (expr)] BEGIN ... END`. INSTEAD OF triggers make views writable. Transition tables work as virtual tables in trigger bodies. `ALTER TABLE ... DISABLE/ENABLE TRIGGER [name|ALL]`. PG-faithful name-order firing. Introspection via `information_schema.triggers` and `SHOW TRIGGERS [ON table]`.

**TEMP tables** - `CREATE TEMP TABLE ...` lives in a per-connection in-memory database, dropped on disconnect. Full DDL/DML/index/constraint/trigger parity with persistent tables.

**Functions** - COUNT, SUM, AVG, MIN, MAX, LENGTH, UPPER, LOWER, SUBSTR/SUBSTRING, TRIM/LTRIM/RTRIM, REPLACE, INSTR, CONCAT, HEX, ABS, ROUND, CEIL/CEILING, FLOOR, SIGN, SQRT, RANDOM, COALESCE, NULLIF, CAST, TYPEOF, IIF

**Date/Time Functions** - NOW, CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME, LOCALTIMESTAMP, LOCALTIME, CLOCK_TIMESTAMP, EXTRACT, DATE_PART, DATE_TRUNC, DATE_BIN, AGE, MAKE_DATE, MAKE_TIME, MAKE_TIMESTAMP, MAKE_INTERVAL, JUSTIFY_DAYS, JUSTIFY_HOURS, JUSTIFY_INTERVAL, ISFINITE, DATE, TIME, DATETIME, STRFTIME, JULIANDAY, UNIXEPOCH, TIMEDIFF, AT TIME ZONE. Supports `INTERVAL '1 year 2 months'`, `DATE '2024-01-15'`, `TIMESTAMP '2024-01-15 12:30:00Z'`, `infinity`/`-infinity` sentinels, BC dates, full IANA zone parsing (jiff), PG-normalized INTERVAL comparison.

**Full-text search** - `tsvector` / `tsquery` types, `to_tsvector` / `to_tsquery` / `plainto_tsquery` / `phraseto_tsquery` / `websearch_to_tsquery` builders, `@@` match operator, `ts_rank` / `ts_rank_cd` ranking with weighted positions (A/B/C/D), prefix matching (`term:*`), phrase distance (`<N>`), inverted indexes via `CREATE INDEX ... USING fts`

**System catalog** - `information_schema.tables`, `information_schema.columns`, `information_schema.key_column_usage`, `information_schema.table_constraints`, `information_schema.triggers`, `pg_timezone_names`, `pg_timezone_abbrevs`, `pg_matviews` (virtual tables, queryable). `SHOW TRIGGERS [ON table]` and `SHOW MATERIALIZED VIEWS` shorthands for the corresponding catalog queries.

**Prepared statements** - `$1, $2, ...` positional parameters with LRU statement cache plus snapshot-tagged plan caching for joins and compound queries (cache invalidates only on commit, never per-call)

**Multi-statement scripts** - `Connection::execute_script(sql)` runs `;`-separated statements in one call, returning per-statement outcomes with partial-success preserved. WASM: `db.run(sql)` returns `[{type, ...}, ...]`.

**UPSERT** - `INSERT ... ON CONFLICT (cols) DO NOTHING` / `DO UPDATE SET col = excluded.col ... WHERE ...` and `ON CONFLICT ON CONSTRAINT idx_name`. `excluded.*` refers to the proposed row; bare `col` refers to the existing row.

## Security

**No plaintext on disk.** Every page is encrypted before writing and authenticated before reading.

**Separate key file.** Encryption keys live in `{dbname}.citadel-keys`, not inside the database. The passphrase derives a master key in memory via Argon2id (or PBKDF2 in the FIPS-oriented at-rest profile) and never touches disk.

**Key backup.** Export an encrypted key backup with a separate recovery passphrase. Restore access without re-encrypting the entire database.

**Instant rekey.** Changing the passphrase re-wraps the root encryption key. No page re-encryption - instant regardless of database size.

**Encrypted sync.** Noise protocol (`NNpsk0_25519_ChaChaPoly_BLAKE2s`) with a 256-bit pre-shared key. Ephemeral Curve25519 keys per session for forward secrecy.

## Architecture

```
Clients and bindings:
+---------------------------------------------+
|               citadel-studio                |  Memory, SQL, and vault client
+----------------------+----------------------+
|     citadel-cli      |    citadel-python    |  CLI, Python wheel
+----------------------+----------------------+
|     citadel-ffi      |     citadel-wasm     |  C FFI, WebAssembly
+----------------------+----------------------+

Agent layer:
+---------------------------------------------+
|                 citadel-ai                  |  Agent runtime (ReAct + Reflexion)
+---------------------------------------------+
|                 citadel-llm                 |  LLM clients: Claude, OpenAI, Ollama, Gemini
+---------------------------------------------+

Memory layer:
+---------------------------------------------+
|                 citadel-mcp                 |  MCP server for memory tools
+---------------------------------------------+
|                 citadel-mem                 |  Regions, atoms, recall, erasure
+---------------------------------------------+
|                citadel-vector               |  VECTOR(N) type + PRISM filtered ANN
+---------------------------------------------+

Encrypted database engine:
+----------------------+----------------------+
|     citadel-sql      |    sql-json-path     |  SQL frontend, SQL/JSON paths
+----------------------+----------------------+
|                   citadel                   |  Database API, builder, vault lifecycle
+-------------+--------------+----------------+
| citadel-txn | citadel-sync | citadel-crypto |  Transactions, replication, keys
+-------------+--------------+----------------+
|       citadel-buffer       |  citadel-page  |  Buffer pool (SIEVE), page codec
+----------------------------+----------------+
|                 citadel-io                  |  File I/O, fsync, io_uring
+---------------------------------------------+
|                citadel-core                 |  Types, errors, cancellation
+---------------------------------------------+

Evaluation harnesses:
+----------------------+----------------------+
|   citadel-membench   |     citadel-swe      |  Memory and agent benchmarks
+----------------------+----------------------+
```

Studio calls the database and SQL APIs directly and uses `MemoryMaintenance` for
stored-memory inspection and erasure. It needs no MCP server or embedding model.

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
- **Commit slots** have two accepted formats. V1 slots carry a truncated HMAC-SHA256 over every field except the MAC itself; legacy slots carry only a keyless checksum over a prefix. Checksum-valid legacy slots remain readable only while no V1 requirement is recorded. Once both physical slots are valid V1 and the vault records that one-way requirement, any checksum-valid legacy slot is rejected as downgrade evidence, and writers refuse to create one.
- **Rollback to an older genuine state** is outside this boundary. An earlier authenticated slot plus its matching pages can pass the data-file checks; an older internally consistent snapshot of all local vault state, including the data, key, and retained audit files, also passes local authentication. Detecting freshness requires an external anchor - for example, store the latest commit's `txn_id` and Merkle root outside the attacker's reach and compare them after opening.

## Language Bindings

### C / C++

Static or dynamic library with auto-generated `citadel.h` (cbindgen). Exported entry points are panic-safe.

```c
#include "citadel.h"

int main(void) {
    struct CitadelDb *db = NULL;
    struct CitadelSqlConn *conn = NULL;
    struct CitadelSqlResult *result = NULL;
    citadel_error_t status = citadel_create(
        "my.db", (const uint8_t *)"secret", 6, NULL, &db);
    if (status != CITADEL_ERROR_T_OK) goto cleanup;

    status = citadel_sql_open(db, &conn);
    if (status != CITADEL_ERROR_T_OK) goto cleanup;
    status = citadel_sql_execute(conn, "SELECT 1 + 1 AS value;", &result);

cleanup:
    citadel_sql_result_free(result);
    citadel_sql_close(conn);
    citadel_close(db);
    return status == CITADEL_ERROR_T_OK ? 0 : 1;
}
```

### WebAssembly

Install with `npm install @citadeldb/wasm`.

```js
import init, { CitadelDb } from "@citadeldb/wasm";

await init();

const db = new CitadelDb("secret");
db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);");
db.execute("INSERT INTO t (id, name) VALUES (1, 'Alice');");

const result = db.query("SELECT * FROM t;");
// { columns: ["id", "name"], rows: [[1, "Alice"]] }

db.put(new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6]));
db.free();
```

Build the npm package: `bash scripts/publish-wasm.sh`

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

Rust 1.95+.

```bash
git clone https://github.com/yp3y5akh0v/citadel.git
cd citadel
cargo build --release
```

### Feature Flags

| Flag | Description |
|------|-------------|
| `audit-log` | HMAC-SHA256-chained audit log (default: on); no external anti-rollback anchor |
| `fips` | At-rest PBKDF2 + AES-256-CTR profile; not whole-product validation |
| `io-uring` | Linux io_uring async I/O |

## License

[Apache-2.0](https://github.com/yp3y5akh0v/citadel/blob/HEAD/LICENSE-APACHE)
