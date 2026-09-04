# citadel-membench RUNBOOK (LongMemEval)

End-to-end procedure for the LongMemEval benchmark. Path/credentials are placeholders
(`<...>`); substitute your own. Examples use PowerShell.

## Prerequisites
- An Ampere+ NVIDIA GPU for the fast path (the `cuda-embed` feature enables TF32 +
  length-bucketed embedding). CPU works via `candle-embed` but is much slower.
- A local embedder model dir, e.g. `e5-large` (`<EMBEDDER_DIR>`).
- The LongMemEval dataset (`<DATASET>`): `longmemeval_s_cleaned.json` for the full-haystack
  headline run, or `longmemeval_oracle.json` for the reader-ceiling.
- The official LongMemEval repo cloned (`<LME_REPO>`) for scoring, and a Python venv
  with `openai backoff tqdm numpy` (`<PY>` = its python).
- An OpenAI API key for the QA run + scoring (the diagnostic below needs neither).

## Build
Use `--release` for full runs (debug is fine for a small `CITADEL_LONGMEMEVAL_MAX_SAMPLES` smoke).
GPU: `cargo run -q --release -p citadeldb-membench --features openai,cuda-embed --bin longmemeval -- <DATASET>`
CPU: swap `cuda-embed` -> `candle-embed`.

## Env knobs (see the bin header for the full list)
- `OPENAI_API_KEY` - load inline; never commit/echo.
- `PYO3_PYTHON` - the real python.exe so cargo can build the pyo3 crates (clippy/build).
- `CITADEL_EMBEDDER_DIR=<EMBEDDER_DIR>` - the embedder model dir (any e5/bge/granite dir).
- `CITADEL_LONGMEMEVAL_EMBEDDER` - e5-large|e5-large-v2|bge-large|bge-base|bge-small|granite-r2 (default e5-large).
- `CITADEL_RERANKER_DIR=<RERANKER_DIR>` - cross-encoder reranker dir (ms-marco-MiniLM-L-6-v2); the best-recall config, matching LoCoMo. Omit for embedder-only.
- `CITADEL_LONGMEMEVAL_RERANK_STRATEGY` - replace|rrf (default rrf).
- `CITADEL_LONGMEMEVAL_READER_MODEL` - reader model (default `gpt-4o`, the headline/comparable tier). Set `gpt-4o-mini` explicitly only for a lower-cost diagnostic.
- `CITADEL_LONGMEMEVAL_OUT` - prediction JSONL path.
- `CITADEL_LONGMEMEVAL_READER_CONCURRENCY` - reader calls in flight.
- `CITADEL_LONGMEMEVAL_READER_TPM` - per-model tokens/min (default is model-aware: gpt-4o-mini -> 2M, else 200k).
- `CITADEL_LONGMEMEVAL_MAX_SAMPLES=N` - cap to the first N questions.
- `CITADEL_LONGMEMEVAL_ENCRYPTED=true` - seal atoms per-region key (default plaintext).
- `CITADEL_LONGMEMEVAL_DB_PATH=<PATH>.cdl` - persist the DB; later runs validate and reuse its stored corpus. Keep `ENCRYPTED` identical between build and reuse.
- `CITADEL_LONGMEMEVAL_RETRIEVAL_DIAG=1` - token-free recall@k diagnostic (no reader/key).
- `CITADEL_MEMBENCH_MAX_TOKENS` - reader output cap OVERRIDE (LongMemEval defaults to 800 = CoT gen_length).

## QA run (the score)
Set `OPENAI_API_KEY`, `PYO3_PYTHON`, `CITADEL_EMBEDDER_DIR=<EMBEDDER_DIR>`,
`CITADEL_LONGMEMEVAL_EMBEDDER=e5-large`, `CITADEL_RERANKER_DIR=<RERANKER_DIR>` (best recall),
`CITADEL_LONGMEMEVAL_OUT=<OUT>`, `CITADEL_LONGMEMEVAL_READER_CONCURRENCY=8`, then run the build
command above. The full-haystack headline reader defaults to `gpt-4o`.
Phase 1 ingests one region per question (`ingested N/500`); phase 2 runs the reader
(`answered N/500`, where OpenAI charges happen) and writes the JSONL at the end.
Reader defaults: gpt-4o, the official CoT prompt, max_tokens 800.

## Reuse a persisted DB

Set `CITADEL_LONGMEMEVAL_DB_PATH=<PATH>.cdl` to retain the ingested corpus:

- First run (path missing): create the database and ingest the selected haystacks.
- Later runs (path present): attach existing regions, verify their stored turns against
  the current ingestion inputs, then recall from the stored vectors without re-embedding.

Missing, extra or changed live turns fail validation before reader calls. A different encryption
mode or embedder identity also fails. Use a new database path to rebuild; validation never
overwrites an existing corpus. Older LongMemEval corpora without `session_occurrence`
metadata must be rebuilt. This field distinguishes repeated session IDs; the official
session ID remains the retrieval-scoring key.

The retrieval diagnostic performs the same validation. The cache is the `.cdl` PLUS
its sidecars - `.cdl.citadel-keys`, `.citadel-regions`,
`.citadel-atomkeys`, `.citadel-audit` - copy or delete them as a set (a missing `.citadel-regions`
fails re-attach with `RegionForgotten`). LoCoMo has the same `CITADEL_LOCOMO_DB_PATH` for its
scored run (its token-free modes need a fresh DB).

## Score (official; Windows gotcha)
The official scripts `open()` with the platform default encoding, which on Windows is
cp1252 and chokes on the UTF-8 hypotheses. Set `PYTHONUTF8=1`.
```
& <PY> <LME_REPO>/src/evaluation/evaluate_qa.py gpt-4o <OUT> <DATASET>      # gpt-4o judge -> <OUT>.eval-results-gpt-4o
& <PY> <LME_REPO>/src/evaluation/print_qa_metrics.py <OUT>.eval-results-gpt-4o <DATASET>
```
Reports per-question-type + Task-averaged + Overall + Abstention accuracy.

## Retrieval diagnostic (token-free, no API key)
`CITADEL_LONGMEMEVAL_RETRIEVAL_DIAG=1` ingests + recalls top-k once per question (no
reader) and prints recall any%/all% @10/30/50 vs the gold, at session granularity
(`answer_session_ids`) and turn granularity (`has_answer`) - mirroring the official
LongMemEval retrieval metric. This is citadel's own retrieval-quality measure.

## Reader prompt = official protocol
`benchmarks/longmemeval/prompts.rs::build_reader_prompt` replicates the official
`run_generation.py` CoT template: generic instruction, retrieved chats sorted by date,
`Current Date: {question_date}`, single user message, `Answer (step by step):`. No
per-type tailoring; the reader never sees the type label, gold, or `has_answer`. This
canonical date/conversation ordering is fixed; LongMemEval does not expose the LoCoMo
reader-order switch because regrouping here would erase that input ordering.

## Verify before any commit
```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings              # needs PYO3_PYTHON
cargo clippy -p citadeldb-membench --features openai,cuda-embed --all-targets -- -D warnings
cargo test -p citadeldb-membench
```
