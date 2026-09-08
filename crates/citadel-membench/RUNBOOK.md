# LongMemEval runbook

Procedure for the LongMemEval benchmark. Path/credentials are placeholders
(`<...>`); substitute your own. Examples use PowerShell.

## Prerequisites

- CUDA execution uses the `cuda-embed` feature; Ampere+ GPUs support TF32.
  CPU execution uses `candle-embed`.
- A local embedder model dir, e.g. `e5-large` (`<EMBEDDER_DIR>`).
- The LongMemEval dataset (`<DATASET>`): `longmemeval_s_cleaned.json` for the full-haystack
  evaluation, or `longmemeval_oracle.json` for oracle-context evaluation.
- The official LongMemEval repo cloned (`<LME_REPO>`) for scoring, and a Python venv
  with `openai backoff tqdm numpy` (`<PY>` = its python).
- An OpenAI API key for answer generation and scoring. Retrieval diagnostics require no API key.

## Build

GPU: `cargo build --release -p citadeldb-membench --features openai,cuda-embed --bins --locked`
CPU: swap `cuda-embed` for `candle-embed`.
Building the binaries does not run the benchmark or call a hosted model.

## Configuration

These environment variables configure direct binary invocation. With `run.ps1`,
use its parameters, such as `-Reader`, `-Encrypted`, and `-DbPath`.

- `OPENAI_API_KEY` - API credential for answer generation. Keep it out of source control and logs.
- `CITADEL_EMBEDDER_DIR=<EMBEDDER_DIR>` - the embedder model dir (any e5/bge/granite dir).
- `CITADEL_LONGMEMEVAL_EMBEDDER` - e5-large|e5-large-v2|bge-large|bge-base|bge-small|granite-r2 (default e5-large).
- `CITADEL_RERANKER_DIR=<RERANKER_DIR>` - cross-encoder reranker dir (ms-marco-MiniLM-L-6-v2). Omit for fusion without cross-encoder reranking.
- `CITADEL_LONGMEMEVAL_RERANK_STRATEGY` - replace|rrf (default rrf).
- `CITADEL_LONGMEMEVAL_READER_MODEL` - reader model (default `gpt-4o`). Record the selected model with the reported score.
- `CITADEL_LONGMEMEVAL_OUT` - new prediction JSONL path; existing files are never overwritten.
- `CITADEL_LONGMEMEVAL_AUDIT_PATH` - optional new JSONL path for retrieval-order atom/session IDs and each reader call's actual rendered atom order, request fingerprint, token usage and run configuration.
- `CITADEL_LONGMEMEVAL_MODE` - `scored` (default), `retrieval-diag`, or `dry-run` (parse and validate the dataset without loading models).
- `CITADEL_LONGMEMEVAL_READER_CONCURRENCY` - reader calls in flight.
- `CITADEL_LONGMEMEVAL_READER_TPM` - per-model tokens/min (default is model-aware: gpt-4o-mini -> 2M, else 200k).
- `CITADEL_LONGMEMEVAL_MAX_SAMPLES=N` - cap to the first N questions.
- `CITADEL_LONGMEMEVAL_ENCRYPTED=true` - seal atoms per-region key (default plaintext).
- `CITADEL_LONGMEMEVAL_DB_PATH=<PATH>.cdl` - persist the DB; later runs validate and reuse its stored corpus. Keep `ENCRYPTED` identical between build and reuse.
- `CITADEL_LONGMEMEVAL_TOP_K` - recall depth (default 50).
- `CITADEL_MEMBENCH_MAX_TOKENS` - reader output-token limit (default 800, matching the CoT `gen_length`).

## Generate hypotheses

From the repository root:

```powershell
./crates/citadel-membench/run.ps1 -Benchmark longmemeval -Mode scored -Label baseline `
  -Dataset <DATASET> -EmbedderDir <EMBEDDER_DIR> -RerankDir <RERANKER_DIR> `
  -Reader gpt-4o -KeyFile <KEY_FILE>
```

The launcher writes `hypotheses.jsonl`, `audit.jsonl` and `run.log` in a new run
directory. Use that `hypotheses.jsonl` path as `<OUT>` in the scoring commands below.
The reader defaults to `gpt-4o` with the official CoT prompt and an 800-token output limit.
The runner ingests one region per question, then generates answers. Predictions are flushed as each
question completes; the official scorer joins them by `question_id`, not line order.
Complete the selected question set before reporting a benchmark score.

## Reuse a persisted DB

Pass `-DbPath "<PATH>.cdl"` to `run.ps1` to retain the ingested corpus.
For direct binary invocation, set `CITADEL_LONGMEMEVAL_DB_PATH=<PATH>.cdl`.

- First run (path missing): create the database and ingest the selected haystacks.
- Later runs (path present): attach existing regions, verify their stored turns against
  the current ingestion inputs, then recall from the stored vectors without re-embedding.

Missing, extra or changed live turns fail validation before reader calls. A different encryption
mode or embedder identity also fails. Use a new database path to rebuild; validation never
overwrites an existing corpus. Older LongMemEval corpora without `session_occurrence`
metadata must be rebuilt. This field distinguishes repeated session IDs; the official
session ID remains the retrieval-scoring key.

The retrieval diagnostic performs the same validation. The corpus consists of the `.cdl` file
and its sidecars - `.cdl.citadel-keys`, `.citadel-regions`,
`.citadel-atomkeys`, `.citadel-audit` - copy or delete them as a set (a missing `.citadel-regions`
fails re-attach with `RegionForgotten`). LoCoMo has the same `CITADEL_LOCOMO_DB_PATH` for its
scored run (its token-free modes need a fresh DB).

## Official scoring

On Windows, set `PYTHONUTF8=1` so the evaluator reads UTF-8 hypotheses consistently.
```
& <PY> <LME_REPO>/src/evaluation/evaluate_qa.py gpt-4o <OUT> <DATASET>      # gpt-4o judge -> <OUT>.eval-results-gpt-4o
& <PY> <LME_REPO>/src/evaluation/print_qa_metrics.py <OUT>.eval-results-gpt-4o <DATASET>
```
Reports per-question-type + Task-averaged + Overall + Abstention accuracy.

## Retrieval diagnostic (token-free, no API key)

```powershell
./crates/citadel-membench/run.ps1 -Benchmark longmemeval -Mode retrieval-diag -Label retrieval `
  -Dataset <DATASET> -EmbedderDir <EMBEDDER_DIR> -RerankDir <RERANKER_DIR>
```

`CITADEL_LONGMEMEVAL_MODE=retrieval-diag` ingests or validates the corpus, then recalls
top-k with default and semantic-only profiles (no reader). It prints recall
any%/all% @10/30/50 vs the gold, at session granularity
(`answer_session_ids`) and turn granularity (`has_answer`) - mirroring the official
LongMemEval retrieval metric. This is citadel's own retrieval-quality measure.
With a nondefault top-k, the reported cutoffs are `min(10,k)`, `min(30,k)`, and `k`.
Unset neighbor expansion for this diagnostic; it measures recall before rendering.
Select the mode with `-Mode` or `CITADEL_LONGMEMEVAL_MODE`.

## Reader prompt

`benchmarks/longmemeval/prompts.rs::build_reader_prompt` replicates the official
`run_generation.py` CoT template: generic instruction, retrieved chats sorted by date,
`Current Date: {question_date}`, single user message, `Answer (step by step):`. No
per-type tailoring; the reader never sees the type label, gold, or `has_answer`. This
canonical date/conversation ordering is fixed; LongMemEval does not expose the LoCoMo
reader-order switch because regrouping here would erase that input ordering.

Corpus validation, request fingerprints and reader-usage accounting use the shared
harness. Estimated input tokens are
not exact tokenizer counts or an enforced input-token budget.

## Verification

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings              # needs PYO3_PYTHON
cargo clippy -p citadeldb-membench --features openai,cuda-embed --all-targets -- -D warnings
cargo test -p citadeldb-membench
```
