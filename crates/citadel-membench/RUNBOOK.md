# citadel-membench RUNBOOK (LongMemEval)

End-to-end procedure for the LongMemEval benchmark. Path/credentials are placeholders
(`<...>`); substitute your own. Examples use PowerShell.

## Prerequisites
- An Ampere+ NVIDIA GPU for the fast path (the `cuda-embed` feature enables TF32 +
  length-bucketed embedding). CPU works via `candle-embed` but is much slower.
- A local embedder model dir, e.g. `bge-large-en-v1.5` (`<EMBEDDER_DIR>`).
- The LongMemEval oracle dataset `longmemeval_oracle.json` (`<DATASET>`).
- The official LongMemEval repo cloned (`<LME_REPO>`) for scoring, and a Python venv
  with `openai backoff tqdm numpy` (`<PY>` = its python).
- An OpenAI API key for the QA run + scoring (the diagnostic below needs neither).

## Build
GPU: `cargo run -q -p citadeldb-membench --features openai,cuda-embed --bin longmemeval -- <DATASET>`
CPU: swap `cuda-embed` -> `candle-embed`.

## Env knobs (see the bin header for the full list)
- `OPENAI_API_KEY` - load inline; never commit/echo.
- `PYO3_PYTHON` - the real python.exe so cargo can build the pyo3 crates (clippy/build).
- `CITADEL_EMBEDDER_DIR=<EMBEDDER_DIR>` - the embedder model dir (any bge/e5/granite dir).
- `CITADEL_LONGMEMEVAL_EMBEDDER` - bge-large|bge-base|bge-small|e5-large|granite-r2 (default bge-small).
- `CITADEL_LONGMEMEVAL_OUT` - prediction JSONL path.
- `CITADEL_LONGMEMEVAL_READER_CONCURRENCY` - reader calls in flight.
- `CITADEL_LONGMEMEVAL_READER_TPM` - per-model tokens/min (default is model-aware: gpt-4o-mini -> 2M, else 200k).
- `CITADEL_LONGMEMEVAL_MAX_SAMPLES=N` - cap to the first N questions.
- `CITADEL_LONGMEMEVAL_ENCRYPTED=true` - seal atoms per-region key (default plaintext).
- `CITADEL_LONGMEMEVAL_RETRIEVAL_DIAG=1` - token-free recall@k diagnostic (no reader/key).
- `CITADEL_MEMBENCH_MAX_TOKENS` - reader output cap OVERRIDE (LongMemEval defaults to 800 = CoT gen_length).

## QA run (the score)
Set `OPENAI_API_KEY`, `PYO3_PYTHON`, `CITADEL_EMBEDDER_DIR=<EMBEDDER_DIR>`,
`CITADEL_LONGMEMEVAL_EMBEDDER=bge-large`, `CITADEL_LONGMEMEVAL_OUT=<OUT>`,
`CITADEL_LONGMEMEVAL_READER_CONCURRENCY=8`, then run the build command above.
Phase 1 ingests one region per question (`ingested N/500`); phase 2 runs the reader
(`answered N/500`, where OpenAI charges happen) and writes the JSONL at the end.
Reader defaults: gpt-4o-mini, the official CoT prompt, max_tokens 800.

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
per-type tailoring; the reader never sees the type label, gold, or `has_answer`.

## Verify before any commit
```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings              # needs PYO3_PYTHON
cargo clippy -p citadeldb-membench --features openai,cuda-embed --all-targets -- -D warnings
cargo test -p citadeldb-membench
```

## Caveats to report with any number
Oracle = retrieval-complete (a reader-ceiling upper bound, not end-to-end); always NAME
the reader model (headline figures are reader-dependent). Paper references (arxiv
2410.10813): gpt-4o oracle 0.870, gpt-4o-mini oracle 0.744. The full-haystack split is
the run that stresses citadel's retrieval.
