# citadeldb-membench

LoCoMo and LongMemEval long-term-memory benchmark harnesses for
[`citadeldb-mem`](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-mem). Runs the
benchmarks (LoCoMo on encrypted regions, LongMemEval_S on the full-haystack split), scores answers,
and emits self-describing reports. LoCoMo uses the harness's reader and judge prompts;
LongMemEval uses the official CoT reader prompt and external judge protocol.

Historical results and their configurations are in [RESULTS.md](RESULTS.md); the end-to-end run procedure is in
[RUNBOOK.md](RUNBOOK.md). This crate is part of the Citadel workspace and is not published to
crates.io.

## Running

Build the runners with `cargo build --release -p citadeldb-membench --features openai,candle-embed --bins --locked`.
Use `cuda-embed` instead of `candle-embed` for CUDA execution.

[run.ps1](run.ps1) launches the native benchmark binaries and writes to a new run directory.
LoCoMo scored runs produce a report and per-question audits:

```powershell
./crates/citadel-membench/run.ps1 -Benchmark locomo -Mode scored -Label baseline `
  -Dataset ./data/locomo10.json -EmbedderDir ./models/e5-large `
  -RerankDir ./models/ms-marco-minilm -KeyFile ./openai-key.txt
```

Use `-Mode retrieval-diag` for retrieval measurements without reader or judge calls.
The configured-recall diagnostic uses the same memory-engine recipe as scored runs;
its other layers are explicitly labelled controls. Annotated-evidence coverage is
not an answer score. `-Mode dry-run` validates the dataset without loading models.

`-Benchmark longmemeval` selects the LongMemEval binary. Scored mode produces
`hypotheses.jsonl` for the official evaluator; it does not report the final QA score.
See [RUNBOOK.md](RUNBOOK.md) for generation and scoring commands.

Both benchmarks use `citadel-mem` for ingestion and retrieval, including the selected
embedder, fusion, reranker and encryption mode. They validate reused corpora and
record the memories rendered into reader requests. Missing model prices are
reported as unknown. Hosted calls require the selected providers' credentials.

## Verification

```powershell
cargo test -p citadeldb-membench --locked
cargo test -p citadeldb-membench --features openai,candle-embed --all-targets --locked
pwsh -File crates/citadel-membench/tests/launcher.ps1
pwsh -File crates/citadel-membench/tests/audit-scripts.ps1
```

## License

Apache-2.0
