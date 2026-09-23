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

[run.ps1](run.ps1) requires PowerShell 7 or later, launches the native benchmark
binaries, and writes to a new run directory. Progress appears live on stderr and
in `run.log`; stdout is saved separately. LongMemEval reports region ingestion
or reuse progress and elapsed embedding time separately from other preparation work.
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

## Temporal context

Add `-TemporalGlosses` to a scored run to enable `conservative-session-v2`
(default: off). The renderer appends calendar dates to supported English relative
expressions using the source session date: for example, `Yesterday (5 October
2022)` in a session dated 6 October 2022. It preserves the original dialogue,
speaker attribution and image metadata; stored text and retrieval are unchanged.

The policy handles literal day references, last/this/next month or year, and
counted days/weeks/months/years ago. Weeks ago use an approximate date (`around`);
months and years retain that precision. Ambiguous periods such as `last week`
and `this weekend` remain unchanged. It skips quoted text, code and expressions
after a recognized date anchor in the same sentence. Supported expressions are
assumed to refer to their session; these lexical checks do not resolve every
narrative or reported-speech anchor. The flag and policy are recorded in the run audits.

## Requests and failure records

Reader and judge requests use temperature 0 and seed 1. The seed is part of
request identity; hosted outputs can still vary between runs. Audits record
request hashes, rendered atom IDs, finish reasons and individual attempts.
With `-Agentic`, invalid or truncated extraction output fails explicitly;
only the protocol's `NOT_ENUMERATION` response selects the ordinary reader.

The launcher writes question completion and failure records to
`audit.json.events.jsonl` for LoCoMo and `hypotheses.jsonl.events.jsonl` for
LongMemEval. A failed run exits nonzero and retains completed questions and
available call records. Missing usage remains unknown; reported token totals
and estimated costs are not billing receipts. A later failure event for the
same question replaces its earlier completion event for accounting.

## Verification

```powershell
cargo test -p citadeldb-membench --locked
cargo test -p citadeldb-membench --features openai,candle-embed --all-targets --locked
pwsh -File crates/citadel-membench/tests/launcher.ps1
pwsh -File crates/citadel-membench/tests/launcher-progress.ps1
pwsh -File crates/citadel-membench/tests/audit-scripts.ps1
```

## License

Apache-2.0
