# citadeldb-membench

LoCoMo and LongMemEval long-term-memory benchmark harnesses for
[`citadeldb-mem`](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-mem). Runs the
benchmarks (LoCoMo on encrypted regions, LongMemEval on the oracle split), scores answers with
the official LLM judge, and emits reproducible, self-describing reports.

Results and protocol are in [RESULTS.md](RESULTS.md); the end-to-end run procedure is in
[RUNBOOK.md](RUNBOOK.md). This crate is part of the Citadel workspace and is not published to
crates.io.

## License

MIT OR Apache-2.0
