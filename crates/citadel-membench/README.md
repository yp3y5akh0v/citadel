# citadeldb-membench

LoCoMo and LongMemEval long-term-memory benchmark harnesses for
[`citadeldb-mem`](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-mem). Runs the
benchmarks (LoCoMo on encrypted regions, LongMemEval_S on the full-haystack split), scores answers,
and emits self-describing reports. LoCoMo uses the harness's reader and judge prompts;
LongMemEval uses the official CoT reader prompt and external judge protocol.

Historical results and their configurations are in [RESULTS.md](RESULTS.md); the end-to-end run procedure is in
[RUNBOOK.md](RUNBOOK.md). This crate is part of the Citadel workspace and is not published to
crates.io.

## License

Apache-2.0
