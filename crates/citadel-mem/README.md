# citadeldb-mem

Encrypted-first memory engine, built on the
[Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database. Stores memory as
regions of typed atoms connected by typed edges, retrieves through a hybrid pipeline (vector
ANN + BM25 keyword + cross-encoder reranker), and **forgets by destroying keys** -
cryptographic erasure at whole-store, per-region, and per-atom granularity. On encrypted regions every atom is sealed and HMAC-authenticated, and can be
re-verified off disk.

It uses **no LLM** at ingest or retrieval - raw turns in, vector + keyword + reranker out -
so remembering costs zero tokens and the conversation is never sent to an LLM to build or
search the memory. On the LoCoMo
long-term conversational-memory benchmark, on encrypted regions with a matched `gpt-4o-mini`
reader and judge, it scores 87.2% (3-run mean); the deterministic retrieval ceiling is 94.5%. On the
LongMemEval_S full-haystack split it scores 86.2% with a `gpt-4o` reader. Full protocol, audit,
comparison, and numbers:
[citadel-membench](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-membench).

This crate is part of the Citadel workspace.

## License

Apache-2.0
