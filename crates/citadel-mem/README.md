# citadeldb-mem

Encrypted-first memory engine, built on the
[Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database. Stores memory as
regions of typed atoms connected by typed edges. Retrieval combines vector ANN, keyword
matching, recency, and importance, with an optional cross-encoder reranker. Encrypted
regions seal and HMAC-authenticate each atom, support off-disk verification, and forget
atoms by destroying their keys.

Memory storage and retrieval do not require a summarizer or generative LLM. Embeddings
and reranking use the backends selected by the application; a remote backend may send
text to its provider. See [memory benchmarks](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-membench)
for measured configurations and evaluation results.

## Embeddings and retrieval

The default crate has no model runtime. Supply an `Embedder`, or enable `candle-embed`
for local Candle models and `CrossEncoder`. `cuda-embed` adds NVIDIA GPU support.
Models must be supplied locally; the separate `citadeldb-mcp pull` command downloads
verified model snapshots. `MockEmbedder` is a lexical test backend, not semantic recall.

With `cuda-embed`, both `CandleEmbedder` and `CrossEncoder` require CUDA GPU 0;
initialization failures return an error. For CPU inference, enable `candle-embed`
without `cuda-embed`. Both CUDA constructors enable Candle's process-wide TF32
mode for f32 matrix multiplication, including standalone reranking.

In 2.2, `Embedder` requires `embed_with_cancel`, and `Reranker` requires
`rerank_with_cancel`. Implementations must poll the optional cancellation token during
bounded work. Asymmetric embedders override `embed_queries_with_cancel`.

`MemoryEngine::recall_mmr` selects diverse results from stored candidate vectors in a
cosine region, without re-embedding documents. `AtomHit` exposes optional `relevance`,
`distance`, and `graph_depth` separately from stored `importance` and `confidence`.

Regions persist the embedder's identity, dimension, and metric. Changing the model
requires `reembed_region` or a new region. `reclassify_region` updates provenance only;
use it only when the vector-producing model and pipeline are unchanged.

## Inspection and erasure

`MemoryMaintenance` inventories, reads, verifies, and forgets existing records without
an embedder. It cannot remember or recall.

Cryptographic erasure applies to encrypted regions and the keys controlled by the live
store. It does not revoke exported plaintext or keys retained in pre-erasure backups or
snapshots. Plaintext regions do not provide per-atom cryptographic erasure.

This crate is part of the Citadel workspace.

## License

Apache-2.0
