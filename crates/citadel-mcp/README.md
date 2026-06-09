# citadeldb-mcp

Model Context Protocol (MCP) server for the
[Citadel](https://github.com/yp3y5akh0v/citadel) encrypted memory engine. Gives any MCP client
(Claude Desktop, an IDE, an agent) persistent, encrypted memory.

Memory lives in a local [`citadeldb-mem`](https://crates.io/crates/citadeldb-mem) region:
AES-256 encrypted at rest, per-atom sealed and HMAC-authenticated, recalled through a hybrid
vector + keyword + recency + importance fusion over a [PRISM](https://github.com/yp3y5akh0v/prism) approximate nearest-neighbor index,
connected by a typed edge graph, and forgotten by **destroying keys** (cryptographic erasure).

Tools (over a synchronous, hand-rolled JSON-RPC 2.0 stdio transport):

- `mem_recall` - hybrid retrieval (vector + keyword + recency + importance); filter by kind/payload, expand along the memory graph, override fusion weights, and optionally attach provenance (`derived_from`), per-hit integrity verdicts (`attest`), and `resource_link`s to each hit
- `mem_fetch` - deterministic listing of a kind (no embedding)
- `mem_edges` - typed graph introspection
- `mem_profile` - what the memory knows about a query: recall plus its graph neighborhood
- `mem_summarize` - per-kind digest of a region
- `mem_verify` - re-authenticate atoms off disk: per-atom integrity verdict (authentic / tampered / key_erased / missing / plaintext_unattested)
- `mem_remember` / `mem_remember_batch` - store atoms with payload, importance, TTL, immutability
- `mem_update` - replace a stored atom's payload in place (preserves id, edges, and embedding)
- `mem_link` - connect atoms with a typed edge
- `mem_evolve` - recompute an atom's neighbor links and score
- `mem_evict` - selective forgetting by policy (cryptographic erasure on encrypted regions)
- `mem_forget` - forget atoms by id and return a verifiable **erasure receipt** (cryptographic erasure on encrypted regions; skips immutable atoms unless forced)

The `citadel-mcp` binary reads the passphrase from `CITADEL_KEY` and serves one region
(encrypted by default); only protocol messages go to stdout, diagnostics to stderr.

Recall is keyword-only (a mock embedder) until you opt into a real semantic model. The
(CPU) Candle embedder is compiled into the default build, but models are fetched only on
explicit request, never automatically:

```sh
citadel-mcp pull bge-small                       # one-time download to ~/.citadel/models
citadel-mcp --db memory.cdl --embedder bge-small
```

Pull names: `bge-small`, `bge-base`, `bge-large`, `minilm`, `e5-large`. Or point `--model-dir`
at a local model directory for a fully offline setup, and build with `--features cuda-embed` to
run the model on an NVIDIA GPU.

For better recall ordering, add a cross-encoder reranker (off by default, also explicit):

```sh
citadel-mcp pull ms-marco-minilm
citadel-mcp --db memory.cdl --embedder bge-small --reranker ms-marco-minilm
```

This crate is part of the Citadel workspace.

## License

MIT OR Apache-2.0
