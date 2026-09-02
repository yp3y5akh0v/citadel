# citadeldb-mcp

mcp-name: dev.citadeldb/mcp

Model Context Protocol (MCP) server for the
[Citadel](https://github.com/yp3y5akh0v/citadel) encrypted memory engine. Gives any MCP client
(Claude Desktop, an IDE, an agent) persistent, encrypted memory.

Memory lives in a local [`citadeldb-mem`](https://crates.io/crates/citadeldb-mem) region:
AES-256 encrypted at rest, per-atom sealed and HMAC-authenticated, recalled through a hybrid
vector + keyword + recency + importance fusion over a [PRISM](https://github.com/yp3y5akh0v/prism) approximate nearest-neighbor index,
connected by a typed edge graph, and forgotten by **destroying keys** (cryptographic erasure).

## Install

Run it with no install. For semantic recall, pull the recommended embedder and
cross-encoder reranker once, then enable both:

```sh
uvx citadeldb-mcp pull e5-large
uvx citadeldb-mcp pull ms-marco-minilm
```

The pull commands do not open a vault. Before serving, set `CITADEL_KEY` to the vault
passphrase (`export CITADEL_KEY="your-passphrase"` on macOS/Linux or
`$env:CITADEL_KEY = "your-passphrase"` in PowerShell), then run:

```sh
uvx citadeldb-mcp --db memory.cdl --embedder e5-large --reranker ms-marco-minilm
```

`e5-large` + `ms-marco-minilm` is the recommended semantic-recall setup. See the
[memory benchmarks](https://github.com/yp3y5akh0v/citadel/tree/HEAD/crates/citadel-membench)
for measured configurations. Models are never downloaded automatically.

Built-in pulls use release-pinned Hugging Face revisions. Each complete snapshot is verified
against compiled sizes and SHA-256 digests, then stored with a BLAKE3 manifest under
`<models-dir>/<name>/<revision>/`. A flat cache created by an earlier CitadelDB release is not
trusted; run `pull` again to create the pinned snapshot. `--model-dir` and `--reranker-dir`
remain explicit bring-your-own-artifact paths and bypass the managed-cache manifest. Citadel
treats those directories as user-trusted and does not attest their contents.

Or install the command with `pip install citadeldb-mcp` or `cargo install citadeldb-mcp`,
then wire it into Claude Desktop (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "citadel": {
      "command": "citadeldb-mcp",
      "args": ["--db", "/absolute/path/to/memory.cdl", "--embedder", "e5-large", "--reranker", "ms-marco-minilm"],
      "env": { "CITADEL_KEY": "your-passphrase" }
    }
  }
}
```

The server supports stateless MCP `2026-07-28` discovery and per-request metadata, plus the
initialize-based `2025-11-25` and `2025-06-18` revisions for existing clients.

Tools (over a bounded, cancellable JSON-RPC 2.0 stdio transport):

- `mem_recall` - hybrid retrieval (vector + keyword + recency + importance); filter by kind/payload, expand along the memory graph, override fusion weights, and optionally attach provenance (`derived_from`), per-hit integrity verdicts (`attest`), and `resource_link`s to each hit
- `mem_fetch` - deterministic cursor-based listing of a kind (no embedding)
- `mem_get` - bounded exact-id batch retrieval in request order, with missing atoms marked explicitly
- `mem_edges` - cursor-based typed graph introspection
- `mem_profile` - what the memory knows about a query: recall plus its graph neighborhood
- `mem_summarize` - per-kind digest of a region
- `mem_verify` - re-authenticate atoms off disk: per-atom integrity verdict (authentic / tampered / key_erased / missing / plaintext_unattested)
- `mem_remember` - store one atom with optional provenance and retry-safe idempotency
- `mem_remember_batch` - atomically store an ordered batch; every entry carries a distinct idempotency key, and a changed reuse rejects the whole batch
- `mem_update` - replace a stored atom's payload in place (preserves id, edges, and embedding)
- `mem_link` / `mem_unlink` - add or remove one exact typed edge
- `mem_evolve` - recompute an atom's neighbor links and stored importance
- `mem_evict` - selective forgetting by policy (cryptographic erasure on encrypted regions)
- `mem_forget` - forget atoms by id and return a verifiable **erasure receipt**; optionally erase their region-local `derived_from` closure atomically (cryptographic erasure on encrypted regions; immutable atoms require the protected-erasure opt-in)

The `citadeldb-mcp` binary reads the passphrase from `CITADEL_KEY` and serves one region
(encrypted by default); only protocol messages go to stdout, diagnostics to stderr.
`--region-mode plaintext` disables per-atom encryption and cryptographic erasure for
that region; the database remains encrypted at rest. Erasure does not revoke plaintext
exports or keys retained in pre-erasure backups or snapshots.

Every server invocation must select an embedder. The CPU Candle embedder is compiled into the standalone server's
default build, including the `citadeldb-mcp` Python package; it is not included in the
default `citadeldb` library wheel. Models are fetched only on explicit `pull`.

Embedder pull names: `e5-large` (recommended), `e5-large-v2`, `bge-small`, `bge-base`,
`bge-large`, `minilm`. Reranker: `ms-marco-minilm`. Or point `--model-dir` at a
compatible local checkpoint for the selected catalog pipeline, and build with
`--features cuda-embed` to run on an NVIDIA GPU.

This crate is part of the Citadel workspace.

## License

Apache-2.0
