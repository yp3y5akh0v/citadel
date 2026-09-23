# citadeldb

Local-first encrypted memory for AI agents. Raw conversation turns are stored as
written, with no summarizer LLM in the ingest path. Citadel runs inside your process;
the database uses a companion `.citadel-keys` file for its encryption keys.

## Install

```
pip install citadeldb
```

Requires Python 3.10 or later. The only runtime dependency is NumPy; embeddings are
bring-your-own. `CandleEmbedder` and `CrossEncoder` are not included in the default wheel.

To build from source, activate a virtual environment and run these commands from
the repository root with Rust installed:

```console
pip install maturin
maturin develop --release
```

## Semantic embeddings

The default wheel can use a local [Sentence Transformers](https://sbert.net/docs/package_reference/sentence_transformer/model.html)
model through Citadel's cancellation-aware embedder protocol:

```console
pip install sentence-transformers
```

```python
from sentence_transformers import SentenceTransformer


class LocalEmbedder:
    metric = "cosine"

    def __init__(self):
        name = "sentence-transformers/all-MiniLM-L6-v2"
        revision = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"
        self.model = SentenceTransformer(name, revision=revision, device="cpu")
        self.dim = self.model.get_sentence_embedding_dimension()
        self.model_id = f"{name}@{revision}:sentence-transformers:normalized:v1"

    def embed_with_cancel(self, texts, cancel_token):
        vectors = []
        for start in range(0, len(texts), 16):
            if cancel_token is not None:
                cancel_token.check()
            vectors.extend(self.model.encode(
                texts[start:start + 16], normalize_embeddings=True,
                show_progress_bar=False,
            ).tolist())
            if cancel_token is not None:
                cancel_token.check()
        return vectors


embedder = LocalEmbedder()
```

The model is downloaded on first construction and runs locally. Keep the model
revision and encoding settings unchanged when reopening a region. This wrapper checks
cancellation between batches, not during a batch's model inference.

Custom embedders require `dim`, `metric`, `model_id`, and
`embed_with_cancel(texts, cancel_token)`. Return one `dim`-wide vector per input and
accept `None` as the token. Asymmetric models should also implement
`embed_queries_with_cancel` for query-specific encoding. `MockEmbedder` is only a
deterministic lexical test backend, not a semantic model.

## Memory

Use the `embedder` from the example above:

```python
import citadeldb

db = citadeldb.connect("memory.cdl", key="your-passphrase", region_keys=True)
mem = db.memory()
mem.create_encrypted_region("chat", embedder)

mochi = mem.remember("chat", {"kind": "fact", "text": "Alice's cat is named Mochi"})
berlin = mem.remember("chat", {"kind": "fact", "text": "Alice lives in Berlin"})
mem.link("chat", berlin, mochi, "refines")

for hit in mem.recall("chat", text="Where does Alice live?", k=2):
    assert hit.relevance is not None
    print(f"{hit.relevance:.3f}  {hit.text}")

for edge in mem.fetch_edges("chat", src=berlin, limit=100):
    print(edge["kind"], edge["dst"])
```

Recall fuses vector similarity, keyword match, recency, and importance. Stored text
is not summarized or rewritten. `AtomHit.relevance` is a higher-is-better ranking
score; `distance` is the metric distance when available. Either can be `None` on
hits returned by operations that do not calculate that value.

`mem.recall_mmr("chat", text="Alice", k=2, fetch_k=10, lambda_mult=0.5)` selects
diverse results using the stored candidate vectors, without re-embedding documents.
MMR requires a cosine region.

Edges are region-scoped: both endpoints must be live in the named region. Kind
summaries are bounded pages; pass `next_after_kind` back as `after_kind` until it
is `None`. `mem.profile(...)` returns recalled atoms together with their induced
region-local edges, and `mem.unlink(...)` removes one exact typed edge.

## Local Candle models

To use the Rust-native embedder and cross-encoder in Python, activate a virtual
environment and run these commands from the repository root. Building requires Rust:

```console
pip install maturin
maturin develop --release --features candle-embed
pip install citadeldb-mcp
citadeldb-mcp pull e5-large
citadeldb-mcp pull ms-marco-minilm
```

Use the snapshot directories printed by `pull` below. For a new region, replace
`LocalEmbedder()` in the memory example with this `CandleEmbedder`, then set the
reranker on `mem`. An existing region requires re-embedding to change its model.

```python
embedder = citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large")
mem.set_reranker(citadeldb.CrossEncoder("/path/to/ms-marco-minilm"))
```

`CandleEmbedder.model_id` binds the model, tokenizer, configuration, preset, and
embedding pipeline revision. These local directories are user-trusted inputs.

## Inspection and model changes

Inspect stored memory without loading an embedder:

```python
maintenance = db.memory_maintenance()
for item in maintenance.inventory():
    print(item.region.name, item.region.model_id, item.live_atoms, item.unavailable)
```

`MemoryMaintenance` can inventory, fetch, verify, and forget existing atoms. It
cannot remember or recall.

When a model changes, `mem.reembed_region("chat", embedder)` recomputes vectors from
stored text while preserving atom ids and authored edges. Managed similarity edges
are rebuilt. It refuses vector-bearing atoms whose original text was not stored.

For a provenance-only repair, use `mem.reclassify_region("chat", embedder.model_id)`
and then `mem.attach_existing_region("chat", embedder)`. Do this only if the model,
artifacts, and encoding settings are exactly those that produced the stored vectors:
reclassification neither verifies nor recomputes them.

## Forgetting

On encrypted regions, forgetting destroys the key that sealed each erased atom.
It does not erase plaintext already exported or copies of keys held in pre-erasure
backups or snapshots. Plaintext regions do not provide per-atom cryptographic erasure.
Targeted forgetting skips immutable atoms unless `force=True`; their ids appear in
`receipt.immutable_skipped`.

```python
receipt = mem.forget("chat", [berlin])
print(receipt.cryptographic_erasure, receipt.algorithm)
# True AES-256-KW(RFC3394)
```

Pass `cascade_dependents=True` to erase the selected atoms together with their
transitive `derived_from` dependents. Without `force=True`, an immutable atom in
that closure rejects the entire cascade. The default remains targeted deletion.

## SQL and vector search

The same file is a full SQL database with JSON, full-text search, and filtered
vector search.

```python
db.execute("CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT)")
db.execute("INSERT INTO notes VALUES (1, $1)", ["hello"])
print(db.query("SELECT body FROM notes").to_dicts())  # [{'body': 'hello'}]
```

Long SQL, integrity, and memory operations can be stopped cooperatively from
another Python thread. Tokens are one-shot; install a fresh one for each unit
of work and clear it afterwards.

```python
token = citadeldb.CancelToken()
db.set_cancel(token)
# another thread may call token.cancel()

try:
    report = db.integrity_check(quiet=True)
    for finding in report["errors"]:
        print(finding["kind"], finding["message"], finding["tampered"])
finally:
    db.set_cancel(None)
```

## Agents and LLM clients

`LLMClient.complete()` returns a response dictionary whose `usage` can be `None`.
Custom clients should return both `input_tokens` and `output_tokens` as nonnegative
integer counts inside `usage`; omitted or invalid counts mean usage is unavailable.
Explicit zero counts remain zero. `usage["cost_usd"]` is an optional estimate.
Returning only a string leaves usage unavailable. Callback request dictionaries
include `seed`, with `None` when unset; replay hashes include this field.

Agents do not automatically retry LLM requests. A run stops with
`terminated_by == "token_usage_unavailable"` when token accounting is unknown,
or `"cost_usage_unavailable"` when a configured cost cap
cannot be checked. A nonfinite or negative cost cap gives `"invalid_cost_limit"`.

On trace-storage failure, `AgentError.recovery` contains `usage`, `calls`, and
`confirmed_persisted`; each call retains its request, identities, and response or
provider error. `AgentError.storage_error` holds the original storage exception.
Both attributes are `None` for other agent errors. The confirmed prefix counts
acknowledged writes; a failed write may still have persisted. Reconcile the retained
calls with stored traces before retrying persistence or making new calls.

## MCP

`pip install citadeldb-mcp` installs the server executable for Claude Desktop,
Cursor, and other Model Context Protocol clients. The recommended setup is:

```console
citadeldb-mcp pull e5-large
citadeldb-mcp pull ms-marco-minilm
citadeldb-mcp --db memory.cdl --embedder e5-large --reranker ms-marco-minilm
```

Set `CITADEL_KEY` to the vault passphrase before serving.

Full documentation is at [citadeldb.dev](https://citadeldb.dev); source and the Rust
API are in the [main repository](https://github.com/yp3y5akh0v/citadel).

## License

Apache-2.0
