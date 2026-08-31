# citadeldb

Local-first encrypted memory for AI agents. Raw conversation turns are stored as
written, with no summarizer LLM in the ingest path, in a single encrypted file that
lives inside your process.

## Install

```
pip install citadeldb
```

The only runtime dependency is NumPy; embeddings are bring-your-own.

## Repairing memory-model provenance

`CandleEmbedder.model_id` binds the friendly model name to the exact model,
tokenizer, and configuration bytes, the selected preset, and Citadel's embedding
pipeline revision. `MockEmbedder` records a versioned algorithm identity instead of
a generic `mock` label. A legacy region carrying an earlier identity will not attach
to the current embedder automatically.

If—and only if—the embedder is exactly the one that produced the stored vectors,
update the recorded provenance and reattach. For a region created with the old mock
identity, use its original dimension and metric (replace these example values):

```python
embedder = citadeldb.MockEmbedder(dim=64, metric="cosine")
mem.reclassify_region("chat", embedder.model_id)
mem.attach_existing_region("chat", embedder)
```

For a Candle region, builds with the `candle-embed` feature can perform the same
repair when the model files and preset are unchanged:

```python
embedder = citadeldb.CandleEmbedder("/path/to/model", preset="e5-large")
mem.reclassify_region("chat", embedder.model_id)
mem.attach_existing_region("chat", embedder)
```

`reclassify_region` changes provenance only; it does not verify or recompute vectors.
If the embedder changed, or you cannot prove it is unchanged, recompute the vectors
instead:

```python
report = mem.reembed_region("chat", embedder)
```

Re-embedding refuses a vector-bearing atom whose original text was not stored,
because such a vector cannot be recomputed safely.

## Memory

The default wheel accepts bring-your-own embeddings. The zero-download example below uses
`MockEmbedder` only as a deterministic lexical API demo; it does not provide semantic
recall. For production semantic recall, use the e5-large MCP setup below or a source build
with the `candle-embed` feature.

```python
import citadeldb

db = citadeldb.connect("memory.cdl", key="your-passphrase", region_keys=True)
mem = db.memory()
mem.create_encrypted_region("chat", citadeldb.MockEmbedder(dim=64))

mochi = mem.remember("chat", {"kind": "fact", "text": "Alice's cat is named Mochi"})
berlin = mem.remember("chat", {"kind": "fact", "text": "Alice lives in Berlin"})
mem.link("chat", berlin, mochi, "refines")

for hit in mem.recall("chat", text="Alice lives", k=2):
    assert hit.relevance is not None
    print(f"{hit.relevance:.3f}  {hit.text}")

for edge in mem.fetch_edges("chat", src=berlin, limit=100):
    print(edge["kind"], edge["dst"])
```

Recall fuses vector similarity, keyword match, recency, and importance. Nothing is
summarized or rewritten on the way in, so a date or a number recalls exactly as it
was said.

Edges are region-scoped: both endpoints must be live in the named region. Kind
summaries are bounded pages; pass `next_after_kind` back as `after_kind` until it
is `None`. `mem.profile(...)` returns recalled atoms together with their induced
region-local edges, and `mem.unlink(...)` removes one exact typed edge.

With a `candle-embed` build, replace the region setup above with the real embedder and
attach a local cross-encoder:

```python
embedder = citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large")
mem.create_encrypted_region("chat", embedder)  # replaces the MockEmbedder call above
mem.set_reranker(citadeldb.CrossEncoder("/path/to/ms-marco-minilm"))
```

Operational memory always has an explicit embedder. Inspection and erasure are a
separate capability for tools that reopen an unfamiliar vault and do not have its
model:

A bring-your-own Python embedder exposes `dim`, `metric`, `model_id`, and
`embed_with_cancel(texts, cancel_token)`. Poll `cancel_token.check()` between bounded
batches; an asymmetric model may also provide `embed_queries_with_cancel`.

```python
maintenance = db.memory_maintenance()
for item in maintenance.inventory():
    print(item.region.name, item.region.model_id, item.live_atoms, item.unavailable)
```

`MemoryMaintenance` can inventory, fetch, verify, and forget existing atoms. It
cannot remember or recall, so opening it never invents a replacement embedding model.

## Forgetting

Deleting a memory destroys the key its ciphertext was sealed with, so the bytes on
disk stay unreadable rather than being marked deleted.

```python
receipt = mem.forget("chat", [berlin])
print(receipt.cryptographic_erasure, receipt.algorithm)
# True AES-256-KW(RFC3394)
```

Pass `cascade_dependents=True` to erase the selected atoms together with their
transitive `derived_from` dependents. The default remains targeted deletion.

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

report = db.integrity_check(quiet=True)
for finding in report["errors"]:
    print(finding["kind"], finding["message"], finding["tampered"])
```

## MCP

`pip install citadeldb-mcp` installs the server executable for Claude Desktop,
Cursor, and other Model Context Protocol clients. The recommended setup is:

```console
citadeldb-mcp pull e5-large
citadeldb-mcp pull ms-marco-minilm
citadeldb-mcp --db memory.cdl --embedder e5-large --reranker ms-marco-minilm
```

Set `CITADEL_KEY` before serving an encrypted vault. Use `--embedder mock` only for an
intentional lexical-only smoke test.

The main `citadeldb` wheel also exposes
`citadeldb.mcp.serve("memory.cdl", embedder="mock")` for programmatic keyword-only
serving; set `CITADEL_KEY` before calling it.

Full documentation is at [citadeldb.dev](https://citadeldb.dev); source and the Rust
API are in the [main repository](https://github.com/yp3y5akh0v/citadel).

## License

Apache-2.0
