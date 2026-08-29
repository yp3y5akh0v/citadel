# citadeldb

Local-first encrypted memory for AI agents. Raw conversation turns are stored as
written, with no summarizer LLM in the ingest path, in a single encrypted file that
lives inside your process.

## Install

```
pip install citadeldb
```

The only runtime dependency is NumPy; embeddings are bring-your-own.

## Memory

```python
import citadeldb

db = citadeldb.connect("memory.cdl", key="your-passphrase", region_keys=True)
mem = db.memory()
mem.create_encrypted_region("chat", citadeldb.MockEmbedder(dim=64))

mem.remember("chat", {"kind": "fact", "text": "Alice's cat is named Mochi"})
berlin = mem.remember("chat", {"kind": "fact", "text": "Alice lives in Berlin"})

for hit in mem.recall("chat", text="where does Alice live?", k=2):
    print(f"{hit.score:.3f}  {hit.text}")
# 0.850  Alice lives in Berlin
# 0.200  Alice's cat is named Mochi
```

Recall fuses vector similarity, keyword match, recency, and importance. Nothing is
summarized or rewritten on the way in, so a date or a number recalls exactly as it
was said.

`MockEmbedder` needs no download and is enough to try the API. For real recall
quality use `CandleEmbedder` with a local e5-large.

Operational memory always has an explicit embedder. Inspection and erasure are a
separate capability for tools that reopen an unfamiliar vault and do not have its
model:

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

`pip install citadeldb-mcp` exposes the memory engine to Claude Desktop, Cursor, and
any Model Context Protocol client.

Full documentation is at [citadeldb.dev](https://citadeldb.dev); source and the Rust
API are in the [main repository](https://github.com/yp3y5akh0v/citadel).

## License

Apache-2.0
