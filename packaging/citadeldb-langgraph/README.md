# citadeldb-langgraph

A [LangGraph](https://github.com/langchain-ai/langgraph) `BaseStore` backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and deletes
that destroy the key rather than the row.

```
pip install citadeldb-langgraph
```

```python
from citadeldb_langgraph import CitadelStore

store = CitadelStore("memory.cdl", key="your-passphrase")

store.put(("users", "alice"), "profile", {"city": "Berlin", "pet": "Mochi"})
print(store.get(("users", "alice"), "profile").value)
# {'city': 'Berlin', 'pet': 'Mochi'}
```

Pass it to a graph the same way as any other store:

```python
graph = builder.compile(store=store)
```

## Search is semantic

`search` runs Citadel's hybrid recall (vector + keyword + recency), not a `LIKE`. The query
below shares no words with the document it finds:

```python
store.put(("notes",), "n1", {"text": "the deployment failed because the disk was full"})
store.put(("notes",), "n2", {"text": "lunch plans for friday"})

store.search(("notes",), query="why did the release break?", limit=1)
# [SearchItem(value={'text': 'the deployment failed because the disk was full'}, ...)]
```

## Deletes destroy the key

Every value is sealed under its own key. Deleting destroys that key, so the bytes on disk
stay unreadable instead of being marked deleted and living on in backups.

```python
store.delete(("users", "alice"), "profile")
```

`forget_namespace` does the same for a whole subtree, which is what a data-deletion request
usually needs:

```python
store.forget_namespace(("users", "alice"))   # returns the number of values erased
```

## TTL

```python
store.put(("session",), "token", {"v": 1}, ttl=60.0)     # minutes
store.get(("session",), "token", refresh_ttl=True)       # extends the lifetime
```

A refresh preserves both `created_at` and `updated_at`, so reading never looks like a write.

## Notes

Citadel is embedded and takes an exclusive lock on the file, so build **one** store per
database and share it. Separate concerns with namespaces rather than with a second store.

`MockEmbedder` is the default and needs no download, which is enough to build and test a
graph. For production recall quality pass a real embedder:

```python
import citadeldb
store = CitadelStore(
    "memory.cdl",
    key="your-passphrase",
    embedder=citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5_large"),
)
```

## License

Apache-2.0
