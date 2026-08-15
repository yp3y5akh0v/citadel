# citadeldb-langgraph

A [LangGraph](https://github.com/langchain-ai/langgraph) `BaseStore` backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and deletes
that destroy the key, not just the row.

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
graph = builder.compile(store=store)   # `builder` is your StateGraph
```

## Search is ranked recall, not a `LIKE`

`search` runs Citadel's hybrid recall: vector distance, keyword rank and recency, fused
into one score.

```python
store.put(("notes",), "n1", {"text": "the deployment failed because the disk was full"})
store.put(("notes",), "n2", {"text": "lunch plans for friday"})

store.search(("notes",), query="why did the release break?", limit=1)
# [Item(namespace=['notes'], key='n1', value={'text': 'the deployment failed ...'}, ...)]
```

The default `MockEmbedder` is a hashed bag-of-words, so out of the box the vector half is
lexical: it ranks on shared wording, not on meaning. Pass a real embedder (see Notes) to
match a question against a differently worded answer.

## Deletes destroy the key

Every value is sealed under its own key. Deleting destroys that key, so the bytes on disk
stay unreadable. A backup taken before the delete carries its own copy of the wrapped key
and is out of scope.

```python
store.delete(("users", "alice"), "profile")
```

`forget_namespace` does the same for a whole subtree:

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

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so this can sit on the same database as another
Citadel adapter; construct them on the same thread.

`MockEmbedder` is the default and needs no download, which is enough to build and test a
graph. For semantic recall pass a real embedder. `CandleEmbedder` is not in the default
`citadeldb` wheel and needs a source build (`maturin build --features candle-embed`); any
object exposing `dim`, `metric`, `model_id`, `embed` and `embed_queries` works too:

A region is pinned to its embedder's width and model id when created, so an existing file
cannot be upgraded in place:

```python
import citadeldb
store = CitadelStore(
    "memory-e5.cdl",
    key="your-passphrase",
    embedder=citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large"),
)
```

## License

Apache-2.0
