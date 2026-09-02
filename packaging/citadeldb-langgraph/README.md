# citadeldb-langgraph

A [LangGraph](https://github.com/langchain-ai/langgraph) `BaseStore` backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and deletes
that destroy the key, not just the row.

```
pip install citadeldb-langgraph
```

Requires `citadeldb>=2.2,<3`, `langgraph>=0.2.32,<2`, and
`langgraph-checkpoint>=2.0.19,<5`.

This example uses local e5-large and requires the [Candle source build and model setup](https://github.com/yp3y5akh0v/citadel/blob/HEAD/python/README.md#local-candle-models).
The default wheel accepts a [bring-your-own semantic embedder](https://github.com/yp3y5akh0v/citadel/blob/HEAD/python/README.md#semantic-embeddings)
instead; it does not include `CandleEmbedder`.

```python
import citadeldb
from citadeldb_langgraph import CitadelStore

store = CitadelStore(
    "memory.cdl",
    key="your-passphrase",
    embedder=citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large"),
)

store.put(("users", "alice"), "profile", {"city": "Berlin", "pet": "Mochi"})
print(store.get(("users", "alice"), "profile").value)
# {'city': 'Berlin', 'pet': 'Mochi'}
```

Pass it to a graph the same way as any other store:

```python
graph = builder.compile(store=store)  # `builder` is your StateGraph
```

## Search

`search` runs Citadel's hybrid recall: vector distance, keyword rank and recency, fused
into one score.

Results are returned in recall order. Ranked `SearchItem.score` values preserve Citadel's
query-specific relevance. Queryless results and unindexed fallback rows have no score.

```python
store.put(
    ("notes",), "n1", {"text": "the deployment failed because the disk was full"}
)
store.put(("notes",), "n2", {"text": "lunch plans for friday"})

for item in store.search(("notes",), query="why did the release break?", limit=1):
    print(item.key, item.value)
```

`index=False` omits a value from ranked semantic recall, though it can still appear when
filling the requested window. `index=[...]` restricts searchable text to those JSON
paths. Citadel concatenates the selected strings into one vector per value; unlike LangGraph's
reference store, it does not embed each selected string separately and max-pool their scores.

## Deletes destroy the key

Every value is sealed under its own key. Deleting destroys that key, so the bytes on disk
stay unreadable. A backup taken before the delete carries its own copy of the wrapped key
and is out of scope.

```python
store.delete(("users", "alice"), "profile")
```

`forget_namespace` erases non-expired values in a namespace subtree:

```python
store.forget_namespace(("users", "alice"))  # returns the number of values erased
```

## TTL

```python
store.put(("session",), "token", {"v": 1}, ttl=60.0)  # minutes
store.get(("session",), "token", refresh_ttl=True)  # extends the lifetime
```

A refresh preserves both `created_at` and `updated_at`.
Expiration hides a value from reads; it does not erase its key. Expired values are
not included in `forget_namespace`. Core `Memory.evict(region, EvictionPolicy.expired())`
erases expired, non-immutable atoms across the region, not just one namespace.

## Notes

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so this can sit on the same database as another
Citadel adapter; construct them on the same thread.

`embedder=` is required. A bring-your-own object exposes `dim`, `metric`, `model_id`, and
`embed_with_cancel(texts, cancel_token)`; `embed_queries_with_cancel` is optional. Accept
`None` as the token and poll `cancel_token.check()` between bounded batches when present.

A region is pinned to its embedder's width and model id when created. Changing model means
re-embedding from the stored text. `Memory.reembed_region` preserves atom ids and
authored edges, and rebuilds managed similarity edges.

## License

Apache-2.0
