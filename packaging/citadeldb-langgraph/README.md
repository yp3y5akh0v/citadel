# citadeldb-langgraph

A [LangGraph](https://github.com/langchain-ai/langgraph) `BaseStore` backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and deletes
that destroy the key, not just the row.

```
pip install citadeldb-langgraph
```

Requires `langgraph>=0.2.32,<2` and `langgraph-checkpoint>=2.0.19,<5`.
The checkpoint package owns the `TTLConfig` surface used by this adapter.

This first example performs key-value reads only. Its explicit mock avoids model work and
must not be reused for semantic `search`; the e5-large setup follows below.

```python
import citadeldb
from citadeldb_langgraph import CitadelStore

store = CitadelStore(
    "memory.cdl",
    key="your-passphrase",
    embedder=citadeldb.MockEmbedder(dim=64),  # intentional non-semantic store
)

store.put(("users", "alice"), "profile", {"city": "Berlin", "pet": "Mochi"})
print(store.get(("users", "alice"), "profile").value)
# {'city': 'Berlin', 'pet': 'Mochi'}
```

Pass it to a graph the same way as any other store:

```python
graph = builder.compile(store=store)  # `builder` is your StateGraph
```

## Search is ranked recall, not a `LIKE`

`search` runs Citadel's hybrid recall: vector distance, keyword rank and recency, fused
into one score.

The semantic example uses a local e5-large model. `CandleEmbedder` requires a
`citadeldb` source wheel built with `--features candle-embed`; the default wheel accepts
an equivalent real bring-your-own embedder.

```python
semantic_store = CitadelStore(
    "semantic-memory.cdl",
    key="your-passphrase",
    embedder=citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large"),
)
semantic_store.put(
    ("notes",), "n1", {"text": "the deployment failed because the disk was full"}
)
semantic_store.put(("notes",), "n2", {"text": "lunch plans for friday"})

semantic_store.search(("notes",), query="why did the release break?", limit=1)
# [Item(namespace=['notes'], key='n1', value={'text': 'the deployment failed ...'}, ...)]
```

`MockEmbedder` is a hashed bag-of-words: it ranks on shared wording, not meaning, and is
only appropriate for intentional lexical tests.

`index=False` omits a value from ranked semantic recall, though it can still appear without a
score when filling the requested window. `index=[...]` restricts searchable text to those JSON
paths. Citadel concatenates the selected strings into one vector per value; unlike LangGraph's
reference store, it does not embed each selected string separately and max-pool their scores.

## Deletes destroy the key

Every value is sealed under its own key. Deleting destroys that key, so the bytes on disk
stay unreadable. A backup taken before the delete carries its own copy of the wrapped key
and is out of scope.

```python
store.delete(("users", "alice"), "profile")
```

`forget_namespace` does the same for a whole subtree:

```python
store.forget_namespace(("users", "alice"))  # returns the number of values erased
```

## TTL

```python
store.put(("session",), "token", {"v": 1}, ttl=60.0)  # minutes
store.get(("session",), "token", refresh_ttl=True)  # extends the lifetime
```

A refresh preserves both `created_at` and `updated_at`, so reading never looks like a write.

## Notes

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so this can sit on the same database as another
Citadel adapter; construct them on the same thread.

`embedder=` is required. There is no default: changing the model changes ranking semantics
and persisted provenance. A bring-your-own object exposes `dim`, `metric`, `model_id`, and
`embed_with_cancel(texts, cancel_token)`; `embed_queries_with_cancel` is optional.

A region is pinned to its embedder's width and model id when created. Changing model means
re-embedding from the stored text. CitadelDB 2.1's `Memory.reembed_region` does that in place,
keeping every atom id and therefore every edge.

## License

Apache-2.0
