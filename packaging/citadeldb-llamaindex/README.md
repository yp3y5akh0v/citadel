# citadeldb-llamaindex

A [LlamaIndex](https://github.com/run-llama/llama_index) vector store backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and deletes
that destroy the key, not just the row.

```
pip install citadeldb-llamaindex
```

```python
from llama_index.core import Document, StorageContext, VectorStoreIndex
from citadeldb_llamaindex import CitadelVectorStore

store = CitadelVectorStore("corpus.cdl", key="your-passphrase", dim=1536)
documents = [Document(text="the deployment failed because the disk was full")]

index = VectorStoreIndex.from_documents(
    documents,
    storage_context=StorageContext.from_defaults(vector_store=store),
)

index.as_query_engine().query("why did the release break?")
```

`dim` must match your embedding model: 1536 for OpenAI `text-embedding-3-small`, 3072 for
`text-embedding-3-large`, 1024 for `e5-large`.

## Deletes destroy the key

Every node is sealed under its own key. Deleting destroys that key and then removes the
row, so any ciphertext surviving elsewhere stays unreadable.

```python
index.delete_ref_doc("doc-42")          # every node from that document
store.forget_document("doc-42")         # the same, returning a count for the record
store.clear()                           # the whole corpus
```

The node's key is gone, not just its entry in an index.

## Filters

Filtering matches LlamaIndex's own evaluator, and `similarity_top_k` is honoured: a filter
matching only distant nodes still returns them, however many others outrank them. The same
holds for `node_ids` and `doc_ids`, which name nodes exactly.

String equality under a top-level `AND` is pushed into the scan so it narrows candidates
before top-k. Everything else is evaluated afterwards, so the two agree: numbers are not
pushed, because `EQ` here is Python's `==` (`1 == 1.0`) where the stored comparison is
JSON-type exact.

```python
from llama_index.core.vector_stores.types import (
    FilterOperator, MetadataFilter, MetadataFilters,
)

index.as_retriever(
    filters=MetadataFilters(filters=[
        MetadataFilter(key="year", value=2026, operator=FilterOperator.EQ),
    ])
).retrieve("...")
```

Filter semantics come from LlamaIndex's own evaluator, so every operator behaves exactly
as it does with the reference store. Under `OR` or `NOT` nothing is pushed, because a
pushed leaf would drop rows the filter keeps.

## Notes

LlamaIndex embeds before it calls a store, so a node arrives with its vector already
computed and that vector is written straight onto the atom. Nothing here re-embeds, so
writes and queries stay in one vector space.

The node is stored whole, minus its text, which is kept once as the atom's searchable
content and restored on read. Metadata, relationships and node type all round-trip.

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so this can sit on the same database as another
Citadel adapter; construct them on the same thread.

## License

Apache-2.0
