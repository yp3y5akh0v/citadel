# citadeldb-llamaindex

A [LlamaIndex](https://github.com/run-llama/llama_index) vector store backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and deletes
that destroy the key, not just the row.

```
pip install citadeldb-llamaindex
```

Requires `citadeldb>=2.2,<3` and `llama-index-core>=0.13.1,<0.15`.
The example also requires `pip install llama-index-embeddings-openai` and
`OPENAI_API_KEY`; document and query text are sent to the embedding provider.

```python
from llama_index.core import Document, StorageContext, VectorStoreIndex
from llama_index.embeddings.openai import OpenAIEmbedding
from citadeldb_llamaindex import CitadelVectorStore

embed_model = OpenAIEmbedding(model="text-embedding-3-small")
store = CitadelVectorStore(
    "corpus.cdl",
    key="your-passphrase",
    embed_model=embed_model,
    dim=1536,
)
documents = [Document(
    id_="doc-42",
    text="the deployment failed because the disk was full",
    metadata={"year": 2026},
)]

index = VectorStoreIndex.from_documents(
    documents,
    storage_context=StorageContext.from_defaults(vector_store=store),
    embed_model=embed_model,
)

index.as_retriever(similarity_top_k=1).retrieve("why did the release break?")
```

`dim` must match your embedding model: 1536 for OpenAI `text-embedding-3-small`, 3072 for
`text-embedding-3-large`, 1024 for `e5-large`.
Models exposing `model_id`, `model_name`, or `model` record that identity automatically,
in that order. For a custom model without any of those attributes, pass a stable
`model_id=` explicitly; Citadel refuses to guess from the Python class name.

## Deletes destroy the key

Every node is sealed under its own key. Deleting destroys that key and removes the
row. Pre-erasure backups or snapshots containing keys, and exported plaintext, are outside
that erasure.

```python
index.delete_ref_doc("doc-42")  # every node from that document
store.forget_document("doc-42")  # the same, returning a count for the record
store.clear()  # the whole corpus
```

## Filters

Metadata filters, `node_ids`, and `doc_ids` restrict candidates before final top-k selection.

String equality under a top-level `AND` is passed to Citadel as a payload filter. Other
predicates filter ranked candidates; the search window expands until
`similarity_top_k` matches survive or the region is exhausted.

```python
from llama_index.core.vector_stores.types import (
    FilterOperator,
    MetadataFilter,
    MetadataFilters,
)

index.as_retriever(
    filters=MetadataFilters(
        filters=[
            MetadataFilter(key="year", value=2026, operator=FilterOperator.EQ),
        ]
    )
).retrieve("...")
```

Leaf operators use LlamaIndex's filter evaluator. The adapter handles `AND`, `OR`,
and `NOT`, including `NOT` on versions whose evaluator does not implement it.

## Notes

Supplied vectors are stored as-is; nodes without a vector use the required
`embed_model`. Pass the same model to the index and store.

The node is stored whole, minus its text, which is kept once as the atom's searchable
content and restored on read. Metadata, relationships and node type all round-trip.

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so this can sit on the same database as another
Citadel adapter; construct them on the same thread.

## License

Apache-2.0
