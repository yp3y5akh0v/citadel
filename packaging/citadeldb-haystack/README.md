# citadeldb-haystack

A [Haystack](https://github.com/deepset-ai/haystack) `DocumentStore` backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and deletes
that destroy the key, not just the row.

```
pip install citadeldb-haystack sentence-transformers
```

Requires `citadeldb>=2.2,<3` and `haystack-ai>=2.9,<4`. Set `CITADEL_KEY` before
running the example. The embedding model downloads on first use and runs locally.

```python
from haystack import Document
from haystack.components.embedders import SentenceTransformersTextEmbedder
from haystack.utils import Secret
from citadeldb_haystack import CitadelDocumentStore

embedder = SentenceTransformersTextEmbedder(model="sentence-transformers/all-mpnet-base-v2")
store = CitadelDocumentStore(
    "corpus.cdl",
    Secret.from_env_var("CITADEL_KEY"),
    embedder=embedder,
    dim=768,
    embedding_similarity_function="cosine",
)
store.write_documents([
    Document(id="d1", content="The deployment failed because the disk was full.",
             meta={"chapter": "intro"}),
])
store.filter_documents({"field": "meta.chapter", "operator": "==", "value": "intro"})
```

`dim` defaults to 768 and must match your embedding model.
`embedding_similarity_function` is `"cosine"` or `"dot_product"` and is persisted
with the store. It defaults to Haystack's `"dot_product"`; choose `"cosine"`
explicitly when that is what produced the supplied vectors.
Embedders exposing `model_id`, `model`, or `model_name` record that identity automatically,
in that order. For a custom component without any of those attributes, pass a stable
`model_id=` explicitly; Citadel refuses to guess from the Python class name.

## Pipeline serialization

Use a Haystack environment-variable `Secret` for pipeline serialization. Literal
passphrases cannot be serialized:

```python
CitadelDocumentStore(
    "literal.cdl", "literal-passphrase", embedder=embedder, dim=768
).to_dict()
# ValueError: Cannot serialize token-based secret.

CitadelDocumentStore(
    "corpus.cdl", Secret.from_env_var("CITADEL_KEY"), embedder=embedder, dim=768,
    embedding_similarity_function="cosine",
).to_dict()
# {... "key": {"type": "env_var", "env_vars": ["CITADEL_KEY"], ...}}
```

Use `Secret.from_env_var` for any store that goes into a saved pipeline.

## Deletes destroy the key

Every document is sealed under its own key. Deleting destroys that key and removes the
row. Pre-erasure backups or snapshots containing keys, and exported plaintext, are outside
that erasure.

```python
store.delete_documents(["d1"])
store.delete_all()  # returns the number erased
```

`DuplicatePolicy.NONE` is treated as `FAIL`: duplicate ids are rejected.

## Retrieval

```python
embedder.warm_up()
query_embedding = embedder.run(text="Why did the release break?")["embedding"]

store.embedding_retrieval(
    query_embedding,
    top_k=5,
    filters={"field": "meta.chapter", "operator": "==", "value": "intro"},
    scale_score=False,
    return_embedding=False,
)
```

Filters use Haystack's evaluator.
Top-level `AND` string equalities, including nested paths such as `meta.person.name`,
are passed to Citadel as payload filters. Other predicates filter ranked candidates; the
search window expands until `top_k` matches survive or the region is exhausted.

## Notes

The store requires a Haystack text embedder. Documents that arrive without a vector are
embedded with it, while vectors already supplied by the pipeline are stored as-is. Pass the
same model to the pipeline and store so both paths remain in one vector space. The store warms
the embedder lazily before its first model call and includes its configuration in pipeline
serialization.

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so this can sit on the same database as another
Citadel adapter; construct them on the same thread.

## License

Apache-2.0
