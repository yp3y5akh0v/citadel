# citadeldb-haystack

A [Haystack](https://github.com/deepset-ai/haystack) `DocumentStore` backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and deletes
that destroy the key, not just the row.

Passes deepset's own `DocumentStoreBaseTests` conformance suite.

```
pip install citadeldb-haystack
```

```python
from haystack import Document
from haystack.components.embedders import SentenceTransformersTextEmbedder
from haystack.utils import Secret
from citadeldb_haystack import CitadelDocumentStore

embedder = SentenceTransformersTextEmbedder()
store = CitadelDocumentStore(
    "corpus.cdl",
    Secret.from_env_var("CITADEL_KEY"),
    embedder=embedder,
    dim=768,
    embedding_similarity_function="cosine",
)
# CITADEL_KEY must be set: an env-var secret is what lets a pipeline serialize.

store.write_documents([Document(id="d1", content="...", meta={"chapter": "intro"})])
store.filter_documents({"field": "meta.chapter", "operator": "==", "value": "intro"})
```

`dim` defaults to 768 and must match your embedding model.
`embedding_similarity_function` is `"cosine"` or `"dot_product"` and is persisted
with the store. It defaults to Haystack's `"dot_product"`; choose `"cosine"`
explicitly when that is what produced the supplied vectors.
Embedders exposing `model_id`, `model`, or `model_name` record that identity automatically,
in that order. For a custom component without any of those attributes, pass a stable
`model_id=` explicitly; Citadel refuses to guess from the Python class name.

## The passphrase never lands in a pipeline file

The passphrase is a Haystack `Secret`. Pipelines are serialized to disk, and a literal
token refuses to serialize, so a passphrase cannot be written into a pipeline by accident:

```python
CitadelDocumentStore(
    "literal.cdl", "literal-passphrase", embedder=embedder, dim=768
).to_dict()
# ValueError: Cannot serialize token-based secret.

CitadelDocumentStore(
    "corpus.cdl", Secret.from_env_var("CITADEL_KEY"), embedder=embedder, dim=768
).to_dict()
# {... "key": {"type": "env_var", "env_vars": ["CITADEL_KEY"], ...}}
```

Use `Secret.from_env_var` for any store that goes into a saved pipeline.

## Deletes destroy the key

Every document is sealed under its own key. Deleting destroys that key and then removes the
row, so any ciphertext surviving elsewhere stays unreadable.

```python
store.delete_documents(["d1"])
store.delete_all()  # returns the number erased
```

`DuplicatePolicy.NONE` falls back to `FAIL`, as `InMemoryDocumentStore` does, so an
accidental re-write is reported rather than silently replacing a document whose key would
then be destroyed.

## Retrieval

```python
query_embedding = [0.0] * 768  # from your Haystack text embedder, `dim` wide

store.embedding_retrieval(
    query_embedding,
    top_k=5,
    filters={"field": "meta.chapter", "operator": "==", "value": "intro"},
    scale_score=False,
    return_embedding=False,
)
```

Filtering uses Haystack's own evaluator, so the whole filter language, date comparisons
included, matches `InMemoryDocumentStore` operator for operator. `top_k` is `top_k`: a
filter matching only distant documents still returns them, however many others outrank
them.

A top-level `AND` of string equality conditions is pushed into the scan, including nested
paths like `meta.person.name`. Everything else is evaluated afterwards, so the two agree:
nothing is pushed under `OR` or `NOT`, and numbers are not pushed either, because `==` here
is Python's (`1 == 1.0`) where the stored comparison is JSON-type exact.

## Notes

The store requires a Haystack text embedder. Documents that arrive without a vector are
embedded with it, while vectors already supplied by the pipeline are stored as-is. Pass the
same model to the pipeline and store so both paths remain in one vector space. The store warms
the embedder lazily before its first model call and serializes it, so pipeline round-trips retain
the model instead of reopening with a hidden fallback.

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so this can sit on the same database as another
Citadel adapter; construct them on the same thread.

## License

Apache-2.0
