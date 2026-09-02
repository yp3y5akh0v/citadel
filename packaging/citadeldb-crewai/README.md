# citadeldb-crewai

[CrewAI](https://github.com/crewAIInc/crewAI) memory backed by [Citadel](https://citadeldb.dev).
Encrypted at rest, embedded in your process, and deletes that destroy the record's key, not
just its row.

```
pip install citadeldb-crewai
```

Requires `citadeldb>=2.2,<3` and `crewai>=1.14.7,<2`.

Provide a cancellation-aware embedder for the same model CrewAI uses. The
[Python semantic-embedder example](https://github.com/yp3y5akh0v/citadel/blob/HEAD/python/README.md#semantic-embeddings) shows
the required protocol. In the examples below, `my_embeddings` is your application
module exporting that configured `embedder`; it is not part of this package.

Route CrewAI's default memory backend through Citadel at startup:

```python
from citadeldb_crewai import use_citadel
from my_embeddings import embedder

use_citadel("crew_memory.cdl", key="your-passphrase", embedder=embedder)
```

Enable memory on the crew:

```python
from crewai import Crew

crew = Crew(agents=[...], tasks=[...], memory=True)
```

`use_citadel` handles CrewAI's default storage, `storage="lancedb"`, and
`storage="citadel"`. Other backend names and LanceDB paths are left unchanged.

To route one crew instead of the whole process, hand the backend over directly and skip the
startup call:

```python
from crewai import Crew
from crewai.memory.unified_memory import Memory
from citadeldb_crewai import CitadelBackend

backend = CitadelBackend("crew_memory.cdl", key="your-passphrase", embedder=embedder)
crew = Crew(agents=[...], tasks=[...], memory=Memory(storage=backend))
```

## Direct use

```python
from citadeldb_crewai import CitadelBackend
from crewai.memory.storage.backend import MemoryRecord

backend = CitadelBackend("crew_memory.cdl", key="your-passphrase", embedder=embedder)

backend.save(
    [
        MemoryRecord(
            content="the deploy failed because the disk was full",
            scope="/team/ops",
            categories=["incident"],
            metadata={"env": "prod"},
            importance=0.9,
        )
    ]
)

embed_query = getattr(embedder, "embed_queries_with_cancel", embedder.embed_with_cancel)
query_embedding = embed_query(["Why did the release break?"], None)[0]
hits = backend.search(query_embedding, scope_prefix="/team", limit=5)
for record, score in hits:
    print(f"{score:.3f}  {record.content}")
```

## Deletes destroy the key

Every record is sealed under its own key. Deleting destroys that key, so the bytes on disk
stay unreadable. A backup taken before the delete carries its own copy of the wrapped key
and is out of scope.

```python
from datetime import datetime, timedelta, timezone

cutoff = datetime.now(timezone.utc) - timedelta(days=30)

backend.delete(record_ids=["abc123"])  # one record
backend.delete(scope_prefix="/team/ops", categories=["incident"])
backend.delete(scope_prefix="/team", older_than=cutoff)
backend.reset("/users/alice")  # a whole subtree
```

`reset` on a per-user scope destroys the key of every record in that subtree.

## Importance

`MemoryRecord.importance` is retained in the record. Backend `search` returns
vector similarity; CrewAI applies its composite
scoring, including importance, after the storage call.

## Notes

CrewAI embeds queries itself and hands the backend a vector, so `search` runs vector recall
plus the scope, category, and metadata predicates the protocol defines.

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so this can sit on the same database as another
Citadel adapter; construct them on the same thread.

Supplied vectors are stored as-is. Records without a vector are embedded by the
configured model. An update without a supplied vector preserves the stored vector
when the text is unchanged.

The embedder must expose `dim`, `metric`, and `model_id`, plus
`embed_with_cancel(list[str], cancel_token) -> list[list[float]]`; asymmetric models may
also provide `embed_queries_with_cancel`. Accept `None` as the token; otherwise poll
`cancel_token.check()` between bounded batches. Pass the same model to CrewAI and Citadel so supplied
and generated vectors share one space.
A cosine metric is required. Regions persist the model identity and dimension;
switching models requires re-embedding or a new region.

## License

Apache-2.0
