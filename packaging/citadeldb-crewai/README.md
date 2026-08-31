# citadeldb-crewai

[CrewAI](https://github.com/crewAIInc/crewAI) memory backed by [Citadel](https://citadeldb.dev).
Encrypted at rest, embedded in your process, and deletes that destroy the record's key, not
just its row.

```
pip install citadeldb-crewai
```

Requires `crewai>=1.14.7,<2`. CrewAI 1.14.7 is the first stable release with
both the storage-backend protocol and the storage-factory hook used by this adapter.

Route your crews' memory through Citadel in one call at startup:

```python
from citadeldb_crewai import use_citadel

embedder = MyEmbedder()  # see the embedder contract below
use_citadel("crew_memory.cdl", key="your-passphrase", embedder=embedder)
```

Crews then work unchanged:

```python
from crewai import Crew

crew = Crew(agents=[...], tasks=[...], memory=True)
```

A crew naming a backend Citadel does not claim keeps it, so this will not displace a
deliberate `storage="qdrant-edge"` or a LanceDB path. `"lancedb"` is CrewAI's default spec
and is claimed, so a crew naming it explicitly still routes here. Crews may also name
Citadel outright once `use_citadel` has run:
`Memory(storage="citadel")`.

To route one crew instead of the whole process, hand the backend over directly and skip the
startup call:

```python
from crewai import Crew
from crewai.memory.unified_memory import Memory
from citadeldb_crewai import CitadelBackend

backend = CitadelBackend("crew_memory.cdl", key="your-passphrase", embedder=embedder)
crew = Crew(agents=[...], tasks=[...], memory=Memory(storage=backend))
```

## Or drive the backend directly

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

query_embedding = [0.0] * 1536  # whatever your crew embedded the query with
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

## Importance is a real ranking signal

`MemoryRecord.importance` maps onto Citadel's native atom importance, so it survives as something
recall ranks by rather than as metadata the store carries and ignores.

## Notes

CrewAI embeds queries itself and hands the backend a vector, so `search` runs vector recall
plus the scope, category, and metadata predicates the protocol defines.

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so this can sit on the same database as another
Citadel adapter; construct them on the same thread.

Your crew's own embeddings are stored as-is, so recall runs in the same vector space the crew
queries with. A record that arrives without a vector is embedded by the required model instead
of receiving a placeholder. A record read back carries no embedding, which is what
`Memory.update()` saves after editing a field, so an update that leaves the content alone keeps
the stored vector rather than replacing it.

The embedder must expose `dim`, `metric`, and `model_id`, plus
`embed_with_cancel(list[str], cancel_token) -> list[list[float]]`; asymmetric models may
also provide `embed_queries_with_cancel`. Poll `cancel_token.check()` between bounded
batches. Pass the same model (or a thin adapter over it) to CrewAI and Citadel so supplied
and generated vectors share one space.
A cosine metric is required because CrewAI's storage contract exposes normalized similarity
scores; L2 and inner-product distances have no equivalent bounded score without inventing a
model-specific calibration.
A region is pinned to that model identity and width, so switching models requires an explicit
re-embed or a new region.

A missing `embedder=` is an error; the adapter never substitutes a mock model.

## License

Apache-2.0
