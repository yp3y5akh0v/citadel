# citadeldb-crewai

[CrewAI](https://github.com/crewAIInc/crewAI) memory backed by [Citadel](https://citadeldb.dev).
Encrypted at rest, embedded in your process, and deletes that destroy the record's key rather
than its row.

```
pip install citadeldb-crewai
```

Route your crews' memory through Citadel in one call at startup:

```python
from citadeldb_crewai import use_citadel

use_citadel("crew_memory.cdl", key="your-passphrase")
```

Crews then work unchanged:

```python
crew = Crew(agents=[...], tasks=[...], memory=True)
```

A crew that names its own backend keeps it, so this will not displace a deliberate
`storage="qdrant-edge"`. Crews may also name Citadel outright once `use_citadel` has run:
`Memory(storage="citadel")`.

To route one crew instead of the whole process, hand the backend over directly and skip the
startup call:

```python
from crewai.memory.unified_memory import Memory
from citadeldb_crewai import CitadelBackend

backend = CitadelBackend("crew_memory.cdl", key="your-passphrase")
crew = Crew(agents=[...], tasks=[...], memory=Memory(storage=backend))
```

## Or drive the backend directly

```python
from citadeldb_crewai import CitadelBackend
from crewai.memory.storage.backend import MemoryRecord

backend = CitadelBackend("crew_memory.cdl", key="your-passphrase")

backend.save([
    MemoryRecord(
        content="the deploy failed because the disk was full",
        scope="/team/ops",
        categories=["incident"],
        metadata={"env": "prod"},
        importance=0.9,
    )
])

hits = backend.search(query_embedding, scope_prefix="/team", limit=5)
for record, score in hits:
    print(f"{score:.3f}  {record.content}")
```

## Deletes destroy the key

Every record is sealed under its own key. Deleting destroys that key, so the bytes on disk
stay unreadable instead of being marked deleted and living on in backups.

```python
backend.delete(record_ids=["abc123"])                      # one record
backend.delete(scope_prefix="/team/ops", categories=["incident"])
backend.delete(scope_prefix="/team", older_than=cutoff)    # retention sweep
backend.reset("/users/alice")                              # a whole subtree
```

`reset` on a per-user scope is what a data-deletion request usually needs: the subtree becomes
unreadable rather than merely unlisted.

## Importance is a real ranking signal

`MemoryRecord.importance` maps onto Citadel's native atom score, so it survives as something
recall ranks by rather than as metadata the store carries and ignores.

## Notes

CrewAI embeds queries itself and hands the backend a vector, so `search` runs vector recall
plus the scope, category, and metadata predicates the protocol defines.

Citadel is embedded and takes an exclusive lock on the file, so build **one** backend per
database and share it. `use_citadel` does that for you.

Your crew's own embeddings are stored as-is, so recall runs in the same vector space the crew
queries with, and no text is re-embedded. Set `dim` to your embedding model's width if it is
not OpenAI's 1536-wide `text-embedding-3-small`:

```python
use_citadel("crew_memory.cdl", key="pw", dim=768)
```

A record saved without an embedding still stores and still comes back through `get_record`,
`list_records`, and every delete filter; it just does not rank in vector search.

## License

Apache-2.0
