# citadeldb-google-adk

A [Google ADK](https://github.com/google/adk-python) `BaseMemoryService` backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and deletes
that destroy the key, not just the row.

```
pip install citadeldb-google-adk
```

Requires `citadeldb>=2.2,<3` and `google-adk>=2.0,<3`.

The semantic example uses a local e5-large model. `CandleEmbedder` requires a
[Candle source build and model setup](https://github.com/yp3y5akh0v/citadel/blob/HEAD/python/README.md#local-candle-models);
the default wheel accepts a [bring-your-own semantic embedder](https://github.com/yp3y5akh0v/citadel/blob/HEAD/python/README.md#semantic-embeddings)
instead.

```python
import citadeldb
from google.adk.runners import Runner
from citadeldb_google_adk import CitadelMemoryService

memory = CitadelMemoryService(
    "adk_memory.cdl",
    key="your-passphrase",
    embedder=citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large"),
)

runner = Runner(
    app_name="my_app",
    agent=agent,  # your root agent
    session_service=session_service,  # your session service
    memory_service=memory,
)
```

## Search

Search combines vector similarity, keyword rank, recency, and importance:

```python
await memory.add_session_to_memory(session)  # a Session your Runner already ran

result = await memory.search_memory(
    app_name="my_app", user_id="alice", query="why did the release break?"
)
print(result)
```

## Deletes destroy the key

Every event is sealed under its own key. Erasing destroys those keys, so the bytes on disk
stay unreadable. A backup taken before the delete carries its own copy of the wrapped key
and is out of scope.

```python
memory.forget_user("my_app", "alice")  # returns the number erased
memory.forget_session("my_app", "alice", "s-42")
```

## Direct writes

`add_memory` writes memories without a session:

```python
from google.adk.memory.memory_entry import MemoryEntry
from google.genai import types

entry = MemoryEntry(content=types.Content(parts=[types.Part(text="prefers dark mode")]))
await memory.add_memory(app_name="my_app", user_id="alice", memories=[entry])
```

`add_events_to_memory` stores text-bearing events without requiring a whole session.
An event with an existing id replaces that event; anonymous events are always appended.

## Notes

`add_session_to_memory` replaces the session's stored text-bearing events.
`add_events_to_memory` adds new ids and replaces existing ids.

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so this can sit on the same database as another
Citadel adapter; construct them on the same thread.

`embedder=` is required. A bring-your-own object exposes `dim`,
`metric`, `model_id`, and `embed_with_cancel(texts, cancel_token)`; asymmetric models
may also provide `embed_queries_with_cancel`. Accept `None` as the cancellation token; otherwise poll
`cancel_token.check()` between bounded batches.

## License

Apache-2.0
