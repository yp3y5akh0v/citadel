# citadeldb-google-adk

A [Google ADK](https://github.com/google/adk-python) `BaseMemoryService` backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and deletes
that destroy the key, not just the row.

```
pip install citadeldb-google-adk
```

```python
from google.adk.runners import Runner
from citadeldb_google_adk import CitadelMemoryService

memory = CitadelMemoryService("adk_memory.cdl", key="your-passphrase")

runner = Runner(
    app_name="my_app",
    agent=agent,                       # your root agent
    session_service=session_service,   # your session service
    memory_service=memory,
)
```

## Search is ranked recall, not word matching

ADK hands the service a query string, so Citadel embeds it and runs hybrid recall: vector
distance, keyword rank and recency, fused into one score. The reference
`InMemoryMemoryService` returns only turns sharing a word with the query; nothing here is
dropped for lacking one. With the default `MockEmbedder` that ranking is still lexical:

```python
await memory.add_session_to_memory(session)   # a Session your Runner already ran

await memory.search_memory(app_name="my_app", user_id="alice",
                           query="why did the release break?")
# SearchMemoryResponse(memories=[MemoryEntry(...disk was full...)])
```

## Deletes destroy the key

Every event is sealed under its own key. Erasing destroys those keys, so the bytes on disk
stay unreadable. A backup taken before the delete carries its own copy of the wrapped key
and is out of scope.

```python
memory.forget_user("my_app", "alice")               # returns the number erased
memory.forget_session("my_app", "alice", "s-42")
```

ADK's own memory services expose no erasure method.

## Direct writes are supported

`add_memory` writes memories without going through a session. The reference
`InMemoryMemoryService` raises `NotImplementedError` for it.

```python
from google.adk.memory.memory_entry import MemoryEntry
from google.genai import types

entry = MemoryEntry(content=types.Content(parts=[types.Part(text="prefers dark mode")]))
await memory.add_memory(app_name="my_app", user_id="alice", memories=[entry])
```

`add_events_to_memory` likewise persists the events you pass rather than a whole session.

## Notes

`add_session_to_memory` sets the session's events, as `InMemoryMemoryService` does:
re-adding never duplicates rows, and an event dropped from the session is dropped from
memory. `add_events_to_memory` is the additive one, skipping ids already stored.

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so this can sit on the same database as another
Citadel adapter; construct them on the same thread.

`MockEmbedder` is the default and needs no download, which is enough to run an agent and to
test. For semantic recall pass a real embedder. `CandleEmbedder` is not in the default
`citadeldb` wheel and needs a source build (`maturin build --features candle-embed`); any
object exposing `dim`, `metric`, `model_id`, `embed` and `embed_queries` works too:

```python
import citadeldb
memory = CitadelMemoryService(
    "adk_memory.cdl",
    key="your-passphrase",
    embedder=citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large"),
)
```

## License

Apache-2.0
