# citadeldb-ms-agent-framework

[Microsoft Agent Framework](https://github.com/microsoft/agent-framework) storage backed
by [Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and
deletes that destroy the key, not just the row.

```
pip install citadeldb-ms-agent-framework
```

Two providers for two jobs, matching how the framework's own Redis integration is split:

| Class | Implements | Use when |
|---|---|---|
| `CitadelHistoryProvider` | `HistoryProvider` | a session must recover its complete transcript |
| `CitadelContextProvider` | `ContextProvider` | an agent should recall relevant facts across sessions |

```python
from agent_framework import Agent
import citadeldb
from citadeldb_ms_agent_framework import CitadelContextProvider, CitadelHistoryProvider

embedder = citadeldb.MockEmbedder(dim=64)

agent = Agent(
    client=chat_client,  # any agent_framework chat client
    context_providers=[
        CitadelHistoryProvider("agent.cdl", key="your-passphrase", embedder=embedder),
        CitadelContextProvider(
            "agent.cdl", key="your-passphrase", scope="user-123", embedder=embedder
        ),
    ],
)
```

Both can share one encrypted file: a path already open on this thread, under the same
passphrase, is shared. Construct them on the same thread.

These are the framework's own extension points, with the file encrypted and a key per
message. The built-in `FileHistoryProvider` writes plaintext JSONL or MessagePack.

## Deletes destroy the key

```python
history = CitadelHistoryProvider("agent.cdl", key="your-passphrase", embedder=embedder)
memory = CitadelContextProvider(
    "agent.cdl", key="your-passphrase", scope="user-123", embedder=embedder
)

await history.forget("session-42")  # returns the number erased
await memory.forget()  # this provider's whole scope
```

Clearing a conversation destroys each message's own key and then deletes its row, so any
ciphertext surviving elsewhere stays unreadable.

## History provider

Implements `get_messages` and `save_messages`; the base class's `before_run`/`after_run`
handle loading and storing according to its configuration flags, so an audit-only or
evaluation-only provider works as documented:

```python
CitadelHistoryProvider(
    "agent.cdl",
    key="your-passphrase",
    embedder=embedder,
    load_messages=False,
)  # stores, never loads
```

Messages round-trip through the framework's own serialization, so roles, author names,
multi-part contents and `additional_properties` all survive.

## Context provider

Recalls with Citadel's hybrid search: vector distance, keyword rank and recency, fused
into one score. `embedder=` is required and has no default: silent substitution would change
ranking semantics and persist different provenance. `MockEmbedder` is lexical and needs no
download; pass a real model to match across wording.

```python
memory = CitadelContextProvider(
    "agent.cdl", key="your-passphrase", scope="user-123", limit=5, embedder=embedder
)
```

Memories are scoped rather than session-bound, so a later conversation can recall an
earlier one. `scope` is the boundary an erasure request applies to.

## License

Apache-2.0
