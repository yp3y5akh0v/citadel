# citadeldb-ms-agent-framework

[Microsoft Agent Framework](https://github.com/microsoft/agent-framework) storage backed
by [Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and
deletes that destroy the key, not just the row.

```
pip install citadeldb-ms-agent-framework
```

Requires `citadeldb>=2.2,<3` and `agent-framework-core>=1.13,<2`.

The package provides two storage interfaces:

| Class | Implements | Use when |
|---|---|---|
| `CitadelHistoryProvider` | `HistoryProvider` | a session must recover its complete transcript |
| `CitadelContextProvider` | `ContextProvider` | an agent should recall relevant facts across sessions |

The context provider performs semantic recall. This example uses local e5-large;
`CandleEmbedder` requires the [Candle source build and model setup](https://github.com/yp3y5akh0v/citadel/blob/HEAD/python/README.md#local-candle-models),
while the default wheel accepts a [bring-your-own semantic embedder](https://github.com/yp3y5akh0v/citadel/blob/HEAD/python/README.md#semantic-embeddings).

```python
from agent_framework import Agent
import citadeldb
from citadeldb_ms_agent_framework import CitadelContextProvider, CitadelHistoryProvider

embedder = citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large")

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

## Deletes destroy the key

```python
history = CitadelHistoryProvider("agent.cdl", key="your-passphrase", embedder=embedder)
memory = CitadelContextProvider(
    "agent.cdl", key="your-passphrase", scope="user-123", embedder=embedder
)

await history.forget("session-42")  # returns the number erased
await memory.forget()  # this provider's whole scope
```

Clearing a conversation destroys each message's own key and deletes its row.
Pre-erasure backups or snapshots containing keys, and exported plaintext, are outside
that erasure.

## History provider

Implements `get_messages` and `save_messages`. Set `load_messages=False` to store
messages without loading history before a run:

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

Uses Citadel's hybrid recall. `embedder=` is required.

```python
memory = CitadelContextProvider(
    "agent.cdl", key="your-passphrase", scope="user-123", limit=5, embedder=embedder
)
```

Memories are scoped rather than session-bound, so a later conversation can recall an
earlier one. `scope` is the boundary an erasure request applies to.

Custom embedders expose `dim`, `metric`, `model_id`, and
`embed_with_cancel(texts, cancel_token)`. Accept `None` as the token; otherwise poll
`cancel_token.check()` between bounded batches. Asymmetric models may also provide
`embed_queries_with_cancel`.

## License

Apache-2.0
