# citadeldb-openai-agents

An [OpenAI Agents SDK](https://github.com/openai/openai-agents-python) `Session` backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and deletes
that destroy the key, not just the row.

```
pip install citadeldb-openai-agents
```

Requires `citadeldb>=2.2,<3` and `openai-agents>=0.20.0,<1`.
Set `OPENAI_API_KEY` for the SDK's default model before running the agent example.

This example uses local e5-large and requires the
[Candle source build and model setup](https://github.com/yp3y5akh0v/citadel/blob/HEAD/python/README.md#local-candle-models).
The default wheel accepts a [bring-your-own semantic embedder](https://github.com/yp3y5akh0v/citadel/blob/HEAD/python/README.md#semantic-embeddings).

```python
import citadeldb
from agents import Agent, Runner
from citadeldb_openai_agents import CitadelSession

embedder = citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large")
session = CitadelSession(
    "user-123",
    "agent.cdl",
    key="your-passphrase",
    embedder=embedder,
)

agent = Agent(name="assistant", instructions="Be brief.")
result = await Runner.run(agent, "remember my dog is called Mochi", session=session)
result = await Runner.run(agent, "what is my dog called?", session=session)
```

Transcripts persist across processes and are read by session id.

## Many sessions, one file

Create multiple sessions through one store:

```python
import citadeldb
from citadeldb_openai_agents import CitadelSessionStore

store = CitadelSessionStore(
    "agent.cdl",
    key="your-passphrase",
    embedder=embedder,
)
alice = store.session("user-alice")
bob = store.session("user-bob")
```

Sessions constructed on the same thread can share a database path and passphrase.

## Deletes destroy the key

Every item is sealed under its own key. `clear_session` erases non-expired items in
the session. Pre-erasure backups, copied keys, and exported plaintext are outside
that erasure.

```python
await session.clear_session()
```

`pop_item` erases and returns the latest non-expired item.

## Search the transcript

`search` uses Citadel's hybrid recall over a session's text. Call it explicitly;
the SDK session protocol loads transcripts by id.

```python
semantic_store = CitadelSessionStore(
    "semantic-sessions.cdl",
    key="your-passphrase",
    embedder=embedder,
)
semantic_session = semantic_store.session("user-123")
await semantic_session.add_items(
    [
        {"role": "user", "content": "the deployment failed because the disk was full"},
        {"role": "user", "content": "lunch plans for friday"},
    ]
)

print(await semantic_session.search("why did the release break?", limit=1))
```

## TTL

```python
import citadeldb

store = CitadelSessionStore(
    "agent.cdl",
    key="your-passphrase",
    embedder=embedder,
    ttl=86400,  # seconds
)
```

Expired items are excluded from reads. Expiration alone does not destroy their keys;
`clear_session` and `pop_item` do not include them. Core
`Memory.evict(region, EvictionPolicy.expired())` erases expired, non-immutable atoms
across the region, not just one session.

## Notes

Items are stored as opaque JSON, including function calls, reasoning items, and
multi-part content. A text projection of `content` is used for search ranking.

`embedder=` is required. A bring-your-own object exposes `dim`, `metric`, `model_id`, and
`embed_with_cancel(texts, cancel_token)`; asymmetric models may also provide
`embed_queries_with_cancel`. Accept `None` as the token; otherwise poll
`cancel_token.check()` between bounded batches.

A session created with `store=` inherits that store's database, region, embedder, and TTL;
passing any of those options alongside `store=` is rejected instead of silently ignoring it.

## License

Apache-2.0
