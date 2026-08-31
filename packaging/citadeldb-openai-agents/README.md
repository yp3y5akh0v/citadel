# citadeldb-openai-agents

An [OpenAI Agents SDK](https://github.com/openai/openai-agents-python) `Session` backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and deletes
that destroy the key, not just the row.

```
pip install citadeldb-openai-agents
```

The session protocol below reads complete transcripts by id and never performs semantic
search, so its explicit mock avoids unused model work. The e5-large search setup is shown
separately.

```python
import citadeldb
from agents import Agent, Runner
from citadeldb_openai_agents import CitadelSession

session = CitadelSession(
    "user-123",
    "agent.cdl",
    key="your-passphrase",
    embedder=citadeldb.MockEmbedder(dim=64),  # intentional transcript-only store
)

agent = Agent(name="assistant", instructions="Be brief.")
result = await Runner.run(agent, "remember my dog is called Mochi", session=session)
result = await Runner.run(agent, "what is my dog called?", session=session)
```

The conversation persists across processes, so the second run answers from the
transcript rather than from the prompt.

## Many sessions, one file

Citadel is embedded and one connection owns the file, so sessions are minted from a store
rather than each opening the database:

```python
import citadeldb
from citadeldb_openai_agents import CitadelSessionStore

store = CitadelSessionStore(
    "agent.cdl",
    key="your-passphrase",
    embedder=citadeldb.MockEmbedder(dim=64),  # sessions are read by id
)
alice = store.session("user-alice")
bob = store.session("user-bob")
```

The convenience constructor does this for you: two `CitadelSession` objects on the same
path build their own store but reach one open database. Asking for the same path with a
different passphrase raises rather than quietly serving the first one's settings.

## Deletes destroy the key

Every item is sealed under its own key. `clear_session` destroys those keys, so the bytes
on disk stay unreadable. A backup taken before the delete carries its own copy of the
wrapped key and is out of scope.

```python
await session.clear_session()
```

`pop_item` does the same for a single rolled-back turn.

The SDK's own `EncryptedSession` wrapper encrypts items and skips expired ones on read,
but the ciphertext and its key both remain.

## Search the transcript

Beyond the protocol, a session can be searched with Citadel's hybrid recall, which ranks
on vector distance, keyword rank and recency rather than on an exact match:

This semantic example uses local e5-large. `CandleEmbedder` requires a `citadeldb`
source wheel built with `--features candle-embed`; the default wheel accepts an
equivalent real bring-your-own embedder.

```python
semantic_store = CitadelSessionStore(
    "semantic-sessions.cdl",
    key="your-passphrase",
    embedder=citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large"),
)
semantic_session = semantic_store.session("user-123")
await semantic_session.add_items(
    [
        {"role": "user", "content": "the deployment failed because the disk was full"},
        {"role": "user", "content": "lunch plans for friday"},
    ]
)

await semantic_session.search("why did the release break?", limit=1)
# [{'content': 'the deployment failed because the disk was full', 'role': 'user'}]
```

Nothing in the SDK calls this. `Runner` only ever uses the four protocol methods.

## TTL

```python
import citadeldb

store = CitadelSessionStore(
    "agent.cdl",
    key="your-passphrase",
    embedder=citadeldb.MockEmbedder(dim=64),  # TTL reads are session-id based
    ttl=86400,  # seconds
)
```

Expired items stop being returned and are skipped by the storage engine itself, so a
retention window needs no sweeper.

## Notes

Items are stored verbatim as opaque JSON. The SDK's item type is a large union owned by
the `openai` package, so the stored payload is never normalised: function calls, reasoning
items and multi-part content all round-trip unchanged. Only a plain-text projection of
`content` is derived, for search ranking.

`embedder=` is required. There is no default: changing the model changes ranking semantics
and persisted provenance. A bring-your-own object exposes `dim`, `metric`, `model_id`, and
`embed_with_cancel(texts, cancel_token)`; asymmetric models may also provide
`embed_queries_with_cancel`. Use `MockEmbedder` only for transcript-by-id flows or
deliberate lexical-only tests.

A session created with `store=` inherits that store's database, region, embedder, and TTL;
passing any of those options alongside `store=` is rejected instead of silently ignoring it.

## License

Apache-2.0
