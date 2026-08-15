# citadeldb-openai-agents

An [OpenAI Agents SDK](https://github.com/openai/openai-agents-python) `Session` backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and deletes
that destroy the key, not just the row.

```
pip install citadeldb-openai-agents
```

```python
from agents import Agent, Runner
from citadeldb_openai_agents import CitadelSession

session = CitadelSession("user-123", "agent.cdl", key="your-passphrase")

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
from citadeldb_openai_agents import CitadelSessionStore

store = CitadelSessionStore("agent.cdl", key="your-passphrase")
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

```python
await session.add_items([
    {"role": "user", "content": "the deployment failed because the disk was full"},
    {"role": "user", "content": "lunch plans for friday"},
])

await session.search("why did the release break?", limit=1)
# [{'content': 'the deployment failed because the disk was full', 'role': 'user'}]
```

Nothing in the SDK calls this. `Runner` only ever uses the four protocol methods.

## TTL

```python
store = CitadelSessionStore("agent.cdl", key="your-passphrase", ttl=86400)  # seconds
```

Expired items stop being returned and are skipped by the storage engine itself, so a
retention window needs no sweeper.

## Notes

Items are stored verbatim as opaque JSON. The SDK's item type is a large union owned by
the `openai` package, so the stored payload is never normalised: function calls, reasoning
items and multi-part content all round-trip unchanged. Only a plain-text projection of
`content` is derived, for search ranking.

`MockEmbedder` is the default and needs no download, which is enough to run an agent and
to test. `search` only becomes semantically useful with a real embedder. `CandleEmbedder`
is not in the default `citadeldb` wheel and needs a source build (`maturin build --features
candle-embed`); any object exposing `dim`, `metric`, `model_id`, `embed` and `embed_queries`
works too:

```python
import citadeldb
store = CitadelSessionStore(
    "agent.cdl",
    key="your-passphrase",
    embedder=citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large"),
)
```

## License

Apache-2.0
