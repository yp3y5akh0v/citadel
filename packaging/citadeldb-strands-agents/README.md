# citadeldb-strands-agents

A [Strands Agents](https://strandsagents.com/) session manager backed by
[Citadel](https://citadeldb.dev). Encrypted at rest and embedded in your process.

```
pip install citadeldb-strands-agents
```

Requires `citadeldb>=2.2,<3` and `strands-agents>=1.15,<2`.

The manager reads sessions by key; it has no semantic-recall method. This example
uses local e5-large and requires the [Candle source build and model setup](https://github.com/yp3y5akh0v/citadel/blob/HEAD/python/README.md#local-candle-models).
The default wheel accepts a [bring-your-own semantic embedder](https://github.com/yp3y5akh0v/citadel/blob/HEAD/python/README.md#semantic-embeddings).

```python
import citadeldb
from strands import Agent
from citadeldb_strands_agents import CitadelSessionManager

sessions = CitadelSessionManager(
    "user-123",
    "sessions.cdl",
    key="your-passphrase",
    embedder=citadeldb.CandleEmbedder("/path/to/e5-large", preset="e5-large"),
)
agent = Agent(session_manager=sessions)
```

Implements `SessionRepository` and inherits `RepositorySessionManager`.

## Redaction

For a valid `redact_message`, the adapter destroys the original record's key
and stores the redaction as the replacement message. Ordinary updates store the
supplied message.

Redactions must include `role` and `content`; `{}` is not a valid message.

## Erasure

```python
sessions.forget_session("user-123")  # returns the number of records erased
```

Strands has no delete in its repository protocol. This destroys the session, its agents
and every message key.
Pre-erasure backups or snapshots containing keys, and exported plaintext, are outside
that erasure.

## Notes

Messages are ordered by their conversation index rather than write time, so editing an
earlier turn does not move it to the end of the history.

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so many sessions and other Citadel adapters can share
one database; construct them on the same thread.

`embedder=` is required. Custom embedders expose `dim`, `metric`, `model_id`, and
`embed_with_cancel(texts, cancel_token)`. Accept `None` as the token; otherwise poll
`cancel_token.check()` between bounded batches.

## License

Apache-2.0
