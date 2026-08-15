# citadeldb-strands-agents

A [Strands Agents](https://strandsagents.com/) session manager backed by
[Citadel](https://citadeldb.dev). Encrypted at rest, embedded in your process, and
redaction that removes the redacted content rather than annotating it.

```
pip install citadeldb-strands-agents
```

```python
from strands import Agent
from citadeldb_strands_agents import CitadelSessionManager

sessions = CitadelSessionManager("user-123", "sessions.cdl", key="your-passphrase")
agent = Agent(session_manager=sessions)
```

Built the way the shipped managers are: it implements `SessionRepository` and inherits
`RepositorySessionManager`, exactly as `FileSessionManager` and `S3SessionManager` do.

## Redaction actually redacts

Strands redacts by setting a `redact_message` field beside the original and updating the
record:

```python
# from strands RepositorySessionManager.redact_latest_message
latest_agent_message.redact_message = redact_message
return self.session_repository.update_message(...)
```

The original `message` stays in the record. Strands reads a redacted message through
`to_message()`, which returns the redaction, so the original is never read again. This
manager therefore does not keep it: the superseded record's key is destroyed and the
replacement carries the redaction in place of the original.

```python
after = sessions.read_message("user-123", agent.agent_id, 0)
after.to_message()      # {'content': [{'text': '[REDACTED]'}], 'role': 'user'}
after.message           # the redaction, not the original
```

An ordinary update is unaffected: only a message carrying a redaction drops its original.

## Erasure

```python
sessions.forget_session("user-123")   # returns the number of records erased
```

Strands has no delete in its repository protocol. This destroys the session, its agents
and every message key.

## Notes

Messages are ordered by their conversation index rather than write time, so editing an
earlier turn does not move it to the end of the history.

Citadel is embedded and one process owns the file. A path already open on this thread,
under the same passphrase, is shared, so many sessions and other Citadel adapters can share
one database; construct them on the same thread.

## License

Apache-2.0
