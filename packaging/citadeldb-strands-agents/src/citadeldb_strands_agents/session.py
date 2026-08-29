"""Strands Agents session store on an encrypted Citadel region."""

from __future__ import annotations

from operator import index
from typing import Any

import citadeldb
from strands.session.repository_session_manager import RepositorySessionManager
from strands.session.session_repository import SessionRepository
from strands.types.exceptions import SessionException
from strands.types.session import (
    Session,
    SessionAgent,
    SessionMessage,
    encode_bytes_values,
)

SESSION_KIND = "session"
AGENT_KIND = "agent"
MESSAGE_KIND = "message"
MULTI_AGENT_KIND = "multi_agent"
DEFAULT_PATH = "strands_sessions.cdl"
DEFAULT_REGION = "strands_sessions"
PAGE = 10_000
_METRICS = {"cosine", "cos", "l2", "euclidean", "ip", "inner", "inner_product", "dot"}


class _NormalizedEmbedder:
    def __init__(self, embedder: Any, model_id: str) -> None:
        self._embedder = embedder
        self.model_id = model_id

    def __getattr__(self, name: str) -> Any:
        return getattr(self._embedder, name)


def _require_embedder(embedder: Any) -> Any:
    raw_dim = getattr(embedder, "dim", None)
    try:
        dim = index(raw_dim)
    except TypeError as error:
        raise TypeError(
            "embedder dim must be a positive integer no greater than 65535"
        ) from error
    if isinstance(raw_dim, bool) or not 1 <= dim <= 65_535:
        raise TypeError("embedder dim must be a positive integer no greater than 65535")
    metric = getattr(embedder, "metric", None)
    if not isinstance(metric, str) or metric.lower() not in _METRICS:
        raise TypeError("embedder metric must be cosine, l2, or inner")
    model_id = getattr(embedder, "model_id", None)
    if (
        not isinstance(model_id, str)
        or not model_id.strip()
        or model_id.strip().lower() in {"unknown", "default"}
    ):
        raise TypeError(
            "embedder model_id must be a nonblank string other than 'unknown' or 'default'"
        )
    if not callable(getattr(embedder, "embed", None)):
        raise TypeError("embedder must provide a callable embed(texts) method")
    missing = object()
    embed_queries = getattr(embedder, "embed_queries", missing)
    if embed_queries is not missing and not callable(embed_queries):
        raise TypeError("embedder embed_queries attribute must be callable")
    normalized = model_id.strip()
    return (
        embedder
        if normalized == model_id
        else _NormalizedEmbedder(embedder, normalized)
    )


def _page(mem: Any, region: str, kind: str, criterion: dict[str, Any]) -> list[Any]:
    """Page to the end: one fetch is bounded, and a partial erase must not look whole."""
    out: list[Any] = []
    after = None
    while True:
        got = mem.fetch(
            region, kind, payload_filter=criterion, limit=PAGE, after_id=after
        )
        out.extend(got)
        if len(got) < PAGE:
            return out
        after = got[-1].id


# Memory holds no database reference, so no handle is pinned to a thread.


def _one(mem: Any, region: str, kind: str, criterion: dict[str, Any]):
    hits = mem.fetch(region, kind, payload_filter=criterion, limit=1)
    return hits[0] if hits else None


def _key(*parts: Any) -> str:
    """One engine key from several names, length-prefixed so that no two name
    tuples can spell the same key."""
    return "".join(f"{len(s)}\x1f{s}" for s in (str(p) for p in parts))


def _put(mem: Any, region: str, atom: dict, key: str) -> None:
    """Store `atom` as the sole occupant of `key`. Every reference repository
    keys its records - a file per message, an object per key - so a second write
    replaces the first rather than standing beside it, and two request handlers
    sharing a store cannot each add a copy."""
    mem.remember_replacing_keyed(region, atom, key)


def _persisted(session_message: SessionMessage) -> dict:
    """The record to store; a redaction replaces the original message."""
    data = session_message.to_dict()
    if session_message.redact_message is not None:
        # to_dict() runs the bytes encoder over the whole record, so the
        # replacement has to go through it too. Assigning it raw sends live
        # Python bytes to the engine whenever a redaction carries an image or
        # a document block.
        data["message"] = encode_bytes_values(session_message.redact_message)
    return data


def _text_of(session_message: SessionMessage) -> str:
    """An atom needs text to embed; the index identifies it otherwise."""
    content = session_message.redact_message or session_message.message
    parts = content.get("content") or [] if isinstance(content, dict) else []
    text = " ".join(
        p["text"]
        for p in parts
        if isinstance(p, dict) and isinstance(p.get("text"), str)
    )
    return text or f"message {session_message.message_id}"


class CitadelSessionManager(RepositorySessionManager, SessionRepository):
    """A Strands SessionRepository backed by one encrypted Citadel region."""

    def __init__(
        self,
        session_id: str,
        path: str = DEFAULT_PATH,
        key: str = "",
        *,
        embedder: Any,
        region: str = DEFAULT_REGION,
        **kwargs: Any,
    ) -> None:
        if not key:
            raise ValueError("a passphrase is required: transcripts are the payload")
        embedder = _require_embedder(embedder)
        try:
            self._db = citadeldb.connect(path, key=key, region_keys=True)
        except citadeldb.OperationalError as e:
            if "locked" not in str(e):
                raise
            raise RuntimeError(
                f"{path} is open in another process. Citadel is embedded, so one "
                f"process owns the file."
            ) from e
        self._mem = self._db.memory()
        self._region = region
        # Idempotent for a region of the same width, so a dim clash raises here.
        self._mem.create_encrypted_region(region, embedder)

        super().__init__(session_id=session_id, session_repository=self, **kwargs)

    def create_session(self, session: Session, **kwargs: Any) -> Session:
        if _one(self._mem, self._region, SESSION_KIND, {"sid": session.session_id}):
            raise SessionException(f"Session {session.session_id} already exists")
        _put(
            self._mem,
            self._region,
            {
                "kind": SESSION_KIND,
                "text": session.session_id,
                "payload": {"sid": session.session_id, "session": session.to_dict()},
            },
            _key(session.session_id),
        )
        return session

    def read_session(self, session_id: str, **kwargs: Any) -> Session | None:
        hit = _one(self._mem, self._region, SESSION_KIND, {"sid": session_id})
        return Session.from_dict(hit.payload["session"]) if hit else None

    def create_agent(
        self, session_id: str, session_agent: SessionAgent, **kwargs: Any
    ) -> None:
        _put(
            self._mem,
            self._region,
            {
                "kind": AGENT_KIND,
                "text": session_agent.agent_id,
                "payload": {
                    "sid": session_id,
                    "aid": session_agent.agent_id,
                    "agent": session_agent.to_dict(),
                },
            },
            _key(session_id, session_agent.agent_id),
        )

    def read_agent(
        self, session_id: str, agent_id: str, **kwargs: Any
    ) -> SessionAgent | None:
        hit = _one(
            self._mem, self._region, AGENT_KIND, {"sid": session_id, "aid": agent_id}
        )
        return SessionAgent.from_dict(hit.payload["agent"]) if hit else None

    def update_agent(
        self, session_id: str, session_agent: SessionAgent, **kwargs: Any
    ) -> None:
        agent_id = session_agent.agent_id
        previous = self.read_agent(session_id, agent_id)
        if previous is None:
            raise SessionException(
                f"Agent {agent_id} in session {session_id} does not exist"
            )
        # The reference carries the original creation time forward on update.
        session_agent.created_at = previous.created_at
        _put(
            self._mem,
            self._region,
            {
                "kind": AGENT_KIND,
                "text": agent_id,
                "payload": {
                    "sid": session_id,
                    "aid": agent_id,
                    "agent": session_agent.to_dict(),
                },
            },
            _key(session_id, agent_id),
        )

    def create_message(
        self,
        session_id: str,
        agent_id: str,
        session_message: SessionMessage,
        **kwargs: Any,
    ) -> None:
        _put(
            self._mem,
            self._region,
            {
                "kind": MESSAGE_KIND,
                "text": _text_of(session_message),
                "payload": {
                    "sid": session_id,
                    "aid": agent_id,
                    "mid": session_message.message_id,
                    "message": _persisted(session_message),
                },
            },
            _key(session_id, agent_id, session_message.message_id),
        )

    def read_message(
        self, session_id: str, agent_id: str, message_id: int, **kwargs: Any
    ) -> SessionMessage | None:
        hit = _one(
            self._mem,
            self._region,
            MESSAGE_KIND,
            {"sid": session_id, "aid": agent_id, "mid": message_id},
        )
        return SessionMessage.from_dict(hit.payload["message"]) if hit else None

    def update_message(
        self,
        session_id: str,
        agent_id: str,
        session_message: SessionMessage,
        **kwargs: Any,
    ) -> None:
        message_id = session_message.message_id
        previous = self.read_message(session_id, agent_id, message_id)
        if previous is None:
            raise SessionException(f"Message {message_id} does not exist")
        session_message.created_at = previous.created_at
        # A redaction lands here, so the superseded record's key is destroyed.
        _put(
            self._mem,
            self._region,
            {
                "kind": MESSAGE_KIND,
                "text": _text_of(session_message),
                "payload": {
                    "sid": session_id,
                    "aid": agent_id,
                    "mid": message_id,
                    "message": _persisted(session_message),
                },
            },
            _key(session_id, agent_id, message_id),
        )

    def list_messages(
        self,
        session_id: str,
        agent_id: str,
        limit: int | None = None,
        offset: int = 0,
        **kwargs: Any,
    ) -> list[SessionMessage]:
        hits = _page(
            self._mem, self._region, MESSAGE_KIND, {"sid": session_id, "aid": agent_id}
        )
        # The reference gates on the message container, which create_message
        # makes: an agent with stored messages lists them whether or not an
        # agent record was written first. Refusing on the record alone would
        # hide writes this repository accepted.
        if not hits and self.read_agent(session_id, agent_id) is None:
            raise SessionException(
                f"Messages missing from agent: {agent_id} in session {session_id}"
            )
        # Atom order follows write time and diverges once a message is updated.
        ordered = sorted(hits, key=lambda h: h.payload["mid"])
        window = (
            ordered[offset : offset + limit] if limit is not None else ordered[offset:]
        )
        return [SessionMessage.from_dict(h.payload["message"]) for h in window]

    # Base class raises NotImplementedError; a Swarm fails without these.

    def create_multi_agent(
        self, session_id: str, multi_agent: Any, **kwargs: Any
    ) -> None:
        _put(
            self._mem,
            self._region,
            {
                "kind": MULTI_AGENT_KIND,
                "text": multi_agent.id,
                "payload": {
                    "sid": session_id,
                    "maid": multi_agent.id,
                    "state": multi_agent.serialize_state(),
                },
            },
            _key(session_id, multi_agent.id),
        )

    def read_multi_agent(
        self, session_id: str, multi_agent_id: str, **kwargs: Any
    ) -> dict[str, Any] | None:
        hit = _one(
            self._mem,
            self._region,
            MULTI_AGENT_KIND,
            {"sid": session_id, "maid": multi_agent_id},
        )
        return hit.payload["state"] if hit else None

    def update_multi_agent(
        self, session_id: str, multi_agent: Any, **kwargs: Any
    ) -> None:
        if self.read_multi_agent(session_id, multi_agent.id) is None:
            raise SessionException(
                f"MultiAgent state {multi_agent.id} in session {session_id} "
                f"does not exist"
            )
        _put(
            self._mem,
            self._region,
            {
                "kind": MULTI_AGENT_KIND,
                "text": multi_agent.id,
                "payload": {
                    "sid": session_id,
                    "maid": multi_agent.id,
                    "state": multi_agent.serialize_state(),
                },
            },
            _key(session_id, multi_agent.id),
        )

    def forget_session(self, session_id: str) -> int:
        """Destroy a session's records, returning the number erased."""
        doomed = [
            h
            for kind in (SESSION_KIND, AGENT_KIND, MESSAGE_KIND, MULTI_AGENT_KIND)
            for h in _page(self._mem, self._region, kind, {"sid": session_id})
        ]
        if not doomed:
            return 0
        return self._mem.forget(self._region, [h.id for h in doomed]).erased_count
