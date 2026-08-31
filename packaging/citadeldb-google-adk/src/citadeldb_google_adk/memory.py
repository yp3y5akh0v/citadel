"""Google ADK memory service over an encrypted Citadel region."""

from __future__ import annotations

import asyncio
import functools
import threading
from collections.abc import Mapping, Sequence
from datetime import datetime
from operator import index
from typing import TYPE_CHECKING, Any

import citadeldb
from google.adk.memory.base_memory_service import (
    BaseMemoryService,
    SearchMemoryResponse,
)
from google.adk.memory.memory_entry import MemoryEntry
from google.genai import types

if TYPE_CHECKING:
    from google.adk.events.event import Event
    from google.adk.sessions.session import Session

KIND = "event"
DEFAULT_PATH = "adk_memory.cdl"
DEFAULT_REGION = "adk_memory"
# A scope is read whole and filtered in Python; recall over-fetches too.
PAGE = 10_000
_SESSION_LOCKS = tuple(threading.Lock() for _ in range(256))
# `search_memory` takes no limit, so the service owns one; raise it on the constructor.
DEFAULT_SEARCH_LIMIT = 64
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
    if not callable(getattr(embedder, "embed_with_cancel", None)):
        raise TypeError(
            "embedder must provide a callable "
            "embed_with_cancel(texts, cancel_token) method"
        )
    missing = object()
    embed_queries = getattr(embedder, "embed_queries_with_cancel", missing)
    if embed_queries is not missing and not callable(embed_queries):
        raise TypeError("embedder embed_queries_with_cancel attribute must be callable")
    normalized = model_id.strip()
    return (
        embedder
        if normalized == model_id
        else _NormalizedEmbedder(embedder, normalized)
    )


def _page(mem: Any, region: str, criterion: dict[str, Any] | None = None) -> list[Any]:
    """Page to the end: one fetch is bounded, and a partial erase must not look whole."""
    out: list[Any] = []
    after = None
    while True:
        got = mem.fetch(
            region, KIND, payload_filter=criterion, limit=PAGE, after_id=after
        )
        out.extend(got)
        if len(got) < PAGE:
            return out
        after = got[-1].id


# ADK partitions memory by (app_name, user_id); nothing crosses that boundary.
_UNKNOWN_SESSION = "__unknown_session_id__"


def _text_of(content: types.Content | None) -> str:
    """The searchable text of an event, which is what Citadel embeds."""
    if content is None or not content.parts:
        return ""
    return " ".join(p.text for p in content.parts if getattr(p, "text", None))


def _stamp(timestamp: float | None) -> str | None:
    """ADK formats memory timestamps as local-time ISO 8601, so match it."""
    return datetime.fromtimestamp(timestamp).isoformat() if timestamp else None


def _scope(app_name: str, user_id: str) -> dict[str, str]:
    return {"app": app_name, "user": user_id}


# A Database is pinned to its opening thread, so workers take Memory, not self.


def _memory_key(*parts: str) -> str:
    """One engine key from several names, length-prefixed so that no two name
    tuples can spell the same key."""
    return "".join(f"{len(p)}\x1f{p}" for p in parts)


def _write_events_locked(
    mem: Any,
    region: str,
    app_name: str,
    user_id: str,
    session_id: str,
    events: Sequence[Event],
    custom_metadata: Mapping[str, object] | None,
    *,
    replace_session: bool = False,
) -> None:
    scope = _scope(app_name, user_id)
    # Read before the write, so the erase below cannot take a row this call just
    # made. Rows superseded by the keyed write are already gone by then, and
    # forgetting an id twice is a no-op.
    prior = _page(mem, region, scope | {"sid": session_id}) if replace_session else []
    keyed: dict[str, dict[str, Any]] = {}
    anonymous: list[dict[str, Any]] = []
    for event in events:
        if not event.content or not event.content.parts:
            continue  # the reference skips contentless events
        text = _text_of(event.content)
        if not text:
            continue  # nothing to embed, so nothing recall could ever return
        atom = {
            "kind": KIND,
            "text": text,
            "payload": {
                **scope,
                "sid": session_id,
                "eid": event.id,
                "author": event.author,
                "ts": _stamp(event.timestamp),
                # Stored whole so a memory hands back the content ADK gave us.
                "content": event.content.model_dump(mode="json", exclude_none=True),
                "meta": dict(custom_metadata) if custom_metadata else {},
            },
        }
        if event.id:
            # The first copy of an id in one call stands, as it did when a read
            # of the stored ids decided this.
            keyed.setdefault(_memory_key(app_name, user_id, session_id, event.id), atom)
        else:
            anonymous.append(atom)  # no stable name to key on
    if keyed:
        # Keyed, so re-ingesting a session converges on the stored events instead
        # of duplicating them, and two concurrent ingests of one event cannot
        # both observe it absent and both write it.
        mem.remember_replacing_keyed_batch(
            region, [(atom, k) for k, atom in keyed.items()]
        )
    if anonymous:
        mem.remember_batch(region, anonymous)
    if replace_session:
        # add_session_to_memory SETS the session's events rather than merging
        # them, so an event dropped from the session is dropped from memory too.
        # add_events_to_memory is the additive one and leaves this alone.
        written = {atom["payload"]["eid"] for atom in keyed.values()}
        stale = [h for h in prior if h.payload.get("eid") not in written]
        if stale:
            mem.forget(region, [h.id for h in stale])


def _write_events(
    mem: Any,
    region: str,
    app_name: str,
    user_id: str,
    session_id: str,
    events: Sequence[Event],
    custom_metadata: Mapping[str, object] | None,
    *,
    replace_session: bool = False,
) -> None:
    key = (region, app_name, user_id, session_id)
    lock = _SESSION_LOCKS[hash(key) % len(_SESSION_LOCKS)]
    with lock:
        _write_events_locked(
            mem,
            region,
            app_name,
            user_id,
            session_id,
            events,
            custom_metadata,
            replace_session=replace_session,
        )


def _write_memories(
    mem: Any,
    region: str,
    app_name: str,
    user_id: str,
    memories: Sequence[MemoryEntry],
    custom_metadata: Mapping[str, object] | None,
) -> None:
    scope = _scope(app_name, user_id)
    # An id replaces rather than skips: ignoring an update is a silent no-op.
    keyed: dict[str, dict[str, Any]] = {}
    anonymous: list[dict[str, Any]] = []
    for m in memories:
        text = _text_of(m.content)
        if not text:
            continue
        meta = dict(m.custom_metadata)
        if custom_metadata:
            meta.update(custom_metadata)
        atom = {
            "kind": KIND,
            "text": text,
            "payload": {
                **scope,
                "sid": _UNKNOWN_SESSION,
                "eid": m.id,
                "author": m.author,
                "ts": m.timestamp,
                "content": m.content.model_dump(mode="json", exclude_none=True),
                "meta": meta,
            },
        }
        if m.id:
            keyed[m.id] = atom
        else:
            anonymous.append(atom)  # no identity to replace, so it is always new
    if keyed:
        # Keyed, so an id replaces in one transaction. Two concurrent adds of one
        # id supersede rather than both landing, which the read-then-write-then-
        # erase this replaces could not prevent.
        mem.remember_replacing_keyed_batch(
            region,
            [
                (atom, _memory_key(app_name, user_id, eid))
                for eid, atom in keyed.items()
            ],
        )
    if anonymous:
        mem.remember_batch(region, anonymous)


def _entry(hit) -> MemoryEntry:
    p = hit.payload
    return MemoryEntry(
        content=types.Content.model_validate(p["content"]),
        custom_metadata=p.get("meta") or {},
        id=p.get("eid"),
        author=p.get("author"),
        timestamp=p.get("ts"),
    )


def _search(
    mem: Any, region: str, app_name: str, user_id: str, query: str, limit: int
) -> SearchMemoryResponse:
    hits = mem.recall(
        region,
        text=query,
        k=limit,
        kinds=[KIND],
        options=citadeldb.RecallOptions(payload_filter=_scope(app_name, user_id)),
    )
    return SearchMemoryResponse(memories=[_entry(h) for h in hits])


class CitadelMemoryService(BaseMemoryService):
    """An ADK `BaseMemoryService` backed by one encrypted Citadel region."""

    def __init__(
        self,
        path: str = DEFAULT_PATH,
        key: str = "",
        *,
        embedder: Any,
        region: str = DEFAULT_REGION,
        search_limit: int = DEFAULT_SEARCH_LIMIT,
    ) -> None:
        if not key:
            raise ValueError("a passphrase is required: conversations are the payload")
        embedder = _require_embedder(embedder)
        self._search_limit = search_limit
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

    def _owned(self, app_name: str, user_id: str) -> list[Any]:
        """Every atom for one (app, user). Served by the payload index."""
        return _page(self._mem, self._region, _scope(app_name, user_id))

    async def add_session_to_memory(self, session: Session) -> None:
        await asyncio.to_thread(
            functools.partial(
                _write_events,
                self._mem,
                self._region,
                session.app_name,
                session.user_id,
                session.id,
                session.events,
                None,
                replace_session=True,
            )
        )

    async def search_memory(
        self, *, app_name: str, user_id: str, query: str
    ) -> SearchMemoryResponse:
        return await asyncio.to_thread(
            _search,
            self._mem,
            self._region,
            app_name,
            user_id,
            query,
            self._search_limit,
        )

    async def add_events_to_memory(
        self,
        *,
        app_name: str,
        user_id: str,
        events: Sequence[Event],
        session_id: str | None = None,
        custom_metadata: Mapping[str, object] | None = None,
    ) -> None:
        await asyncio.to_thread(
            _write_events,
            self._mem,
            self._region,
            app_name,
            user_id,
            session_id or _UNKNOWN_SESSION,
            events,
            custom_metadata,
        )

    async def add_memory(
        self,
        *,
        app_name: str,
        user_id: str,
        memories: Sequence[MemoryEntry],
        custom_metadata: Mapping[str, object] | None = None,
    ) -> None:
        """Write memories directly, without a session."""
        await asyncio.to_thread(
            _write_memories,
            self._mem,
            self._region,
            app_name,
            user_id,
            memories,
            custom_metadata,
        )

    def forget_user(self, app_name: str, user_id: str) -> int:
        """Destroy everything one user owns, returning the number erased."""
        doomed = self._owned(app_name, user_id)
        if not doomed:
            return 0
        return self._mem.forget(self._region, [h.id for h in doomed]).erased_count

    def forget_session(self, app_name: str, user_id: str, session_id: str) -> int:
        """The same for one session of one user."""
        scope = _scope(app_name, user_id) | {"sid": session_id}
        doomed = _page(self._mem, self._region, scope)
        if not doomed:
            return 0
        return self._mem.forget(self._region, [h.id for h in doomed]).erased_count

    def count(self, app_name: str, user_id: str) -> int:
        return len(self._owned(app_name, user_id))
