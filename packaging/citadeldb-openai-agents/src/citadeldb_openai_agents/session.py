"""OpenAI Agents SDK sessions over an encrypted Citadel region."""
from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any

import citadeldb
from agents.memory.session_settings import (
    SessionSettings,
    coerce_session_settings,
    resolve_session_limit,
)

if TYPE_CHECKING:
    from agents.items import TResponseInputItem

KIND = "item"
DEFAULT_PATH = "agent_sessions.cdl"
DEFAULT_REGION = "sessions"
# fetch is id-ascending and its limit takes oldest rows, so window in Python.
PAGE = 10_000


def _page(mem: Any, region: str, criterion: dict[str, Any] | None = None) -> list[Any]:
    """Page to the end: one fetch is bounded, and a partial erase must not look whole."""
    out: list[Any] = []
    after = None
    while True:
        got = mem.fetch(region, KIND, payload_filter=criterion, limit=PAGE, after_id=after)
        out.extend(got)
        if len(got) < PAGE:
            return out
        after = got[-1].id


def _searchable(item: Any) -> str:
    """Text projection for recall ranking; the payload is authoritative."""
    if not isinstance(item, dict):
        return json.dumps(item, sort_keys=True, default=str)
    content = item.get("content")
    if isinstance(content, str) and content:
        return content
    if isinstance(content, list):
        parts = [
            p["text"]
            for p in content
            if isinstance(p, dict) and isinstance(p.get("text"), str) and p["text"]
        ]
        if parts:
            return " ".join(parts)
    return json.dumps(item, sort_keys=True, default=str)


class CitadelSessionStore:
    """One encrypted Citadel file, shared by every session in it."""

    def __init__(
        self,
        path: str,
        key: str,
        *,
        region: str = DEFAULT_REGION,
        embedder: Any | None = None,
        ttl: float | None = None,
    ) -> None:
        if not key:
            raise ValueError("a passphrase is required: transcripts are the payload")
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
        self._ttl = ttl
        # Idempotent for a region of the same width, so a dim clash raises here.
        self._mem.create_encrypted_region(
            region, embedder or citadeldb.MockEmbedder(dim=64)
        )

    # ---- session minting --------------------------------------------------

    def session(
        self,
        session_id: str,
        *,
        session_settings: SessionSettings | dict[str, Any] | None = None,
    ) -> CitadelSession:
        return CitadelSession(
            session_id, store=self, session_settings=session_settings
        )

    def session_ids(self) -> list[str]:
        """Every session holding at least one item, sorted."""
        hits = _page(self._mem, self._region)
        return sorted({h.payload["sid"] for h in hits})

    def close(self) -> None:
        """Release this store's handle; other holders of the file keep theirs."""
        self._db.close()

    # ---- storage ----------------------------------------------------------

    def items(self, session_id: str) -> list[Any]:
        """Every atom for one session, oldest first."""
        return _items(self._mem, self._region, session_id)

    def append(self, session_id: str, items: list[TResponseInputItem]) -> None:
        _append(self._mem, self._region, self._ttl, session_id, items)

    def erase(self, ids: list[int]) -> int:
        return _erase(self._mem, self._region, ids)


# A Database is pinned to its opening thread, so workers take Memory, not self.


def _items(mem: Any, region: str, session_id: str) -> list[Any]:
    return _page(mem, region, {"sid": session_id})


def _append(
    mem: Any,
    region: str,
    ttl: float | None,
    session_id: str,
    items: list[TResponseInputItem],
) -> None:
    atoms: list[dict[str, Any]] = []
    for item in items:
        atom: dict[str, Any] = {
            "kind": KIND,
            "text": _searchable(item),
            # The item is stored verbatim; only json round-trip is contracted.
            "payload": {"sid": session_id, "item": item},
        }
        if ttl is not None:
            atom["expires_at"] = int((time.time() + ttl) * 1_000_000)
        atoms.append(atom)
    # One batch draws one contiguous id range, so list order becomes id order.
    mem.remember_batch(region, atoms)


def _erase(mem: Any, region: str, ids: list[int]) -> int:
    return mem.forget(region, ids).erased_count


def _claim(mem: Any, region: str, atom_id: int) -> bool:
    """Whether this caller's erase is the one that removed the row.

    `rows_deleted` counts what the delete actually took, so exactly one of two
    callers racing on the same row sees 1. `erased_count` counts destroyed key
    slots and is always 0 on a plaintext region, so it cannot carry the claim.
    """
    return mem.forget(region, [atom_id]).rows_deleted > 0


def _read_items(
    mem: Any, region: str, session_id: str, window: int | None
) -> list[TResponseInputItem]:
    items = [h.payload["item"] for h in _items(mem, region, session_id)]
    if window is None:
        return items
    # A tail slice: the window is the newest items in chronological order.
    return items[-window:] if window > 0 else []


def _pop(mem: Any, region: str, session_id: str) -> TResponseInputItem | None:
    """Remove and return the newest item, or None when the session is empty.

    The reference pops with one atomic DELETE ... RETURNING. Reading the tail and
    then erasing it is two steps, so two in-flight turns read the same row and
    both return it. The erase itself is atomic, so it doubles as the claim: a
    caller that did not remove the row looks again rather than handing out an
    item another turn already took.
    """
    while True:
        hits = _items(mem, region, session_id)
        if not hits:
            return None
        last = hits[-1]
        if _claim(mem, region, last.id):
            return last.payload["item"]


def _clear(mem: Any, region: str, session_id: str) -> None:
    hits = _items(mem, region, session_id)
    if hits:
        _erase(mem, region, [h.id for h in hits])


def _recall(
    mem: Any, region: str, session_id: str, query: str, limit: int
) -> list[TResponseInputItem]:
    hits = mem.recall(
        region,
        text=query,
        k=limit,
        kinds=[KIND],
        options=citadeldb.RecallOptions(payload_filter={"sid": session_id}),
    )
    return [h.payload["item"] for h in hits]


class CitadelSession:
    """Satisfies `Session` structurally; `SessionABC` is documented internal."""

    def __init__(
        self,
        session_id: str,
        db_path: str = DEFAULT_PATH,
        key: str = "",
        *,
        store: CitadelSessionStore | None = None,
        session_settings: SessionSettings | dict[str, Any] | None = None,
        region: str = DEFAULT_REGION,
        embedder: Any | None = None,
        ttl: float | None = None,
    ) -> None:
        self.session_id = session_id
        # The protocol reads this attribute directly, so None must stay None.
        self.session_settings: SessionSettings | None = (
            coerce_session_settings(session_settings)
            if session_settings is not None
            else None
        )
        # Building a store per session is cheap: they share one open database.
        self._store = store or CitadelSessionStore(
            db_path, key, region=region, embedder=embedder, ttl=ttl
        )
        # Held directly so a dispatched call never touches the pinned store.
        self._mem = self._store._mem
        self._region = self._store._region
        self._ttl = self._store._ttl

    # ---- the protocol surface ---------------------------------------------
    # The bindings are sync, so a worker thread keeps the event loop free.

    async def get_items(self, limit: int | None = None) -> list[TResponseInputItem]:
        window = resolve_session_limit(limit, self.session_settings)
        return await asyncio.to_thread(
            _read_items, self._mem, self._region, self.session_id, window
        )

    async def add_items(self, items: list[TResponseInputItem]) -> None:
        if not items:
            return
        await asyncio.to_thread(
            _append, self._mem, self._region, self._ttl, self.session_id, items
        )

    async def pop_item(self) -> TResponseInputItem | None:
        return await asyncio.to_thread(
            _pop, self._mem, self._region, self.session_id
        )

    async def clear_session(self) -> None:
        await asyncio.to_thread(_clear, self._mem, self._region, self.session_id)

    # ---- beyond the protocol ----------------------------------------------

    async def search(self, query: str, *, limit: int = 5) -> list[TResponseInputItem]:
        """Items from this session ranked by hybrid recall, best first."""
        return await asyncio.to_thread(
            _recall, self._mem, self._region, self.session_id, query, limit
        )
