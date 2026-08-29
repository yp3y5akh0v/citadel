"""HistoryProvider over an encrypted Citadel region."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any, ClassVar

import citadeldb
from agent_framework import HistoryProvider, Message

from ._embedder import require_embedder

KIND = "message"
DEFAULT_PATH = "agent_history.cdl"
DEFAULT_REGION = "history"
PAGE = 10_000


def _page(mem: Any, region: str, session_id: str) -> list[Any]:
    """Page to the end: one fetch is bounded, and a partial erase must not look whole."""
    out: list[Any] = []
    after = None
    while True:
        got = mem.fetch(
            region, KIND, payload_filter={"sid": session_id}, limit=PAGE, after_id=after
        )
        out.extend(got)
        if len(got) < PAGE:
            return out
        after = got[-1].id


# session_id is optional in the protocol; keep unattributed history together.
DEFAULT_SESSION = "default"


# A Database is pinned to its opening thread, so workers take Memory, not self.


def _text_of(message: Message) -> str:
    """An atom needs text to embed; a textless message falls back to role."""
    return message.text or str(message.role)


def _load(mem: Any, region: str, session_id: str) -> list[Message]:
    hits = _page(mem, region, session_id)
    return [Message.from_dict(h.payload["msg"]) for h in hits]


def _append(
    mem: Any, region: str, session_id: str, messages: Sequence[Message]
) -> None:
    atoms = [
        {
            "kind": KIND,
            "text": _text_of(m),
            "payload": {"sid": session_id, "msg": m.to_dict()},
        }
        for m in messages
    ]
    if atoms:
        # One batch draws a contiguous id range, so list order survives.
        mem.remember_batch(region, atoms)


def _forget(mem: Any, region: str, session_id: str) -> int:
    hits = _page(mem, region, session_id)
    if not hits:
        return 0
    return mem.forget(region, [h.id for h in hits]).erased_count


def _recall(
    mem: Any, region: str, session_id: str, query: str, limit: int
) -> list[Message]:
    hits = mem.recall(
        region,
        text=query,
        k=limit,
        kinds=[KIND],
        options=citadeldb.RecallOptions(payload_filter={"sid": session_id}),
    )
    return [Message.from_dict(h.payload["msg"]) for h in hits]


class CitadelHistoryProvider(HistoryProvider):
    """A `HistoryProvider` backed by one encrypted Citadel region."""

    DEFAULT_SOURCE_ID: ClassVar[str] = "citadel_history"

    def __init__(
        self,
        path: str = DEFAULT_PATH,
        key: str = "",
        *,
        embedder: Any,
        source_id: str = DEFAULT_SOURCE_ID,
        region: str = DEFAULT_REGION,
        load_messages: bool = True,
        store_inputs: bool = True,
        store_context_messages: bool = False,
        store_context_from: set[str] | None = None,
        store_outputs: bool = True,
    ) -> None:
        super().__init__(
            source_id=source_id,
            load_messages=load_messages,
            store_inputs=store_inputs,
            store_context_messages=store_context_messages,
            store_context_from=store_context_from,
            store_outputs=store_outputs,
        )
        if not key:
            raise ValueError("a passphrase is required: transcripts are the payload")
        embedder = require_embedder(embedder)
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

    # The bindings are sync, so a worker thread keeps the event loop free.

    async def get_messages(
        self,
        session_id: str | None,
        *,
        state: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Message]:
        return await asyncio.to_thread(
            _load, self._mem, self._region, session_id or DEFAULT_SESSION
        )

    async def save_messages(
        self,
        session_id: str | None,
        messages: Sequence[Message],
        *,
        state: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        # Appends rather than replaces: history is a transcript, not a set.
        await asyncio.to_thread(
            _append,
            self._mem,
            self._region,
            session_id or DEFAULT_SESSION,
            list(messages),
        )

    async def search(
        self, session_id: str | None, query: str, *, limit: int = 5
    ) -> list[Message]:
        """Messages from one session ranked by hybrid recall, best first."""
        return await asyncio.to_thread(
            _recall,
            self._mem,
            self._region,
            session_id or DEFAULT_SESSION,
            query,
            limit,
        )

    async def forget(self, session_id: str | None) -> int:
        """Destroy one session's messages, returning the number erased."""
        return await asyncio.to_thread(
            _forget, self._mem, self._region, session_id or DEFAULT_SESSION
        )
