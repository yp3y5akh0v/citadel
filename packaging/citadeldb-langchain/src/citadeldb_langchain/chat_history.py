"""LangChain chat message history over an encrypted Citadel region."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from operator import index
from typing import Any

import citadeldb
from langchain_core.chat_history import BaseChatMessageHistory
from langchain_core.messages import BaseMessage, message_to_dict, messages_from_dict

KIND = "message"
DEFAULT_PATH = "langchain_history.cdl"
DEFAULT_REGION = "chat_history"
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


def _page(mem: Any, region: str, session_id: str) -> list[Any]:
    """Page to the end: one fetch is bounded, and a partial clear must not look whole."""
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


def _messages(mem: Any, region: str, session_id: str) -> list[BaseMessage]:
    hits = _page(mem, region, session_id)
    return messages_from_dict([h.payload["msg"] for h in hits])


def _add(
    mem: Any, region: str, session_id: str, messages: Sequence[BaseMessage]
) -> None:
    atoms = [
        {
            "kind": KIND,
            # The message content is what recall would match on.
            "text": _searchable(m),
            "payload": {"sid": session_id, "msg": message_to_dict(m)},
        }
        for m in messages
    ]
    if atoms:
        # One batch draws one id range, so list order survives as id order.
        mem.remember_batch(region, atoms)


def _clear(mem: Any, region: str, session_id: str) -> int:
    hits = _page(mem, region, session_id)
    if not hits:
        return 0
    return mem.forget(region, [h.id for h in hits]).erased_count


def _searchable(message: BaseMessage) -> str:
    """Message text, or a rendering of it when the content is a block list."""
    content = message.content
    if isinstance(content, str) and content:
        return content
    if isinstance(content, list):
        parts = [
            b["text"]
            for b in content
            if isinstance(b, dict) and isinstance(b.get("text"), str) and b["text"]
        ]
        if parts:
            return " ".join(parts)
    return message.type


class CitadelChatMessageHistory(BaseChatMessageHistory):
    """A LangChain chat history backed by one encrypted Citadel region."""

    def __init__(
        self,
        session_id: str,
        path: str = DEFAULT_PATH,
        key: str = "",
        *,
        embedder: Any,
        region: str = DEFAULT_REGION,
    ) -> None:
        if not key:
            raise ValueError("a passphrase is required: transcripts are the payload")
        embedder = _require_embedder(embedder)
        self.session_id = session_id
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

    @property
    def messages(self) -> list[BaseMessage]:
        return _messages(self._mem, self._region, self.session_id)

    def add_messages(self, messages: Sequence[BaseMessage]) -> None:
        _add(self._mem, self._region, self.session_id, messages)

    def clear(self) -> None:
        _clear(self._mem, self._region, self.session_id)

    # The bindings are sync, so a worker thread keeps the event loop free.

    async def aget_messages(self) -> list[BaseMessage]:
        return await asyncio.to_thread(
            _messages, self._mem, self._region, self.session_id
        )

    async def aadd_messages(self, messages: Sequence[BaseMessage]) -> None:
        await asyncio.to_thread(
            _add, self._mem, self._region, self.session_id, list(messages)
        )

    async def aclear(self) -> None:
        await asyncio.to_thread(_clear, self._mem, self._region, self.session_id)

    def forget(self) -> int:
        """Destroy this session's messages, returning the number erased."""
        return _clear(self._mem, self._region, self.session_id)
