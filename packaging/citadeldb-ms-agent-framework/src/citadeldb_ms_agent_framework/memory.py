"""ContextProvider over an encrypted Citadel region."""
from __future__ import annotations

import asyncio
from typing import Any, ClassVar, Sequence

import citadeldb
from agent_framework import ContextProvider, Message

KIND = "memory"
DEFAULT_PATH = "agent_memory.cdl"
DEFAULT_REGION = "memories"
PAGE = 10_000


def _page(mem: Any, region: str, criterion: dict[str, Any]) -> list[Any]:
    """Page to the end: one fetch is bounded, and a partial erase must not look whole."""
    out: list[Any] = []
    after = None
    while True:
        got = mem.fetch(region, KIND, payload_filter=criterion, limit=PAGE, after_id=after)
        out.extend(got)
        if len(got) < PAGE:
            return out
        after = got[-1].id
# Roles worth remembering; tool traffic is transcript detail, not knowledge.
_REMEMBERED_ROLES = ("user", "assistant", "system")


# A Database is pinned to its opening thread, so workers take Memory, not self.


def _role_of(message: Message) -> str:
    role = message.role
    return getattr(role, "value", None) or str(role)


def _remember(mem: Any, region: str, scope: str, texts: Sequence[str]) -> None:
    atoms = [
        {"kind": KIND, "text": t, "payload": {"scope": scope, "text": t}} for t in texts
    ]
    if atoms:
        mem.remember_batch(region, atoms)


def _recall(mem: Any, region: str, scope: str, query: str, limit: int) -> list[str]:
    """Distinct memories for `scope`, best first.

    after_run stores every turn verbatim, so a fact the user restates is stored
    once per turn. Those copies are one memory to the model, and asking the
    engine for `limit` rows would spend the whole budget on them - 30 repeats of
    one fact deliver one line and hide everything else in the scope. Widen until
    `limit` distinct texts are found or the scope runs out.
    """
    options = citadeldb.RecallOptions(payload_filter={"scope": scope})
    k = max(limit, 32)
    while True:
        hits = mem.recall(region, text=query, k=k, kinds=[KIND], options=options)
        out: list[str] = []
        seen: set[str] = set()
        for h in hits:
            text = h.payload["text"]
            if text in seen:
                continue
            seen.add(text)
            out.append(text)
        if len(out) >= limit or len(hits) < k:
            return out[:limit]
        k *= 2


def _forget(mem: Any, region: str, scope: str) -> int:
    hits = _page(mem, region, {"scope": scope})
    if not hits:
        return 0
    return mem.forget(region, [h.id for h in hits]).erased_count


class CitadelContextProvider(ContextProvider):
    """A `ContextProvider` backed by one encrypted Citadel region."""

    DEFAULT_SOURCE_ID: ClassVar[str] = "citadel_memory"
    DEFAULT_CONTEXT_PROMPT: ClassVar[str] = (
        "## Memories\nConsider the following memories from earlier conversations:"
    )

    def __init__(
        self,
        path: str = DEFAULT_PATH,
        key: str = "",
        *,
        source_id: str = DEFAULT_SOURCE_ID,
        scope: str = "default",
        region: str = DEFAULT_REGION,
        embedder: Any | None = None,
        limit: int = 5,
        context_prompt: str = DEFAULT_CONTEXT_PROMPT,
    ) -> None:
        super().__init__(source_id)
        if not key:
            raise ValueError("a passphrase is required: memories are the payload")
        self.scope = scope
        self.limit = limit
        self.context_prompt = context_prompt
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
        self._mem.create_encrypted_region(
            region, embedder or citadeldb.MockEmbedder(dim=64)
        )

    # ---- the pipeline hooks ----------------------------------------------

    async def before_run(
        self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]
    ) -> None:
        """Recall what is relevant to this turn and add it to the context."""
        query = "\n".join(
            m.text for m in context.input_messages if m and m.text and m.text.strip()
        )
        if not query:
            return
        memories = await asyncio.to_thread(
            _recall, self._mem, self._region, self.scope, query, self.limit
        )
        if not memories:
            return
        context.extend_messages(
            self.source_id,
            [Message("user", [f"{self.context_prompt}\n" + "\n".join(memories)])],
        )

    async def after_run(
        self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]
    ) -> None:
        """Remember this turn, inputs and response alike."""
        turn: list[Message] = list(context.input_messages)
        if context.response and context.response.messages:
            turn.extend(context.response.messages)
        texts = [
            m.text
            for m in turn
            if m and m.text and m.text.strip() and _role_of(m) in _REMEMBERED_ROLES
        ]
        if texts:
            await asyncio.to_thread(
                _remember, self._mem, self._region, self.scope, texts
            )

    # ---- beyond the pipeline ---------------------------------------------

    async def forget(self) -> int:
        """Destroy this scope's memories, returning the number erased."""
        return await asyncio.to_thread(_forget, self._mem, self._region, self.scope)
