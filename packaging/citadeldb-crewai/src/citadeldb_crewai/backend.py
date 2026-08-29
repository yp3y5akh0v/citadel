"""CrewAI StorageBackend over an encrypted Citadel region."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from operator import index
from typing import Any

import citadeldb
from crewai.memory.storage.backend import MemoryRecord, ScopeInfo

KIND = "mem"
PAGE = 10_000
_COSINE_METRICS = {"cosine", "cos"}


class _NormalizedEmbedder:
    def __init__(self, embedder: Any, model_id: str) -> None:
        self._embedder = embedder
        self.model_id = model_id

    def __getattr__(self, name: str) -> Any:
        return getattr(self._embedder, name)


def _require_embedder(embedder: Any) -> tuple[Any, int]:
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
    if not isinstance(metric, str) or metric.lower() not in _COSINE_METRICS:
        raise TypeError(
            "embedder metric must be cosine because CrewAI scores are normalized "
            "cosine similarities"
        )
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
        else _NormalizedEmbedder(embedder, normalized),
        dim,
    )


def _scope_parts(scope: str) -> list[str]:
    return [p for p in scope.split("/") if p]


def _ancestors(scope: str) -> list[str]:
    """Every normalized ancestor of a scope, including the scope itself."""
    parts = _scope_parts(scope)
    return ["/" + "/".join(parts[: i + 1]) for i in range(len(parts))] or ["/"]


def _norm(scope: str) -> str:
    parts = _scope_parts(scope)
    return "/" + "/".join(parts) if parts else "/"


# CrewAI compares stamps against a naive utcnow(); aware datetimes raise.


def _micros(dt: datetime | None) -> int:
    dt = dt or datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def _dt(micros: int | None) -> datetime | None:
    if not micros:
        return None
    return datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc).replace(
        tzinfo=None
    )


# A Database is pinned to its opening thread, so workers take Memory, not self.


def _by_id(mem: Any, region: str, record_id: str):
    hits = mem.fetch(region, KIND, payload_filter={"rid": record_id}, limit=1)
    return hits[0] if hits else None


def _scan(mem: Any, region: str, scope_prefix: str | None) -> list[Any]:
    """Every record, or every record under a scope."""
    scope = _norm(scope_prefix) if scope_prefix else "/"
    # Root is every record, and no nested record lists it: an ancestor list starts
    # one level down, so filtering on it there would return the root's own only.
    pf = {"anc": [scope]} if scope != "/" else None
    # Page to the end: one fetch is bounded, and a partial erase must not look whole.
    out: list[Any] = []
    after = None
    while True:
        page = mem.fetch(region, KIND, payload_filter=pf, limit=PAGE, after_id=after)
        out.extend(page)
        if len(page) < PAGE:
            return out
        after = page[-1].id


def _keep(
    hit,
    categories: list[str] | None,
    metadata_filter: dict[str, Any] | None,
    older_than: datetime | None,
) -> bool:
    p = hit.payload
    if categories and not set(categories) & set(p.get("categories", [])):
        return False
    if metadata_filter:
        meta = p.get("metadata", {})
        if any(meta.get(k) != v for k, v in metadata_filter.items()):
            return False
    return older_than is None or (p.get("created_at") or 0) < _micros(older_than)


def _to_record(hit) -> MemoryRecord:
    p = hit.payload
    return MemoryRecord(
        id=p["rid"],
        content=hit.text,
        scope=p["scope"],
        categories=p.get("categories", []),
        metadata=p.get("metadata", {}),
        # The stored value. A recall hit's score is the fused rank over whichever
        # rows shared its candidate pool, which is neither this quantity nor
        # stable between calls.
        importance=p.get("importance", hit.score),
        created_at=_dt(p.get("created_at")),
        last_accessed=_dt(p.get("last_accessed")),
        source=p.get("source"),
        private=p.get("private", False),
    )


def _save(mem: Any, region: str, dim: int, records: list[MemoryRecord]) -> None:
    # Encrypted payload filters run after decryption, so one pass beats one
    # region scan per record. A repeated id in one call keeps the last value.
    latest = {record.id: record for record in records}
    if not latest:
        return
    for r in latest.values():
        if r.embedding is not None and len(r.embedding) != dim:
            raise ValueError(
                f"crew supplied a {len(r.embedding)}-dimension embedding but this "
                f"region is {dim}. Pass the same {len(r.embedding)}-dimension "
                f"embedder to the backend and your crew."
            )
    needs_existing = {r.id for r in latest.values() if r.embedding is None}
    existing_by_id = (
        {
            h.payload["rid"]: h
            for h in _scan(mem, region, None)
            if h.payload.get("rid") in needs_existing
        }
        if needs_existing
        else {}
    )
    payload_updates: list[tuple[int, dict[str, Any]]] = []
    pending: list[tuple[dict[str, Any], str]] = []
    for r in latest.values():
        existing = existing_by_id.get(r.id)
        scope = _norm(r.scope)
        payload = {
            "rid": r.id,
            "scope": scope,
            "anc": _ancestors(scope),
            "categories": list(r.categories),
            "metadata": dict(r.metadata),
            # Stored, not read back off the hit: a recall hit's score is the
            # fused rank, which is a different quantity from what was saved.
            "importance": r.importance,
            "created_at": _micros(r.created_at),
            "last_accessed": _micros(r.last_accessed) if r.last_accessed else None,
            "source": r.source,
            "private": r.private,
        }
        if r.embedding is None and existing is not None and existing.text == r.content:
            # Memory.update() saves a read-back record, which carries no
            # embedding. The stored vector still describes unchanged content;
            # CrewAI reads importance from the payload and scores it after search.
            payload_updates.append((existing.id, payload))
            continue
        atom: dict[str, Any] = {
            "kind": KIND,
            "text": r.content,
            # Importance is a ranking signal, so it becomes the atom's score too.
            "score": r.importance,
            "payload": payload,
        }
        # The vector rides its own atom so duplicate content cannot swap them.
        if r.embedding is not None:
            atom["embedding"] = list(r.embedding)
        pending.append((atom, r.id))
    if pending:
        mem.remember_replacing_keyed_batch(region, pending)
    for atom_id, payload in payload_updates:
        mem.update_atom_payload(region, atom_id, payload)


def _search(
    mem: Any,
    region: str,
    query_embedding: list[float],
    scope_prefix: str | None,
    categories: list[str] | None,
    metadata_filter: dict[str, Any] | None,
    limit: int,
    min_score: float,
) -> list[tuple[MemoryRecord, float]]:
    want = _norm(scope_prefix) if scope_prefix else None
    criterion: dict[str, Any] = {}
    if want and want != "/":
        criterion["anc"] = [want]
    # StorageBackend.search returns semantic similarity. CrewAI applies recency
    # and importance itself in compute_composite_score after this call.
    options = citadeldb.RecallOptions(
        payload_filter=criterion or None,
        weights=(1.0, 0.0, 0.0, 0.0),
    )

    def surviving(hits: list[Any]) -> list[tuple[MemoryRecord, float]]:
        out: list[tuple[MemoryRecord, float]] = []
        for h in hits:
            if not _keep(h, categories, metadata_filter, None):
                continue
            # crewai's score domain is [0, 1], and cosine distance runs to 2, so
            # a negatively-correlated record maps below zero and the min_score=0.0
            # every caller passes to mean "no threshold" would discard it.
            score = (
                max(0.0, min(1.0, 1.0 - h.distance))
                if h.distance is not None
                else h.score
            )
            if score >= min_score:
                out.append((_to_record(h), score))
        return out

    # The scope rides the scan; category, metadata and score cannot, so widen
    # until `limit` records survive them or the region runs out.
    k = max(limit, 32)
    while True:
        hits = mem.recall(
            region, embedding=query_embedding, k=k, kinds=[KIND], options=options
        )
        out = surviving(hits)
        if len(out) >= limit or len(hits) < k:
            return out[:limit]
        k *= 2


def _delete(
    mem: Any,
    region: str,
    scope_prefix: str | None,
    categories: list[str] | None,
    record_ids: list[str] | None,
    older_than: datetime | None,
    metadata_filter: dict[str, Any] | None,
) -> int:
    if record_ids is not None:
        wanted = set(record_ids)
        if not wanted:
            return 0
        doomed = [h for h in _scan(mem, region, None) if h.payload.get("rid") in wanted]
    else:
        doomed = [
            h
            for h in _scan(mem, region, scope_prefix)
            if _keep(h, categories, metadata_filter, older_than)
        ]
    if not doomed:
        return 0
    # Erasure destroys each record's key, so the ciphertext stays unreadable.
    return mem.forget(region, [h.id for h in doomed]).erased_count


class CitadelBackend:
    """A CrewAI `StorageBackend` backed by an encrypted Citadel region."""

    def __init__(
        self,
        path: str = "crew_memory.cdl",
        key: str = "",
        *,
        embedder: Any,
        region: str = "memory",
    ) -> None:
        if not key:
            raise ValueError("a passphrase is required: memories are the payload")
        embedder, dim = _require_embedder(embedder)
        try:
            self._db = citadeldb.connect(path, key=key, region_keys=True)
        except citadeldb.OperationalError as e:
            if "locked" not in str(e):
                raise
            raise RuntimeError(
                f"{path} is open in another process. Citadel is embedded, so one process "
                f"owns the file."
            ) from e
        self._mem = self._db.memory()
        self._region = region
        self._dim = dim
        # Idempotent for a region of the same width, so a dim clash raises here.
        self._mem.create_encrypted_region(region, embedder)

    def _by_id(self, record_id: str):
        return _by_id(self._mem, self._region, record_id)

    def _scan(self, scope_prefix: str | None) -> list[Any]:
        return _scan(self._mem, self._region, scope_prefix)

    @staticmethod
    def _record(hit) -> MemoryRecord:
        p = hit.payload
        return MemoryRecord(
            id=p["rid"],
            content=hit.text,
            scope=p["scope"],
            categories=p.get("categories", []),
            metadata=p.get("metadata", {}),
            importance=p.get("importance", hit.score),
            created_at=_dt(p.get("created_at")),
            last_accessed=_dt(p.get("last_accessed")),
            source=p.get("source"),
            private=p.get("private", False),
        )

    def save(self, records: list[MemoryRecord]) -> None:
        _save(self._mem, self._region, self._dim, records)

    def update(self, record: MemoryRecord) -> None:
        self.save([record])

    def get_record(self, record_id: str) -> MemoryRecord | None:
        hit = self._by_id(record_id)
        return self._record(hit) if hit else None

    def search(
        self,
        query_embedding: list[float],
        scope_prefix: str | None = None,
        categories: list[str] | None = None,
        metadata_filter: dict[str, Any] | None = None,
        limit: int = 10,
        min_score: float = 0.0,
    ) -> list[tuple[MemoryRecord, float]]:
        return _search(
            self._mem,
            self._region,
            query_embedding,
            scope_prefix,
            categories,
            metadata_filter,
            limit,
            min_score,
        )

    def list_records(
        self, scope_prefix: str | None = None, limit: int = 200, offset: int = 0
    ) -> list[MemoryRecord]:
        records = [self._record(h) for h in self._scan(scope_prefix)]
        records.sort(
            key=lambda record: (record.created_at is not None, record.created_at),
            reverse=True,
        )
        return records[offset : offset + limit]

    def count(self, scope_prefix: str | None = None) -> int:
        if scope_prefix is None:
            return self._mem.count(self._region, KIND)
        return len(self._scan(scope_prefix))

    def list_categories(self, scope_prefix: str | None = None) -> dict[str, int]:
        tally: dict[str, int] = {}
        for h in self._scan(scope_prefix):
            for c in h.payload.get("categories", []):
                tally[c] = tally.get(c, 0) + 1
        return tally

    def list_scopes(self, parent: str = "/") -> list[str]:
        """Immediate children of `parent`."""
        root = _norm(parent)
        depth = len(_scope_parts(root))
        children: set[str] = set()
        for h in self._scan(root):
            parts = _scope_parts(h.payload["scope"])
            if len(parts) > depth:
                children.add("/" + "/".join(parts[: depth + 1]))
        return sorted(children)

    def get_scope_info(self, scope: str) -> ScopeInfo:
        want = _norm(scope)
        hits = self._scan(want)
        stamps = [
            h.payload.get("created_at") for h in hits if h.payload.get("created_at")
        ]
        cats = {c for h in hits for c in h.payload.get("categories", [])}
        return ScopeInfo(
            path=want,
            record_count=len(hits),
            # ScopeInfo carries names only; counts come from list_categories.
            categories=sorted(cats),
            oldest_record=_dt(min(stamps)) if stamps else None,
            newest_record=_dt(max(stamps)) if stamps else None,
            child_scopes=self.list_scopes(want),
        )

    def delete(
        self,
        scope_prefix: str | None = None,
        categories: list[str] | None = None,
        record_ids: list[str] | None = None,
        older_than: datetime | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> int:
        return _delete(
            self._mem,
            self._region,
            scope_prefix,
            categories,
            record_ids,
            older_than,
            metadata_filter,
        )

    def reset(self, scope_prefix: str | None = None) -> None:
        self.delete(scope_prefix=scope_prefix)

    # The bindings are sync, so a worker thread keeps the event loop free.

    async def asave(self, records: list[MemoryRecord]) -> None:
        await asyncio.to_thread(_save, self._mem, self._region, self._dim, records)

    async def asearch(
        self,
        query_embedding: list[float],
        scope_prefix: str | None = None,
        categories: list[str] | None = None,
        metadata_filter: dict[str, Any] | None = None,
        limit: int = 10,
        min_score: float = 0.0,
    ) -> list[tuple[MemoryRecord, float]]:
        return await asyncio.to_thread(
            _search,
            self._mem,
            self._region,
            query_embedding,
            scope_prefix,
            categories,
            metadata_filter,
            limit,
            min_score,
        )

    async def adelete(
        self,
        scope_prefix: str | None = None,
        categories: list[str] | None = None,
        record_ids: list[str] | None = None,
        older_than: datetime | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> int:
        return await asyncio.to_thread(
            _delete,
            self._mem,
            self._region,
            scope_prefix,
            categories,
            record_ids,
            older_than,
            metadata_filter,
        )
