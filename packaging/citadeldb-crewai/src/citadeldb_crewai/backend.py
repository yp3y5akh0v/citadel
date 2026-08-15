"""CrewAI StorageBackend over an encrypted Citadel region."""
from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone
from typing import Any

import citadeldb
from crewai.memory.storage.backend import MemoryRecord, ScopeInfo

KIND = "mem"
PAGE = 10_000
# OpenAI text-embedding-3-small. CrewAI's own default is now 3-large, so an
# unconfigured crew passes dim=3072.
DEFAULT_DIM = 1536


class _PlaceholderEmbedder:
    """Fixes the region's dimension; CrewAI supplies the vectors."""

    metric = "cosine"
    model_id = "crewai-supplied"

    def __init__(self, dim: int) -> None:
        self.dim = dim

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._placeholder(t) for t in texts]

    def embed_queries(self, texts: list[str]) -> list[list[float]]:
        return self.embed(texts)

    def _placeholder(self, text: str) -> list[float]:
        digest = hashlib.sha256(text.encode()).digest()
        return [digest[i % len(digest)] / 255.0 for i in range(self.dim)]


def _scope_parts(scope: str) -> list[str]:
    return [p for p in scope.split("/") if p]


def _ancestors(scope: str) -> list[str]:
    """JSONB containment makes ancestors an indexed lookup, not a scan."""
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
    return datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc).replace(tzinfo=None)


# A Database is pinned to its opening thread, so workers take Memory, not self.


def _by_id(mem: Any, region: str, record_id: str):
    hits = mem.fetch(region, KIND, payload_filter={"rid": record_id}, limit=1)
    return hits[0] if hits else None


def _scan(mem: Any, region: str, scope_prefix: str | None) -> list[Any]:
    """Every record, or every record under a scope; prefix rides the index."""
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
    if older_than and (p.get("created_at") or 0) >= _micros(older_than):
        return False
    return True


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
    for r in records:
        existing = _by_id(mem, region, r.id)
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
            # A metadata-only edit, which is what Memory.update() produces: it
            # reads a record back, changes a field and saves it, and a record
            # read back carries no embedding. Rewriting the atom would swap the
            # crew's vector for a placeholder derived from the unchanged text,
            # so edit the payload in place and leave the vector alone. Changed
            # content is a different case: no stored vector still describes it.
            mem.update_atom_payload(region, existing.id, payload)
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
            if len(r.embedding) != dim:
                raise ValueError(
                    f"crew supplied a {len(r.embedding)}-dimension embedding but this "
                    f"region is {dim}. Build the backend with dim={len(r.embedding)} "
                    f"to match your crew's embedding model."
                )
            atom["embedding"] = list(r.embedding)
        # Keyed on the record id, so the write supersedes any stored version in
        # one transaction rather than racing a separate read and erase.
        mem.remember_replacing_keyed(region, atom, r.id)


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
    options = (
        citadeldb.RecallOptions(payload_filter={"anc": [want]})
        if want and want != "/"
        else None
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
        hits = mem.recall(region, embedding=query_embedding, k=k, kinds=[KIND],
                          options=options)
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
        doomed = [h for rid in record_ids if (h := _by_id(mem, region, rid))]
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
        region: str = "memory",
        dim: int = DEFAULT_DIM,
    ) -> None:
        if not key:
            raise ValueError("a passphrase is required: memories are the payload")
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
        self._embedder = _PlaceholderEmbedder(dim)
        # Idempotent for a region of the same width, so a dim clash raises here.
        self._mem.create_encrypted_region(region, self._embedder)

    # ---- helpers ----------------------------------------------------------

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

    # ---- writes -----------------------------------------------------------

    def save(self, records: list[MemoryRecord]) -> None:
        _save(self._mem, self._region, self._embedder.dim, records)

    def update(self, record: MemoryRecord) -> None:
        self.save([record])

    # ---- reads ------------------------------------------------------------

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
            self._mem, self._region, query_embedding, scope_prefix, categories,
            metadata_filter, limit, min_score,
        )

    def list_records(
        self, scope_prefix: str | None = None, limit: int = 200, offset: int = 0
    ) -> list[MemoryRecord]:
        hits = self._scan(scope_prefix)
        return [self._record(h) for h in hits[offset : offset + limit]]

    def count(self, scope_prefix: str | None = None) -> int:
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
        stamps = [h.payload.get("created_at") for h in hits if h.payload.get("created_at")]
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

    # ---- deletes ----------------------------------------------------------

    def delete(
        self,
        scope_prefix: str | None = None,
        categories: list[str] | None = None,
        record_ids: list[str] | None = None,
        older_than: datetime | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> int:
        return _delete(
            self._mem, self._region, scope_prefix, categories, record_ids,
            older_than, metadata_filter,
        )

    def reset(self, scope_prefix: str | None = None) -> None:
        self.delete(scope_prefix=scope_prefix)

    # ---- async ------------------------------------------------------------
    # The bindings are sync, so a worker thread keeps the event loop free.

    async def asave(self, records: list[MemoryRecord]) -> None:
        await asyncio.to_thread(
            _save, self._mem, self._region, self._embedder.dim, records
        )

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
