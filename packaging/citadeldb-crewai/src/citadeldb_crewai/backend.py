"""CitadelBackend: CrewAI's StorageBackend over an encrypted Citadel region.

CrewAI embeds the query itself and hands the backend a vector, so search here is vector
recall plus the scope/category/metadata predicates the protocol defines.

`MemoryRecord.importance` maps onto Citadel's native atom score, so importance survives as a
ranking signal rather than as metadata the store ignores.

Deletes destroy each record's encryption key. `reset` on a scope therefore makes that
subtree unreadable rather than merely unlisted.
"""
from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone
from typing import Any

import citadeldb
from crewai.memory.storage.backend import MemoryRecord, ScopeInfo

KIND = "mem"
DEFAULT_DIM = 1536  # OpenAI text-embedding-3-small, CrewAI's default


class _SuppliedEmbedder:
    """Returns the vector CrewAI already computed for a record.

    An atom's embedding is produced by its region's embedder, and there is no way to hand one
    in. CrewAI embeds first and passes the vector to the backend, so the two would land in
    different vector spaces and search would compare noise. This embedder closes that gap by
    handing back the supplied vector for text it has seen.

    Text it has not seen falls back to a deterministic hash so a write never fails; those
    records simply do not rank against CrewAI-embedded queries.
    """

    metric = "cosine"
    model_id = "crewai-supplied"

    def __init__(self, dim: int) -> None:
        self.dim = dim
        self._pending: dict[str, list[float]] = {}

    def offer(self, text: str, vector: list[float]) -> None:
        if len(vector) != self.dim:
            raise ValueError(
                f"crew supplied a {len(vector)}-dimension embedding but this region is "
                f"{self.dim}. Build the backend with dim={len(vector)} to match your "
                f"crew's embedding model."
            )
        self._pending[text] = list(vector)

    def _fallback(self, text: str) -> list[float]:
        digest = hashlib.sha256(text.encode()).digest()
        return [digest[i % len(digest)] / 255.0 for i in range(self.dim)]

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._pending.pop(t, None) or self._fallback(t) for t in texts]

    def embed_queries(self, texts: list[str]) -> list[list[float]]:
        return self.embed(texts)


def _scope_parts(scope: str) -> list[str]:
    return [p for p in scope.split("/") if p]


def _ancestors(scope: str) -> list[str]:
    """Every ancestor path of `scope`, itself included, each rooted and slash-terminated.

    JSONB containment tests array membership, so storing the ancestors turns "everything
    under this scope" into an indexed lookup instead of a scan.
    """
    parts = _scope_parts(scope)
    return ["/" + "/".join(parts[: i + 1]) for i in range(len(parts))] or ["/"]


def _norm(scope: str) -> str:
    parts = _scope_parts(scope)
    return "/" + "/".join(parts) if parts else "/"


# CrewAI's datetimes are naive UTC, and its recency scoring subtracts them from a naive
# `utcnow()`. Handing back an aware datetime raises; reading a naive one as local time shifts
# every stamp by the machine's offset, which would silently skew `older_than` sweeps.


def _micros(dt: datetime | None) -> int:
    dt = dt or datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def _dt(micros: int | None) -> datetime | None:
    if not micros:
        return None
    return datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc).replace(tzinfo=None)


class CitadelBackend:
    """A CrewAI `StorageBackend` backed by an encrypted Citadel region."""

    def __init__(
        self,
        path: str = "crew_memory.cdl",
        key: str = "crewai",
        *,
        region: str = "memory",
        dim: int = DEFAULT_DIM,
    ) -> None:
        try:
            self._db = citadeldb.connect(path, key=key, region_keys=True)
        except citadeldb.OperationalError as e:
            if "locked" not in str(e):
                raise
            raise RuntimeError(
                f"{path} is already open in this process or another one. Citadel is embedded, "
                f"so one handle owns the file: build a single CitadelBackend and share it."
            ) from e
        self._mem = self._db.memory()
        self._region = region
        self._embedder = _SuppliedEmbedder(dim)
        try:
            self._mem.create_encrypted_region(region, self._embedder)
        except citadeldb.CitadelError:
            pass  # region already exists from an earlier open

    # ---- helpers ----------------------------------------------------------

    def _by_id(self, record_id: str):
        hits = self._mem.fetch(
            self._region, KIND, payload_filter={"rid": record_id}, limit=1
        )
        return hits[0] if hits else None

    def _scan(self, scope_prefix: str | None) -> list[Any]:
        """Every record, or every record under a scope. Prefix rides the payload index."""
        pf = {"anc": [_norm(scope_prefix)]} if scope_prefix else None
        return self._mem.fetch(self._region, KIND, payload_filter=pf, limit=100_000)

    @staticmethod
    def _record(hit) -> MemoryRecord:
        p = hit.payload
        return MemoryRecord(
            id=p["rid"],
            content=hit.text,
            scope=p["scope"],
            categories=p.get("categories", []),
            metadata=p.get("metadata", {}),
            importance=hit.score,
            created_at=_dt(p.get("created_at")),
            last_accessed=_dt(p.get("last_accessed")),
            source=p.get("source"),
            private=p.get("private", False),
        )

    def _matches(
        self,
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

    # ---- writes -----------------------------------------------------------

    def save(self, records: list[MemoryRecord]) -> None:
        for r in records:
            existing = self._by_id(r.id)
            if existing:
                self._mem.forget(self._region, [existing.id])
            scope = _norm(r.scope)
            atom: dict[str, Any] = {
                "kind": KIND,
                "text": r.content,
                # CrewAI's importance is a ranking signal, so it becomes the atom's score.
                "score": r.importance,
                "payload": {
                    "rid": r.id,
                    "scope": scope,
                    "anc": _ancestors(scope),
                    "categories": list(r.categories),
                    "metadata": dict(r.metadata),
                    "created_at": _micros(r.created_at),
                    "last_accessed": _micros(r.last_accessed) if r.last_accessed else None,
                    "source": r.source,
                    "private": r.private,
                },
            }
            # Hand the supplied vector to the embedder, which returns it for this text.
            if r.embedding is not None:
                self._embedder.offer(r.content, r.embedding)
            self._mem.remember(self._region, atom)

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
        # CrewAI embeds the query, so recall runs on the vector it supplies. Over-fetch:
        # the scope and metadata passes below both discard rows.
        hits = self._mem.recall(
            self._region, embedding=query_embedding, k=max(limit * 4, 32), kinds=[KIND]
        )
        out: list[tuple[MemoryRecord, float]] = []
        want = _norm(scope_prefix) if scope_prefix else None
        for h in hits:
            if want and want not in h.payload.get("anc", []):
                continue
            if not self._matches(h, categories, metadata_filter, None):
                continue
            score = 1.0 - h.distance if h.distance is not None else h.score
            if score < min_score:
                continue
            out.append((self._record(h), score))
            if len(out) >= limit:
                break
        return out

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
        """Immediate children of `parent`, which is what a tree view needs."""
        root = _norm(parent)
        depth = len(_scope_parts(root))
        children: set[str] = set()
        for h in self._scan(None if root == "/" else root):
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
            # ScopeInfo carries the distinct names; per-category counts are list_categories.
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
        if record_ids is not None:
            doomed = [h for rid in record_ids if (h := self._by_id(rid))]
        else:
            doomed = [
                h
                for h in self._scan(scope_prefix)
                if self._matches(h, categories, metadata_filter, older_than)
            ]
        if not doomed:
            return 0
        # Erasure destroys each record's key, so the ciphertext stays unreadable.
        return self._mem.forget(self._region, [h.id for h in doomed]).erased_count

    def reset(self, scope_prefix: str | None = None) -> None:
        self.delete(scope_prefix=scope_prefix)

    # ---- async ------------------------------------------------------------
    # The bindings are sync, so a worker thread keeps the event loop free.

    async def asave(self, records: list[MemoryRecord]) -> None:
        await asyncio.to_thread(self.save, records)

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
            self.search,
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
            self.delete, scope_prefix, categories, record_ids, older_than, metadata_filter
        )
