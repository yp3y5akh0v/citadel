"""CitadelStore: LangGraph's BaseStore over an encrypted Citadel region.

`batch` and `abatch` are the only abstract methods; put/get/delete/search/list_namespaces are
concrete helpers that build Ops and dispatch through them. A PutOp carrying `value=None` is
LangGraph's delete.

Two operations behave differently here than in any other store:
  SearchOp with a query   hybrid vector + keyword recall rather than a SQL LIKE.
  PutOp with value=None   the atom's key is destroyed, leaving unreadable ciphertext instead
                          of a removed row.
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any, Iterable

import citadeldb
from langgraph.store.base import (
    BaseStore,
    GetOp,
    Item,
    ListNamespacesOp,
    Op,
    PutOp,
    SearchItem,
    SearchOp,
    TTLConfig,
)

KIND = "kv"
NS_KIND = "ns"
_SEP = "\x1f"  # unit separator: illegal in a namespace element, so joins stay reversible


def _join(namespace: tuple[str, ...]) -> str:
    return _SEP.join(namespace)


def _split(joined: str) -> tuple[str, ...]:
    return tuple(joined.split(_SEP)) if joined else ()


def _ancestors(namespace: tuple[str, ...]) -> list[str]:
    """Every prefix of `namespace`, itself included.

    JSONB containment tests array membership, so storing the prefixes makes
    "everything under this namespace" an indexed lookup instead of a scan.
    """
    return [_join(namespace[: i + 1]) for i in range(len(namespace))]


def _now() -> int:
    return int(time.time() * 1_000_000)


def _when(micros: int | None) -> datetime | None:
    return datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc) if micros else None


class CitadelStore(BaseStore):
    """LangGraph store over one encrypted Citadel region.

    Namespaces live in the atom payload rather than in separate regions, so a search can span
    a namespace prefix in a single recall. Payload lookups ride Citadel's GIN index on
    `payload`, so key access is indexed rather than scanned.
    """

    supports_ttl = True
    ttl_config = TTLConfig(refresh_on_read=True, omit_expired=True)

    def __init__(
        self,
        path: str,
        key: str,
        *,
        region: str = "store",
        embedder: Any | None = None,
    ) -> None:
        try:
            self._db = citadeldb.connect(path, key=key, region_keys=True)
        except citadeldb.OperationalError as e:
            if "locked" not in str(e):
                raise
            raise RuntimeError(
                f"{path} is already open in this process or another one. Citadel is embedded, "
                f"so one handle owns the file: build a single CitadelStore and share it, and "
                f"separate concerns with namespaces rather than with a second store."
            ) from e
        self._mem = self._db.memory()
        self._region = region
        try:
            self._mem.create_encrypted_region(
                region, embedder or citadeldb.MockEmbedder(dim=64)
            )
        except citadeldb.CitadelError:
            pass  # region already exists from an earlier open

    # ---- storage helpers --------------------------------------------------

    def _find(self, namespace: tuple[str, ...], key: str):
        """The atom holding (namespace, key). Served by the payload GIN index."""
        hits = self._mem.fetch(
            self._region,
            KIND,
            payload_filter={"ns": _join(namespace), "key": key},
            limit=1,
        )
        return hits[0] if hits else None

    def _namespace_has_keys(self, joined: str) -> bool:
        return bool(
            self._mem.fetch(self._region, KIND, payload_filter={"ns": joined}, limit=1)
        )

    def _register_namespace(self, joined: str) -> None:
        """One marker atom per distinct namespace, so listing is O(namespaces)."""
        if self._mem.fetch(
            self._region, NS_KIND, payload_filter={"ns": joined}, limit=1
        ):
            return
        self._mem.remember(
            self._region, {"kind": NS_KIND, "text": joined or "/", "payload": {"ns": joined}}
        )

    def _retire_namespace(self, joined: str) -> None:
        """Drop the marker once the namespace holds no keys."""
        if self._namespace_has_keys(joined):
            return
        stale = self._mem.fetch(
            self._region, NS_KIND, payload_filter={"ns": joined}, limit=8
        )
        if stale:
            self._mem.forget(self._region, [h.id for h in stale])

    def _write(
        self,
        namespace: tuple[str, ...],
        key: str,
        value: dict[str, Any],
        ttl: float | None,
        created_at: int,
        updated_at: int,
    ) -> None:
        joined = _join(namespace)
        atom: dict[str, Any] = {
            "kind": KIND,
            # The value is the searchable text; keys and namespaces are not content.
            "text": " ".join(str(v) for v in value.values()) or json.dumps(value),
            "payload": {
                "ns": joined,
                # Prefix erasure and prefix search both filter on this.
                "anc": _ancestors(namespace),
                "key": key,
                "value": value,
                "created_at": created_at,
                "updated_at": updated_at,
                # Kept so a read can re-apply the same lifetime on refresh.
                "ttl": ttl,
            },
        }
        if ttl is not None:
            atom["expires_at"] = int(time.time() * 1_000_000 + ttl * 60_000_000)
        self._mem.remember(self._region, atom)
        self._register_namespace(joined)

    def _refresh_ttl(self, hit) -> None:
        """Re-apply the stored lifetime. Expiry cannot be moved in place, so the atom is
        rewritten; both timestamps are carried over because a read must not look like a write.
        """
        p = hit.payload
        if p.get("ttl") is None:
            return
        self._mem.forget(self._region, [hit.id])
        self._write(
            _split(p["ns"]),
            p["key"],
            p["value"],
            p["ttl"],
            p["created_at"],
            p["updated_at"],
        )

    # ---- projection -------------------------------------------------------

    @staticmethod
    def _item(hit) -> Item:
        p = hit.payload
        return Item(
            value=p["value"],
            key=p["key"],
            namespace=_split(p["ns"]),
            created_at=_when(p.get("created_at")),
            updated_at=_when(p.get("updated_at")),
        )

    @staticmethod
    def _search_item(hit) -> SearchItem:
        p = hit.payload
        return SearchItem(
            namespace=_split(p["ns"]),
            key=p["key"],
            value=p["value"],
            created_at=_when(p.get("created_at")),
            updated_at=_when(p.get("updated_at")),
            score=getattr(hit, "score", None),
        )

    @staticmethod
    def _matches(ns: tuple[str, ...], conditions) -> bool:
        for c in conditions or ():
            path = tuple(c.path)
            if c.match_type == "prefix":
                seg = ns[: len(path)]
            else:
                seg = ns[-len(path) :] if len(path) <= len(ns) else ns
            if len(seg) != len(path):
                return False
            # "*" matches any single element.
            if any(p != "*" and p != s for p, s in zip(path, seg)):
                return False
        return True

    def _under(self, ns: str, prefix: str) -> bool:
        return not prefix or ns == prefix or ns.startswith(prefix + _SEP)

    # ---- the abstract surface --------------------------------------------

    def batch(self, ops: Iterable[Op]) -> list[Any]:
        results: list[Any] = []
        for op in ops:
            if isinstance(op, GetOp):
                hit = self._find(op.namespace, op.key)
                if hit and op.refresh_ttl:
                    self._refresh_ttl(hit)
                results.append(self._item(hit) if hit else None)

            elif isinstance(op, PutOp):
                existing = self._find(op.namespace, op.key)
                joined = _join(op.namespace)
                if op.value is None:
                    if existing:
                        self._mem.forget(self._region, [existing.id])
                        self._retire_namespace(joined)
                    results.append(None)
                    continue
                now = _now()
                created = existing.payload["created_at"] if existing else now
                if existing:
                    self._mem.forget(self._region, [existing.id])
                self._write(op.namespace, op.key, op.value, op.ttl, created, now)
                results.append(None)

            elif isinstance(op, SearchOp):
                prefix = _join(op.namespace_prefix)
                want = op.limit + op.offset
                if op.query:
                    # Recall ranks the whole region, so the namespace pass happens below;
                    # over-fetch because that pass and the filter both discard rows.
                    hits = self._mem.recall(
                        self._region, text=op.query, k=max(want * 4, 32), kinds=[KIND]
                    )
                else:
                    # No query: the index can do the namespace restriction directly.
                    hits = self._mem.fetch(
                        self._region,
                        KIND,
                        payload_filter={"anc": [prefix]} if prefix else None,
                        limit=max(want * 4, 32),
                    )
                out: list[SearchItem] = []
                for h in hits:
                    p = h.payload
                    if not self._under(p["ns"], prefix):
                        continue
                    if op.filter and any(
                        p["value"].get(k) != v for k, v in op.filter.items()
                    ):
                        continue
                    if op.refresh_ttl:
                        self._refresh_ttl(h)
                    out.append(self._search_item(h))
                results.append(out[op.offset : op.offset + op.limit])

            elif isinstance(op, ListNamespacesOp):
                seen: set[tuple[str, ...]] = set()
                # Marker atoms only, so this is O(namespaces) rather than O(keys). Each is
                # verified against a live key, so a crash mid-delete self-heals on read.
                for marker in self._mem.fetch(self._region, NS_KIND, limit=100_000):
                    joined = marker.payload["ns"]
                    if not self._namespace_has_keys(joined):
                        continue
                    ns = _split(joined)
                    if not self._matches(ns, op.match_conditions):
                        continue
                    seen.add(ns[: op.max_depth] if op.max_depth is not None else ns)
                ordered = sorted(seen)
                results.append(ordered[op.offset : op.offset + op.limit])

            else:
                raise NotImplementedError(f"unsupported op: {type(op).__name__}")
        return results

    async def abatch(self, ops: Iterable[Op]) -> list[Any]:
        # The bindings are sync; a worker thread keeps the event loop free.
        return await asyncio.to_thread(self.batch, list(ops))

    # ---- beyond BaseStore -------------------------------------------------

    def forget_namespace(self, namespace: tuple[str, ...], *, prefix: bool = True) -> int:
        """Destroy every key under `namespace`, returning the number of atoms erased.

        The point of a per-user namespace: one call makes that user's memories unreadable
        rather than merely unlisted. Selection rides the payload index in both modes.
        """
        joined = _join(namespace)
        # Containment on the ancestor array selects the whole subtree; on `ns` it selects
        # exactly one namespace. Either way the index does the work.
        criterion = {"anc": [joined]} if prefix else {"ns": joined}
        doomed = self._mem.fetch(
            self._region, KIND, payload_filter=criterion, limit=100_000
        )
        if not doomed:
            return 0
        erased = self._mem.forget(self._region, [h.id for h in doomed]).erased_count
        for gone in {h.payload["ns"] for h in doomed}:
            self._retire_namespace(gone)
        return erased
