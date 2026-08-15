"""LangGraph BaseStore over an encrypted Citadel region."""
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
PAGE = 10_000


def _page(mem: Any, region: str, kind: str, criterion: dict | None = None) -> list:
    """Page to the end: one fetch is bounded, and a partial erase must not look whole."""
    out: list = []
    after = None
    while True:
        got = mem.fetch(region, kind, payload_filter=criterion, limit=PAGE, after_id=after)
        out.extend(got)
        if len(got) < PAGE:
            return out
        after = got[-1].id


_SEP = "\x1f"  # illegal in a namespace element, so joins stay reversible


def _join(namespace: tuple[str, ...]) -> str:
    return _SEP.join(namespace)


def _split(joined: str) -> tuple[str, ...]:
    return tuple(joined.split(_SEP)) if joined else ()


def _ancestors(namespace: tuple[str, ...]) -> list[str]:
    """Every prefix of `namespace`; containment then makes it indexed."""
    return [_join(namespace[: i + 1]) for i in range(len(namespace))]


def _key_tag(namespace: tuple[str, ...], key: str) -> str:
    """The engine's name for one store key. Length-prefixed: `_SEP` is illegal in
    a namespace element but not in a key, so a plain join would let one pair
    spell another's."""
    joined = _join(namespace)
    return f"{len(joined)}{_SEP}{joined}{key}"


def _now() -> int:
    return int(time.time() * 1_000_000)


def _when(micros: int | None) -> datetime | None:
    return datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc) if micros else None


# A Database is pinned to its opening thread, so workers take Memory, not self.


def _find(mem: Any, region: str, namespace: tuple[str, ...], key: str):
    """The atom holding (namespace, key). Served by the payload GIN index."""
    hits = mem.fetch(
        region, KIND, payload_filter={"ns": _join(namespace), "key": key}, limit=1
    )
    return hits[0] if hits else None


def _namespace_has_keys(mem: Any, region: str, joined: str) -> bool:
    return bool(mem.fetch(region, KIND, payload_filter={"ns": joined}, limit=1))


def _register_namespace(mem: Any, region: str, joined: str) -> None:
    """One marker atom per distinct namespace, so listing is O(namespaces)."""
    if mem.fetch(region, NS_KIND, payload_filter={"ns": joined}, limit=1):
        return
    mem.remember(
        region, {"kind": NS_KIND, "text": joined or "/", "payload": {"ns": joined}}
    )


def _retire_namespace(mem: Any, region: str, joined: str) -> None:
    """Drop the marker once the namespace holds no keys."""
    if _namespace_has_keys(mem, region, joined):
        return
    stale = mem.fetch(region, NS_KIND, payload_filter={"ns": joined}, limit=8)
    if stale:
        mem.forget(region, [h.id for h in stale])


def _write(
    mem: Any,
    region: str,
    namespace: tuple[str, ...],
    key: str,
    value: dict[str, Any],
    ttl: float | None,
    created_at: int,
    updated_at: int,
) -> int:
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
    # Keyed, so the write supersedes any stored version in one transaction. A
    # (namespace, key) is unique in BaseStore, and two writers to one key would
    # otherwise both find it absent and both insert.
    written = mem.remember_replacing_keyed(region, atom, _key_tag(namespace, key))
    _register_namespace(mem, region, joined)
    return written


def _refresh_ttl(mem: Any, region: str, hit) -> None:
    """Expiry cannot be moved in place, so the atom is rewritten in full."""
    p = hit.payload
    if p.get("ttl") is None:
        return
    _write(
        mem, region, _split(p["ns"]), p["key"], p["value"], p["ttl"],
        p["created_at"], p["updated_at"],
    )


def _item(hit) -> Item:
    p = hit.payload
    return Item(
        value=p["value"],
        key=p["key"],
        namespace=_split(p["ns"]),
        created_at=_when(p.get("created_at")),
        updated_at=_when(p.get("updated_at")),
    )


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


def _matches(ns: tuple[str, ...], conditions) -> bool:
    for c in conditions or ():
        path = tuple(c.path)
        if c.match_type == "prefix":
            seg = ns[: len(path)]
        else:
            seg = ns[-len(path) :] if len(path) <= len(ns) else ns
        if len(seg) != len(path):
            return False
        if any(p != "*" and p != s for p, s in zip(path, seg)):
            return False
    return True


def _under(ns: str, prefix: str) -> bool:
    return not prefix or ns == prefix or ns.startswith(prefix + _SEP)


_OPERATORS = {
    "$eq": lambda v, o: v == o,
    "$ne": lambda v, o: v != o,
    "$gt": lambda v, o: float(v) > float(o),
    "$gte": lambda v, o: float(v) >= float(o),
    "$lt": lambda v, o: float(v) < float(o),
    "$lte": lambda v, o: float(v) <= float(o),
}


def _compare(value: Any, wanted: Any) -> bool:
    """One filter value against one stored value, as BaseStore documents it.

    A dict of `$`-prefixed keys is a set of operators over the value; any other
    dict is a nested match; a list matches element-wise at the same length.
    Comparing with `==` throughout, as this did, makes every operator form a
    dict compared against a value and so match nothing at all.
    """
    if isinstance(wanted, dict):
        if any(k.startswith("$") for k in wanted):
            return all(_apply(value, op, arg) for op, arg in wanted.items())
        if not isinstance(value, dict):
            return False
        return all(_compare(value.get(k), v) for k, v in wanted.items())
    if isinstance(wanted, (list, tuple)):
        return (
            isinstance(value, (list, tuple))
            and len(value) == len(wanted)
            and all(_compare(v, w) for v, w in zip(value, wanted))
        )
    return value == wanted


def _apply(value: Any, operator: str, operand: Any) -> bool:
    try:
        compare = _OPERATORS[operator]
    except KeyError:
        raise ValueError(f"Unsupported operator: {operator}") from None
    try:
        return compare(value, operand)
    except (TypeError, ValueError):
        # The ordering operators coerce to float, and a missing or non-numeric
        # value simply does not satisfy them.
        return False


def _passes(hit, prefix: str, filters: dict | None) -> bool:
    """The namespace restriction plus the op's value filter."""
    p = hit.payload
    if not _under(p["ns"], prefix):
        return False
    return not filters or all(
        _compare(p["value"].get(k), v) for k, v in filters.items()
    )


def _scan(mem: Any, region: str, prefix: str, filters: dict | None, want: int) -> list:
    """Page until `want` rows pass: one bounded fetch can hold no match at all."""
    criterion = {"anc": [prefix]} if prefix else None
    out: list = []
    after = None
    chunk = min(max(want * 4, 32), PAGE)
    while len(out) < want:
        got = mem.fetch(
            region, KIND, payload_filter=criterion, limit=chunk, after_id=after
        )
        out.extend(h for h in got if _passes(h, prefix, filters))
        if len(got) < chunk:
            break
        after = got[-1].id
    return out[:want]


def _batch(mem: Any, region: str, ops: Iterable[Op]) -> list[Any]:
    results: list[Any] = []
    for op in ops:
        if isinstance(op, GetOp):
            hit = _find(mem, region, op.namespace, op.key)
            if hit and op.refresh_ttl:
                _refresh_ttl(mem, region, hit)
            results.append(_item(hit) if hit else None)

        elif isinstance(op, PutOp):
            joined = _join(op.namespace)
            if op.value is None:
                # Every row for the key, not the one a bounded read saw: a store
                # predating the keyed write may hold more than one.
                stale = _page(mem, region, KIND,
                              {"ns": joined, "key": op.key})
                if stale:
                    mem.forget(region, [h.id for h in stale])
                    _retire_namespace(mem, region, joined)
                results.append(None)
                continue
            now = _now()
            prior = _page(mem, region, KIND, {"ns": joined, "key": op.key})
            # The only racy read left, and it carries no data: a lost race
            # refreshes created_at, where before it duplicated the key.
            created = min((h.payload["created_at"] for h in prior), default=now)
            written = _write(
                mem, region, op.namespace, op.key, op.value, op.ttl, created, now
            )
            # Rows a store written before keyed writes still holds: the key names
            # none of them, so the replace above superseded none of them either.
            legacy = [h.id for h in prior if h.id != written]
            if legacy:
                mem.forget(region, legacy)
            results.append(None)

        elif isinstance(op, SearchOp):
            prefix = _join(op.namespace_prefix)
            want = op.limit + op.offset
            if op.query:
                # Ranked, so the order is the answer and paging cannot recover a
                # match the ranking dropped. The prefix rides the scan; the value
                # filter cannot, so widen until `want` rows survive it or the
                # region runs out, rather than answering short.
                options = (
                    citadeldb.RecallOptions(payload_filter={"anc": [prefix]})
                    if prefix
                    else None
                )
                k = max(want, 32)
                while True:
                    ranked = mem.recall(
                        region, text=op.query, k=k, kinds=[KIND], options=options
                    )
                    hits = [h for h in ranked if _passes(h, prefix, op.filter)]
                    if len(hits) >= want or len(ranked) < k:
                        break
                    k *= 2
                hits = hits[:want]
            else:
                # Unranked, so page instead: filtering one bounded fetch would return
                # empty whenever the matches sit past the rows it happened to read.
                hits = _scan(mem, region, prefix, op.filter, want)
            if op.refresh_ttl:
                for h in hits:
                    _refresh_ttl(mem, region, h)
            results.append([_search_item(h) for h in hits][op.offset :])

        elif isinstance(op, ListNamespacesOp):
            seen: set[tuple[str, ...]] = set()
            # A marker is checked against a live key, so a stale one self-heals.
            for marker in _page(mem, region, NS_KIND):
                joined = marker.payload["ns"]
                if not _namespace_has_keys(mem, region, joined):
                    continue
                ns = _split(joined)
                if not _matches(ns, op.match_conditions):
                    continue
                seen.add(ns[: op.max_depth] if op.max_depth is not None else ns)
            ordered = sorted(seen)
            results.append(ordered[op.offset : op.offset + op.limit])

        else:
            raise NotImplementedError(f"unsupported op: {type(op).__name__}")
    return results


def _forget_namespace(
    mem: Any, region: str, namespace: tuple[str, ...], prefix: bool
) -> int:
    joined = _join(namespace)
    # Ancestor containment selects the subtree; `ns` selects exactly one.
    criterion = {"anc": [joined]} if prefix else {"ns": joined}
    doomed = _page(mem, region, KIND, criterion)
    if not doomed:
        return 0
    erased = mem.forget(region, [h.id for h in doomed]).erased_count
    for gone in {h.payload["ns"] for h in doomed}:
        _retire_namespace(mem, region, gone)
    return erased


class CitadelStore(BaseStore):
    """LangGraph store over one encrypted Citadel region."""

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
        if not key:
            raise ValueError("a passphrase is required: stored state is the payload")
        try:
            self._db = citadeldb.connect(path, key=key, region_keys=True)
        except citadeldb.OperationalError as e:
            if "locked" not in str(e):
                raise
            raise RuntimeError(
                f"{path} is open in another process. Citadel is embedded, so one process "
                f"owns the file; separate concerns with namespaces rather than with a "
                f"second database."
            ) from e
        self._mem = self._db.memory()
        self._region = region
        # Idempotent for a region of the same width, so a dim clash raises here.
        self._mem.create_encrypted_region(
            region, embedder or citadeldb.MockEmbedder(dim=64)
        )

    # ---- the abstract surface --------------------------------------------

    def batch(self, ops: Iterable[Op]) -> list[Any]:
        return _batch(self._mem, self._region, ops)

    async def abatch(self, ops: Iterable[Op]) -> list[Any]:
        # The bindings are sync, so a worker thread keeps the event loop free.
        return await asyncio.to_thread(_batch, self._mem, self._region, list(ops))

    # ---- beyond BaseStore -------------------------------------------------

    def forget_namespace(self, namespace: tuple[str, ...], *, prefix: bool = True) -> int:
        """Destroy every key under `namespace`, returning atoms erased."""
        return _forget_namespace(self._mem, self._region, namespace, prefix)
