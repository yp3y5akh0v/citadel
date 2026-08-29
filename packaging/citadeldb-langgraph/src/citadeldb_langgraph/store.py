"""LangGraph BaseStore over an encrypted Citadel region."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Iterable
from datetime import datetime, timezone
from operator import index
from typing import Any

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
    get_text_at_path,
)

KIND = "kv"
PAGE = 10_000
_METRICS = {"cosine", "cos", "l2", "euclidean", "ip", "inner", "inner_product", "dot"}
_NAMESPACE_LOCKS = tuple(threading.RLock() for _ in range(256))


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


def _page(mem: Any, region: str, kind: str, criterion: dict | None = None) -> list:
    """Page to the end: one fetch is bounded, and a partial erase must not look whole."""
    out: list = []
    after = None
    while True:
        got = mem.fetch(
            region, kind, payload_filter=criterion, limit=PAGE, after_id=after
        )
        out.extend(got)
        if len(got) < PAGE:
            return out
        after = got[-1].id


_NS_ENCODING = "n1:"


def _join(namespace: tuple[str, ...]) -> str:
    if not namespace:
        return ""
    return _NS_ENCODING + "".join(f"{len(label)}:{label}" for label in namespace)


def _split(joined: str) -> tuple[str, ...]:
    if not joined:
        return ()
    if not joined.startswith(_NS_ENCODING):
        raise ValueError("stored namespace uses an unsupported encoding")

    labels: list[str] = []
    cursor = len(_NS_ENCODING)
    while cursor < len(joined):
        colon = joined.find(":", cursor)
        if colon < 0 or not joined[cursor:colon].isdigit():
            raise ValueError("stored namespace is malformed")
        length = int(joined[cursor:colon])
        cursor = colon + 1
        end = cursor + length
        if end > len(joined):
            raise ValueError("stored namespace is truncated")
        labels.append(joined[cursor:end])
        cursor = end
    return tuple(labels)


def _ancestors(namespace: tuple[str, ...]) -> list[str]:
    """Every prefix of `namespace`, for exact subtree filtering."""
    return [_join(namespace[: i + 1]) for i in range(len(namespace))]


def _key_tag(namespace: tuple[str, ...], key: str) -> str:
    """The engine's unambiguous name for one store key."""
    joined = _join(namespace)
    return f"{len(joined)}:{joined}{key}"


def _namespace_lock(region: str, joined: str) -> threading.RLock:
    return _NAMESPACE_LOCKS[hash((region, joined)) % len(_NAMESPACE_LOCKS)]


def _now() -> int:
    return int(time.time() * 1_000_000)


def _indexed_text(value: dict[str, Any], fields: Any) -> tuple[str, bool]:
    if fields is False:
        return "", False
    if fields is True or (fields is not None and not isinstance(fields, list)):
        raise TypeError("index must be None, False, or a list of field paths")
    if isinstance(fields, list) and not all(isinstance(path, str) for path in fields):
        raise TypeError("index field paths must be strings")
    paths = ["$"] if fields is None else fields
    texts = [text for path in paths for text in get_text_at_path(value, path)]
    return "\n".join(texts), bool(texts)


def _when(micros: int | None) -> datetime | None:
    return (
        datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc) if micros else None
    )


# A Database is pinned to its opening thread, so workers take Memory, not self.


def _find(mem: Any, region: str, namespace: tuple[str, ...], key: str):
    """The atom holding one `(namespace, key)` pair."""
    hits = mem.fetch(
        region, KIND, payload_filter={"ns": _join(namespace), "key": key}, limit=1
    )
    return hits[0] if hits else None


def _write(
    mem: Any,
    region: str,
    namespace: tuple[str, ...],
    key: str,
    value: dict[str, Any],
    ttl: float | None,
    created_at: int,
    updated_at: int,
    index_fields: Any,
    dim: int,
) -> int:
    joined = _join(namespace)
    text, indexed = _indexed_text(value, index_fields)
    atom: dict[str, Any] = {
        "kind": KIND,
        "text": text,
        "payload": {
            "ns": joined,
            # Prefix erasure and prefix search both filter on this.
            "anc": _ancestors(namespace),
            "key": key,
            "value": value,
            "indexed": indexed,
            "index": index_fields,
            "created_at": created_at,
            "updated_at": updated_at,
            # Kept so a read can re-apply the same lifetime on refresh.
            "ttl": ttl,
        },
    }
    if ttl is not None:
        atom["expires_at"] = int(time.time() * 1_000_000 + ttl * 60_000_000)
    if not indexed:
        atom["embedding"] = [1.0] + [0.0] * (dim - 1)
    with _namespace_lock(region, joined):
        return mem.remember_replacing_keyed(region, atom, _key_tag(namespace, key))


def _refresh_ttl(mem: Any, region: str, hit, dim: int):
    p = hit.payload
    if p.get("ttl") is None:
        return hit
    namespace = _split(p["ns"])
    with _namespace_lock(region, p["ns"]):
        current = _find(mem, region, namespace, p["key"])
        if current is None or current.payload.get("ttl") is None:
            return current
        p = current.payload
        _write(
            mem,
            region,
            namespace,
            p["key"],
            p["value"],
            p["ttl"],
            p["created_at"],
            p["updated_at"],
            p.get("index"),
            dim,
        )
        return current


def _item(hit) -> Item:
    p = hit.payload
    return Item(
        value=p["value"],
        key=p["key"],
        namespace=_split(p["ns"]),
        created_at=_when(p.get("created_at")),
        updated_at=_when(p.get("updated_at")),
    )


def _search_item(hit, *, scored: bool) -> SearchItem:
    p = hit.payload
    return SearchItem(
        namespace=_split(p["ns"]),
        key=p["key"],
        value=p["value"],
        created_at=_when(p.get("created_at")),
        updated_at=_when(p.get("updated_at")),
        score=getattr(hit, "score", None) if scored else None,
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
    # Complete length-prefixed labels compose, so an encoded tuple prefix is a
    # string prefix without matching part of a label.
    return not prefix or ns.startswith(prefix)


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


def _scan(
    mem: Any,
    region: str,
    prefix: str,
    filters: dict | None,
    want: int,
    *,
    indexed: bool | None = None,
) -> list:
    """Page until `want` rows pass: one bounded fetch can hold no match at all."""
    criterion: dict[str, Any] = {}
    if prefix:
        criterion["anc"] = [prefix]
    if indexed is not None:
        criterion["indexed"] = indexed
    out: list = []
    after = None
    chunk = min(max(want * 4, 32), PAGE)
    while len(out) < want:
        got = mem.fetch(
            region,
            KIND,
            payload_filter=criterion or None,
            limit=chunk,
            after_id=after,
        )
        out.extend(h for h in got if _passes(h, prefix, filters))
        if len(got) < chunk:
            break
        after = got[-1].id
    return out[:want]


def _batch(mem: Any, region: str, dim: int, ops: Iterable[Op]) -> list[Any]:
    results: list[Any] = []
    for op in ops:
        if isinstance(op, GetOp):
            hit = _find(mem, region, op.namespace, op.key)
            if hit and op.refresh_ttl:
                hit = _refresh_ttl(mem, region, hit, dim)
            results.append(_item(hit) if hit else None)

        elif isinstance(op, PutOp):
            joined = _join(op.namespace)
            with _namespace_lock(region, joined):
                if op.value is None:
                    stale = _page(mem, region, KIND, {"ns": joined, "key": op.key})
                    if stale:
                        mem.forget(region, [h.id for h in stale])
                    results.append(None)
                    continue
                now = _now()
                prior = _page(mem, region, KIND, {"ns": joined, "key": op.key})
                created = min((h.payload["created_at"] for h in prior), default=now)
                written = _write(
                    mem,
                    region,
                    op.namespace,
                    op.key,
                    op.value,
                    op.ttl,
                    created,
                    now,
                    op.index,
                    dim,
                )
                legacy = [h.id for h in prior if h.id != written]
                if legacy:
                    mem.forget(region, legacy)
            results.append(None)

        elif isinstance(op, SearchOp):
            prefix = _join(op.namespace_prefix)
            want = op.limit + op.offset
            scored_ids: set[int] = set()
            if op.query:
                # Ranked, so the order is the answer and paging cannot recover a
                # match the ranking dropped. The prefix rides the scan; the value
                # filter cannot, so widen until `want` rows survive it or the
                # region runs out, rather than answering short.
                criterion: dict[str, Any] = {"indexed": True}
                if prefix:
                    criterion["anc"] = [prefix]
                options = citadeldb.RecallOptions(payload_filter=criterion)
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
                scored_ids = {h.id for h in hits}
                if len(hits) < want:
                    hits.extend(
                        _scan(
                            mem,
                            region,
                            prefix,
                            op.filter,
                            want - len(hits),
                            indexed=False,
                        )
                    )
            else:
                # Unranked, so page instead: filtering one bounded fetch would return
                # empty whenever the matches sit past the rows it happened to read.
                hits = _scan(mem, region, prefix, op.filter, want)
            page = hits[op.offset : op.offset + op.limit]
            if op.refresh_ttl:
                for h in page:
                    _refresh_ttl(mem, region, h, dim)
            results.append([_search_item(h, scored=h.id in scored_ids) for h in page])

        elif isinstance(op, ListNamespacesOp):
            live = {h.payload["ns"] for h in _page(mem, region, KIND)}

            seen: set[tuple[str, ...]] = set()
            for joined in live:
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
    criterion = (
        None
        if prefix and not joined
        else ({"anc": [joined]} if prefix else {"ns": joined})
    )
    doomed = _page(mem, region, KIND, criterion)
    if not doomed:
        return 0
    erased = mem.forget(region, [h.id for h in doomed]).erased_count
    return erased


class CitadelStore(BaseStore):
    """LangGraph store over one encrypted Citadel region."""

    supports_ttl = True
    ttl_config = TTLConfig(refresh_on_read=True)

    def __init__(
        self,
        path: str,
        key: str,
        *,
        embedder: Any,
        region: str = "store",
    ) -> None:
        if not key:
            raise ValueError("a passphrase is required: stored state is the payload")
        embedder = _require_embedder(embedder)
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
        self._dim = int(embedder.dim)
        # Idempotent for a region of the same width, so a dim clash raises here.
        self._mem.create_encrypted_region(region, embedder)

    def batch(self, ops: Iterable[Op]) -> list[Any]:
        return _batch(self._mem, self._region, self._dim, ops)

    async def abatch(self, ops: Iterable[Op]) -> list[Any]:
        # The bindings are sync, so a worker thread keeps the event loop free.
        return await asyncio.to_thread(
            _batch, self._mem, self._region, self._dim, list(ops)
        )

    def forget_namespace(
        self, namespace: tuple[str, ...], *, prefix: bool = True
    ) -> int:
        """Destroy every key under `namespace`, returning atoms erased."""
        return _forget_namespace(self._mem, self._region, namespace, prefix)
