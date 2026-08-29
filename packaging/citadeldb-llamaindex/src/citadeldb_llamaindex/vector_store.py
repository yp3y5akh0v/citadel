"""LlamaIndex vector store over an encrypted Citadel region."""

from __future__ import annotations

import asyncio
from operator import index
from typing import Any

import citadeldb
from llama_index.core.schema import BaseNode, MetadataMode
from llama_index.core.vector_stores.types import (
    BasePydanticVectorStore,
    FilterCondition,
    FilterOperator,
    MetadataFilters,
    VectorStoreQuery,
    VectorStoreQueryMode,
    VectorStoreQueryResult,
)
from llama_index.core.vector_stores.utils import (
    build_metadata_filter_fn,
    metadata_dict_to_node,
    node_to_metadata_dict,
)
from pydantic import PrivateAttr

KIND = "node"
DEFAULT_PATH = "llamaindex.cdl"
DEFAULT_REGION = "nodes"
# OpenAI text-embedding-ada-002 / 3-small, LlamaIndex's default width.
DEFAULT_DIM = 1536
# The region is read whole where containment cannot express the predicate.
PAGE = 10_000

# Recall fuses vector, keyword and recency, so hybrid needs no extra call.
_SUPPORTED_MODES = (VectorStoreQueryMode.DEFAULT, VectorStoreQueryMode.HYBRID)


# A Database is pinned to its opening thread, so workers take Memory, not self.


def _model_id(embed_model: Any, override: str | None) -> str:
    values = (
        (override,)
        if override is not None
        else tuple(
            getattr(embed_model, attr, None)
            for attr in ("model_id", "model_name", "model")
        )
    )
    for value in values:
        name = value
        if isinstance(name, str):
            name = name.strip()
            if name and name.lower() not in {"unknown", "default"}:
                return name
    if override is not None:
        raise ValueError(
            "model_id must be a nonblank string other than 'unknown' or 'default'"
        )
    raise ValueError(
        "model_id is required when the LlamaIndex model does not expose a specific "
        "model_id, model_name, or model"
    )


def _embedding_dim(value: Any) -> int:
    try:
        dim = index(value)
    except TypeError as error:
        raise ValueError(
            "dim must be a positive integer no greater than 65535"
        ) from error
    if isinstance(value, bool) or not 1 <= dim <= 65_535:
        raise ValueError("dim must be a positive integer no greater than 65535")
    return dim


class _LlamaIndexEmbedder:
    """Expose one LlamaIndex model through Citadel's embedder protocol."""

    metric = "cosine"

    def __init__(self, embed_model: Any, dim: int, model_id: str) -> None:
        self._embed_model = embed_model
        self.dim = dim
        self.model_id = model_id

    def embed(self, texts: list[str]) -> list[list[float]]:
        return self._embed_model.get_text_embedding_batch(texts)

    def embed_queries(self, texts: list[str]) -> list[list[float]]:
        return [self._embed_model.get_query_embedding(text) for text in texts]


def _pushdown(filters: MetadataFilters | None) -> dict[str, Any] | None:
    """Equality leaves of a top-level AND; this narrows and never decides.

    Only string leaves are pushed. Containment compares JSON types exactly where
    EQ is Python `==`, which holds `1 == 1.0 == True`, so pushing a number or a
    bool would drop rows `_keep` would have kept - deciding, not narrowing.
    Equal strings are the one case where the two agree.
    """
    if filters is None or not filters.filters:
        return None
    if filters.condition != FilterCondition.AND:
        return None
    eq: dict[str, Any] = {}
    for f in filters.filters:
        if isinstance(f, MetadataFilters):
            return None  # nested filters are refused downstream; do not guess here
        if f.operator == FilterOperator.EQ and isinstance(f.value, str):
            eq[f.key] = f.value
    return {"meta": eq} if eq else None


def _keep(hits: list[Any], filters: MetadataFilters | None) -> list[Any]:
    """Hits whose metadata satisfies `filters`."""
    if filters is None or not filters.filters:
        return hits
    by_id = {h.payload["nid"]: h.payload.get("meta", {}) for h in hits}
    if filters.condition == FilterCondition.NOT:
        # build_metadata_filter_fn first appears in 0.13.1, but that release's
        # evaluator does not yet handle NOT. Match the later reference semantics:
        # a NOT group survives only when none of its leaves match.
        positive = MetadataFilters(
            filters=filters.filters,
            condition=FilterCondition.OR,
        )
        matches = build_metadata_filter_fn(lambda nid: by_id.get(nid, {}), positive)
        return [h for h in hits if not matches(h.payload["nid"])]
    matches = build_metadata_filter_fn(lambda nid: by_id.get(nid, {}), filters)
    return [h for h in hits if matches(h.payload["nid"])]


def _ranked(
    mem: Any,
    region: str,
    *,
    want: int,
    surviving: Any,
    **recall: Any,
) -> list[Any]:
    """Ranked hits that pass `surviving`, widening until `want` of them or the
    region runs out.

    The engine answers `k` rows matching what was pushed to it; a predicate
    containment cannot express is settled out here, and a fixed `k` would answer
    short whenever the survivors rank below it. Fewer hits than asked for means
    the region is exhausted, so the loop ends on the data rather than a constant.
    Widening only appends: recall is ranked, so a larger `k` returns a superset
    in the same order.
    """
    k = max(want, 32)
    while True:
        hits = mem.recall(region, k=k, **recall)
        kept = surviving(hits)
        if len(kept) >= want or len(hits) < k:
            return kept[:want]
        k *= 2


def _node_of(hit: Any) -> BaseNode:
    """Rebuild the node; the text is restored from the atom."""
    return metadata_dict_to_node(hit.payload["meta"], text=hit.text)


def _fetch(mem: Any, region: str, criterion: dict[str, Any] | None) -> list[Any]:
    """Page to the end: one fetch is bounded, and a partial erase must not look whole."""
    out: list[Any] = []
    after = None
    while True:
        page = mem.fetch(
            region, KIND, payload_filter=criterion, limit=PAGE, after_id=after
        )
        out.extend(page)
        if len(page) < PAGE:
            return out
        after = page[-1].id


def _add(mem: Any, region: str, dim: int, nodes: list[BaseNode]) -> list[str]:
    atoms: dict[str, dict[str, Any]] = {}
    ids: list[str] = []
    for node in nodes:
        if node.embedding is not None and len(node.embedding) != dim:
            raise ValueError(
                f"node {node.node_id} has a {len(node.embedding)}-dimension embedding "
                f"but this region is {dim}. Build the store with "
                f"dim={len(node.embedding)} to match your embedding model."
            )
        # An id repeated in one call keeps its last value, like the reference.
        atom = {
            "kind": KIND,
            # What recall matches on, and the text the node is rebuilt with.
            "text": node.get_content(metadata_mode=MetadataMode.NONE) or "",
            "payload": {
                "nid": node.node_id,
                "ref": node.ref_doc_id or "",
                # remove_text: the text would otherwise be stored twice.
                "meta": node_to_metadata_dict(node, remove_text=True),
            },
        }
        if node.embedding is not None:
            # Storing the framework's vector keeps one vector space.
            atom["embedding"] = list(node.embedding)
        atoms[node.node_id] = atom
        ids.append(node.node_id)
    if not atoms:
        return ids
    # Re-adding a stored id replaces it, as the reference store's dict does.
    # Keyed on the node id and committed as one transaction, so two indexers
    # adding the same node supersede rather than each leaving a row behind.
    mem.remember_replacing_keyed_batch(
        region, [(atom, nid) for nid, atom in atoms.items()]
    )
    # One id per input node, duplicates included.
    return ids


def _erase(mem: Any, region: str, hits: list[Any]) -> int:
    if not hits:
        return 0
    return mem.forget(region, [h.id for h in hits]).erased_count


def _delete_ref(mem: Any, region: str, ref_doc_id: str) -> int:
    return _erase(mem, region, _fetch(mem, region, {"ref": ref_doc_id}))


def _selected(
    mem: Any,
    region: str,
    node_ids: list[str] | None,
    filters: MetadataFilters | None,
) -> list[Any]:
    """Atoms named by `node_ids`, narrowed by `filters`. Both may be absent."""
    if node_ids is not None:
        wanted = set(node_ids)
        if not wanted:
            return []
        # Encrypted payloads cannot use the plaintext JSON index. Scan and
        # decrypt the region once rather than once per requested id.
        hits = [
            h
            for h in _fetch(mem, region, _pushdown(filters))
            if h.payload.get("nid") in wanted
        ]
    else:
        hits = _fetch(mem, region, _pushdown(filters))
    return _keep(hits, filters)


def _get_nodes(
    mem: Any, region: str, node_ids: list[str] | None, filters: MetadataFilters | None
) -> list[BaseNode]:
    return [_node_of(h) for h in _selected(mem, region, node_ids, filters)]


def _delete_nodes(
    mem: Any, region: str, node_ids: list[str] | None, filters: MetadataFilters | None
) -> int:
    return _erase(mem, region, _selected(mem, region, node_ids, filters))


def _clear(mem: Any, region: str) -> int:
    return _erase(mem, region, _fetch(mem, region, None))


def _query(mem: Any, region: str, query: VectorStoreQuery) -> VectorStoreQueryResult:
    if query.mode not in _SUPPORTED_MODES:
        raise NotImplementedError(
            f"query mode {query.mode} is not supported; Citadel serves "
            f"{' and '.join(m.value for m in _SUPPORTED_MODES)}"
        )
    if query.query_embedding is None:
        raise ValueError(
            "query_embedding is required by LlamaIndex's vector-store query contract; "
            "it must come from the index's embed_model"
        )
    top_k = max(query.similarity_top_k, 0)
    if top_k == 0:
        return VectorStoreQueryResult(nodes=[], similarities=[], ids=[])

    criterion = _pushdown(query.filters)
    options = citadeldb.RecallOptions(payload_filter=criterion) if criterion else None

    def surviving(hits: list[Any]) -> list[Any]:
        hits = _keep(hits, query.filters)
        if query.node_ids:
            allowed = set(query.node_ids)
            hits = [h for h in hits if h.payload["nid"] in allowed]
        if query.doc_ids:
            allowed = set(query.doc_ids)
            hits = [h for h in hits if h.payload.get("ref") in allowed]
        return hits

    recall: dict[str, Any] = {
        "embedding": query.query_embedding,
        "kinds": [KIND],
        "options": options,
    }
    if query.mode == VectorStoreQueryMode.HYBRID and query.query_str:
        recall["text"] = query.query_str
    hits = _ranked(mem, region, want=top_k, surviving=surviving, **recall)
    return VectorStoreQueryResult(
        nodes=[_node_of(h) for h in hits],
        similarities=[
            h.score
            if query.mode == VectorStoreQueryMode.HYBRID
            else min(1.0, 1.0 - h.distance)
            if h.distance is not None
            else h.score
            for h in hits
        ],
        ids=[h.payload["nid"] for h in hits],
    )


class CitadelVectorStore(BasePydanticVectorStore):
    """A LlamaIndex vector store backed by one encrypted Citadel region."""

    stores_text: bool = True
    is_embedding_query: bool = True

    _db: Any = PrivateAttr(default=None)
    _mem: Any = PrivateAttr(default=None)
    _region: str = PrivateAttr(default=DEFAULT_REGION)
    _dim: int = PrivateAttr(default=DEFAULT_DIM)
    _embed_model: Any = PrivateAttr(default=None)
    _model_id: str = PrivateAttr(default="")

    def __init__(
        self,
        path: str = DEFAULT_PATH,
        key: str = "",
        *,
        embed_model: Any,
        region: str = DEFAULT_REGION,
        dim: int = DEFAULT_DIM,
        model_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if not key:
            raise ValueError("a passphrase is required: the corpus is the payload")
        dim = _embedding_dim(dim)
        if not callable(
            getattr(embed_model, "get_text_embedding_batch", None)
        ) or not callable(getattr(embed_model, "get_query_embedding", None)):
            raise ValueError(
                "embed_model must provide get_text_embedding_batch and get_query_embedding"
            )
        model_id = _model_id(embed_model, model_id)
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
        self._dim = dim
        self._embed_model = embed_model
        self._model_id = model_id
        # Idempotent for a region of the same width, so a dim clash raises here.
        self._mem.create_encrypted_region(
            region, _LlamaIndexEmbedder(embed_model, dim, model_id)
        )

    @classmethod
    def class_name(cls) -> str:
        return "CitadelVectorStore"

    @property
    def client(self) -> Any:
        """The Citadel memory engine backing this store."""
        return self._mem

    def add(self, nodes: list[BaseNode], **kwargs: Any) -> list[str]:
        return _add(self._mem, self._region, self._dim, list(nodes))

    def delete(self, ref_doc_id: str, **delete_kwargs: Any) -> None:
        _delete_ref(self._mem, self._region, ref_doc_id)

    def query(self, query: VectorStoreQuery, **kwargs: Any) -> VectorStoreQueryResult:
        return _query(self._mem, self._region, query)

    def get_nodes(
        self,
        node_ids: list[str] | None = None,
        filters: MetadataFilters | None = None,
    ) -> list[BaseNode]:
        return _get_nodes(self._mem, self._region, node_ids, filters)

    def delete_nodes(
        self,
        node_ids: list[str] | None = None,
        filters: MetadataFilters | None = None,
        **delete_kwargs: Any,
    ) -> None:
        _delete_nodes(self._mem, self._region, node_ids, filters)

    def clear(self) -> None:
        _clear(self._mem, self._region)

    # The bindings are sync, so a worker thread keeps the event loop free.

    async def async_add(self, nodes: list[BaseNode], **kwargs: Any) -> list[str]:
        return await asyncio.to_thread(
            _add, self._mem, self._region, self._dim, list(nodes)
        )

    async def adelete(self, ref_doc_id: str, **delete_kwargs: Any) -> None:
        await asyncio.to_thread(_delete_ref, self._mem, self._region, ref_doc_id)

    async def aquery(
        self, query: VectorStoreQuery, **kwargs: Any
    ) -> VectorStoreQueryResult:
        return await asyncio.to_thread(_query, self._mem, self._region, query)

    async def aget_nodes(
        self,
        node_ids: list[str] | None = None,
        filters: MetadataFilters | None = None,
    ) -> list[BaseNode]:
        return await asyncio.to_thread(
            _get_nodes, self._mem, self._region, node_ids, filters
        )

    async def adelete_nodes(
        self,
        node_ids: list[str] | None = None,
        filters: MetadataFilters | None = None,
        **delete_kwargs: Any,
    ) -> None:
        await asyncio.to_thread(
            _delete_nodes, self._mem, self._region, node_ids, filters
        )

    async def aclear(self) -> None:
        await asyncio.to_thread(_clear, self._mem, self._region)

    def forget_document(self, ref_doc_id: str) -> int:
        """Destroy a source document's nodes, returning the number erased."""
        return _delete_ref(self._mem, self._region, ref_doc_id)

    def count(self) -> int:
        return self._mem.count(self._region, KIND)
