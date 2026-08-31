"""LangChain vector store over an encrypted Citadel region."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterable, Sequence
from operator import index
from pathlib import Path
from typing import Any

import citadeldb
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

KIND = "doc"
DEFAULT_PATH = "langchain.cdl"
DEFAULT_REGION = "vectors"
# A region is read whole where containment cannot express a predicate.
PAGE = 10_000
_DIM_PROBE = "dimension probe"
_EMBED_BATCH = 32


# A Database is pinned to its opening thread, so workers take Memory, not self.


def _model_id(embedding: Embeddings, override: str | None) -> str:
    """Resolve stable vector provenance without guessing from a class name."""
    values = (
        (override,)
        if override is not None
        else tuple(
            getattr(embedding, attr, None)
            for attr in ("model_id", "model", "model_name")
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
        "model_id is required when the LangChain embedding does not expose a specific "
        "model_id, model, or model_name"
    )


class _LangChainEmbedder:
    """Expose one LangChain model through Citadel's embedder protocol."""

    metric = "cosine"

    def __init__(self, embedding: Embeddings, dim: int, model_id: str) -> None:
        self._embedding = embedding
        self.dim = dim
        self.model_id = model_id

    def embed(self, texts: list[str]) -> list[list[float]]:
        return self.embed_with_cancel(texts, None)

    def embed_queries(self, texts: list[str]) -> list[list[float]]:
        return self.embed_queries_with_cancel(texts, None)

    def embed_with_cancel(
        self, texts: list[str], cancel_token: Any | None
    ) -> list[list[float]]:
        vectors: list[list[float]] = []
        for start in range(0, len(texts), _EMBED_BATCH):
            if cancel_token is not None:
                cancel_token.check()
            vectors.extend(
                self._embedding.embed_documents(texts[start : start + _EMBED_BATCH])
            )
            if cancel_token is not None:
                cancel_token.check()
        return vectors

    def embed_queries_with_cancel(
        self, texts: list[str], cancel_token: Any | None
    ) -> list[list[float]]:
        vectors: list[list[float]] = []
        for text in texts:
            if cancel_token is not None:
                cancel_token.check()
            vectors.append(self._embedding.embed_query(text))
            if cancel_token is not None:
                cancel_token.check()
        return vectors


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


def _erase(mem: Any, region: str, hits: list[Any]) -> int:
    if not hits:
        return 0
    return mem.forget(region, [h.id for h in hits]).erased_count


def _document(hit: Any) -> Document:
    return Document(
        id=hit.payload["did"],
        page_content=hit.text,
        metadata=dict(hit.payload.get("meta") or {}),
    )


def _write(
    mem: Any,
    region: str,
    dim: int,
    ids: list[str],
    texts: list[str],
    metadatas: list[dict[str, Any]],
    vectors: list[list[float]],
) -> list[str]:
    if len(vectors) != len(ids):
        raise ValueError(
            f"the embedding model returned {len(vectors)} vectors for {len(ids)} texts"
        )
    for i, v in enumerate(vectors):
        if len(v) != dim:
            raise ValueError(
                f"the embedding model returned a {len(v)}-dimension vector for item "
                f"{i} but this region is {dim}. Build the store with dim={len(v)}, or "
                f"use a fresh path for a different model."
            )
    # An id repeated in one call keeps its last value, as store[id] would.
    latest: dict[str, tuple[str, dict[str, Any], list[float]]] = {}
    for did, text, meta, vector in zip(ids, texts, metadatas, vectors):
        latest[did] = (text, dict(meta or {}), vector)
    atoms = [
        {
            "kind": KIND,
            "text": text,
            # Storing LangChain's vector is what keeps one vector space.
            "embedding": list(vector),
            "payload": {"did": did, "meta": meta},
        }
        for did, (text, meta, vector) in latest.items()
    ]
    # Adding a stored id replaces it, as InMemoryVectorStore does. Keyed on the
    # id and committed as one transaction, so two writers of one id supersede
    # rather than each leaving a row behind.
    mem.remember_replacing_keyed_batch(
        region, [(atom, atom["payload"]["did"]) for atom in atoms]
    )
    # One id per input text, duplicates included.
    return ids


def _rows_by_id(mem: Any, region: str, ids: Sequence[str]) -> dict[str, list[Any]]:
    """Stored rows per id, from one pass over the region.

    A filtered fetch per id re-reads the whole region each time - the payload is
    only readable after decryption, so the filter cannot shorten the scan - which
    makes N ids over M rows cost N*M. One pass and a dict costs M.
    """
    found: dict[str, list[Any]] = {did: [] for did in ids}
    for h in _fetch(mem, region, None):
        rows = found.get(h.payload.get("did"))
        if rows is not None:
            rows.append(h)
    return found


def _delete(mem: Any, region: str, ids: Sequence[str] | None) -> bool:
    # An empty id list is a no-op because erasure is irreversible.
    if not ids:
        return True
    found = _rows_by_id(mem, region, list(dict.fromkeys(ids)))
    _erase(mem, region, [h for rows in found.values() for h in rows])
    return True


def _get_by_ids(mem: Any, region: str, ids: Sequence[str]) -> list[Document]:
    # Contracted never to raise for an unknown id, and duplicates collapse.
    wanted = list(dict.fromkeys(ids))
    found = _rows_by_id(mem, region, wanted)
    return [_document(h) for did in wanted for h in found[did]]


def _search(
    mem: Any,
    region: str,
    embedding: list[float],
    k: int,
    metadata_filter: dict[str, Any] | None,
) -> list[tuple[Document, float]]:
    if k <= 0:
        return []
    options = (
        citadeldb.RecallOptions(payload_filter={"meta": metadata_filter})
        if metadata_filter
        else None
    )
    hits = mem.recall(region, embedding=embedding, k=k, kinds=[KIND], options=options)
    return [(_document(h), _similarity(h)) for h in hits]


def _similarity(hit: Any) -> float:
    """Distance to similarity, capped at 1 for near-zero distances."""
    if hit.distance is None:
        return hit.relevance if hit.relevance is not None else 0.0
    return min(1.0, 1.0 - hit.distance)


def _clear(mem: Any, region: str) -> int:
    return _erase(mem, region, _fetch(mem, region, None))


def _mmr_indices(
    query: list[float], candidates: list[list[float]], *, k: int, lambda_mult: float
) -> list[int]:
    """Rank candidates by maximal marginal relevance."""
    import numpy as np
    from langchain_core.vectorstores.utils import maximal_marginal_relevance

    return maximal_marginal_relevance(
        np.array(query, dtype=np.float32), candidates, k=k, lambda_mult=lambda_mult
    )


def _prepare(
    texts: Iterable[str],
    metadatas: list[dict[str, Any]] | None,
    ids: list[str] | None,
) -> tuple[list[str], list[dict[str, Any]], list[str]]:
    """Validate the parallel arguments and settle an id for every text."""
    items = list(texts)
    if metadatas is not None and len(metadatas) != len(items):
        raise ValueError(
            f"The number of metadatas must match the number of texts. Got "
            f"{len(metadatas)} metadatas and {len(items)} texts."
        )
    if ids is not None and len(ids) != len(items):
        raise ValueError(
            f"The number of ids must match the number of texts. Got {len(ids)} ids "
            f"and {len(items)} texts."
        )
    # add_documents forwards doc.id, which may be None.
    resolved = [
        (ids[i] if ids and ids[i] else str(uuid.uuid4())) for i in range(len(items))
    ]
    metas = list(metadatas) if metadatas is not None else [{}] * len(items)
    return items, metas, resolved


class CitadelVectorStore(VectorStore):
    """A LangChain vector store backed by one encrypted Citadel region."""

    def __init__(
        self,
        embedding: Embeddings,
        path: str = DEFAULT_PATH,
        key: str = "",
        *,
        region: str = DEFAULT_REGION,
        dim: int | None = None,
        model_id: str | None = None,
    ) -> None:
        if not key:
            raise ValueError("a passphrase is required: the corpus is the payload")
        if not callable(getattr(embedding, "embed_documents", None)) or not callable(
            getattr(embedding, "embed_query", None)
        ):
            raise TypeError("embedding must provide embed_documents and embed_query")
        self._embedding = embedding
        self._model_id = _model_id(embedding, model_id)

        def connect(create: bool | None = None):
            try:
                return citadeldb.connect(path, key=key, create=create, region_keys=True)
            except citadeldb.OperationalError as e:
                if "locked" not in str(e):
                    raise
                raise RuntimeError(
                    f"{path} is open in another process. Citadel is embedded, so one "
                    f"process owns the file."
                ) from e

        is_file = path not in {"", ":memory:"}
        existing = dim is None and is_file and Path(path).exists()
        self._db = connect(False) if existing else None
        inferred = dim if dim is not None else len(embedding.embed_query(_DIM_PROBE))
        self._dim = _embedding_dim(inferred)
        if self._db is None:
            self._db = connect(True if dim is None and is_file else None)
        self._mem = self._db.memory()
        self._region = region
        # Idempotent for a region of the same width, so a dim clash raises here.
        self._mem.create_encrypted_region(
            region, _LangChainEmbedder(embedding, self._dim, self._model_id)
        )

    @property
    def embeddings(self) -> Embeddings:
        return self._embedding

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: list[dict[str, Any]] | None = None,
        *,
        ids: list[str] | None = None,
        **kwargs: Any,
    ) -> list[str]:
        items, metas, resolved = _prepare(texts, metadatas, ids)
        if not items:
            return []
        vectors = self._embedding.embed_documents(items)
        return _write(
            self._mem, self._region, self._dim, resolved, items, metas, vectors
        )

    def delete(self, ids: list[str] | None = None, **kwargs: Any) -> bool:
        return _delete(self._mem, self._region, ids)

    def get_by_ids(self, ids: Sequence[str], /) -> list[Document]:
        return _get_by_ids(self._mem, self._region, ids)

    def similarity_search(
        self,
        query: str,
        k: int = 4,
        *,
        filter: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Document]:
        return [
            doc for doc, _ in self.similarity_search_with_score(query, k, filter=filter)
        ]

    def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        *,
        filter: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Document, float]]:
        return _search(
            self._mem, self._region, self._embedding.embed_query(query), k, filter
        )

    def similarity_search_by_vector(
        self,
        embedding: list[float],
        k: int = 4,
        *,
        filter: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Document]:
        return [
            doc for doc, _ in _search(self._mem, self._region, embedding, k, filter)
        ]

    # `as_retriever(search_type="mmr")` reaches these; the base class raises.

    def max_marginal_relevance_search_by_vector(
        self,
        embedding: list[float],
        k: int = 4,
        fetch_k: int = 20,
        lambda_mult: float = 0.5,
        *,
        filter: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Document]:
        candidates = _search(self._mem, self._region, embedding, fetch_k, filter)
        if not candidates:
            return []
        docs = [doc for doc, _ in candidates]
        # Recall does not expose stored ANN vectors. Re-embedding avoids duplicating
        # every high-dimensional vector in the encrypted JSON payload.
        vectors = self._embedding.embed_documents([d.page_content for d in docs])
        chosen = _mmr_indices(embedding, vectors, k=k, lambda_mult=lambda_mult)
        return [docs[i] for i in chosen]

    def max_marginal_relevance_search(
        self,
        query: str,
        k: int = 4,
        fetch_k: int = 20,
        lambda_mult: float = 0.5,
        *,
        filter: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Document]:
        return self.max_marginal_relevance_search_by_vector(
            self._embedding.embed_query(query), k, fetch_k, lambda_mult, filter=filter
        )

    def _select_relevance_score_fn(self):
        """Scores are cosine similarity; relevance only clamps to [0, 1]."""
        return lambda score: max(0.0, min(1.0, score))

    # The bindings are sync, so a worker thread keeps the event loop free.

    async def aadd_texts(
        self,
        texts: Iterable[str],
        metadatas: list[dict[str, Any]] | None = None,
        *,
        ids: list[str] | None = None,
        **kwargs: Any,
    ) -> list[str]:
        items, metas, resolved = _prepare(texts, metadatas, ids)
        if not items:
            return []
        # Embedding is the model's own async call, so only the write threads.
        vectors = await self._embedding.aembed_documents(items)
        return await asyncio.to_thread(
            _write, self._mem, self._region, self._dim, resolved, items, metas, vectors
        )

    async def adelete(self, ids: list[str] | None = None, **kwargs: Any) -> bool:
        return await asyncio.to_thread(_delete, self._mem, self._region, ids)

    async def aget_by_ids(self, ids: Sequence[str], /) -> list[Document]:
        return await asyncio.to_thread(_get_by_ids, self._mem, self._region, list(ids))

    async def asimilarity_search(
        self,
        query: str,
        k: int = 4,
        *,
        filter: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Document]:
        vector = await self._embedding.aembed_query(query)
        pairs = await asyncio.to_thread(
            _search, self._mem, self._region, vector, k, filter
        )
        return [doc for doc, _ in pairs]

    @classmethod
    def from_texts(
        cls,
        texts: list[str],
        embedding: Embeddings,
        metadatas: list[dict[str, Any]] | None = None,
        *,
        ids: list[str] | None = None,
        path: str = DEFAULT_PATH,
        key: str = "",
        region: str = DEFAULT_REGION,
        dim: int | None = None,
        model_id: str | None = None,
        **kwargs: Any,
    ) -> CitadelVectorStore:
        store = cls(
            embedding,
            path,
            key,
            region=region,
            dim=dim,
            model_id=model_id,
            **kwargs,
        )
        store.add_texts(texts, metadatas, ids=ids)
        return store

    def clear(self) -> int:
        """Destroy every document's key, returning the number erased."""
        return _clear(self._mem, self._region)

    def count(self) -> int:
        return self._mem.count(self._region, KIND)
