"""Haystack DocumentStore over an encrypted Citadel region."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from operator import index
from typing import Any, Literal

import citadeldb
from haystack import default_from_dict, default_to_dict
from haystack.core.serialization import component_to_dict
from haystack.dataclasses import Document
from haystack.document_stores.errors import DuplicateDocumentError
from haystack.document_stores.types import DuplicatePolicy
from haystack.utils import Secret, deserialize_secrets_inplace, expit
from haystack.utils.filters import document_matches_filter

try:
    from haystack.utils.deserialization import (
        deserialize_component_inplace as _deserialize_component_inplace,
    )
except ImportError:  # Haystack 2.9-2.28
    from haystack.core.serialization import component_from_dict, import_class_by_name

    def _deserialize_component_inplace(data: dict[str, Any], key: str) -> None:
        serialized = data[key]
        cls = import_class_by_name(serialized["type"])
        data[key] = component_from_dict(cls, serialized, key)


KIND = "doc"
DEFAULT_PATH = "haystack.cdl"
DEFAULT_REGION = "documents"
_DEFAULT_KEY = Secret.from_env_var("CITADEL_KEY")
# The default width of Haystack's own embedders and of its conformance fixtures.
DEFAULT_DIM = 768
PAGE = 10_000
# Haystack prefixes metadata fields; bare names address the document.
_META_PREFIX = "meta."
_SIMILARITY_METRICS = {"cosine": "cosine", "dot_product": "inner"}


# A Database is pinned to its opening thread, so workers take Memory, not self.


def _model_id(embedder: Any, override: str | None) -> str:
    values = (
        (override,)
        if override is not None
        else tuple(
            getattr(embedder, attr, None)
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
        "model_id is required when the Haystack embedder does not expose a specific "
        "model_id, model, or model_name"
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


def _similarity_function(value: str) -> tuple[str, str]:
    if value not in _SIMILARITY_METRICS:
        raise ValueError(
            "embedding_similarity_function must be 'cosine' or 'dot_product'"
        )
    return value, _SIMILARITY_METRICS[value]


class _HaystackEmbedder:
    """Expose one Haystack text embedder through Citadel's batch protocol."""

    def __init__(self, embedder: Any, dim: int, model_id: str, metric: str) -> None:
        self._embedder = embedder
        self.dim = dim
        self.model_id = model_id
        self.metric = metric
        self._warm_up = getattr(embedder, "warm_up", None)
        self._warm_lock = threading.Lock()
        self._warmed = not callable(self._warm_up)

    def _ensure_warm(self) -> None:
        if self._warmed:
            return
        with self._warm_lock:
            if not self._warmed:
                self._warm_up()
                self._warmed = True

    def _one(self, text: str) -> list[float]:
        self._ensure_warm()
        result = self._embedder.run(text=text)
        vector = result.get("embedding")
        if not isinstance(vector, list):
            raise ValueError("the Haystack embedder did not return an 'embedding' list")
        return vector

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._one(text) for text in texts]

    def embed_queries(self, texts: list[str]) -> list[list[float]]:
        return self.embed(texts)


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
    # Unflattened so the document round-trips whole.
    return Document.from_dict(hit.payload["doc"])


def _pushdown(filters: dict[str, Any] | None) -> dict[str, Any] | None:
    """Equality leaves of a top-level AND; this narrows and never decides."""
    if not filters:
        return None
    if filters.get("operator") == "AND":
        conditions = filters.get("conditions", [])
    elif "field" in filters:
        conditions = [filters]  # a bare condition is an AND of one
    else:
        return None
    leaves: list[tuple[tuple[str, ...], Any]] = []
    for c in conditions:
        field = c.get("field", "")
        if (
            c.get("operator") == "=="
            and field.startswith(_META_PREFIX)
            # Only strings: containment compares JSON types exactly where
            # Haystack's `==` is Python's, which holds 1 == 1.0 == True, so
            # pushing a number would decide rather than narrow. None means
            # absent, which containment cannot express at all.
            and isinstance(c.get("value"), str)
        ):
            path = tuple(p for p in field[len(_META_PREFIX) :].split(".") if p)
            if path:
                leaves.append((path, c["value"]))
    eq = _needle(leaves)
    # The needle is shaped like the payload, not like the filter.
    return {"doc": {"meta": eq}} if eq else None


def _needle(leaves: list[tuple[tuple[str, ...], Any]]) -> dict[str, Any]:
    """Nested-object needle for dotted metadata paths.

    `meta.person.name` addresses a nested value, so it has to become
    `{"person": {"name": v}}`; one flat key spelled with a dot can never exist in
    the payload and would match nothing. Two leaves that collide, or one whose
    path runs through another's, are dropped rather than merged - containment
    holds one value per position, and guessing which wins would decide.
    """
    out: dict[str, Any] = {}
    for path, value in leaves:
        if any(
            other is not path
            and (other[: len(path)] == path or path[: len(other)] == other)
            for other, _ in leaves
        ):
            continue
        node = out
        for part in path[:-1]:
            node = node.setdefault(part, {})
        node[path[-1]] = value
    return out


def _filter(mem: Any, region: str, filters: dict[str, Any] | None) -> list[Document]:
    docs = [_document(h) for h in _fetch(mem, region, _pushdown(filters))]
    if not filters:
        return docs
    return [d for d in docs if document_matches_filter(filters, d)]


def _atom(doc: Document, dim: int) -> dict[str, Any]:
    if doc.embedding is not None and len(doc.embedding) != dim:
        raise ValueError(
            f"document {doc.id} has a {len(doc.embedding)}-dimension embedding but "
            f"this region is {dim}. Build the store with dim={len(doc.embedding)} to "
            f"match your embedding model."
        )
    atom = {
        "kind": KIND,
        "text": doc.content or "",
        # Unflattened so the document round-trips whole.
        "payload": {
            "did": doc.id,
            "doc": doc.to_dict(flatten=False),
        },
    }
    if doc.embedding is not None:
        atom["embedding"] = list(doc.embedding)
    return atom


def _write(
    mem: Any,
    region: str,
    dim: int,
    embedder: _HaystackEmbedder,
    documents: list[Document],
    policy: DuplicatePolicy,
) -> int:
    if not documents:
        return 0
    if policy == DuplicatePolicy.NONE:
        policy = DuplicatePolicy.FAIL  # as InMemoryDocumentStore defaults

    # OVERWRITE decides nothing from a prior read, so it does not pay for one:
    # the keyed write below supersedes whatever the id already named.
    seen: set[str] = set()
    if policy != DuplicatePolicy.OVERWRITE:
        wanted = {doc.id for doc in documents}
        # Encrypted payload filters cannot avoid decrypting the region, so build
        # the live id set in one pass rather than one pass per input document.
        seen = {
            h.payload["did"]
            for h in _fetch(mem, region, None)
            if h.payload.get("did") in wanted
        }
    written = len(documents)
    accepted: dict[str, Document] = {}
    for doc in documents:
        if policy != DuplicatePolicy.OVERWRITE and doc.id in seen:
            if policy == DuplicatePolicy.FAIL:
                raise DuplicateDocumentError(f"ID '{doc.id}' already exists.")
            written -= 1  # SKIP: the copy already accepted stands
            continue
        # Last copy of an id in one batch wins, as the reference's dict does.
        accepted[doc.id] = doc
        seen.add(doc.id)
    atoms: dict[str, dict[str, Any]] = {}
    for did, doc in accepted.items():
        atom = _atom(doc, dim)
        if doc.embedding is None:
            generated = embedder._one(doc.content or "")
            atom["embedding"] = generated
            atom["payload"]["generated_embedding"] = generated
        atoms[did] = atom
    if atoms:
        # Keyed on the document id and committed as one transaction: two writers
        # of one id supersede rather than each adding a row, which a read out
        # here cannot prevent. Every policy binds the key, so a document written
        # under SKIP is still the one a later OVERWRITE replaces.
        mem.remember_replacing_keyed_batch(
            region, [(atom, did) for did, atom in atoms.items()]
        )
    return written


def _delete(mem: Any, region: str, document_ids: list[str]) -> int:
    # Contracted not to fail on an id that is not present.
    wanted = set(document_ids)
    return _erase(
        mem,
        region,
        [h for h in _fetch(mem, region, None) if h.payload.get("did") in wanted],
    )


def _count(mem: Any, region: str) -> int:
    return mem.count(region, KIND)


class CitadelDocumentStore:
    """A Haystack `DocumentStore` backed by one encrypted Citadel region."""

    def __init__(
        self,
        path: str = DEFAULT_PATH,
        key: Secret | str = _DEFAULT_KEY,
        *,
        embedder: Any,
        region: str = DEFAULT_REGION,
        dim: int = DEFAULT_DIM,
        model_id: str | None = None,
        embedding_similarity_function: Literal["cosine", "dot_product"] = "dot_product",
    ) -> None:
        self._key = Secret.from_token(key) if isinstance(key, str) else key
        passphrase = self._key.resolve_value()
        if not passphrase:
            raise ValueError("a passphrase is required: the corpus is the payload")
        dim = _embedding_dim(dim)
        if not callable(getattr(embedder, "run", None)):
            raise ValueError(
                "embedder must be a Haystack text embedder with run(text=...)"
            )
        model_id = _model_id(embedder, model_id)
        embedding_similarity_function, metric = _similarity_function(
            embedding_similarity_function
        )
        memory_embedder = _HaystackEmbedder(embedder, dim, model_id, metric)
        self._path = path
        self._region = region
        self._dim = dim
        self._model_id = model_id
        self._embedder = embedder
        self._memory_embedder = memory_embedder
        self.embedding_similarity_function = embedding_similarity_function
        try:
            self._db = citadeldb.connect(path, key=passphrase, region_keys=True)
        except citadeldb.OperationalError as e:
            if "locked" not in str(e):
                raise
            raise RuntimeError(
                f"{path} is open in another process. Citadel is embedded, so one "
                f"process owns the file."
            ) from e
        self._mem = self._db.memory()
        # Idempotent for a region of the same width, so a dim clash raises here.
        self._mem.create_encrypted_region(region, memory_embedder)

    def to_dict(self) -> dict[str, Any]:
        """Serialize for a pipeline file; a literal passphrase refuses."""
        return default_to_dict(
            self,
            path=self._path,
            key=self._key.to_dict(),
            embedder=component_to_dict(self._embedder, name="embedder"),
            region=self._region,
            dim=self._dim,
            model_id=self._model_id,
            embedding_similarity_function=self.embedding_similarity_function,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CitadelDocumentStore:
        deserialize_secrets_inplace(data["init_parameters"], keys=["key"])
        _deserialize_component_inplace(data["init_parameters"], key="embedder")
        return default_from_dict(cls, data)

    def count_documents(self) -> int:
        return _count(self._mem, self._region)

    def filter_documents(self, filters: dict[str, Any] | None = None) -> list[Document]:
        return _filter(self._mem, self._region, filters)

    def write_documents(
        self, documents: list[Document], policy: DuplicatePolicy = DuplicatePolicy.NONE
    ) -> int:
        if not isinstance(documents, list) or any(
            not isinstance(d, Document) for d in documents
        ):
            raise ValueError("Please provide a list of Documents.")
        return _write(
            self._mem,
            self._region,
            self._dim,
            self._memory_embedder,
            documents,
            policy,
        )

    def delete_documents(self, document_ids: list[str]) -> None:
        _delete(self._mem, self._region, document_ids)

    # The bindings are sync, so a worker thread keeps the event loop free.

    async def count_documents_async(self) -> int:
        return await asyncio.to_thread(_count, self._mem, self._region)

    async def filter_documents_async(
        self, filters: dict[str, Any] | None = None
    ) -> list[Document]:
        return await asyncio.to_thread(_filter, self._mem, self._region, filters)

    async def write_documents_async(
        self, documents: list[Document], policy: DuplicatePolicy = DuplicatePolicy.NONE
    ) -> int:
        if not isinstance(documents, list) or any(
            not isinstance(d, Document) for d in documents
        ):
            raise ValueError("Please provide a list of Documents.")
        return await asyncio.to_thread(
            _write,
            self._mem,
            self._region,
            self._dim,
            self._memory_embedder,
            documents,
            policy,
        )

    async def delete_documents_async(self, document_ids: list[str]) -> None:
        await asyncio.to_thread(_delete, self._mem, self._region, document_ids)

    def embedding_retrieval(
        self,
        query_embedding: list[float],
        filters: dict[str, Any] | None = None,
        top_k: int = 10,
        scale_score: bool = False,
        return_embedding: bool = False,
    ) -> list[Document]:
        """Documents ranked by recall, best first, with their score set.

        Documents without a supplied vector are embedded by the store's model.
        """
        if not query_embedding or not isinstance(query_embedding[0], float):
            raise ValueError("query_embedding should be a non-empty list of floats.")
        criterion = _pushdown(filters)
        options = (
            citadeldb.RecallOptions(payload_filter=criterion) if criterion else None
        )

        def surviving(hits: list[Any]) -> list[Document]:
            out: list[Document] = []
            for h in hits:
                doc = _document(h)
                if filters and not document_matches_filter(filters, doc):
                    continue
                if h.distance is None:
                    score = h.score
                elif self.embedding_similarity_function == "dot_product":
                    score = -h.distance
                else:
                    score = max(-1.0, min(1.0, 1.0 - h.distance))
                if scale_score:
                    score = (
                        expit(score / 100.0)
                        if self.embedding_similarity_function == "dot_product"
                        else (score + 1.0) / 2.0
                    )
                returned_embedding = None
                if return_embedding:
                    returned_embedding = (
                        doc.embedding
                        if doc.embedding is not None
                        else h.payload.get("generated_embedding")
                    )
                # Mutating a shared Document would affect other pipeline steps.
                out.append(
                    replace(
                        doc,
                        score=score,
                        embedding=returned_embedding,
                    )
                )
            return out

        # A filter containment cannot express is settled above, so the window has
        # to widen until top_k survive it rather than answer short.
        k = max(top_k, 32)
        while True:
            hits = self._mem.recall(
                self._region,
                embedding=query_embedding,
                k=k,
                kinds=[KIND],
                options=options,
            )
            out = surviving(hits)
            if len(out) >= top_k or len(hits) < k:
                return out[:top_k]
            k *= 2

    def delete_all(self) -> int:
        """Destroy every document's key, returning the number erased."""
        return _erase(self._mem, self._region, _fetch(self._mem, self._region, None))
