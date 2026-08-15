"""Haystack DocumentStore over an encrypted Citadel region."""
from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from typing import Any

import citadeldb
from haystack import default_from_dict, default_to_dict
from haystack.dataclasses import Document
from haystack.document_stores.errors import DuplicateDocumentError
from haystack.document_stores.types import DuplicatePolicy
from haystack.utils import Secret, deserialize_secrets_inplace
from haystack.utils.filters import document_matches_filter

KIND = "doc"
DEFAULT_PATH = "haystack.cdl"
DEFAULT_REGION = "documents"
# The default width of Haystack's own embedders and of its conformance fixtures.
DEFAULT_DIM = 768
PAGE = 10_000
# Haystack prefixes metadata fields; bare names address the document.
_META_PREFIX = "meta."


# A Database is pinned to its opening thread, so workers take Memory, not self.


def _placeholder(text: str, dim: int) -> list[float]:
    """A deterministic vector for a document Haystack did not embed."""
    digest = hashlib.sha256(text.encode()).digest()
    return [digest[i % len(digest)] / 255.0 for i in range(dim)]


def _fetch(mem: Any, region: str, criterion: dict[str, Any] | None) -> list[Any]:
    """Page to the end: one fetch is bounded, and a partial erase must not look whole."""
    out: list[Any] = []
    after = None
    while True:
        page = mem.fetch(region, KIND, payload_filter=criterion, limit=PAGE, after_id=after)
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
            path = tuple(p for p in field[len(_META_PREFIX):].split(".") if p)
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
            other is not path and (other[: len(path)] == path or path[: len(other)] == other)
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
    return {
        "kind": KIND,
        "text": doc.content or "",
        "embedding": (
            list(doc.embedding)
            if doc.embedding is not None
            else _placeholder(doc.content or doc.id, dim)
        ),
        # Unflattened so the document round-trips whole. `emb` records whether
        # the vector above is Haystack's or the placeholder, which is what keeps
        # a hash of the text out of embedding_retrieval's answers.
        "payload": {
            "did": doc.id,
            "emb": doc.embedding is not None,
            "doc": doc.to_dict(flatten=False),
        },
    }


def _write(
    mem: Any,
    region: str,
    dim: int,
    documents: list[Document],
    policy: DuplicatePolicy,
) -> int:
    if policy == DuplicatePolicy.NONE:
        policy = DuplicatePolicy.FAIL  # as InMemoryDocumentStore defaults

    # OVERWRITE decides nothing from a prior read, so it does not pay for one:
    # the keyed write below supersedes whatever the id already named.
    seen: set[str] = set()
    if policy != DuplicatePolicy.OVERWRITE:
        # A live id set: a second copy in one batch collides like a stored one.
        seen = {
            h.payload["did"]
            for doc in documents
            for h in _fetch(mem, region, {"did": doc.id})
        }
    written = len(documents)
    atoms: dict[str, dict[str, Any]] = {}
    for doc in documents:
        if policy != DuplicatePolicy.OVERWRITE and doc.id in seen:
            if policy == DuplicatePolicy.FAIL:
                raise DuplicateDocumentError(f"ID '{doc.id}' already exists.")
            written -= 1  # SKIP: the copy already accepted stands
            continue
        # Last copy of an id in one batch wins, as the reference's dict does.
        atoms[doc.id] = _atom(doc, dim)
        seen.add(doc.id)
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
    return _erase(
        mem,
        region,
        [h for did in dict.fromkeys(document_ids) for h in _fetch(mem, region, {"did": did})],
    )


def _count(mem: Any, region: str) -> int:
    return len(_fetch(mem, region, None))


class CitadelDocumentStore:
    """A Haystack `DocumentStore` backed by one encrypted Citadel region."""

    def __init__(
        self,
        path: str = DEFAULT_PATH,
        key: Secret | str = Secret.from_env_var("CITADEL_KEY"),
        *,
        region: str = DEFAULT_REGION,
        dim: int = DEFAULT_DIM,
    ) -> None:
        self._key = Secret.from_token(key) if isinstance(key, str) else key
        passphrase = self._key.resolve_value()
        if not passphrase:
            raise ValueError("a passphrase is required: the corpus is the payload")
        self._path = path
        self._region = region
        self._dim = dim
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
        self._mem.create_encrypted_region(region, citadeldb.MockEmbedder(dim=dim))

    # ---- serialization ----------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serialize for a pipeline file; a literal passphrase refuses."""
        return default_to_dict(
            self,
            path=self._path,
            key=self._key.to_dict(),
            region=self._region,
            dim=self._dim,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CitadelDocumentStore:
        deserialize_secrets_inplace(data["init_parameters"], keys=["key"])
        return default_from_dict(cls, data)

    # ---- the protocol -----------------------------------------------------

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
        return _write(self._mem, self._region, self._dim, documents, policy)

    def delete_documents(self, document_ids: list[str]) -> None:
        _delete(self._mem, self._region, document_ids)

    # ---- async ------------------------------------------------------------
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
            _write, self._mem, self._region, self._dim, documents, policy
        )

    async def delete_documents_async(self, document_ids: list[str]) -> None:
        await asyncio.to_thread(_delete, self._mem, self._region, document_ids)

    # ---- beyond the protocol ----------------------------------------------

    def embedding_retrieval(
        self,
        query_embedding: list[float],
        top_k: int = 10,
        filters: dict[str, Any] | None = None,
    ) -> list[Document]:
        """Documents ranked by recall, best first, with their score set.

        Only documents Haystack embedded take part, as its own store does: an
        unembedded one carries a hash of its text, and ranking that against a
        real query vector produces a plausible score with no meaning behind it.
        """
        criterion = _pushdown(filters) or {}
        criterion = {**criterion, "emb": True}
        options = citadeldb.RecallOptions(payload_filter=criterion)

        def surviving(hits: list[Any]) -> list[Document]:
            out: list[Document] = []
            for h in hits:
                doc = _document(h)
                if filters and not document_matches_filter(filters, doc):
                    continue
                # Mutating a shared Document would affect other pipeline steps.
                out.append(
                    replace(
                        doc,
                        score=min(1.0, 1.0 - h.distance)
                        if h.distance is not None
                        else h.score,
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
