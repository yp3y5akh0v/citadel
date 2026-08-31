"""Validation shared by both Agent Framework providers."""

from operator import index
from typing import Any

_METRICS = {"cosine", "cos", "l2", "euclidean", "ip", "inner", "inner_product", "dot"}


class _NormalizedEmbedder:
    def __init__(self, embedder: Any, model_id: str) -> None:
        self._embedder = embedder
        self.model_id = model_id

    def __getattr__(self, name: str) -> Any:
        return getattr(self._embedder, name)


def require_embedder(embedder: Any) -> Any:
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
    if not callable(getattr(embedder, "embed_with_cancel", None)):
        raise TypeError(
            "embedder must provide a callable "
            "embed_with_cancel(texts, cancel_token) method"
        )
    missing = object()
    embed_queries = getattr(embedder, "embed_queries_with_cancel", missing)
    if embed_queries is not missing and not callable(embed_queries):
        raise TypeError("embedder embed_queries_with_cancel attribute must be callable")
    normalized = model_id.strip()
    return (
        embedder
        if normalized == model_id
        else _NormalizedEmbedder(embedder, normalized)
    )
