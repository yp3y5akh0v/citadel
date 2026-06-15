"""Vector search: in-memory ANN index, filters, and distance metrics."""

from citadeldb._core import (
    Filter,
    VectorIndex,
    vector_cosine as cosine,
    vector_distance as distance,
    vector_inner_product as inner_product,
    vector_l2_squared as l2_squared,
    vector_normalize as normalize,
)

__all__ = [
    "VectorIndex",
    "Filter",
    "l2_squared",
    "inner_product",
    "cosine",
    "distance",
    "normalize",
]
