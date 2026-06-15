"""Vector search: VectorIndex build/search, filters, distance helpers."""

import numpy as np
import pytest

import citadeldb
from citadeldb import vector


def test_build_search_l2():
    vecs = np.array([[1, 0, 0], [0, 1, 0], [0, 0, 1], [1, 1, 0]], dtype=np.float32)
    ids = np.array([10, 20, 30, 40], dtype=np.int64)
    idx = citadeldb.VectorIndex(vecs, ids, metric="l2")
    assert len(idx) == 4 and idx.dim == 3 and idx.metric == "l2"
    out, dists = idx.search(np.array([1, 0, 0], dtype=np.float32), k=2)
    assert int(out[0]) == 10
    assert float(dists[0]) == pytest.approx(0.0, abs=1e-5)


def test_metrics():
    vecs = np.array([[1, 0], [0, 1]], dtype=np.float32)
    ids = np.array([1, 2], dtype=np.int64)
    for m in ("l2", "cosine", "inner"):
        idx = citadeldb.VectorIndex(vecs, ids, metric=m)
        out, _ = idx.search(np.array([1, 0], dtype=np.float32), k=1)
        assert int(out[0]) == 1


def test_filtered_search():
    vecs = np.array([[1, 0]] * 4, dtype=np.float32)
    ids = np.array([1, 2, 3, 4], dtype=np.int64)
    attrs = np.array([[0], [0], [1], [1]], dtype=np.uint32)
    idx = citadeldb.VectorIndex(vecs, ids, metric="l2", attrs=attrs)
    out, _ = idx.search(np.array([1, 0], dtype=np.float32), k=10, filter=citadeldb.Filter.eq(0, 1))
    assert {int(x) for x in out} == {3, 4}
    assert citadeldb.Filter.eq(0, 1).strength() == 1
    assert citadeldb.Filter().strength() == 0


def test_dim_mismatch_raises():
    idx = citadeldb.VectorIndex(
        np.array([[1, 0, 0]], dtype=np.float32), np.array([1], dtype=np.int64)
    )
    with pytest.raises(ValueError):
        idx.search(np.array([1, 0], dtype=np.float32), k=1)


def test_distance_helpers():
    a = np.array([1, 0, 0, 0], dtype=np.float32)
    b = np.array([0, 1, 0, 0], dtype=np.float32)
    assert vector.l2_squared(a, b) == pytest.approx(2.0)
    assert vector.inner_product(a, b) == pytest.approx(0.0)
    assert vector.cosine(a, b) == pytest.approx(1.0)
    assert vector.distance(a, a, "l2") == pytest.approx(0.0)
    norm = vector.normalize(np.array([3, 4], dtype=np.float32))
    assert float(np.linalg.norm(norm)) == pytest.approx(1.0, abs=1e-5)


def test_non_finite_rejected():
    vecs = np.array([[1, 0, 0], [0, 1, 0]], dtype=np.float32)
    ids = np.array([1, 2], dtype=np.int64)
    idx = citadeldb.VectorIndex(vecs, ids)
    with pytest.raises(ValueError):  # NaN query must raise, not panic in the distance sort
        idx.search(np.array([np.nan, 0, 0], dtype=np.float32), k=1)
    with pytest.raises(ValueError):  # a non-finite build vector is rejected too
        citadeldb.VectorIndex(
            np.array([[1, 0, 0], [np.inf, 0, 0]], dtype=np.float32),
            np.array([1, 2], dtype=np.int64),
        )


def test_bad_metric_and_empty():
    with pytest.raises(ValueError):
        citadeldb.VectorIndex(
            np.array([[1.0]], dtype=np.float32), np.array([1], dtype=np.int64), metric="bogus"
        )
    with pytest.raises(ValueError):
        citadeldb.VectorIndex(np.zeros((0, 3), dtype=np.float32), np.array([], dtype=np.int64))
