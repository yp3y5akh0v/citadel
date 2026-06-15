"""Complex / edge-case coverage: vectors-in-SQL, BYO-embedder error paths, JOINs."""

import numpy as np
import pytest

import citadeldb


def test_vector_column_in_sql():
    db = citadeldb.connect(key="k")
    db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, emb VECTOR(3))")
    for i, v in [(1, [1, 0, 0]), (2, [0, 1, 0]), (3, [1, 1, 0])]:
        db.execute("INSERT INTO items VALUES ($1, $2)", [i, np.array(v, dtype=np.float32)])
    emb = db.query("SELECT emb FROM items WHERE id = 1").rows[0][0]
    assert list(emb) == [1.0, 0.0, 0.0]
    r = db.query(
        "SELECT id FROM items ORDER BY emb <-> $1 LIMIT 2",
        [np.array([1, 0, 0], dtype=np.float32)],
    )
    assert r.rows[0][0] == 1


def test_byo_embedder_error_propagates():
    class Bad:
        dim = 4
        metric = "cosine"
        model_id = "bad"

        def embed(self, texts):
            raise ValueError("boom from python embedder")

    mem = citadeldb.connect(key="k").memory()
    mem.create_region("r", Bad())  # only reads dim/metric/model_id
    with pytest.raises(citadeldb.OperationalError) as exc:
        mem.remember("r", {"kind": "fact", "text": "x"})  # triggers embed()
    assert "boom" in str(exc.value)


def test_complex_query_join_aggregate():
    db = citadeldb.connect(key="k")
    db.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)")
    db.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)")
    db.execute("INSERT INTO users VALUES (1, 'alice')")
    db.execute("INSERT INTO users VALUES (2, 'bob')")
    for oid, uid, amt in [(1, 1, 10.0), (2, 1, 5.0), (3, 2, 7.0)]:
        db.execute("INSERT INTO orders VALUES ($1, $2, $3)", [oid, uid, amt])
    r = db.query(
        "SELECT u.name AS name, COUNT(o.id) AS n, SUM(o.amount) AS total "
        "FROM users u JOIN orders o ON o.user_id = u.id "
        "GROUP BY u.name ORDER BY total DESC"
    )
    assert r.to_dicts() == [
        {"name": "alice", "n": 2, "total": 15.0},
        {"name": "bob", "n": 1, "total": 7.0},
    ]


def test_unsupported_param_type_raises():
    db = citadeldb.connect(key="k")
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    with pytest.raises(ValueError):
        db.execute("INSERT INTO t VALUES ($1)", [object()])  # no SQL mapping
