"""Memory engine: regions, remember/recall, links, eviction, forgetting, BYO embedders."""

import os
import tempfile

import pytest

import citadeldb


def mem_db(**kw):
    return citadeldb.connect(key="k", **kw).memory()


def region(mem, name="r", dim=64):
    mem.create_region(name, citadeldb.MockEmbedder(dim))
    return name


def test_remember_recall_payload():
    mem = mem_db()
    region(mem)
    i = mem.remember("r", {"kind": "fact", "text": "the sky is blue", "payload": {"src": "x"}})
    assert isinstance(i, int)
    ids = mem.remember_batch(
        "r", [{"kind": "fact", "text": "grass is green"}, {"kind": "note", "text": "todo"}]
    )
    assert len(ids) == 2
    assert mem.count("r", "fact") == 2
    hits = mem.recall("r", text="sky color", k=3)
    sky = [h for h in hits if "sky" in h.text]
    assert sky and sky[0].payload == {"src": "x"}
    assert sky[0].kind == "fact" and sky[0].immutable is False


def test_recall_kinds_filter():
    mem = mem_db()
    region(mem)
    mem.remember("r", {"kind": "fact", "text": "alpha"})
    mem.remember("r", {"kind": "note", "text": "alpha note"})
    hits = mem.recall("r", text="alpha", k=10, kinds=["note"])
    assert hits and all(h.kind == "note" for h in hits)


def test_recall_by_embedding():
    mem = mem_db()
    region(mem, dim=8)
    emb = citadeldb.MockEmbedder(8)
    mem.remember("r", {"kind": "fact", "text": "hello world"})
    vec = emb.embed(["hello world"])[0]
    hits = mem.recall("r", embedding=vec, k=1)
    assert len(hits) == 1


def test_recall_non_finite_embedding_rejected():
    mem = mem_db()
    region(mem, dim=8)
    mem.remember("r", {"kind": "fact", "text": "hello world"})
    with pytest.raises(ValueError):  # NaN embedding must raise, not panic in the sort
        mem.recall("r", embedding=[float("nan")] * 8, k=1)


def test_fetch_and_update_payload():
    mem = mem_db()
    region(mem)
    i = mem.remember("r", {"kind": "fact", "text": "x", "payload": {"v": 1}})
    assert mem.fetch_one("r", i).payload == {"v": 1}
    assert mem.fetch_last("r", "fact").id == i
    mem.update_atom_payload("r", i, {"v": 2, "nested": [1, 2, 3]})
    assert mem.fetch_one("r", i).payload == {"v": 2, "nested": [1, 2, 3]}
    assert len(mem.fetch("r", "fact")) == 1


def test_links_and_edges():
    mem = mem_db()
    region(mem)
    a = mem.remember("r", {"kind": "fact", "text": "a"})
    b = mem.remember("r", {"kind": "fact", "text": "b"})
    mem.link(a, b, "refines", weight=0.7)
    edges = mem.fetch_edges(src=a)
    assert len(edges) == 1
    e = edges[0]
    assert e["src"] == a and e["dst"] == b and e["kind"] == "refines"
    assert e["weight"] == pytest.approx(0.7)
    mem.link(a, b, "depends_on")
    with pytest.raises(citadeldb.IntegrityError):  # cycle on an acyclic kind
        mem.link(b, a, "depends_on")
    with pytest.raises(ValueError):
        mem.link(a, b, "bogus")


def test_evict_summarize_and_immutable():
    mem = mem_db()
    region(mem)
    for n in range(5):
        mem.remember("r", {"kind": "fact", "text": f"item {n}", "score": 0.0, "confidence": 0.0})
    mem.remember("r", {"kind": "fact", "text": "keep", "immutable": True, "score": 0.0, "confidence": 0.0})
    summ = mem.summarize("r", 0)
    assert summ["total"] == 6
    assert any(k["kind"] == "fact" and k["count"] == 6 for k in summ["kinds"])
    removed = mem.evict("r", citadeldb.EvictionPolicy.low_score(0.5, 0.5))
    assert removed == 5  # the immutable atom survives
    assert mem.count("r", "fact") == 1


def test_byo_python_embedder():
    class Bucketed:
        dim = 8
        metric = "cosine"
        model_id = "byo-test"

        def embed(self, texts):
            out = []
            for t in texts:
                v = [0.0] * 8
                v[len(t) % 8] = 1.0
                out.append(v)
            return out

    mem = mem_db()
    mem.create_region("r", Bucketed())
    mem.remember("r", {"kind": "fact", "text": "abcd"})
    hits = mem.recall("r", text="wxyz", k=1)  # same length bucket as 'abcd'
    assert len(hits) == 1 and hits[0].text == "abcd"


def test_evolve():
    mem = mem_db()
    region(mem)
    a = mem.remember("r", {"kind": "fact", "text": "red green blue"})
    mem.remember("r", {"kind": "fact", "text": "red green yellow"})
    rep = mem.evolve("r", a, 5, 2.0)
    assert "links_added" in rep and "score" in rep


def test_encrypted_forget_verify():
    path = os.path.join(tempfile.mkdtemp(), "e.cdl")
    mem = citadeldb.connect(path, key="k", create=True, region_keys=True).memory()
    mem.create_encrypted_region("s", citadeldb.MockEmbedder(64))
    a = mem.remember("s", {"kind": "fact", "text": "secret"})
    assert mem.verify("s", [a])[0].verdict == "authentic"
    r = mem.forget("s", [a])
    assert r.cryptographic_erasure is True and r.erased_count == 1 and r.algorithm
    assert mem.verify("s", [a])[0].verdict in ("missing", "key_erased")


def test_encrypted_requires_region_keys():
    mem = mem_db()
    with pytest.raises(citadeldb.ProgrammingError):
        mem.create_encrypted_region("s", citadeldb.MockEmbedder(64))


def test_unknown_region():
    mem = mem_db()
    with pytest.raises(citadeldb.ProgrammingError):
        mem.remember("nope", {"kind": "fact", "text": "x"})


def test_recall_options_payload_filter():
    mem = mem_db()
    region(mem)
    mem.remember("r", {"kind": "fact", "text": "alpha one", "payload": {"topic": "x"}})
    mem.remember("r", {"kind": "fact", "text": "alpha two", "payload": {"topic": "y"}})
    opts = citadeldb.RecallOptions(payload_filter={"topic": "x"})
    hits = mem.recall("r", text="alpha", k=10, options=opts)
    assert hits and all(h.payload.get("topic") == "x" for h in hits)


def test_recall_options_weights_and_graph_expand():
    mem = mem_db()
    region(mem)
    a = mem.remember("r", {"kind": "fact", "text": "alpha"})
    b = mem.remember("r", {"kind": "fact", "text": "beta"})
    mem.link(a, b, "derived_from")
    opts = citadeldb.RecallOptions(
        weights=(0.5, 0.2, 0.2, 0.1), as_of_micros=0, graph_expand=(2, ["derived_from"])
    )
    ids = {h.id for h in mem.recall("r", text="alpha", k=10, options=opts)}
    assert {a, b} <= ids


def test_set_reranker_mock_then_clear():
    mem = mem_db()
    region(mem)
    # Pure "alpha" wins linear fusion (cosine 1.0, clean BM25); the diluted atom
    # repeats "alpha" so the word-overlap MockReranker surfaces it first. The order
    # flip + revert proves set_reranker and clear_reranker actually engage.
    pure = mem.remember("r", {"kind": "fact", "text": "alpha"})
    rep = mem.remember("r", {"kind": "fact", "text": "alpha alpha beta gamma"})
    assert mem.recall("r", text="alpha", k=2)[0].id == pure  # linear-fusion baseline
    mem.set_reranker(citadeldb.MockReranker(), strategy="replace")
    assert mem.recall("r", text="alpha", k=2)[0].id == rep  # reranker flips hits[0]
    mem.clear_reranker()
    assert mem.recall("r", text="alpha", k=2)[0].id == pure  # clear reverts to fusion


def test_set_reranker_python_object_reorders():
    class ByLength:
        model_id = "bylen"

        def rerank(self, query, passages):
            return [float(len(p)) for p in passages]  # prefer the longest passage

    mem = mem_db()
    region(mem)
    pure = mem.remember("r", {"kind": "fact", "text": "alpha"})
    long_ = mem.remember("r", {"kind": "fact", "text": "alpha alpha beta gamma"})
    assert mem.recall("r", text="alpha", k=2)[0].id == pure  # fusion: pure match first
    mem.set_reranker(ByLength(), strategy="replace")
    assert mem.recall("r", text="alpha", k=2)[0].id == long_  # ByLength flips to the longer


def test_memory_ann_persist_and_status():
    mem = mem_db()
    region(mem, dim=8)
    for i in range(40):
        mem.remember("r", {"kind": "fact", "text": f"item number {i}"})
    mem.recall("r", text="item number 1", k=3)  # build the ANN index
    info = mem.persist_ann_index("r")
    assert isinstance(info["segment_b3"], bytes) and info["n"] == 40
    status = mem.ann_cache_status("r")
    assert status is not None and status["source"] in ("loaded", "built")


def test_erasure_receipt_slot_proof():
    path = os.path.join(tempfile.mkdtemp(), "slots.cdl")
    mem = citadeldb.connect(path, key="k", create=True, region_keys=True).memory()
    mem.create_encrypted_region("s", citadeldb.MockEmbedder(64))
    a = mem.remember("s", {"kind": "fact", "text": "secret"})
    r = mem.forget("s", [a])
    assert r.erased_count == 1 and len(r.slots_erased) == 1
    se = r.slots_erased[0]
    assert se.atom_id == a and se.new_gen == se.old_gen + 1
    assert r.wrapped_key_size > 0 and isinstance(r.fsync, bool)


def test_byo_embedder_embed_queries_used_for_query():
    calls = {"embed": 0, "embed_queries": 0}

    class Asym:
        dim = 4
        metric = "cosine"
        model_id = "asym"

        def embed(self, texts):
            calls["embed"] += 1
            return [[1.0, 0.0, 0.0, 0.0] for _ in texts]

        def embed_queries(self, texts):
            calls["embed_queries"] += 1
            return [[1.0, 0.0, 0.0, 0.0] for _ in texts]

    mem = mem_db()
    mem.create_region("r", Asym())
    mem.remember("r", {"kind": "fact", "text": "doc"})  # passage side -> embed
    mem.recall("r", text="q", k=1)  # query side -> embed_queries
    assert calls["embed_queries"] >= 1
