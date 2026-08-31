"""Memory engine: regions, remember/recall, links, eviction, forgetting, BYO embedders."""

import os
import tempfile
import threading
import time

import pytest

import citadeldb


def mem_db(**kw):
    return citadeldb.connect(key="k", **kw).memory()


def region(mem, name="r", dim=64):
    mem.create_region(name, citadeldb.MockEmbedder(dim))
    return name


@pytest.mark.parametrize(
    "method",
    [
        "create_region",
        "create_encrypted_region",
        "attach_existing_region",
        "reembed_region",
    ],
)
def test_memory_regions_require_an_explicit_embedder(tmp_path, method):
    db = citadeldb.connect(
        str(tmp_path / f"{method}.cdl"), key="k", create=True, region_keys=True
    )
    mem = db.memory()
    with pytest.raises(TypeError, match="embedder"):
        getattr(mem, method)("missing")
    assert mem.region("missing") is None


@pytest.mark.parametrize("dim", [True, -1, 0, 65_536])
def test_mock_embedder_rejects_dimensions_the_memory_format_cannot_store(dim):
    with pytest.raises(ValueError, match="positive integer"):
        citadeldb.MockEmbedder(dim)


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
    assert sky[0].importance == pytest.approx(0.0)
    assert sky[0].relevance is not None
    assert sky[0].distance is not None
    assert sky[0].graph_depth is None


def test_non_replacing_keyed_remember_reports_insert_and_replay():
    mem = mem_db()
    region(mem)
    atom = {"kind": "fact", "text": "stable"}

    first = mem.remember_if_absent_keyed("r", atom, "request-1")
    replay = mem.remember_if_absent_keyed("r", atom, "request-1")
    assert isinstance(first, citadeldb.memory.RememberOutcome)
    assert first.inserted is True
    assert replay.inserted is False
    assert replay.id == first.id

    with pytest.raises(citadeldb.DataError, match="idempotency key"):
        mem.remember_if_absent_keyed(
            "r", {"kind": "fact", "text": "changed"}, "request-1"
        )
    assert mem.fetch_one("r", first.id).text == "stable"


def test_non_replacing_keyed_batch_is_atomic_and_preserves_order():
    mem = mem_db()
    region(mem)
    entries = [
        ({"kind": "fact", "text": "first"}, "batch-1"),
        ({"kind": "fact", "text": "second"}, "batch-2"),
    ]

    inserted = mem.remember_if_absent_keyed_batch("r", entries)
    replayed = mem.remember_if_absent_keyed_batch("r", entries)
    assert [outcome.inserted for outcome in inserted] == [True, True]
    assert [outcome.inserted for outcome in replayed] == [False, False]
    assert [outcome.id for outcome in replayed] == [outcome.id for outcome in inserted]

    conflicting = [
        ({"kind": "fact", "text": "first"}, "batch-1"),
        ({"kind": "fact", "text": "never written"}, "batch-2"),
    ]
    with pytest.raises(citadeldb.DataError, match="idempotency key"):
        mem.remember_if_absent_keyed_batch("r", conflicting)
    assert mem.fetch_one("r", inserted[1].id).text == "second"


def test_cancel_from_another_python_thread_interrupts_a_memory_batch():
    entered = threading.Event()

    class BlockingEmbedder:
        dim = 8
        metric = "cosine"
        model_id = "blocking-test"

        def embed_with_cancel(self, texts, cancel_token):
            entered.set()
            deadline = time.monotonic() + 5
            while not cancel_token.is_cancelled and time.monotonic() < deadline:
                time.sleep(0.001)
            cancel_token.check()
            return [[0.0] * self.dim for _ in texts]

    db = citadeldb.connect(key="k")
    mem = db.memory()
    mem.create_region("r", BlockingEmbedder())
    token = citadeldb.CancelToken()
    db.set_cancel(token)

    def cancel_during_embedding():
        assert entered.wait(5), "memory operation never reached the embedder"
        token.cancel()

    stopper = threading.Thread(target=cancel_during_embedding)
    stopper.start()
    with pytest.raises(citadeldb.OperationalError, match="cancel"):
        mem.remember_batch("r", [{"kind": "fact", "text": "item"}])
    stopper.join()

    db.set_cancel(None)
    assert mem.count("r", "fact") == 0


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
    expires_at = 4_000_000_000_000_000
    i = mem.remember(
        "r",
        {
            "kind": "fact",
            "text": "x",
            "payload": {"v": 1},
            "confidence": 0.375,
            "created_at": -10,
            "expires_at": expires_at,
        },
    )
    fetched = mem.fetch_one("r", i)
    assert fetched.payload == {"v": 1}
    assert fetched.importance == pytest.approx(0.0)
    assert fetched.confidence == pytest.approx(0.375)
    assert fetched.relevance is None
    assert fetched.distance is None
    assert fetched.graph_depth is None
    assert fetched.created_at == -10
    assert fetched.expires_at == expires_at
    assert mem.fetch_last("r", "fact").id == i
    missing = i + 10_000
    exact = mem.fetch_by_ids("r", [i, missing])
    assert exact[0].id == i
    assert exact[1] is None
    payload = {"v": 2, "nested": [1, 2, 3]}
    assert mem.update_atom_payload("r", i, payload) is True
    assert mem.update_atom_payload("r", i, payload) is False
    assert mem.fetch_one("r", i).payload == {"v": 2, "nested": [1, 2, 3]}
    assert len(mem.fetch("r", "fact")) == 1


def test_links_and_edges():
    mem = mem_db()
    region(mem)
    a = mem.remember("r", {"kind": "fact", "text": "a"})
    b = mem.remember("r", {"kind": "fact", "text": "b"})
    mem.link("r", a, b, "refines", weight=0.7)
    edges = mem.fetch_edges("r", src=a)
    assert len(edges) == 1
    e = edges[0]
    assert e["src"] == a and e["dst"] == b and e["kind"] == "refines"
    assert e["weight"] == pytest.approx(0.7)
    mem.link("r", a, b, "depends_on")
    with pytest.raises(citadeldb.IntegrityError):  # cycle on an acyclic kind
        mem.link("r", b, a, "depends_on")
    with pytest.raises(ValueError):
        mem.link("r", a, b, "bogus")

    region(mem, "other")
    foreign = mem.remember("other", {"kind": "fact", "text": "foreign"})
    with pytest.raises(citadeldb.DataError):
        mem.link("r", a, foreign, "refines")
    assert mem.fetch_edges("r", src=a, kind="refines", limit=1) == [e]


def test_summary_pages_expose_the_next_kind_cursor():
    mem = mem_db()
    region(mem)
    for kind in ("alpha", "beta", "gamma"):
        mem.remember("r", {"kind": kind, "text": kind})

    first = mem.summarize("r", 0, limit=2)
    assert first["total"] == 3
    assert [entry["kind"] for entry in first["kinds"]] == ["alpha", "beta"]
    assert first["next_after_kind"] == "beta"

    second = mem.summarize("r", 0, after_kind=first["next_after_kind"], limit=2)
    assert second["total"] == 3
    assert [entry["kind"] for entry in second["kinds"]] == ["gamma"]
    assert second["next_after_kind"] is None


def test_evict_summarize_and_immutable():
    mem = mem_db()
    region(mem)
    for n in range(5):
        mem.remember(
            "r",
            {
                "kind": "fact",
                "text": f"item {n}",
                "importance": 0.0,
                "confidence": 0.0,
            },
        )
    mem.remember(
        "r",
        {
            "kind": "fact",
            "text": "keep",
            "immutable": True,
            "importance": 0.0,
            "confidence": 0.0,
        },
    )
    summ = mem.summarize("r", 0)
    assert summ["total"] == 6
    assert any(k["kind"] == "fact" and k["count"] == 6 for k in summ["kinds"])
    removed = mem.evict("r", citadeldb.EvictionPolicy.low_importance(0.5, 0.5))
    assert removed == 5  # the immutable atom survives
    assert mem.count("r", "fact") == 1


def test_evict_expired_removes_only_lapsed_ttl():
    mem = mem_db()
    region(mem)
    mem.remember("r", {"kind": "fact", "text": "lapsed", "expires_at": 1})
    mem.remember("r", {"kind": "fact", "text": "kept"})
    removed = mem.evict("r", citadeldb.EvictionPolicy.expired())
    assert removed == 1
    assert mem.count("r", "fact") == 1


def test_byo_python_embedder():
    class Bucketed:
        dim = 8
        metric = "cosine"
        model_id = "byo-test"

        def embed_with_cancel(self, texts, cancel_token):
            if cancel_token is not None:
                cancel_token.check()
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


def test_legacy_only_python_model_callbacks_are_rejected():
    class LegacyEmbedder:
        dim = 8
        metric = "cosine"
        model_id = "legacy-embedder"

        def embed(self, texts):
            return [[0.0] * self.dim for _ in texts]

    class LegacyReranker:
        model_id = "legacy-reranker"

        def rerank(self, query, passages):
            return [0.0] * len(passages)

    mem = mem_db()
    with pytest.raises(TypeError, match="embed_with_cancel"):
        mem.create_region("legacy", LegacyEmbedder())
    with pytest.raises(TypeError, match="rerank_with_cancel"):
        mem.set_reranker(LegacyReranker())


@pytest.mark.parametrize(
    "attribute", ["embed_with_cancel", "embed_queries_with_cancel"]
)
def test_region_rejects_a_non_callable_embedder_method(attribute):
    class Invalid:
        dim = 8
        metric = "cosine"
        model_id = "invalid"

        def embed_with_cancel(self, texts, cancel_token):
            return [[0.0] * self.dim for _ in texts]

        def embed_queries_with_cancel(self, texts, cancel_token):
            return self.embed_with_cancel(texts, cancel_token)

    setattr(Invalid, attribute, None)
    mem = citadeldb.connect(key="k").memory()

    with pytest.raises(TypeError, match=attribute):
        mem.create_region("r", Invalid())
    assert mem.region("r") is None


@pytest.mark.parametrize(
    ("attribute", "value", "message"),
    [
        ("dim", True, "positive integer"),
        ("dim", -1, "positive integer"),
        ("dim", 0, "positive integer"),
        ("dim", 65_536, "65535"),
        ("metric", "unknown", "unknown embedding metric"),
        ("model_id", "  ", "nonblank"),
        ("model_id", "default", "unknown.*default"),
        ("model_id", "UNKNOWN", "unknown.*default"),
    ],
)
def test_region_rejects_invalid_embedder_metadata(attribute, value, message):
    class Invalid:
        dim = 8
        metric = "cosine"
        model_id = "invalid"

        def embed_with_cancel(self, texts, cancel_token):
            return [[0.0] * self.dim for _ in texts]

    setattr(Invalid, attribute, value)
    mem = citadeldb.connect(key="k").memory()

    with pytest.raises(ValueError, match=message):
        mem.create_region("r", Invalid())
    assert mem.region("r") is None


def test_evolve():
    mem = mem_db()
    region(mem)
    a = mem.remember("r", {"kind": "fact", "text": "red green blue"})
    mem.remember("r", {"kind": "fact", "text": "red green yellow"})
    rep = mem.evolve("r", a, 5, 2.0)
    assert "links_added" in rep and "importance" in rep


def test_encrypted_forget_verify():
    path = os.path.join(tempfile.mkdtemp(), "e.cdl")
    mem = citadeldb.connect(path, key="k", create=True, region_keys=True).memory()
    mem.create_encrypted_region("s", citadeldb.MockEmbedder(64))
    a = mem.remember("s", {"kind": "fact", "text": "secret"})
    assert mem.verify("s", [a])[0].verdict == "authentic"
    r = mem.forget("s", [a])
    assert r.cryptographic_erasure is True and r.erased_count == 1 and r.algorithm
    assert mem.verify("s", [a])[0].verdict in ("missing", "key_erased")


def test_forget_cascade_is_opt_in():
    mem = mem_db()
    region(mem)

    root = mem.remember("r", {"kind": "turn", "text": "targeted root"})
    dependent = mem.remember("r", {"kind": "fact", "text": "targeted dependent"})
    mem.link("r", dependent, root, "derived_from")
    mem.forget("r", [root])
    assert mem.fetch_one("r", root) is None
    assert mem.fetch_one("r", dependent) is not None

    root = mem.remember("r", {"kind": "turn", "text": "cascade root"})
    dependent = mem.remember("r", {"kind": "fact", "text": "cascade dependent"})
    mem.link("r", dependent, root, "derived_from")
    receipt = mem.forget("r", [root], cascade_dependents=True)
    assert receipt.rows_deleted == 2
    assert mem.fetch_one("r", root) is None
    assert mem.fetch_one("r", dependent) is None


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
    b = mem.remember("r", {"kind": "fact", "text": "beta", "importance": 0.8})
    mem.link("r", a, b, "derived_from")
    opts = citadeldb.RecallOptions(
        weights=(0.5, 0.2, 0.2, 0.1), as_of_micros=0, graph_expand=(2, ["derived_from"])
    )
    hits = mem.recall("r", text="alpha", k=1, options=opts)
    by_id = {h.id: h for h in hits}
    assert {a, b} <= by_id.keys()
    assert by_id[a].relevance is not None
    assert by_id[a].distance is not None
    assert by_id[a].graph_depth is None
    assert by_id[b].importance == pytest.approx(0.8)
    assert by_id[b].relevance is None
    assert by_id[b].distance is None
    assert by_id[b].graph_depth == 1


def test_profile_and_unlink_expose_the_core_graph_operations():
    mem = mem_db()
    region(mem)
    source = mem.remember("r", {"kind": "fact", "text": "alpha"})
    derived = mem.remember("r", {"kind": "fact", "text": "beta"})
    mem.link("r", source, derived, "derived_from")

    profile = mem.profile(
        "r",
        text="alpha",
        k=1,
        options=citadeldb.RecallOptions(graph_expand=(1, ["derived_from"])),
        edge_limit=10,
    )
    assert {hit.id for hit in profile["atoms"]} == {source, derived}
    assert profile["edges"] == [
        {
            "src": source,
            "dst": derived,
            "kind": "derived_from",
            "weight": 1.0,
            "evidence": None,
        }
    ]
    assert profile["edges_truncated"] is False

    assert mem.unlink("r", source, derived, "derived_from") is True
    assert mem.unlink("r", source, derived, "derived_from") is False
    assert mem.fetch_edges("r", src=source) == []


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

        def rerank_with_cancel(self, query, passages, cancel_token):
            if cancel_token is not None:
                cancel_token.check()
            return [float(len(p)) for p in passages]  # prefer the longest passage

    mem = mem_db()
    region(mem)
    pure = mem.remember("r", {"kind": "fact", "text": "alpha"})
    long_ = mem.remember("r", {"kind": "fact", "text": "alpha alpha beta gamma"})
    assert mem.recall("r", text="alpha", k=2)[0].id == pure  # fusion: pure match first
    mem.set_reranker(ByLength(), strategy="replace")
    assert mem.recall("r", text="alpha", k=2)[0].id == long_  # ByLength flips to the longer


@pytest.mark.parametrize("rrf_k", [0.0, -1.0, float("nan"), float("inf")])
def test_set_reranker_rejects_an_invalid_rrf_constant_before_installing(rrf_k):
    mem = mem_db()
    with pytest.raises(ValueError, match="finite and greater than zero"):
        mem.set_reranker(citadeldb.MockReranker(), rrf_k=rrf_k)


@pytest.mark.parametrize("model_id", ["", "   "])
def test_set_reranker_rejects_invalid_python_metadata(model_id):
    class Invalid:
        def rerank_with_cancel(self, query, passages, cancel_token):
            return [0.0] * len(passages)

    Invalid.model_id = model_id
    mem = mem_db()
    with pytest.raises(ValueError, match="nonblank"):
        mem.set_reranker(Invalid())


def test_set_reranker_rejects_a_non_callable_python_method():
    class Invalid:
        model_id = "invalid"
        rerank_with_cancel = None

    mem = mem_db()
    with pytest.raises(TypeError, match="callable rerank_with_cancel"):
        mem.set_reranker(Invalid())


def test_python_reranker_rejects_non_finite_scores():
    class Invalid:
        model_id = "invalid-score"

        def rerank_with_cancel(self, query, passages, cancel_token):
            return [float("nan")] * len(passages)

    mem = mem_db()
    region(mem)
    mem.remember("r", {"kind": "fact", "text": "alpha"})
    mem.set_reranker(Invalid(), strategy="replace")
    with pytest.raises(citadeldb.OperationalError, match="non-finite score"):
        mem.recall("r", text="alpha", k=1)


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

        def embed_with_cancel(self, texts, cancel_token):
            if cancel_token is not None:
                cancel_token.check()
            calls["embed"] += 1
            return [[1.0, 0.0, 0.0, 0.0] for _ in texts]

        def embed_queries_with_cancel(self, texts, cancel_token):
            if cancel_token is not None:
                cancel_token.check()
            calls["embed_queries"] += 1
            return [[1.0, 0.0, 0.0, 0.0] for _ in texts]

    mem = mem_db()
    mem.create_region("r", Asym())
    mem.remember("r", {"kind": "fact", "text": "doc"})  # passage side -> embed
    mem.recall("r", text="q", k=1)  # query side -> embed_queries
    assert calls["embed_queries"] >= 1


def test_recall_excludes_superseded_unless_opted_in():
    mem = mem_db()
    region(mem)
    old = mem.remember("r", {"kind": "fact", "text": "alpha old value"})
    new = mem.remember("r", {"kind": "fact", "text": "alpha new value"})
    mem.link("r", new, old, "supersedes")

    default_ids = {h.id for h in mem.recall("r", text="alpha", k=10)}
    assert old not in default_ids, "a superseded atom is hidden by default"
    assert new in default_ids

    opts = citadeldb.RecallOptions(include_superseded=True)
    opted_in = {h.id for h in mem.recall("r", text="alpha", k=10, options=opts)}
    assert old in opted_in, "include_superseded must bring the old atom back"
