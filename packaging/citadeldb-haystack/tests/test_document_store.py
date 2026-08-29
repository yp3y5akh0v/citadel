import citadeldb
import pytest
from _embedder import DeterministicTextEmbedder
from citadeldb_haystack import CitadelDocumentStore
from citadeldb_haystack.document_store import KIND, _model_id
from haystack import Pipeline
from haystack.dataclasses import Document
from haystack.document_stores.errors import DuplicateDocumentError
from haystack.document_stores.types import DuplicatePolicy
from haystack.utils import Secret

DIM = 8


EMBEDDER = DeterministicTextEmbedder()


def vec(axis: int) -> list[float]:
    v = [0.0] * DIM
    v[axis] = 1.0
    return v


@pytest.fixture()
def store(tmp_path):
    return CitadelDocumentStore(
        str(tmp_path / "d.cdl"), "pw", embedder=EMBEDDER, dim=DIM
    )


def test_count_does_not_materialize_document_content(store):
    class CountOnly:
        def count(self, region, kind):
            assert region == store._region and kind == KIND
            return 37

        def fetch(self, *args, **kwargs):
            raise AssertionError("count must not fetch or decrypt documents")

    store._mem = CountOnly()
    assert store.count_documents() == 37


@pytest.fixture()
def reference():
    """The store whose behaviour the protocol is defined by."""
    from haystack.document_stores.in_memory import InMemoryDocumentStore

    return InMemoryDocumentStore()


def test_satisfies_the_protocol(store):
    """DocumentStore is not runtime_checkable, so conformance is by shape."""
    for name in (
        "to_dict",
        "from_dict",
        "count_documents",
        "filter_documents",
        "write_documents",
        "delete_documents",
    ):
        assert callable(getattr(store, name)), name


def test_default_similarity_matches_the_reference(store, reference):
    assert store.embedding_similarity_function == "dot_product"
    assert (
        store.embedding_similarity_function == reference.embedding_similarity_function
    )


def test_an_equality_filter_returns_the_matching_documents(store):
    """Exercises the pushed-down filter path, which conformance never hits."""
    store.write_documents(
        [
            Document(id="a", content="1", meta={"cat": "x", "n": 1}),
            Document(id="b", content="2", meta={"cat": "y", "n": 2}),
        ]
    )
    got = store.filter_documents({"field": "meta.cat", "operator": "==", "value": "y"})
    assert [d.id for d in got] == ["b"]


def test_an_and_of_equalities_returns_the_matching_documents(store):
    store.write_documents(
        [
            Document(id="a", content="1", meta={"cat": "x", "n": 1}),
            Document(id="b", content="2", meta={"cat": "x", "n": 2}),
        ]
    )
    got = store.filter_documents(
        {
            "operator": "AND",
            "conditions": [
                {"field": "meta.cat", "operator": "==", "value": "x"},
                {"field": "meta.n", "operator": "==", "value": 2},
            ],
        }
    )
    assert [d.id for d in got] == ["b"]


def test_skip_leaves_the_original_and_reports_fewer_written(store):
    store.write_documents([Document(id="a", content="first")])
    written = store.write_documents(
        [Document(id="a", content="second"), Document(id="b", content="new")],
        policy=DuplicatePolicy.SKIP,
    )
    assert written == 1
    assert (
        store.filter_documents({"field": "id", "operator": "==", "value": "a"})[
            0
        ].content
        == "first"
    )


def test_overwrite_replaces_and_counts_every_input(store):
    store.write_documents([Document(id="a", content="first")])
    assert (
        store.write_documents(
            [Document(id="a", content="second")], policy=DuplicatePolicy.OVERWRITE
        )
        == 1
    )
    assert store.count_documents() == 1


def test_duplicate_ids_within_one_call_keep_the_last(store):
    """The reference assigns storage[doc.id], so the last write wins."""
    store.write_documents(
        [Document(id="d", content="one"), Document(id="d", content="two")],
        policy=DuplicatePolicy.OVERWRITE,
    )
    assert store.count_documents() == 1
    assert store.filter_documents()[0].content == "two"


def test_fail_raises_before_anything_is_written(store):
    store.write_documents([Document(id="a", content="first")])
    with pytest.raises(DuplicateDocumentError):
        store.write_documents(
            [Document(id="b", content="new"), Document(id="a", content="dup")]
        )
    assert store.count_documents() == 1, "a failed batch must not half-apply"


def test_non_documents_are_refused(store):
    with pytest.raises(ValueError, match="list of Documents"):
        store.write_documents(["not a document"])


# These compare against the reference directly, not against its docstring.


def test_overwrite_with_duplicates_counts_every_input(store, reference):
    """Two copies of one id is still two documents in input."""
    docs = [Document(id="d", content="one"), Document(id="d", content="two")]
    assert store.write_documents(docs, policy=DuplicatePolicy.OVERWRITE) == (
        reference.write_documents(docs, policy=DuplicatePolicy.OVERWRITE)
    )


def test_skip_with_duplicates_counts_like_the_reference(store, reference):
    docs = [Document(id="d", content="one"), Document(id="d", content="two")]
    assert store.write_documents(docs, policy=DuplicatePolicy.SKIP) == (
        reference.write_documents(docs, policy=DuplicatePolicy.SKIP)
    )


def test_fail_raises_on_a_duplicate_within_one_batch(store, reference):
    """A second copy collides even though neither was stored before."""
    docs = [Document(id="d", content="one"), Document(id="d", content="two")]
    with pytest.raises(DuplicateDocumentError):
        reference.write_documents(docs, policy=DuplicatePolicy.FAIL)
    with pytest.raises(DuplicateDocumentError):
        store.write_documents(docs, policy=DuplicatePolicy.FAIL)


def test_skip_of_a_fresh_duplicate_stores_the_first(store):
    """SKIP drops the later copy, so the earlier one is what survives."""
    store.write_documents(
        [Document(id="d", content="first"), Document(id="d", content="second")],
        policy=DuplicatePolicy.SKIP,
    )
    assert store.count_documents() == 1
    assert store.filter_documents()[0].content == "first"


def test_embeddings_round_trip(store):
    """The conformance suite compares whole Documents, embeddings included."""
    store.write_documents([Document(id="e", content="x", embedding=vec(3))])
    assert store.filter_documents()[0].embedding == vec(3)


def test_documents_without_embeddings_are_storable(store):
    store.write_documents([Document(id="n", content="no vector")])
    got = store.filter_documents()[0]
    assert got.embedding is None and got.content == "no vector"


def test_nested_metadata_round_trips(store):
    store.write_documents([Document(id="m", content="x", meta={"o": {"i": [1, 2]}})])
    assert store.filter_documents()[0].meta["o"] == {"i": [1, 2]}


def test_a_wrong_width_embedding_names_the_fix(store):
    with pytest.raises(ValueError, match="dim=3"):
        store.write_documents(
            [Document(id="w", content="x", embedding=[1.0, 2.0, 3.0])]
        )


def test_delete_destroys_keys(store):
    store.write_documents([Document(id="s", content="secret")])
    store.delete_documents(["s"])
    assert store.count_documents() == 0


def test_delete_tolerates_unknown_and_duplicate_ids(store):
    store.write_documents([Document(id="a", content="x")])
    store.delete_documents(["missing", "a", "a"])
    assert store.count_documents() == 0


def test_delete_all_empties_the_store(store):
    store.write_documents([Document(content="a"), Document(content="b")])
    assert store.delete_all() == 2
    assert store.count_documents() == 0


def test_a_literal_passphrase_refuses_to_serialize(store):
    """Haystack writes pipelines to disk; a passphrase must not travel."""
    with pytest.raises(ValueError, match="Cannot serialize"):
        store.to_dict()


def test_an_env_var_passphrase_serializes_by_reference(tmp_path, monkeypatch):
    monkeypatch.setenv("CITADEL_TEST_KEY", "pw")
    s = CitadelDocumentStore(
        str(tmp_path / "s.cdl"),
        Secret.from_env_var("CITADEL_TEST_KEY"),
        embedder=EMBEDDER,
        dim=DIM,
    )
    data = s.to_dict()
    assert data["init_parameters"]["key"]["env_vars"] == ["CITADEL_TEST_KEY"]
    assert "pw" not in str(data), "the passphrase itself must never be serialized"


def test_round_trip_through_from_dict(tmp_path, monkeypatch):
    try:
        from haystack.core.serialization import allow_deserialization_module
    except ImportError:  # Haystack 2.x predates the deserialization allowlist.
        pass
    else:
        allow_deserialization_module(DeterministicTextEmbedder.__module__)

    monkeypatch.setenv("CITADEL_TEST_KEY", "pw")
    path = str(tmp_path / "r.cdl")
    s = CitadelDocumentStore(
        path,
        Secret.from_env_var("CITADEL_TEST_KEY"),
        embedder=EMBEDDER,
        dim=DIM,
        embedding_similarity_function="dot_product",
    )
    s.write_documents([Document(id="a", content="kept")])
    revived = CitadelDocumentStore.from_dict(s.to_dict())
    assert revived.count_documents() == 1
    assert revived.embedding_similarity_function == "dot_product"


def test_a_missing_passphrase_is_refused(tmp_path, monkeypatch):
    monkeypatch.setenv("CITADEL_EMPTY_KEY", "")
    with pytest.raises(ValueError, match="passphrase"):
        CitadelDocumentStore(
            str(tmp_path / "k.cdl"),
            Secret.from_env_var("CITADEL_EMPTY_KEY"),
            embedder=EMBEDDER,
            dim=DIM,
        )


def test_an_embedder_is_required_before_a_vault_is_created(tmp_path):
    with pytest.raises(TypeError, match="embedder"):
        CitadelDocumentStore(str(tmp_path / "no-model.cdl"), "pw", dim=DIM)
    with pytest.raises(ValueError, match="embedder"):
        CitadelDocumentStore(
            str(tmp_path / "invalid-model.cdl"), "pw", embedder=object(), dim=DIM
        )
    with pytest.raises(ValueError, match="embedding_similarity_function"):
        CitadelDocumentStore(
            str(tmp_path / "invalid-metric.cdl"),
            "pw",
            embedder=EMBEDDER,
            dim=DIM,
            embedding_similarity_function="l2",
        )
    with pytest.raises(ValueError, match="65535"):
        CitadelDocumentStore(
            str(tmp_path / "wide-model.cdl"), "pw", embedder=EMBEDDER, dim=65_536
        )
    assert list(tmp_path.iterdir()) == []


def test_an_unnamed_embedder_requires_explicit_provenance(tmp_path):
    class UnnamedEmbedder:
        def run(self, *, text):
            return {"embedding": vec(0)}

    unnamed = UnnamedEmbedder()
    with pytest.raises(ValueError, match="model_id"):
        CitadelDocumentStore(
            str(tmp_path / "missing.cdl"), "pw", embedder=unnamed, dim=DIM
        )
    with pytest.raises(ValueError, match="model_id"):
        CitadelDocumentStore(
            str(tmp_path / "invalid.cdl"),
            "pw",
            embedder=unnamed,
            dim=DIM,
            model_id=123,
        )
    assert list(tmp_path.iterdir()) == []

    store = CitadelDocumentStore(
        str(tmp_path / "named.cdl"),
        "pw",
        embedder=unnamed,
        dim=DIM,
        model_id="deployment-a",
    )
    assert store.count_documents() == 0


def test_model_id_attribute_is_preferred_and_reserved_names_are_rejected():
    named = type(
        "NamedEmbedder",
        (),
        {"model_id": "protocol-id", "model": "framework-id"},
    )()
    assert _model_id(named, None) == "protocol-id"
    assert _model_id(object(), "  deployment-a  ") == "deployment-a"

    fallback = type(
        "FallbackEmbedder",
        (),
        {"model_id": "default", "model": "framework-id"},
    )()
    assert _model_id(fallback, None) == "framework-id"

    for reserved in ("", "unknown", "DEFAULT"):
        invalid = type("InvalidEmbedder", (), {"model_id": reserved})()
        with pytest.raises(ValueError, match="model_id"):
            _model_id(invalid, None)
        with pytest.raises(ValueError, match="model_id"):
            _model_id(object(), reserved)


def test_embedding_retrieval_ranks_and_scores(store):
    store.write_documents(
        [
            Document(id="near", content="a", embedding=vec(0)),
            Document(id="far", content="b", embedding=vec(7)),
        ]
    )
    got = store.embedding_retrieval(vec(0), top_k=2)
    assert [d.id for d in got] == ["near", "far"]
    assert got[0].score is not None and 0.0 <= got[0].score <= 1.0


def test_retrieval_matches_haystack_score_and_embedding_options(tmp_path):
    s = CitadelDocumentStore(
        str(tmp_path / "dot.cdl"),
        "pw",
        embedder=EMBEDDER,
        dim=DIM,
        embedding_similarity_function="dot_product",
    )
    s.write_documents(
        [
            Document(id="one", content="one", embedding=vec(0)),
            Document(id="two", content="two", embedding=[2.0] + [0.0] * (DIM - 1)),
        ]
    )

    raw = s.embedding_retrieval(vec(0), top_k=2)
    assert [d.id for d in raw] == ["two", "one"]
    assert raw[0].score == pytest.approx(2.0)
    assert all(d.embedding is None for d in raw)

    scaled = s.embedding_retrieval(
        vec(0), top_k=1, scale_score=True, return_embedding=True
    )
    assert scaled[0].score == pytest.approx(0.50499983)
    assert scaled[0].embedding == [2.0] + [0.0] * (DIM - 1)


def test_automatically_generated_embedding_can_be_returned(tmp_path):
    s = CitadelDocumentStore(
        str(tmp_path / "generated.cdl"), "pw", embedder=EMBEDDER, dim=DIM
    )
    s.write_documents([Document(id="generated", content="generate me")])
    query = EMBEDDER.run(text="generate me")["embedding"]
    found = s.embedding_retrieval(query, top_k=1, return_embedding=True)
    assert found[0].embedding == query


def test_returning_a_generated_embedding_does_not_run_the_model_again(tmp_path):
    class ChangingEmbedder:
        model = "changing-model"

        def __init__(self):
            self.calls = 0

        def run(self, *, text):
            self.calls += 1
            return {"embedding": [float(self.calls)] + [0.0] * (DIM - 1)}

    embedder = ChangingEmbedder()
    s = CitadelDocumentStore(
        str(tmp_path / "stable-generated.cdl"),
        "pw",
        embedder=embedder,
        dim=DIM,
    )
    s.write_documents([Document(id="generated", content="generate once")])
    assert embedder.calls == 1

    found = s.embedding_retrieval(vec(0), top_k=1, return_embedding=True)
    assert found[0].embedding == [1.0] + [0.0] * (DIM - 1)
    assert embedder.calls == 1


def test_store_warms_the_embedder_only_when_it_has_to_embed(tmp_path):
    class WarmEmbedder:
        model = "warm-model"

        def __init__(self):
            self.ready = False
            self.warm_calls = 0

        def warm_up(self):
            self.warm_calls += 1
            self.ready = True

        def run(self, *, text):
            if not self.ready:
                raise RuntimeError("call warm_up")
            return {"embedding": vec(0)}

    embedder = WarmEmbedder()
    s = CitadelDocumentStore(
        str(tmp_path / "warm.cdl"), "pw", embedder=embedder, dim=DIM
    )
    assert embedder.warm_calls == 0
    s.write_documents([Document(id="supplied", content="text", embedding=vec(0))])
    assert embedder.warm_calls == 0
    s.write_documents([Document(id="ready", content="text")])
    s.write_documents([Document(id="again", content="text")])
    assert embedder.warm_calls == 1
    assert s.count_documents() == 3


def test_wrong_passphrase_is_rejected_before_embedder_warm_up(tmp_path):
    import gc

    path = str(tmp_path / "authenticated-first.cdl")
    first = CitadelDocumentStore(path, "right", embedder=EMBEDDER, dim=DIM)
    first.write_documents([Document(id="seed", content="seed", embedding=vec(0))])
    del first
    gc.collect()

    class TrackingEmbedder:
        model = "tracking-model"

        def __init__(self):
            self.warm_calls = 0

        def warm_up(self):
            self.warm_calls += 1

        def run(self, *, text):
            return {"embedding": vec(0)}

    embedder = TrackingEmbedder()
    with pytest.raises(citadeldb.EncryptionError):
        CitadelDocumentStore(path, "wrong", embedder=embedder, dim=DIM)
    assert embedder.warm_calls == 0


def test_bulk_write_and_delete_scan_encrypted_payloads_once(tmp_path):
    s = CitadelDocumentStore(
        str(tmp_path / "bulk.cdl"), "pw", embedder=EMBEDDER, dim=DIM
    )
    inner = s._mem

    class CountingMemory:
        def __init__(self):
            self.fetches = 0

        def __getattr__(self, name):
            return getattr(inner, name)

        def fetch(self, *args, **kwargs):
            self.fetches += 1
            return inner.fetch(*args, **kwargs)

    counted = CountingMemory()
    s._mem = counted
    docs = [Document(id=f"bulk-{i}", content=f"text {i}") for i in range(12)]
    s.write_documents(docs)
    assert counted.fetches == 1

    counted.fetches = 0
    s.delete_documents([doc.id for doc in docs])
    assert counted.fetches == 1


def test_embedding_retrieval_respects_filters(store):
    store.write_documents(
        [
            Document(id="x", content="a", meta={"cat": "x"}, embedding=vec(0)),
            Document(id="y", content="b", meta={"cat": "y"}, embedding=vec(0)),
        ]
    )
    got = store.embedding_retrieval(
        vec(0), top_k=5, filters={"field": "meta.cat", "operator": "==", "value": "y"}
    )
    assert [d.id for d in got] == ["y"]


def test_embedding_retrieval_respects_top_k(store):
    store.write_documents(
        [Document(id=f"d{i}", content="t", embedding=vec(i % DIM)) for i in range(6)]
    )
    assert len(store.embedding_retrieval(vec(0), top_k=2)) == 2


def test_writing_nothing_is_not_an_error(store):
    assert store.write_documents([]) == 0
    assert store.count_documents() == 0


def test_filtering_an_empty_store_is_empty(store):
    assert store.filter_documents() == []
    assert (
        store.filter_documents({"field": "meta.x", "operator": "==", "value": 1}) == []
    )


def test_an_or_filter_is_not_narrowed_by_pushdown(store):
    """Pushing a leaf of an OR would drop rows the filter keeps."""
    store.write_documents(
        [
            Document(id="a", content="1", meta={"cat": "x"}),
            Document(id="b", content="2", meta={"cat": "y"}),
        ]
    )
    got = store.filter_documents(
        {
            "operator": "OR",
            "conditions": [
                {"field": "meta.cat", "operator": "==", "value": "x"},
                {"field": "meta.cat", "operator": "==", "value": "y"},
            ],
        }
    )
    assert sorted(d.id for d in got) == ["a", "b"]


def test_a_none_valued_equality_filter_is_not_pushed(store):
    """None means absent, which containment cannot express."""
    store.write_documents(
        [
            Document(id="has", content="1", meta={"n": 1}),
            Document(id="hasnt", content="2", meta={}),
        ]
    )
    got = store.filter_documents({"field": "meta.n", "operator": "==", "value": None})
    assert [d.id for d in got] == ["hasnt"]


def test_a_document_with_no_content_is_storable(store):
    store.write_documents([Document(id="empty", content=None, meta={"k": 1})])
    got = store.filter_documents()[0]
    assert got.content is None and got.meta["k"] == 1


def test_filtering_on_a_document_field_not_metadata(store):
    store.write_documents([Document(id="wanted", content="x")])
    got = store.filter_documents({"field": "id", "operator": "==", "value": "wanted"})
    assert [d.id for d in got] == ["wanted"]


def test_a_not_filter_is_not_narrowed_by_pushdown(store):
    store.write_documents(
        [
            Document(id="a", content="1", meta={"cat": "x"}),
            Document(id="b", content="2", meta={"cat": "y"}),
        ]
    )
    got = store.filter_documents(
        {
            "operator": "NOT",
            "conditions": [{"field": "meta.cat", "operator": "==", "value": "x"}],
        }
    )
    assert [d.id for d in got] == ["b"]


def test_retrieval_on_an_empty_store_is_empty(store):
    assert store.embedding_retrieval(vec(0), top_k=5) == []


def test_scores_do_not_leak_onto_stored_documents(store):
    """Mutating a shared Document affects other pipeline steps."""
    store.write_documents([Document(id="a", content="x", embedding=vec(0))])
    store.embedding_retrieval(vec(0), top_k=1)
    assert store.filter_documents()[0].score is None


def test_it_survives_a_reopen(tmp_path):
    """A region's embedder lives in memory, so a reopen must rebuild it."""
    import gc

    p = str(tmp_path / "reopen.cdl")
    first = CitadelDocumentStore(p, "pw", embedder=EMBEDDER, dim=DIM)
    first.write_documents(
        [
            Document(
                id="d1",
                content="the disk was full",
                meta={"chapter": "intro"},
                embedding=vec(3),
            )
        ]
    )
    del first
    gc.collect()

    again = CitadelDocumentStore(p, "pw", embedder=EMBEDDER, dim=DIM)
    assert again.count_documents() == 1
    got = again.filter_documents(
        {"field": "meta.chapter", "operator": "==", "value": "intro"}
    )
    assert [d.id for d in got] == ["d1"]
    assert got[0].embedding == vec(3), "the embedding did not survive the reopen"
    assert [d.id for d in again.embedding_retrieval(vec(3), top_k=1)] == ["d1"]


def test_a_wrong_passphrase_cannot_reopen(tmp_path):
    """Pins the encryption claim rather than inferring it from a reopen."""
    import gc

    import citadeldb

    p = str(tmp_path / "enc.cdl")
    first = CitadelDocumentStore(p, "right", embedder=EMBEDDER, dim=DIM)
    first.write_documents(
        [Document(id="d1", content="the disk was full", embedding=vec(3))]
    )
    del first
    gc.collect()

    with pytest.raises(citadeldb.EncryptionError):
        CitadelDocumentStore(p, "wrong", embedder=EMBEDDER, dim=DIM)


def test_concurrent_writes_all_land(tmp_path):
    """Indexing pipelines run in parallel; the engine is shared."""
    import concurrent.futures as cf

    s = CitadelDocumentStore(
        str(tmp_path / "conc.cdl"), "pw", embedder=EMBEDDER, dim=DIM
    )
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        list(
            ex.map(
                lambda i: s.write_documents(
                    [Document(id=f"c{i}", content=f"body {i}", embedding=vec(i % DIM))],
                    policy=DuplicatePolicy.OVERWRITE,
                ),
                range(40),
            )
        )
    assert s.count_documents() == 40


def test_a_nested_metadata_filter_finds_its_document(tmp_path):
    """`meta.person.name` addresses a nested value. Pushing it as one flat key
    spelled with a dot matches nothing, and the post-filter never sees the row."""
    s = CitadelDocumentStore(
        str(tmp_path / "nested.cdl"), "pw", embedder=EMBEDDER, dim=DIM
    )
    s.write_documents(
        [
            Document(
                id="n1",
                content="nested",
                meta={"person": {"name": "ada"}},
                embedding=vec(0),
            ),
            Document(
                id="n2",
                content="other",
                meta={"person": {"name": "bob"}},
                embedding=vec(1),
            ),
        ]
    )
    f = {"field": "meta.person.name", "operator": "==", "value": "ada"}
    assert [d.id for d in s.filter_documents(filters=f)] == ["n1"]
    assert [d.id for d in s.embedding_retrieval(vec(0), top_k=5, filters=f)] == ["n1"]


def test_a_numeric_metadata_filter_is_not_decided_by_the_pushdown(tmp_path):
    """Haystack's `==` is Python's, which holds 1 == 1.0; containment is type
    strict, so pushing a number would drop a row the filter keeps."""
    s = CitadelDocumentStore(
        str(tmp_path / "num.cdl"), "pw", embedder=EMBEDDER, dim=DIM
    )
    s.write_documents(
        [Document(id="i1", content="int", meta={"year": 2024}, embedding=vec(0))]
    )
    f = {"field": "meta.year", "operator": "==", "value": 2024.0}
    assert [d.id for d in s.filter_documents(filters=f)] == ["i1"]


def test_documents_without_vectors_are_embedded_by_the_store(tmp_path):
    s = CitadelDocumentStore(
        str(tmp_path / "unemb.cdl"),
        "pw",
        embedder=EMBEDDER,
        dim=DIM,
        embedding_similarity_function="cosine",
    )
    s.write_documents(
        [Document(id=f"u{i}", content=f"unembedded {i}") for i in range(20)]
    )
    s.write_documents([Document(id="real", content="embedded", embedding=vec(0))])
    assert s.count_documents() == 21
    query = EMBEDDER.run(text="unembedded 0")["embedding"]
    found = s.embedding_retrieval(query, top_k=1)
    assert [d.id for d in found] == ["u0"]
    assert len(s.filter_documents()) == 21


def test_an_unpushable_filter_survives_a_window_of_other_documents(tmp_path):
    """`in` cannot be pushed, so it is settled after ranking; the only matching
    document must still be found under a corpus that outranks it."""
    s = CitadelDocumentStore(
        str(tmp_path / "window.cdl"), "pw", embedder=EMBEDDER, dim=DIM
    )
    s.write_documents(
        [Document(id="gold", content="needle", meta={"tag": "keep"}, embedding=vec(1))]
    )
    s.write_documents(
        [
            Document(
                id=f"c{i}",
                content=f"nearer {i}",
                meta={"tag": "drop"},
                embedding=vec(0),
            )
            for i in range(400)
        ]
    )
    f = {"field": "meta.tag", "operator": "in", "value": ["keep"]}
    assert [d.id for d in s.embedding_retrieval(vec(0), top_k=1, filters=f)] == ["gold"]


def test_concurrent_overwrites_of_one_id_leave_one_document(tmp_path):
    """A document id is unique, so parallel pipelines overwriting the same one
    must supersede rather than each add a row."""
    import concurrent.futures as cf

    s = CitadelDocumentStore(
        str(tmp_path / "oneid.cdl"), "pw", embedder=EMBEDDER, dim=DIM
    )
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        list(
            ex.map(
                lambda i: s.write_documents(
                    [Document(id="same", content=f"v{i}", embedding=vec(i % DIM))],
                    policy=DuplicatePolicy.OVERWRITE,
                ),
                range(32),
            )
        )
    assert s.count_documents() == 1
    # Retrieval must not fill top_k with copies of the one document either.
    found = s.embedding_retrieval(query_embedding=vec(0), top_k=5)
    assert [d.id for d in found] == ["same"]
    s.delete_documents(["same"])
    assert s.count_documents() == 0


def test_two_stores_share_one_database_file(tmp_path):
    path = str(tmp_path / "shared.cdl")
    a = CitadelDocumentStore(path, "pw", embedder=EMBEDDER, region="a", dim=DIM)
    b = CitadelDocumentStore(path, "pw", embedder=EMBEDDER, region="b", dim=DIM)
    a.write_documents([Document(id="1", content="in a")])
    b.write_documents([Document(id="2", content="in b")])
    assert a.count_documents() == 1 and b.count_documents() == 1


def test_a_large_batch_keeps_every_document(store):
    store.write_documents(
        [Document(id=f"b{i}", content=f"body {i}") for i in range(300)]
    )
    assert store.count_documents() == 300


def test_it_serializes_inside_a_pipeline(tmp_path, monkeypatch):
    """A store is only useful if a pipeline carrying it can be written out."""
    monkeypatch.setenv("CITADEL_TEST_KEY", "pw")
    s = CitadelDocumentStore(
        str(tmp_path / "p.cdl"),
        Secret.from_env_var("CITADEL_TEST_KEY"),
        embedder=EMBEDDER,
        dim=DIM,
    )
    from haystack.components.writers import DocumentWriter

    pipe = Pipeline()
    pipe.add_component("writer", DocumentWriter(document_store=s))
    assert "CitadelDocumentStore" in str(pipe.to_dict())


async def test_async_surface_round_trips(store):
    assert await store.write_documents_async([Document(id="a", content="x")]) == 1
    assert await store.count_documents_async() == 1
    assert len(await store.filter_documents_async()) == 1
    await store.delete_documents_async(["a"])
    assert await store.count_documents_async() == 0


async def test_the_event_loop_is_not_blocked(store):
    import asyncio

    ticks = 0

    async def tick():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    ticker = asyncio.create_task(tick())
    await asyncio.sleep(0)
    await store.write_documents_async(
        [Document(id=f"L{i}", content=f"loop {i}") for i in range(40)]
    )
    await store.filter_documents_async()
    ticker.cancel()
    assert ticks > 1, "the loop made no progress during a store call"
