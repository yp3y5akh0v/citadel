import pytest
from citadeldb_langchain import CitadelVectorStore
from citadeldb_langchain.vector_store import KIND, _model_id
from langchain_core.documents import Document
from langchain_core.embeddings import DeterministicFakeEmbedding
from langchain_core.vectorstores import VectorStore

DIM = 16
MODEL_ID = "deterministic-fake-16"


def embedder():
    embedding = DeterministicFakeEmbedding(size=DIM)
    object.__setattr__(embedding, "model_name", MODEL_ID)
    return embedding


@pytest.fixture()
def store(tmp_path):
    # Each test gets its own file: a store is a corpus, not a scratch pad.
    return CitadelVectorStore(embedder(), str(tmp_path / "v.cdl"), key="pw", dim=DIM)


def test_count_does_not_materialize_document_content(store):
    class CountOnly:
        def count(self, region, kind):
            assert region == store._region and kind == KIND
            return 37

        def fetch(self, *args, **kwargs):
            raise AssertionError("count must not fetch or decrypt documents")

    store._mem = CountOnly()
    assert store.count() == 37


def test_is_a_vector_store(store):
    assert isinstance(store, VectorStore)
    assert store.embeddings is not None


def test_dimension_is_inferred_when_not_given(tmp_path):
    """A caller should not have to look up their model's width."""
    s = CitadelVectorStore(embedder(), str(tmp_path / "d.cdl"), key="pw")
    s.add_texts(["probe"])
    assert s.count() == 1


def test_add_texts_returns_ids_and_stores(store):
    ids = store.add_texts(["alpha", "beta"])
    assert len(ids) == 2 and all(ids)
    assert store.count() == 2


@pytest.mark.parametrize("returned", [1, 3])
def test_a_malformed_embedding_batch_is_rejected_without_partial_writes(
    tmp_path, returned
):
    class MalformedEmbedding:
        model_id = "malformed-batch"

        def embed_documents(self, texts):
            return [[0.0] * DIM for _ in range(returned)]

        def embed_query(self, text):
            return [0.0] * DIM

    s = CitadelVectorStore(
        MalformedEmbedding(),
        str(tmp_path / f"malformed-{returned}.cdl"),
        key="pw",
        dim=DIM,
    )
    with pytest.raises(ValueError, match=rf"{returned} vectors for 2 texts"):
        s.add_texts(["one", "two"], ids=["one", "two"])
    assert s.count() == 0


async def test_async_malformed_embedding_batch_is_rejected_without_partial_writes(
    tmp_path,
):
    class MalformedEmbedding:
        model_id = "async-malformed-batch"

        def embed_documents(self, texts):
            return [[0.0] * DIM for _ in texts]

        async def aembed_documents(self, texts):
            return [[0.0] * DIM]

        def embed_query(self, text):
            return [0.0] * DIM

    s = CitadelVectorStore(
        MalformedEmbedding(), str(tmp_path / "async-malformed.cdl"), key="pw", dim=DIM
    )
    with pytest.raises(ValueError, match="1 vectors for 2 texts"):
        await s.aadd_texts(["one", "two"], ids=["one", "two"])
    assert s.count() == 0


def test_supplied_ids_are_used(store):
    assert store.add_texts(["a"], ids=["mine"]) == ["mine"]
    assert [d.id for d in store.get_by_ids(["mine"])] == ["mine"]


def test_re_adding_an_id_replaces_it(store):
    """InMemoryVectorStore assigns store[doc_id], so a re-add replaces."""
    store.add_texts(["first"], ids=["x"])
    store.add_texts(["second"], ids=["x"])
    assert store.count() == 1
    assert store.get_by_ids(["x"])[0].page_content == "second"


def test_add_documents_forwards_document_ids(store):
    """The base class turns doc.id into the ids kwarg."""
    store.add_documents([Document(id="d1", page_content="one")])
    store.add_documents([Document(id="d1", page_content="one again")])
    assert store.count() == 1
    assert store.get_by_ids(["d1"])[0].page_content == "one again"


def test_documents_without_ids_get_generated_ones(store):
    """`add_documents` may hand over a partly-None id list."""
    ids = store.add_documents(
        [Document(id="named", page_content="a"), Document(page_content="b")]
    )
    assert "named" in ids and len(ids) == 2 and all(ids)
    assert store.count() == 2


def test_metadata_round_trips(store):
    store.add_texts(["body"], metadatas=[{"src": "f.pdf", "page": 3}], ids=["m"])
    got = store.get_by_ids(["m"])[0]
    assert got.metadata == {"src": "f.pdf", "page": 3}
    assert got.page_content == "body"


def test_mismatched_metadatas_are_refused(store):
    with pytest.raises(ValueError, match="number of metadatas"):
        store.add_texts(["a", "b"], metadatas=[{"x": 1}])


def test_mismatched_ids_are_refused(store):
    with pytest.raises(ValueError, match="number of ids"):
        store.add_texts(["a", "b"], ids=["only-one"])


def test_adding_nothing_is_not_an_error(store):
    assert store.add_texts([]) == []
    assert store.count() == 0


def test_similarity_search_finds_the_nearest(store):
    store.add_texts(["the deploy failed", "lunch on friday"], ids=["a", "b"])
    got = store.similarity_search("the deploy failed", k=1)
    assert [d.id for d in got] == ["a"]


def test_scores_are_similarity_not_distance(store):
    store.add_texts(["exactly this"], ids=["a"])
    ((_doc, score),) = store.similarity_search_with_score("exactly this", k=1)
    assert score == pytest.approx(1.0, abs=1e-3)


def test_relevance_scores_are_bounded(store):
    store.add_texts(["a", "b"], ids=["1", "2"])
    for _doc, score in store.similarity_search_with_relevance_scores("a", k=2):
        assert 0.0 <= score <= 1.0


def test_k_is_respected(store):
    store.add_texts([f"text {i}" for i in range(6)])
    assert len(store.similarity_search("text 1", k=2)) == 2


def test_k_of_zero_is_empty(store):
    store.add_texts(["a"])
    assert store.similarity_search("a", k=0) == []


def test_search_on_an_empty_store_is_empty(store):
    assert store.similarity_search("anything", k=4) == []


def test_search_by_vector(store):
    store.add_texts(["target"], ids=["t"])
    vector = store.embeddings.embed_query("target")
    assert [d.id for d in store.similarity_search_by_vector(vector, k=1)] == ["t"]


def test_metadata_filter_narrows(store):
    store.add_texts(
        ["one", "two"], metadatas=[{"cat": "x"}, {"cat": "y"}], ids=["a", "b"]
    )
    got = store.similarity_search("one", k=5, filter={"cat": "y"})
    assert [d.id for d in got] == ["b"]


def test_a_filter_matching_nothing_returns_nothing(store):
    store.add_texts(["one"], metadatas=[{"cat": "x"}], ids=["a"])
    assert store.similarity_search("one", k=5, filter={"cat": "absent"}) == []


def test_the_id_paths_read_the_region_once(store, monkeypatch):
    """The payload is only readable after decryption, so a filtered fetch cannot
    shorten the scan: one fetch per id costs ids x rows, which put get_by_ids at
    103s and delete at 170s over 4000 documents."""
    from citadeldb_langchain import vector_store as vs

    ids = store.add_texts([f"doc {i}" for i in range(50)])
    real, calls = vs._fetch, []

    def counting(*a, **kw):
        calls.append(1)
        return real(*a, **kw)

    monkeypatch.setattr(vs, "_fetch", counting)
    assert len(store.get_by_ids(ids)) == 50
    assert len(calls) == 1, f"{len(calls)} region reads for 50 ids"

    calls.clear()
    store.delete(ids)
    assert len(calls) == 1, f"{len(calls)} region reads for 50 ids"
    assert store.count() == 0


def test_get_by_ids_ignores_unknown_ids(store):
    """Contracted not to raise, and to return fewer than asked for."""
    store.add_texts(["a"], ids=["known"])
    got = store.get_by_ids(["known", "missing"])
    assert [d.id for d in got] == ["known"]


def test_get_by_ids_collapses_duplicates(store):
    store.add_texts(["a"], ids=["dup"])
    assert len(store.get_by_ids(["dup", "dup"])) == 1


def test_get_by_ids_on_nothing_is_empty(store):
    assert store.get_by_ids([]) == []


def test_delete_removes_named_ids(store):
    store.add_texts(["a", "b"], ids=["1", "2"])
    assert store.delete(["1"]) is True
    assert [d.id for d in store.get_by_ids(["1", "2"])] == ["2"]


def test_delete_with_no_ids_does_not_empty_the_store(store):
    """InMemoryVectorStore ignores an empty id list; erasure is final."""
    store.add_texts(["a", "b"])
    store.delete()
    assert store.count() == 2


def test_clear_is_the_deliberate_way_to_empty_it(store):
    store.add_texts(["a", "b"])
    assert store.clear() == 2
    assert store.count() == 0


def test_deleting_an_unknown_id_is_not_an_error(store):
    assert store.delete(["nope"]) is True


def test_a_passphrase_is_required(tmp_path):
    with pytest.raises(ValueError, match="passphrase"):
        CitadelVectorStore(embedder(), str(tmp_path / "k.cdl"), key="", dim=DIM)


def test_an_embedding_model_is_validated_before_a_vault_is_created(tmp_path):
    with pytest.raises(TypeError, match="embed_documents"):
        CitadelVectorStore(
            object(), str(tmp_path / "invalid-model.cdl"), key="pw", dim=DIM
        )
    with pytest.raises(ValueError, match="65535"):
        CitadelVectorStore(
            embedder(), str(tmp_path / "wide-model.cdl"), key="pw", dim=65_536
        )
    assert list(tmp_path.iterdir()) == []


def test_an_unnamed_embedding_requires_explicit_provenance(tmp_path):
    unnamed = DeterministicFakeEmbedding(size=DIM)
    with pytest.raises(ValueError, match="model_id"):
        CitadelVectorStore(unnamed, str(tmp_path / "missing.cdl"), key="pw", dim=DIM)
    with pytest.raises(ValueError, match="model_id"):
        CitadelVectorStore(
            unnamed,
            str(tmp_path / "invalid.cdl"),
            key="pw",
            dim=DIM,
            model_id=123,
        )
    assert list(tmp_path.iterdir()) == []

    store = CitadelVectorStore(
        unnamed,
        str(tmp_path / "named.cdl"),
        key="pw",
        dim=DIM,
        model_id="deployment-a",
    )
    assert store.count() == 0


def test_model_id_attribute_is_preferred_and_reserved_names_are_rejected():
    named = type(
        "NamedEmbedding",
        (),
        {"model_id": "protocol-id", "model": "framework-id"},
    )()
    assert _model_id(named, None) == "protocol-id"
    assert _model_id(object(), "  deployment-a  ") == "deployment-a"

    fallback = type(
        "FallbackEmbedding",
        (),
        {"model_id": "default", "model": "framework-id"},
    )()
    assert _model_id(fallback, None) == "framework-id"

    for reserved in ("", "unknown", "DEFAULT"):
        invalid = type("InvalidEmbedding", (), {"model_id": reserved})()
        with pytest.raises(ValueError, match="model_id"):
            _model_id(invalid, None)
        with pytest.raises(ValueError, match="model_id"):
            _model_id(object(), reserved)


def test_from_texts_builds_a_populated_store(tmp_path):
    s = CitadelVectorStore.from_texts(
        ["alpha", "beta"], embedder(), path=str(tmp_path / "f.cdl"), key="pw", dim=DIM
    )
    assert s.count() == 2
    assert s.similarity_search("alpha", k=1)[0].page_content == "alpha"


def test_from_texts_forwards_constructor_kwargs_and_explicit_provenance(tmp_path):
    class ConstructorProbe(CitadelVectorStore):
        def __init__(self, *args, marker, **kwargs):
            super().__init__(*args, **kwargs)
            self.marker = marker

    unnamed = DeterministicFakeEmbedding(size=DIM)
    marker = object()
    s = ConstructorProbe.from_texts(
        ["alpha"],
        unnamed,
        path=str(tmp_path / "explicit.cdl"),
        key="pw",
        dim=DIM,
        model_id="deployment-a",
        marker=marker,
    )
    assert s.marker is marker
    assert s.count() == 1
    assert s.similarity_search("alpha", k=1)[0].page_content == "alpha"


def test_duplicate_ids_within_one_call_keep_the_last(store):
    """InMemoryVectorStore assigns store[id], so the last write wins."""
    store.add_texts(["first", "second"], ids=["same", "same"])
    assert store.count() == 1
    assert store.get_by_ids(["same"])[0].page_content == "second"


def test_duplicate_ids_across_documents_keep_the_last(store):
    store.add_documents(
        [
            Document(id="d", page_content="one"),
            Document(id="d", page_content="two"),
        ]
    )
    assert store.count() == 1
    assert store.get_by_ids(["d"])[0].page_content == "two"


def test_delete_tolerates_duplicate_ids(store):
    store.add_texts(["a"], ids=["x"])
    assert store.delete(["x", "x"]) is True
    assert store.count() == 0


def test_k_larger_than_the_corpus_returns_everything(store):
    store.add_texts(["a", "b"])
    assert len(store.similarity_search("a", k=50)) == 2


def test_metadata_keys_cannot_collide_with_internals(store):
    """User metadata is nested, so payload-like keys stay safe."""
    store.add_texts(["t"], metadatas=[{"did": "not-the-id", "meta": "x"}], ids=["real"])
    got = store.get_by_ids(["real"])[0]
    assert got.id == "real"
    assert got.metadata == {"did": "not-the-id", "meta": "x"}


def test_empty_text_is_storable(store):
    store.add_texts([""], ids=["blank"])
    assert store.get_by_ids(["blank"])[0].page_content == ""


def test_unicode_round_trips(store):
    store.add_texts(["a and b"], metadatas=[{"k": "value"}], ids=["u"])
    got = store.get_by_ids(["u"])[0]
    assert got.page_content == "a and b" and got.metadata["k"] == "value"


def test_nested_metadata_round_trips(store):
    store.add_texts(["t"], metadatas=[{"o": {"i": [1, 2]}}], ids=["n"])
    assert store.get_by_ids(["n"])[0].metadata["o"] == {"i": [1, 2]}


def test_a_large_batch_keeps_every_document(store):
    store.add_texts([f"body {i}" for i in range(300)])
    assert store.count() == 300


def test_two_stores_share_one_database_file(tmp_path):
    path = str(tmp_path / "shared.cdl")
    a = CitadelVectorStore(embedder(), path, key="pw", region="a", dim=DIM)
    b = CitadelVectorStore(embedder(), path, key="pw", region="b", dim=DIM)
    a.add_texts(["in a"], ids=["1"])
    b.add_texts(["in b"], ids=["2"])
    assert a.count() == 1 and b.count() == 1


def test_a_wrong_width_vector_names_the_fix(tmp_path):
    """A store opened for one model must refuse another model's vectors."""
    s = CitadelVectorStore(
        DeterministicFakeEmbedding(size=8),
        str(tmp_path / "w.cdl"),
        key="pw",
        dim=DIM,
        model_id="deterministic-fake-8",
    )
    with pytest.raises(ValueError, match="dim=8"):
        s.add_texts(["mismatched"])


def test_mmr_search_works(store):
    """`as_retriever(search_type="mmr")` reaches this; the base class raises."""
    store.add_texts(["alpha one", "alpha two", "beta far"], ids=["a", "b", "c"])
    got = store.max_marginal_relevance_search("alpha", k=2, fetch_k=3)
    assert len(got) == 2
    assert len({d.id for d in got}) == 2, "mmr returned the same document twice"


def test_mmr_by_vector_works(store):
    store.add_texts(["alpha one", "alpha two"], ids=["a", "b"])
    vector = store.embeddings.embed_query("alpha")
    assert (
        len(store.max_marginal_relevance_search_by_vector(vector, k=2, fetch_k=2)) == 2
    )


def test_mmr_respects_a_filter(store):
    store.add_texts(
        ["one", "two"], metadatas=[{"cat": "x"}, {"cat": "y"}], ids=["a", "b"]
    )
    got = store.max_marginal_relevance_search(
        "one", k=2, fetch_k=5, filter={"cat": "y"}
    )
    assert [d.id for d in got] == ["b"]


def test_mmr_on_an_empty_store_is_empty(store):
    assert store.max_marginal_relevance_search("anything", k=2) == []


def test_the_mmr_retriever_works(store):
    """The path a user actually takes to reach MMR."""
    store.add_texts(["alpha one", "alpha two", "beta"], ids=["a", "b", "c"])
    got = store.as_retriever(search_type="mmr", search_kwargs={"k": 2}).invoke("alpha")
    assert len(got) == 2


def test_search_dispatches_both_types(store):
    store.add_texts(["alpha"], ids=["a"])
    assert store.search("alpha", search_type="similarity")
    assert store.search("alpha", search_type="mmr")


def test_it_survives_a_reopen(tmp_path):
    """A region's embedder lives in memory, so reattach can fail."""
    import gc

    p = str(tmp_path / "reopen.cdl")
    first = CitadelVectorStore(embedder(), p, key="pw", dim=DIM)
    first.add_texts(["the disk was full"], metadatas=[{"src": "log"}], ids=["d1"])
    del first
    gc.collect()

    again = CitadelVectorStore(embedder(), p, key="pw", dim=DIM)
    assert again.count() == 1
    got = again.get_by_ids(["d1"])[0]
    assert got.page_content == "the disk was full" and got.metadata["src"] == "log"
    assert [d.id for d in again.similarity_search("the disk was full", k=1)] == ["d1"]


def test_a_region_width_clash_raises_at_construction(tmp_path):
    """A dim clash must not be deferred to the first write."""
    import citadeldb

    p = str(tmp_path / "clash.cdl")
    CitadelVectorStore(embedder(), p, key="pw", dim=DIM)
    with pytest.raises(citadeldb.DataError, match="dim"):
        CitadelVectorStore(embedder(), p, key="pw", dim=DIM + 8)


def test_a_wrong_passphrase_cannot_reopen(tmp_path):
    """The corpus is the payload, so the encryption claim is pinned."""
    import gc

    import citadeldb

    p = str(tmp_path / "enc.cdl")
    first = CitadelVectorStore(embedder(), p, key="right", dim=DIM)
    first.add_texts(["the disk was full"], ids=["d1"])
    del first
    gc.collect()

    with pytest.raises(citadeldb.EncryptionError):
        CitadelVectorStore(embedder(), p, key="wrong", dim=DIM)


def test_existing_vault_authenticates_before_dimension_probe(tmp_path):
    import gc

    import citadeldb

    path = str(tmp_path / "auth-before-probe.cdl")
    first = CitadelVectorStore(embedder(), path, key="right", dim=DIM)
    del first
    gc.collect()

    class ProbeEmbedding:
        model_id = "probe-model"

        def __init__(self):
            self.probes = 0

        def embed_documents(self, texts):
            return [[0.0] * DIM for _ in texts]

        def embed_query(self, text):
            self.probes += 1
            return [0.0] * DIM

    embedding = ProbeEmbedding()
    with pytest.raises(citadeldb.EncryptionError):
        CitadelVectorStore(embedding, path, key="wrong")
    assert embedding.probes == 0


def test_failed_dimension_probe_leaves_no_vault_artifacts(tmp_path):
    class ExplodingEmbedding:
        model_id = "exploding-probe"

        def embed_documents(self, texts):
            return [[0.0] * DIM for _ in texts]

        def embed_query(self, text):
            raise RuntimeError("dimension probe failed")

    with pytest.raises(RuntimeError, match="dimension probe failed"):
        CitadelVectorStore(
            ExplodingEmbedding(), str(tmp_path / "probe-failed.cdl"), key="pw"
        )
    assert list(tmp_path.iterdir()) == []


def test_concurrent_writes_all_land(tmp_path):
    """Chains index in parallel; the engine is shared across threads."""
    import concurrent.futures as cf

    s = CitadelVectorStore(embedder(), str(tmp_path / "conc.cdl"), key="pw", dim=DIM)
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(lambda i: s.add_texts([f"body {i}"], ids=[f"c{i}"]), range(40)))
    assert s.count() == 40


async def test_async_surface_round_trips(store):
    ids = await store.aadd_texts(["async body"], ids=["a1"])
    assert ids == ["a1"]
    assert [d.id for d in await store.aget_by_ids(["a1"])] == ["a1"]
    assert [d.id for d in await store.asimilarity_search("async body", k=1)] == ["a1"]
    assert await store.adelete(["a1"]) is True
    assert store.count() == 0


async def test_the_event_loop_is_not_blocked(store):
    """The base class calls sync straight through, so each is overridden."""
    import asyncio

    ticks = 0

    async def tick():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    ticker = asyncio.create_task(tick())
    await asyncio.sleep(0)
    await store.aadd_texts([f"loop body {i}" for i in range(40)])
    await store.asimilarity_search("loop body 1", k=5)
    ticker.cancel()
    assert ticks > 1, "the loop made no progress during a store call"
