import pytest
from citadeldb_llamaindex import CitadelVectorStore
from citadeldb_llamaindex.vector_store import DEFAULT_REGION, KIND, _model_id
from llama_index.core.embeddings import MockEmbedding
from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode
from llama_index.core.vector_stores.types import (
    BasePydanticVectorStore,
    FilterCondition,
    FilterOperator,
    MetadataFilter,
    MetadataFilters,
    VectorStoreQuery,
    VectorStoreQueryMode,
)

DIM = 8
EMBED_MODEL = MockEmbedding(embed_dim=DIM)
EMBED_MODEL.model_name = "llamaindex-mock-8"


def vec(axis: int) -> list[float]:
    v = [0.0] * DIM
    v[axis] = 1.0
    return v


def node(nid, text, axis, *, meta=None, ref=None):
    n = TextNode(id_=nid, text=text, metadata=dict(meta or {}))
    n.embedding = vec(axis)
    if ref:
        n.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=ref)
    return n


@pytest.fixture()
def store(tmp_path):
    # Each test gets its own file: a store is a corpus, not a scratch pad.
    return CitadelVectorStore(
        str(tmp_path / "v.cdl"),
        key="test-passphrase",
        embed_model=EMBED_MODEL,
        dim=DIM,
    )


def test_count_does_not_materialize_node_content(store):
    class CountOnly:
        def count(self, region, kind):
            assert region == store._region and kind == KIND
            return 37

        def fetch(self, *args, **kwargs):
            raise AssertionError("count must not fetch or decrypt nodes")

    store._mem = CountOnly()
    assert store.count() == 37


def query(axis, k=10, **kw):
    return VectorStoreQuery(query_embedding=vec(axis), similarity_top_k=k, **kw)


def test_is_a_pydantic_vector_store(store):
    assert isinstance(store, BasePydanticVectorStore)
    assert store.stores_text is True
    assert store.client is not None


def test_the_supplied_vector_is_the_one_stored(store):
    """Identical text, different vectors: the only proof of no re-embed."""
    store.add([node("a", "same text", 0), node("b", "same text", 7)])
    res = store.query(query(7, k=1))
    assert res.ids == ["b"]
    assert res.similarities[0] == pytest.approx(1.0, abs=1e-3)


def test_duplicate_content_keeps_its_own_vector(store):
    """Vectors travel with their atom, so equal text cannot swap them."""
    store.add([node("d1", "identical", 0), node("d2", "identical", 7)])
    assert store.query(query(0, k=1)).ids == ["d1"]
    assert store.query(query(7, k=1)).ids == ["d2"]


def test_similarity_never_exceeds_one_at_a_realistic_width(tmp_path):
    """Small test widths do not surface the sub-zero distance."""
    import random

    wide = 1536
    s = CitadelVectorStore(
        str(tmp_path / "wide.cdl"),
        key="pw",
        embed_model=MockEmbedding(embed_dim=wide),
        dim=wide,
        model_id="llamaindex-wide-mock",
    )
    nodes = []
    for seed in range(10):
        r = random.Random(seed)
        n = TextNode(id_=f"w{seed}", text="x")
        n.embedding = [r.random() for _ in range(wide)]
        nodes.append(n)
    s.add(nodes)
    for n in nodes:
        res = s.query(VectorStoreQuery(query_embedding=n.embedding, similarity_top_k=1))
        assert res.similarities[0] <= 1.0, (n.node_id, res.similarities[0])


def test_a_node_without_an_embedding_uses_the_store_model(store):
    bare = TextNode(id_="bare", text="no vector")
    store.add([bare])
    result = store.query(
        VectorStoreQuery(
            query_embedding=EMBED_MODEL.get_query_embedding("no vector"),
            similarity_top_k=1,
        )
    )
    assert result.ids == ["bare"]


def test_a_wrong_width_embedding_names_the_fix(store):
    wide = TextNode(id_="wide", text="x")
    wide.embedding = [0.1, 0.2, 0.3]
    with pytest.raises(ValueError, match="dim=3"):
        store.add([wide])


def test_text_and_metadata_round_trip(store):
    store.add([node("m1", "the payload text", 0, meta={"page": 4, "tag": "intro"})])
    got = store.query(query(0, k=1)).nodes[0]
    assert got.get_content() == "the payload text"
    assert got.metadata["page"] == 4
    assert got.metadata["tag"] == "intro"
    assert got.node_id == "m1"


def test_relationships_round_trip(store):
    """The node is stored whole, so its graph edges survive with it."""
    store.add([node("r1", "child", 0, ref="parent-doc")])
    got = store.query(query(0, k=1)).nodes[0]
    assert got.ref_doc_id == "parent-doc"


def test_text_is_not_stored_twice(store):
    """The node JSON is stored with its text stripped."""
    store.add([node("t1", "unique body text", 0)])
    raw = store.client.fetch(DEFAULT_REGION, KIND, limit=5)[0]
    assert "unique body text" not in raw.payload["meta"]["_node_content"]
    assert raw.text == "unique body text"


def test_similarity_orders_results(store):
    store.add([node("near", "a", 0), node("far", "b", 7)])
    res = store.query(query(0, k=2))
    assert res.ids[0] == "near"
    assert res.similarities[0] > res.similarities[1]


def test_top_k_is_respected(store):
    store.add([node(f"n{i}", "t", i % DIM) for i in range(6)])
    assert len(store.query(query(0, k=2)).ids) == 2


def test_top_k_zero_is_empty(store):
    store.add([node("z", "t", 0)])
    res = store.query(query(0, k=0))
    assert res.ids == [] and res.nodes == []


def test_empty_framework_allowlists_are_unrestricted(store):
    """VectorStoreIndex passes node_ids=[] when stores_text is true."""
    store.add([node("present", "text", 0, ref="doc")])
    assert store.query(query(0, node_ids=[])).ids == ["present"]
    assert store.query(query(0, doc_ids=[])).ids == ["present"]


def test_a_query_without_an_embedding_is_refused(store):
    with pytest.raises(ValueError, match="query_embedding is required"):
        store.query(VectorStoreQuery(similarity_top_k=1))


def test_an_unsupported_mode_is_refused(store):
    with pytest.raises(NotImplementedError, match="not supported"):
        store.query(query(0, mode=VectorStoreQueryMode.SPARSE))


def test_query_narrows_by_node_ids(store):
    store.add([node("k1", "a", 0), node("k2", "b", 0)])
    assert store.query(query(0, node_ids=["k2"])).ids == ["k2"]


def test_query_narrows_by_doc_ids(store):
    store.add([node("c1", "a", 0, ref="doc-a"), node("c2", "b", 0, ref="doc-b")])
    assert store.query(query(0, doc_ids=["doc-b"])).ids == ["c2"]


def filters(*fs, condition=FilterCondition.AND):
    return MetadataFilters(filters=list(fs), condition=condition)


def seeded(store):
    store.add(
        [
            node("x1", "one", 0, meta={"cat": "x", "n": 1}),
            node("y2", "two", 1, meta={"cat": "y", "n": 2}),
            node("z3", "three", 2, meta={"cat": "z", "n": 3}),
        ]
    )
    return store


def test_equality_filter(store):
    seeded(store)
    f = filters(MetadataFilter(key="cat", value="y", operator=FilterOperator.EQ))
    assert store.query(query(0, filters=f)).ids == ["y2"]


def test_comparison_filter(store):
    seeded(store)
    f = filters(MetadataFilter(key="n", value=2, operator=FilterOperator.GT))
    assert store.query(query(0, filters=f)).ids == ["z3"]


def test_in_filter(store):
    seeded(store)
    f = filters(MetadataFilter(key="cat", value=["x", "z"], operator=FilterOperator.IN))
    assert sorted(store.query(query(0, filters=f)).ids) == ["x1", "z3"]


def test_and_of_two_equalities(store):
    seeded(store)
    f = filters(
        MetadataFilter(key="cat", value="x", operator=FilterOperator.EQ),
        MetadataFilter(key="n", value=1, operator=FilterOperator.EQ),
    )
    assert store.query(query(0, filters=f)).ids == ["x1"]


def test_or_does_not_push_equality_into_the_scan(store):
    """Pushing a leaf of an OR would drop rows the filter keeps."""
    seeded(store)
    f = filters(
        MetadataFilter(key="cat", value="x", operator=FilterOperator.EQ),
        MetadataFilter(key="n", value=3, operator=FilterOperator.EQ),
        condition=FilterCondition.OR,
    )
    assert sorted(store.query(query(0, filters=f)).ids) == ["x1", "z3"]


def test_not_does_not_push_equality_into_the_scan(store):
    seeded(store)
    f = filters(
        MetadataFilter(key="cat", value="x", operator=FilterOperator.EQ),
        condition=FilterCondition.NOT,
    )
    assert sorted(store.query(query(0, filters=f)).ids) == ["y2", "z3"]


def test_not_rejects_a_row_when_any_leaf_matches(store):
    seeded(store)
    f = filters(
        MetadataFilter(key="cat", value="x", operator=FilterOperator.EQ),
        MetadataFilter(key="n", value=2, operator=FilterOperator.EQ),
        condition=FilterCondition.NOT,
    )
    assert store.query(query(0, filters=f)).ids == ["z3"]


def test_a_pushed_filter_agrees_with_an_unpushed_one(store):
    """Pushdown must narrow, never decide: both paths answer the same."""
    seeded(store)
    pushed = filters(MetadataFilter(key="cat", value="z", operator=FilterOperator.EQ))
    unpushed = filters(
        MetadataFilter(key="cat", value="z", operator=FilterOperator.TEXT_MATCH)
    )
    assert (
        store.query(query(0, filters=pushed)).ids
        == store.query(query(0, filters=unpushed)).ids
    )


def test_a_numeric_filter_is_not_decided_by_the_pushdown(store):
    """Containment compares JSON types exactly where EQ is Python `==`, which
    holds 1 == 1.0. Pushing a number would drop a row the filter keeps, on both
    the query path and the erase path."""
    store.add([node("n-int", "stored as an int", 0, meta={"year": 2024})])
    same = filters(MetadataFilter(key="year", value=2024.0, operator=FilterOperator.EQ))

    assert store.query(query(0, filters=same)).ids == ["n-int"]
    assert [n.node_id for n in store.get_nodes(filters=same)] == ["n-int"]
    # A silently partial erase is the worst direction for a store whose delete
    # is irreversible key destruction.
    store.delete_nodes(filters=same)
    assert store.count() == 0


def test_a_named_node_is_reachable_however_it_ranks(store):
    """node_ids is an exact restriction, so a named node must come back even
    when it ranks below a window of better-matching rows."""
    store.add([node("wanted", "the one asked for", 1)])
    store.add([node(f"chaff{i}", f"nearer {i}", 0) for i in range(400)])
    got = store.query(query(0, k=1, node_ids=["wanted"]))
    assert got.ids == ["wanted"]


def test_an_unpushable_filter_survives_a_window_of_other_rows(store):
    """TEXT_MATCH cannot be pushed, so it is settled after ranking; the only
    matching row must still be found under a corpus that outranks it."""
    store.add([node("gold", "the needle", 1, meta={"cat": "zebra"})])
    store.add(
        [node(f"c{i}", f"nearer {i}", 0, meta={"cat": "other"}) for i in range(400)]
    )
    unpushable = filters(
        MetadataFilter(key="cat", value="zeb", operator=FilterOperator.TEXT_MATCH)
    )
    assert store.query(query(0, k=1, filters=unpushable)).ids == ["gold"]


def test_get_nodes_by_id(store):
    seeded(store)
    got = store.get_nodes(node_ids=["y2"])
    assert [n.node_id for n in got] == ["y2"]


def test_get_and_delete_many_ids_scan_the_encrypted_region_once(store):
    nodes = [node(f"many-{i}", f"text {i}", i % DIM) for i in range(12)]
    store.add(nodes)
    inner = store._mem

    class CountingMemory:
        def __init__(self):
            self.fetches = 0

        def __getattr__(self, name):
            return getattr(inner, name)

        def fetch(self, *args, **kwargs):
            self.fetches += 1
            return inner.fetch(*args, **kwargs)

    counted = CountingMemory()
    store._mem = counted
    ids = [n.node_id for n in nodes]
    assert {n.node_id for n in store.get_nodes(node_ids=ids)} == set(ids)
    assert counted.fetches == 1

    counted.fetches = 0
    store.delete_nodes(node_ids=ids)
    assert counted.fetches == 1


def test_get_nodes_by_filter(store):
    seeded(store)
    f = filters(MetadataFilter(key="n", value=1, operator=FilterOperator.EQ))
    assert [n.node_id for n in store.get_nodes(filters=f)] == ["x1"]


def test_delete_removes_a_whole_document(store):
    store.add(
        [
            node("p1", "a", 0, ref="doc-1"),
            node("p2", "b", 1, ref="doc-1"),
            node("p3", "c", 2, ref="doc-2"),
        ]
    )
    store.delete("doc-1")
    assert sorted(store.query(query(0, k=9)).ids) == ["p3"]


def test_delete_destroys_keys_rather_than_rows(store):
    store.add([node("s1", "secret", 0, ref="doc-x")])
    assert store.forget_document("doc-x") == 1
    assert store.count() == 0


def test_delete_nodes_by_id_and_filter(store):
    seeded(store)
    store.delete_nodes(node_ids=["x1"])
    assert sorted(store.query(query(0, k=9)).ids) == ["y2", "z3"]
    store.delete_nodes(
        filters=filters(MetadataFilter(key="n", value=2, operator=FilterOperator.EQ))
    )
    assert store.query(query(0, k=9)).ids == ["z3"]


def test_clear_empties_the_store(store):
    seeded(store)
    store.clear()
    assert store.count() == 0
    assert store.query(query(0, k=9)).ids == []


def test_deleting_nothing_is_not_an_error(store):
    store.delete("no-such-doc")
    assert store.count() == 0


def test_a_passphrase_is_required(tmp_path):
    with pytest.raises(ValueError, match="passphrase"):
        CitadelVectorStore(
            str(tmp_path / "nokey.cdl"), key="", embed_model=EMBED_MODEL, dim=DIM
        )


def test_an_embedder_is_required_before_a_vault_is_created(tmp_path):
    with pytest.raises(TypeError, match="embed_model"):
        CitadelVectorStore(str(tmp_path / "no-model.cdl"), key="pw", dim=DIM)
    with pytest.raises(ValueError, match="embed_model"):
        CitadelVectorStore(
            str(tmp_path / "invalid-model.cdl"), key="pw", embed_model=object(), dim=DIM
        )
    with pytest.raises(ValueError, match="65535"):
        CitadelVectorStore(
            str(tmp_path / "wide-model.cdl"),
            key="pw",
            embed_model=EMBED_MODEL,
            dim=65_536,
        )
    assert list(tmp_path.iterdir()) == []


def test_an_unnamed_embedder_requires_explicit_provenance(tmp_path):
    unnamed = MockEmbedding(embed_dim=DIM)
    with pytest.raises(ValueError, match="model_id"):
        CitadelVectorStore(
            str(tmp_path / "missing.cdl"), key="pw", embed_model=unnamed, dim=DIM
        )
    with pytest.raises(ValueError, match="model_id"):
        CitadelVectorStore(
            str(tmp_path / "invalid.cdl"),
            key="pw",
            embed_model=unnamed,
            dim=DIM,
            model_id=123,
        )
    assert list(tmp_path.iterdir()) == []

    store = CitadelVectorStore(
        str(tmp_path / "named.cdl"),
        key="pw",
        embed_model=unnamed,
        dim=DIM,
        model_id="deployment-a",
    )
    assert store.count() == 0


def test_model_id_attribute_is_preferred_and_reserved_names_are_rejected():
    named = type(
        "NamedEmbedding",
        (),
        {"model_id": "protocol-id", "model_name": "framework-id"},
    )()
    assert _model_id(named, None) == "protocol-id"
    assert _model_id(object(), "  deployment-a  ") == "deployment-a"

    fallback = type(
        "FallbackEmbedding",
        (),
        {"model_id": "default", "model_name": "framework-id"},
    )()
    assert _model_id(fallback, None) == "framework-id"

    for reserved in ("", "unknown", "DEFAULT"):
        invalid = type("InvalidEmbedding", (), {"model_id": reserved})()
        with pytest.raises(ValueError, match="model_id"):
            _model_id(invalid, None)
        with pytest.raises(ValueError, match="model_id"):
            _model_id(object(), reserved)


async def test_async_surface_round_trips(store):
    await store.async_add([node("a1", "async body", 0)])
    res = await store.aquery(query(0, k=1))
    assert res.ids == ["a1"]
    assert [n.node_id for n in await store.aget_nodes(node_ids=["a1"])] == ["a1"]
    await store.adelete_nodes(node_ids=["a1"])
    assert store.count() == 0
    await store.aclear()


async def test_the_event_loop_is_not_blocked(store):
    """The base class would call the sync method straight through."""
    import asyncio

    ticks = 0

    async def tick():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    ticker = asyncio.create_task(tick())
    await asyncio.sleep(0)
    await store.async_add([node(f"L{i}", "loop body", i % DIM) for i in range(40)])
    await store.aquery(query(0, k=5))
    ticker.cancel()
    assert ticks > 1, "the loop made no progress during a store call"


def test_re_adding_the_same_node_id_replaces_it(store):
    """Appending instead of replacing would double the corpus."""
    store.add([node("same", "first version", 0)])
    store.add([node("same", "second version", 0)])
    assert store.count() == 1
    assert store.query(query(0, k=5)).nodes[0].get_content() == "second version"


def test_duplicate_ids_within_one_call_keep_the_last(store):
    """The reference is dict-keyed by node id, so the last write wins."""
    store.add([node("same", "first", 0), node("same", "second", 7)])
    assert store.count() == 1
    assert store.query(query(7, k=1)).nodes[0].get_content() == "second"


def test_a_node_with_empty_text_is_storable(store):
    """An image or placeholder node can carry no text at all."""
    store.add([node("blank", "", 0)])
    assert store.query(query(0, k=1)).ids == ["blank"]


def test_adding_nothing_is_not_an_error(store):
    assert store.add([]) == []
    assert store.count() == 0


def test_query_on_an_empty_store_is_empty(store):
    res = store.query(query(0, k=5))
    assert res.ids == [] and res.nodes == [] and res.similarities == []


def test_empty_node_ids_selects_nothing_unlike_none(store):
    """`[]` means none of them; `None` means no id restriction."""
    store.add([node("a", "x", 0), node("b", "y", 1)])
    assert store.get_nodes(node_ids=[]) == []
    assert len(store.get_nodes()) == 2


def test_delete_nodes_with_no_arguments_clears_everything(store):
    """Matches SimpleVectorStore, whose filter is true for every node."""
    store.add([node("a", "x", 0), node("b", "y", 1)])
    store.delete_nodes()
    assert store.count() == 0


def test_a_negative_top_k_is_empty_not_a_tail_slice(store):
    store.add([node("a", "x", 0)])
    assert store.query(query(0, k=-1)).ids == []


def test_nested_and_list_metadata_round_trip(store):
    store.add([node("n", "t", 0, meta={"outer": {"inner": [1, 2]}})])
    assert store.query(query(0, k=1)).nodes[0].metadata["outer"] == {"inner": [1, 2]}


def test_contains_filter_on_a_list(store):
    store.add(
        [
            node("t1", "a", 0, meta={"tags": ["red", "blue"]}),
            node("t2", "b", 1, meta={"tags": ["green"]}),
        ]
    )
    f = filters(
        MetadataFilter(key="tags", value="blue", operator=FilterOperator.CONTAINS)
    )
    assert store.query(query(0, filters=f)).ids == ["t1"]


def test_a_filter_on_a_missing_key_excludes_rather_than_raises(store):
    store.add([node("a", "x", 0, meta={"present": 1})])
    f = filters(MetadataFilter(key="absent", value=1, operator=FilterOperator.EQ))
    assert store.query(query(0, filters=f)).ids == []


def test_unicode_round_trips(store):
    store.add([node("u", "unicode - a and b", 0, meta={"k": "value"})])
    got = store.query(query(0, k=1)).nodes[0]
    assert got.get_content() == "unicode - a and b"


def test_two_stores_share_one_database_file(tmp_path):
    """The composition the shared-handle work exists to allow."""
    path = str(tmp_path / "shared.cdl")
    a = CitadelVectorStore(
        path, key="pw", embed_model=EMBED_MODEL, region="corpus_a", dim=DIM
    )
    b = CitadelVectorStore(
        path, key="pw", embed_model=EMBED_MODEL, region="corpus_b", dim=DIM
    )
    a.add([node("a1", "in a", 0)])
    b.add([node("b1", "in b", 0)])
    assert a.query(query(0, k=5)).ids == ["a1"]
    assert b.query(query(0, k=5)).ids == ["b1"]


def test_a_large_batch_keeps_every_node(store):
    store.add([node(f"b{i}", f"body {i}", i % DIM) for i in range(300)])
    assert store.count() == 300
    assert len(store.query(query(0, k=50)).ids) == 50


def test_it_survives_a_reopen(tmp_path):
    """A region's embedder lives in memory, so reattach is the risk."""
    import gc

    p = str(tmp_path / "reopen.cdl")
    first = CitadelVectorStore(p, key="pw", embed_model=EMBED_MODEL, dim=DIM)
    first.add([node("n1", "the disk was full", 0, meta={"page": 3})])
    del first
    gc.collect()

    again = CitadelVectorStore(p, key="pw", embed_model=EMBED_MODEL, dim=DIM)
    assert again.count() == 1
    got = again.get_nodes(node_ids=["n1"])[0]
    assert got.get_content() == "the disk was full" and got.metadata["page"] == 3
    assert again.query(query(0, k=1)).ids == ["n1"], "recall did not survive the reopen"


def test_a_wrong_passphrase_cannot_reopen(tmp_path):
    """Pins the encryption claim rather than inferring it from a reopen."""
    import gc

    import citadeldb

    p = str(tmp_path / "enc.cdl")
    first = CitadelVectorStore(p, key="right", embed_model=EMBED_MODEL, dim=DIM)
    first.add([node("n1", "the disk was full", 0)])
    del first
    gc.collect()

    with pytest.raises(citadeldb.EncryptionError):
        CitadelVectorStore(p, key="wrong", embed_model=EMBED_MODEL, dim=DIM)


def test_concurrent_writes_all_land(tmp_path):
    """Indexing pipelines share one engine across threads."""
    import concurrent.futures as cf

    s = CitadelVectorStore(
        str(tmp_path / "conc.cdl"), key="pw", embed_model=EMBED_MODEL, dim=DIM
    )
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(lambda i: s.add([node(f"c{i}", f"body {i}", i % DIM)]), range(40)))
    assert s.count() == 40


def test_end_to_end_through_vector_store_index(tmp_path):
    """The ABC is only half the integration; this drives LlamaIndex itself."""
    from llama_index.core import StorageContext, VectorStoreIndex
    from llama_index.core.schema import Document

    embed = MockEmbedding(embed_dim=DIM)
    store = CitadelVectorStore(
        str(tmp_path / "e2e.cdl"),
        key="pw",
        embed_model=embed,
        dim=DIM,
        model_id="llamaindex-e2e-mock",
    )
    index = VectorStoreIndex.from_documents(
        [
            Document(text="the deploy failed because the disk was full"),
            Document(text="lunch plans for friday"),
        ],
        storage_context=StorageContext.from_defaults(vector_store=store),
        embed_model=embed,
    )
    hits = index.as_retriever(similarity_top_k=2).retrieve("why did the release break?")
    assert hits, "retrieval through VectorStoreIndex returned nothing"
    assert store.count() == 2


def test_hybrid_retriever_sends_both_keyword_and_vector_evidence(tmp_path):
    from llama_index.core import StorageContext, VectorStoreIndex
    from llama_index.core.schema import Document

    embed = MockEmbedding(embed_dim=DIM)
    store = CitadelVectorStore(
        str(tmp_path / "hybrid.cdl"),
        key="pw",
        embed_model=embed,
        dim=DIM,
        model_id="llamaindex-hybrid-mock",
    )
    index = VectorStoreIndex.from_documents(
        [Document(text="unique deployment incident"), Document(text="lunch plans")],
        storage_context=StorageContext.from_defaults(vector_store=store),
        embed_model=embed,
    )
    inner = store._mem

    class RecordingMemory:
        def __init__(self):
            self.recall_kwargs = []

        def __getattr__(self, name):
            return getattr(inner, name)

        def recall(self, *args, **kwargs):
            self.recall_kwargs.append(kwargs)
            return inner.recall(*args, **kwargs)

    recorded = RecordingMemory()
    store._mem = recorded
    hits = index.as_retriever(
        similarity_top_k=2,
        vector_store_query_mode=VectorStoreQueryMode.HYBRID,
    ).retrieve("unique deployment")

    assert hits
    assert recorded.recall_kwargs[-1]["text"] == "unique deployment"
    assert "embedding" in recorded.recall_kwargs[-1]


def test_hybrid_similarities_report_the_fused_ranking(store):
    store.add(
        [
            node("keyword", "quasar quasar quasar", 0),
            node("plain", "ordinary text", 0),
        ]
    )
    result = store.query(
        VectorStoreQuery(
            query_embedding=vec(0),
            query_str="quasar",
            similarity_top_k=2,
            mode=VectorStoreQueryMode.HYBRID,
        )
    )

    assert result.ids == ["keyword", "plain"]
    assert result.similarities[0] > result.similarities[1]
