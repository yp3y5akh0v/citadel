import time

import citadeldb
import pytest
from citadeldb_langgraph import CitadelStore
from citadeldb_langgraph.store import _join, _require_embedder
from langgraph.store.base import ListNamespacesOp, MatchCondition

MOCK = citadeldb.MockEmbedder(dim=64)


@pytest.fixture(scope="module")
def store(tmp_path_factory):
    # Citadel takes an exclusive lock, so the whole module shares one handle.
    path = tmp_path_factory.mktemp("store") / "s.cdl"
    return CitadelStore(str(path), key="test-passphrase", embedder=MOCK)


def test_put_get_roundtrip(store):
    store.put(("users", "alice"), "profile", {"city": "Berlin", "pet": "Mochi"})
    item = store.get(("users", "alice"), "profile")
    assert item.value == {"city": "Berlin", "pet": "Mochi"}
    assert item.namespace == ("users", "alice")
    assert item.key == "profile"
    assert item.created_at is not None


def test_missing_key_is_none(store):
    assert store.get(("users", "nobody"), "profile") is None


def test_overwrite_preserves_created_at(store):
    store.put(("ow",), "k", {"n": 1})
    first = store.get(("ow",), "k")
    time.sleep(0.01)
    store.put(("ow",), "k", {"n": 2})
    second = store.get(("ow",), "k")
    assert second.value == {"n": 2}
    assert second.created_at == first.created_at
    assert second.updated_at >= first.updated_at


def test_overwrite_does_not_duplicate(store):
    store.put(("dup",), "k", {"n": 1})
    store.put(("dup",), "k", {"n": 2})
    assert len(store.search(("dup",), limit=10)) == 1


def test_search_is_semantic(store):
    store.put(
        ("notes",), "n1", {"text": "the deployment failed because the disk was full"}
    )
    store.put(("notes",), "n2", {"text": "lunch plans for friday"})
    # The query shares no words with the match, so a LIKE would return nothing.
    hits = store.search(("notes",), query="why did the release break?", limit=2)
    assert hits[0].value["text"].startswith("the deployment failed")


def test_search_descends_into_child_namespaces(store):
    store.put(("tree",), "a", {"v": 1})
    store.put(("tree", "child"), "b", {"v": 2})
    assert len(store.search(("tree",), limit=10)) == 2
    assert len(store.search(("tree", "child"), limit=10)) == 1


def test_search_prefix_is_not_a_string_prefix(store):
    """('org',) must not match 'orgX', which a string-prefix filter would."""
    store.put(("org", "acme"), "k", {"v": 1})
    store.put(("orgX",), "k", {"v": 2})
    found = {h.namespace for h in store.search(("org",), limit=10)}
    assert ("org", "acme") in found
    assert ("orgX",) not in found


def test_search_filter(store):
    store.put(("filt",), "a", {"kind": "x", "n": 1})
    store.put(("filt",), "b", {"kind": "y", "n": 2})
    hits = store.search(("filt",), filter={"kind": "y"}, limit=5)
    assert [h.value["n"] for h in hits] == [2]


def test_index_false_is_scoreless_and_cannot_outrank_indexed_items(tmp_path):
    s = CitadelStore(str(tmp_path / "index-false.cdl"), key="pw", embedder=MOCK)
    namespace = ("indexing",)
    s.put(namespace, "indexed", {"text": "ordinary indexed value"}, index=["text"])
    s.put(namespace, "unindexed", {"text": "unique secret phrase"}, index=False)

    first = s.search(namespace, query="unique secret phrase", limit=1)
    assert [item.key for item in first] == ["indexed"]

    filled = s.search(namespace, query="unique secret phrase", limit=2)
    assert [item.key for item in filled] == ["indexed", "unindexed"]
    assert filled[1].score is None
    assert s.get(namespace, "unindexed").value["text"] == "unique secret phrase"


def test_unindexed_values_do_not_invoke_the_embedder(tmp_path):
    class CountingEmbedder:
        dim = 8
        metric = "cosine"
        model_id = "counting-index-model"

        def __init__(self):
            self.calls = []

        def embed(self, texts):
            self.calls.extend(texts)
            return [[1.0] + [0.0] * 7 for _ in texts]

    embedder = CountingEmbedder()
    s = CitadelStore(str(tmp_path / "index-calls.cdl"), key="pw", embedder=embedder)
    namespace = ("index-calls",)

    s.put(namespace, "disabled", {"text": "do not embed"}, index=False)
    s.put(namespace, "missing", {"text": "also do not embed"}, index=["absent"])
    assert embedder.calls == []

    s.put(namespace, "enabled", {"text": "embed this"}, index=["text"])
    assert embedder.calls == ["embed this"]


def test_invalid_boolean_index_is_rejected_without_a_write(tmp_path):
    s = CitadelStore(str(tmp_path / "index-true.cdl"), key="pw", embedder=MOCK)
    with pytest.raises(TypeError, match="index"):
        s.put(("index",), "key", {"text": "value"}, index=True)
    assert s.get(("index",), "key") is None


def test_selected_index_paths_are_the_only_searchable_text(tmp_path):
    s = CitadelStore(str(tmp_path / "index-fields.cdl"), key="pw", embedder=MOCK)
    namespace = ("index-fields",)
    s.put(
        namespace,
        "item",
        {
            "title": "visible title",
            "private": "excluded secret",
            "sections": [{"body": "first"}, {"body": "second"}],
        },
        index=["title", "sections[*].body"],
        ttl=60.0,
    )

    def stored_text():
        return s._mem.fetch(
            s._region,
            "kv",
            payload_filter={"ns": _join(namespace), "key": "item"},
            limit=1,
        )[0].text

    assert stored_text() == "visible title\nfirst\nsecond"
    assert "excluded secret" not in stored_text()
    s.get(namespace, "item", refresh_ttl=True)
    assert stored_text() == "visible title\nfirst\nsecond"


def test_search_refreshes_only_rows_after_offset(tmp_path, monkeypatch):
    from citadeldb_langgraph import store as mod

    s = CitadelStore(str(tmp_path / "refresh-offset.cdl"), key="pw", embedder=MOCK)
    namespace = ("refresh-offset",)
    s.put(namespace, "first", {"v": 1}, ttl=60.0)
    s.put(namespace, "second", {"v": 2}, ttl=60.0)
    refreshed = []

    def record_refresh(mem, region, hit, dim):
        refreshed.append(hit.payload["key"])
        return hit

    monkeypatch.setattr(mod, "_refresh_ttl", record_refresh)
    returned = s.search(namespace, limit=1, offset=1, refresh_ttl=True)

    assert len(returned) == 1
    assert refreshed == [returned[0].key]


def test_search_without_a_query_pages_past_the_rows_it_first_reads(tmp_path):
    """A bounded fetch reads the oldest rows, which can hold no match at all."""
    deep = CitadelStore(
        str(tmp_path / "deep.cdl"), key="test-passphrase", embedder=MOCK
    )
    for i in range(80):
        deep.put(("bulk",), f"chaff{i}", {"kind": "chaff"})
    deep.put(("bulk",), "wanted", {"kind": "gold"})
    hits = deep.search(("bulk",), filter={"kind": "gold"}, limit=5)
    assert [h.key for h in hits] == ["wanted"]


def test_search_honours_offset_after_filtering(tmp_path):
    offset = CitadelStore(
        str(tmp_path / "offset.cdl"), key="test-passphrase", embedder=MOCK
    )
    for i in range(6):
        offset.put(("page",), f"k{i}", {"kind": "keep" if i % 2 else "drop"})
    hits = offset.search(("page",), filter={"kind": "keep"}, limit=2, offset=1)
    assert [h.key for h in hits] == ["k3", "k5"]


def test_namespace_element_may_contain_a_slash(store):
    store.put(("a/b",), "k", {"v": "slash"})
    assert store.get(("a/b",), "k").value["v"] == "slash"
    assert store.get(("a", "b"), "k") is None


def test_namespace_encoding_distinguishes_a_separator_from_a_tuple_boundary(store):
    store.put(("a\x1fb",), "same-key", {"v": "one label"})
    store.put(("a", "b"), "same-key", {"v": "two labels"})

    assert store.get(("a\x1fb",), "same-key").value == {"v": "one label"}
    assert store.get(("a", "b"), "same-key").value == {"v": "two labels"}
    assert [item.value for item in store.search(("a\x1fb",), limit=10)] == [
        {"v": "one label"}
    ]
    assert [item.value for item in store.search(("a", "b"), limit=10)] == [
        {"v": "two labels"}
    ]
    assert ("a\x1fb",) in store.list_namespaces()
    assert ("a", "b") in store.list_namespaces()


def test_list_namespaces(store):
    store.put(("ln", "x"), "k", {"v": 1})
    assert ("ln", "x") in store.list_namespaces()
    assert ("ln",) in store.list_namespaces(max_depth=1)


def test_list_namespaces_prefix_match(store):
    store.put(("mc", "one"), "k", {"v": 1})
    got = store.batch(
        [
            ListNamespacesOp(
                match_conditions=(MatchCondition(match_type="prefix", path=("mc",)),),
                max_depth=None,
                limit=10,
                offset=0,
            )
        ]
    )[0]
    assert got and all(ns[0] == "mc" for ns in got)


def test_listing_namespaces_scans_keys_once(tmp_path):
    s = CitadelStore(str(tmp_path / "list-cost.cdl"), key="pw", embedder=MOCK)
    for i in range(12):
        s.put(("namespace", str(i)), "key", {"v": i})
    inner = s._mem

    class CountingMemory:
        def __init__(self):
            self.fetches = []

        def __getattr__(self, name):
            return getattr(inner, name)

        def fetch(self, region, kind, **kwargs):
            self.fetches.append(kind)
            return inner.fetch(region, kind, **kwargs)

    counted = CountingMemory()
    s._mem = counted
    assert len(s.list_namespaces()) == 12
    assert counted.fetches.count("kv") == 1


def test_delete_removes_only_its_key(store):
    store.put(("del", "keep"), "k", {"v": 1})
    store.put(("del", "drop"), "k", {"v": 2})
    store.delete(("del", "drop"), "k")
    assert store.get(("del", "drop"), "k") is None
    assert store.get(("del", "keep"), "k") is not None


def test_emptied_namespace_stops_being_listed(store):
    store.put(("gone",), "k", {"v": 1})
    assert ("gone",) in store.list_namespaces()
    store.delete(("gone",), "k")
    assert ("gone",) not in store.list_namespaces()


def test_ttl_refresh_is_not_a_write(store):
    store.put(("ttl",), "k", {"v": 1}, ttl=60.0)
    before = store.get(("ttl",), "k")
    time.sleep(0.02)
    after = store.get(("ttl",), "k", refresh_ttl=True)
    assert after.created_at == before.created_at
    assert after.updated_at == before.updated_at
    assert after.value == {"v": 1}


def test_ttl_refresh_cannot_restore_a_value_replaced_concurrently(tmp_path):
    import threading

    s = CitadelStore(str(tmp_path / "ttl-race.cdl"), key="pw", embedder=MOCK)
    namespace = ("ttl-race",)
    s.put(namespace, "key", {"version": "old"}, ttl=60.0)
    inner = s._mem
    read_done = threading.Event()
    resume = threading.Event()

    class PauseAfterRead:
        def __getattr__(self, name):
            return getattr(inner, name)

        def fetch(self, *args, **kwargs):
            hits = inner.fetch(*args, **kwargs)
            if (
                threading.current_thread().name == "ttl-reader"
                and (kwargs.get("payload_filter") or {}).get("key") == "key"
                and not read_done.is_set()
            ):
                read_done.set()
                assert resume.wait(5), "writer coordination timed out"
            return hits

    s._mem = PauseAfterRead()
    reader = threading.Thread(
        name="ttl-reader", target=lambda: s.get(namespace, "key", refresh_ttl=True)
    )
    writer_done = threading.Event()

    def write_new():
        s.put(namespace, "key", {"version": "new"}, ttl=60.0)
        writer_done.set()

    reader.start()
    assert read_done.wait(5), "refresh did not reach the captured-read window"
    writer = threading.Thread(target=write_new)
    writer.start()
    assert writer_done.wait(5), (
        "writer did not replace the value before refresh resumed"
    )
    resume.set()
    reader.join(5)
    writer.join(5)

    assert not reader.is_alive() and not writer.is_alive()
    assert s.get(namespace, "key", refresh_ttl=False).value == {"version": "new"}


def test_ttl_config_declares_refresh(store):
    assert CitadelStore.supports_ttl is True
    assert CitadelStore.ttl_config["refresh_on_read"] is True


def test_forget_namespace_erases_the_subtree(store):
    store.put(("gdpr", "u9"), "a", {"v": 1})
    store.put(("gdpr", "u9"), "b", {"v": 2})
    store.put(("gdpr", "u8"), "a", {"v": 3})
    assert store.forget_namespace(("gdpr", "u9")) == 2
    assert store.get(("gdpr", "u9"), "a") is None
    assert store.get(("gdpr", "u8"), "a") is not None
    assert ("gdpr", "u9") not in store.list_namespaces()


def test_forget_namespace_exact_spares_children(store):
    store.put(("ex",), "a", {"v": 1})
    store.put(("ex", "child"), "b", {"v": 2})
    assert store.forget_namespace(("ex",), prefix=False) == 1
    assert store.get(("ex", "child"), "b") is not None


def test_forget_root_namespace_erases_every_namespace(tmp_path):
    s = CitadelStore(str(tmp_path / "forget-root.cdl"), key="pw", embedder=MOCK)
    s.put(("one",), "child", {"v": 2})
    s.put(("two", "deep"), "grandchild", {"v": 3})

    assert s.forget_namespace(()) == 2
    assert s.search((), limit=10) == []
    assert s.list_namespaces() == []


def test_a_second_store_shares_the_open_handle(store, tmp_path):
    """`citadeldb.connect` reopens onto the live database, so both see one file."""
    path = str(tmp_path / "shared.cdl")
    first = CitadelStore(path, key="pw", embedder=MOCK)
    second = CitadelStore(path, key="pw", embedder=MOCK)
    first.put(("ns",), "k", {"v": 1})
    assert second.get(("ns",), "k").value == {"v": 1}


def test_a_second_store_cannot_use_a_different_passphrase(store, tmp_path):
    """The same error a first open raises, so it reports nothing about the file."""
    path = str(tmp_path / "terms.cdl")
    first = CitadelStore(path, key="pw", embedder=MOCK)
    with pytest.raises(citadeldb.EncryptionError):
        CitadelStore(path, key="other", embedder=MOCK)
    assert first is not None


def test_erasure_is_complete_past_one_page(tmp_path, monkeypatch):
    """A capped fetch would erase one page and report the whole namespace erased."""
    from citadeldb_langgraph import store as mod

    monkeypatch.setattr(mod, "PAGE", 7)
    s = CitadelStore(str(tmp_path / "page.cdl"), key="pw", embedder=MOCK)
    for i in range(50):
        s.put(("bulk", "u1"), f"k{i}", {"v": i})

    assert len(s.search(("bulk",), limit=500)) == 50
    assert s.forget_namespace(("bulk", "u1")) == 50
    assert s.search(("bulk",), limit=500) == []


def test_a_passphrase_is_required(tmp_path):
    """An empty key opens an unprotected file that still looks encrypted."""
    with pytest.raises(ValueError, match="passphrase"):
        CitadelStore(str(tmp_path / "nokey.cdl"), key="", embedder=MOCK)


def test_a_compiled_graph_injects_the_store_into_a_node(tmp_path):
    from typing import TypedDict

    from langgraph.graph import END, START, StateGraph
    from langgraph.store.base import BaseStore

    class State(TypedDict):
        out: str

    def node(state: State, *, store: BaseStore) -> State:
        store.put(("graph",), "k", {"text": "written from inside a node"})
        return {"out": store.get(("graph",), "k").value["text"]}

    s = CitadelStore(str(tmp_path / "graph.cdl"), key="pw", embedder=MOCK)
    builder = StateGraph(State)
    builder.add_node("n", node)
    builder.add_edge(START, "n")
    builder.add_edge("n", END)

    result = builder.compile(store=s).invoke({"out": ""})
    assert result["out"] == "written from inside a node"
    assert s.get(("graph",), "k").value == {"text": "written from inside a node"}


def test_a_compiled_graph_works_on_the_async_path(tmp_path):
    """No asyncio_mode here, so an `async def` test would be skipped."""
    import asyncio
    from typing import TypedDict

    from langgraph.graph import END, START, StateGraph
    from langgraph.store.base import BaseStore

    class State(TypedDict):
        out: str

    async def node(state: State, *, store: BaseStore) -> State:
        await store.aput(("graph",), "k", {"text": "async node"})
        got = await store.aget(("graph",), "k")
        return {"out": got.value["text"]}

    s = CitadelStore(str(tmp_path / "agraph.cdl"), key="pw", embedder=MOCK)
    builder = StateGraph(State)
    builder.add_node("n", node)
    builder.add_edge(START, "n")
    builder.add_edge("n", END)

    result = asyncio.run(builder.compile(store=s).ainvoke({"out": ""}))
    assert result["out"] == "async node"


def test_it_survives_a_reopen(tmp_path):
    """A region's embedder lives in memory, so reattach is the failure point."""
    import gc

    p = str(tmp_path / "reopen.cdl")
    first = CitadelStore(p, key="pw", embedder=MOCK)
    first.put(("users", "alice"), "prefs", {"text": "the disk was full"})
    del first
    gc.collect()

    again = CitadelStore(p, key="pw", embedder=MOCK)
    assert again.get(("users", "alice"), "prefs").value == {"text": "the disk was full"}
    assert again.list_namespaces() == [("users", "alice")]
    assert again.search(("users",), query="why did it break?", limit=1)


def test_a_wrong_passphrase_cannot_reopen(tmp_path):
    import gc

    p = str(tmp_path / "enc.cdl")
    first = CitadelStore(p, key="right", embedder=MOCK)
    first.put(("ns",), "k", {"secret": "value"})
    del first
    gc.collect()

    with pytest.raises(citadeldb.EncryptionError):
        CitadelStore(p, key="wrong", embedder=MOCK)


def test_concurrent_writes_all_land(tmp_path):
    """The engine is shared across threads."""
    import concurrent.futures as cf

    s = CitadelStore(str(tmp_path / "conc.cdl"), key="pw", embedder=MOCK)
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(lambda i: s.put(("c",), f"k{i}", {"v": i}), range(40)))
    assert len(s.search(("c",), limit=100)) == 40


def test_filter_operators_match_the_reference(store):
    """BaseStore documents $eq/$ne/$gt/$gte/$lt/$lte. Comparing a filter value
    with == makes every operator form a dict compared against a number, which
    matches nothing at all."""
    store.put(("ops",), "a", {"score": 5, "tag": "x"})
    store.put(("ops",), "b", {"score": 1, "tag": "y"})

    def keys(f):
        return sorted(i.key for i in store.search(("ops",), filter=f, limit=10))

    assert keys({"score": {"$eq": 5}}) == ["a"]
    assert keys({"score": {"$gt": 4.99}}) == ["a"]
    assert keys({"score": {"$gte": 5}}) == ["a"]
    assert keys({"score": {"$lt": 5}}) == ["b"]
    assert keys({"score": {"$lte": 1}}) == ["b"]
    assert keys({"tag": {"$ne": "x"}}) == ["b"]
    # Two operators on one field are an AND, and a plain value still means equal.
    assert keys({"score": {"$gte": 1, "$lte": 1}}) == ["b"]
    assert keys({"tag": "x"}) == ["a"]


def test_a_ranked_search_with_a_filter_finds_a_buried_match(tmp_path):
    """op.filter is settled after ranking, so a fixed k answers short whenever
    the only matching row ranks below it."""
    s = CitadelStore(str(tmp_path / "buried.cdl"), key="pw", embedder=MOCK)
    for i in range(400):
        s.put(
            ("ns",),
            f"chaff{i}",
            {"text": f"why did the release break run {i}", "keep": False},
        )
    s.put(("ns",), "gold", {"text": "the deployment failed", "keep": True})
    found = s.search(
        ("ns",), query="why did the release break?", filter={"keep": True}, limit=1
    )
    assert [i.key for i in found] == ["gold"]


def test_concurrent_writes_to_one_key_leave_one_item(tmp_path):
    """A (namespace, key) is unique in BaseStore, so writers racing on one key
    must supersede rather than each add a row."""
    import concurrent.futures as cf

    s = CitadelStore(str(tmp_path / "onekey.cdl"), key="pw", embedder=MOCK)
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(lambda i: s.put(("c",), "same", {"v": i}), range(64)))

    found = s.search(("c",), limit=500)
    assert len(found) == 1, f"one key, {len(found)} rows"
    # And the key really is gone afterwards, not merely one row lighter.
    s.delete(("c",), "same")
    assert s.get(("c",), "same") is None
    assert s.search(("c",), limit=500) == []


def test_the_event_loop_is_not_blocked(store):
    """The bindings are sync, so abatch has to run them off the loop."""
    import asyncio

    async def main():
        ticks = 0

        async def tick():
            nonlocal ticks
            while True:
                ticks += 1
                await asyncio.sleep(0)

        ticker = asyncio.create_task(tick())
        await asyncio.sleep(0)
        for i in range(40):
            await store.aput(("loop",), f"k{i}", {"v": i})
        await store.asearch(("loop",), limit=5)
        ticker.cancel()
        assert ticks > 1, "the loop made no progress during a store call"

    asyncio.run(main())


def test_async_surface(store):
    import asyncio

    async def main():
        await store.aput(("async",), "k", {"v": 1})
        assert (await store.aget(("async",), "k")).value == {"v": 1}
        assert len(await store.asearch(("async",), query="v", limit=1)) == 1
        await store.adelete(("async",), "k")
        assert await store.aget(("async",), "k") is None

    asyncio.run(main())


def test_an_embedder_is_required(tmp_path):
    with pytest.raises(TypeError, match="embedder"):
        CitadelStore(str(tmp_path / "no-embedder.cdl"), key="pw")
    partial = type(
        "PartialEmbedder",
        (),
        {"dim": 8, "metric": "cosine", "embed": lambda self, texts: []},
    )()
    with pytest.raises(TypeError, match="model_id"):
        CitadelStore(str(tmp_path / "invalid-embedder.cdl"), key="pw", embedder=partial)
    partial.model_id = "default"
    with pytest.raises(TypeError, match="unknown.*default"):
        CitadelStore(
            str(tmp_path / "placeholder-embedder.cdl"), key="pw", embedder=partial
        )
    artifacts = list(tmp_path.iterdir())
    assert artifacts == [], f"invalid construction created vault sidecars: {artifacts}"


def test_embedder_model_id_is_normalized_without_mutating_the_caller():
    embedder = type(
        "PaddedEmbedder",
        (),
        {
            "dim": 8,
            "metric": "cosine",
            "model_id": "  stable-model  ",
            "embed": lambda self, texts: [[0.0] * 8 for _ in texts],
        },
    )()

    normalized = _require_embedder(embedder)

    assert normalized.model_id == "stable-model"
    assert embedder.model_id == "  stable-model  "
    assert len(normalized.embed(["probe"])[0]) == 8
