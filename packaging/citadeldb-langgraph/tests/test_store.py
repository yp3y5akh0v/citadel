import time

import pytest
from langgraph.store.base import ListNamespacesOp, MatchCondition

from citadeldb_langgraph import CitadelStore


@pytest.fixture(scope="module")
def store(tmp_path_factory):
    # Citadel takes an exclusive lock, so the whole module shares one handle.
    path = tmp_path_factory.mktemp("store") / "s.cdl"
    return CitadelStore(str(path), key="test-passphrase")


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
    store.put(("notes",), "n1", {"text": "the deployment failed because the disk was full"})
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


def test_namespace_element_may_contain_a_slash(store):
    store.put(("a/b",), "k", {"v": "slash"})
    assert store.get(("a/b",), "k").value["v"] == "slash"
    assert store.get(("a", "b"), "k") is None


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


def test_second_handle_explains_the_lock(store, tmp_path):
    path = str(tmp_path / "locked.cdl")
    first = CitadelStore(path, key="pw")
    with pytest.raises(RuntimeError, match="one handle owns the file"):
        CitadelStore(path, key="pw")
    assert first is not None


def test_async_surface(store):
    """abatch runs the sync batch on a worker thread, so the loop is never blocked."""
    import asyncio

    async def main():
        await store.aput(("async",), "k", {"v": 1})
        assert (await store.aget(("async",), "k")).value == {"v": 1}
        assert len(await store.asearch(("async",), query="v", limit=1)) == 1
        await store.adelete(("async",), "k")
        assert await store.aget(("async",), "k") is None

    asyncio.run(main())
