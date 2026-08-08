import random
from datetime import datetime, timedelta, timezone

import pytest
from crewai.memory.storage.backend import MemoryRecord, StorageBackend

from citadeldb_crewai import CitadelBackend

DIM = 1536


def vec(seed: int) -> list[float]:
    r = random.Random(seed)
    return [r.random() for _ in range(DIM)]


@pytest.fixture(scope="module")
def backend(tmp_path_factory):
    # Citadel takes an exclusive lock, so the whole module shares one handle.
    path = tmp_path_factory.mktemp("crew") / "m.cdl"
    return CitadelBackend(str(path), key="test-passphrase")


def rec(rid, content, scope="/", **kw):
    return MemoryRecord(id=rid, content=content, scope=scope, **kw)


def test_satisfies_the_protocol(backend):
    assert isinstance(backend, StorageBackend)


def test_save_and_get(backend):
    backend.save([rec("r1", "the deploy failed", scope="/team/ops", importance=0.9)])
    got = backend.get_record("r1")
    assert got.content == "the deploy failed"
    assert got.scope == "/team/ops"
    assert got.importance == pytest.approx(0.9)


def test_missing_record_is_none(backend):
    assert backend.get_record("nope") is None


def test_importance_is_stored_not_dropped(backend):
    backend.save([rec("imp", "x", importance=0.25)])
    assert backend.get_record("imp").importance == pytest.approx(0.25)


def test_update_replaces_without_duplicating(backend):
    backend.save([rec("u1", "first", scope="/upd")])
    backend.update(rec("u1", "second", scope="/upd"))
    assert backend.get_record("u1").content == "second"
    assert backend.count("/upd") == 1


def test_search_returns_scored_records(backend):
    backend.save([rec("s1", "alpha", scope="/s", embedding=vec(1))])
    backend.save([rec("s2", "beta", scope="/s", embedding=vec(2))])
    hits = backend.search(vec(1), scope_prefix="/s", limit=2)
    assert hits, "vector search returned nothing"
    assert hits[0][0].content == "alpha", "nearest vector should rank first"
    assert isinstance(hits[0][1], float)


def test_supplied_embedding_is_the_one_stored(backend):
    """An exact-vector query must score ~1.0.

    Citadel embeds text through the region's embedder and cannot be handed a vector, so a
    regression here would silently re-embed the text instead and leave the crew's query
    vector comparing against an unrelated space.
    """
    backend.save([rec("e1", "supplied", scope="/emb", embedding=vec(11))])
    (_, score), = backend.search(vec(11), scope_prefix="/emb", limit=1)
    assert score == pytest.approx(1.0, abs=1e-3), f"stored vector is not the supplied one ({score})"


def test_record_without_an_embedding_is_still_saved(backend):
    backend.save([rec("e2", "no vector", scope="/novec")])
    assert backend.get_record("e2").content == "no vector"


def test_dimension_mismatch_names_the_fix(backend):
    with pytest.raises(ValueError, match="dim=3"):
        backend.save([rec("e3", "short", scope="/bad", embedding=[0.1, 0.2, 0.3])])


def test_search_respects_scope(backend):
    backend.save([rec("in", "inside", scope="/scoped/a", embedding=vec(3))])
    backend.save([rec("out", "outside", scope="/other", embedding=vec(3))])
    found = {r.id for r, _ in backend.search(vec(3), scope_prefix="/scoped", limit=10)}
    assert "in" in found and "out" not in found


def test_scope_prefix_is_not_a_string_prefix(backend):
    """/team must not match /teamX, which a string-prefix filter would."""
    backend.save([rec("t1", "a", scope="/team/ops")])
    backend.save([rec("t2", "b", scope="/teamX")])
    ids = {r.id for r in backend.list_records("/team")}
    assert "t1" in ids and "t2" not in ids


def test_search_filters_by_category_and_metadata(backend):
    backend.save([rec("c1", "x", scope="/c", categories=["bug"], metadata={"env": "prod"},
                      embedding=vec(4))])
    backend.save([rec("c2", "y", scope="/c", categories=["chore"], metadata={"env": "dev"},
                      embedding=vec(4))])
    by_cat = {r.id for r, _ in backend.search(vec(4), scope_prefix="/c", categories=["bug"],
                                              limit=10)}
    assert by_cat == {"c1"}
    by_meta = {r.id for r, _ in backend.search(vec(4), scope_prefix="/c",
                                               metadata_filter={"env": "dev"}, limit=10)}
    assert by_meta == {"c2"}


def test_min_score_filters(backend):
    backend.save([rec("m1", "z", scope="/min", embedding=vec(5))])
    assert backend.search(vec(5), scope_prefix="/min", limit=5, min_score=1.5) == []


def test_count_and_list_categories(backend):
    backend.save([rec("k1", "a", scope="/cat", categories=["x", "y"])])
    backend.save([rec("k2", "b", scope="/cat", categories=["x"])])
    assert backend.count("/cat") == 2
    assert backend.list_categories("/cat") == {"x": 2, "y": 1}


def test_list_scopes_returns_immediate_children(backend):
    backend.save([rec("n1", "a", scope="/tree/one")])
    backend.save([rec("n2", "b", scope="/tree/two/deep")])
    assert backend.list_scopes("/tree") == ["/tree/one", "/tree/two"]


def test_get_scope_info(backend):
    backend.save([rec("i1", "a", scope="/info", categories=["z"])])
    backend.save([rec("i2", "b", scope="/info/child")])
    info = backend.get_scope_info("/info")
    assert info.path == "/info"
    assert info.record_count == 2
    assert info.categories == ["z"]
    assert info.child_scopes == ["/info/child"]
    assert info.oldest_record is not None


def test_delete_by_record_id(backend):
    backend.save([rec("d1", "a", scope="/del"), rec("d2", "b", scope="/del")])
    assert backend.delete(record_ids=["d1"]) == 1
    assert backend.get_record("d1") is None
    assert backend.get_record("d2") is not None


def test_delete_by_category(backend):
    backend.save([rec("dc1", "a", scope="/dc", categories=["drop"]),
                  rec("dc2", "b", scope="/dc", categories=["keep"])])
    assert backend.delete(scope_prefix="/dc", categories=["drop"]) == 1
    assert backend.get_record("dc2") is not None


def test_delete_by_metadata(backend):
    backend.save([rec("dm1", "a", scope="/dm", metadata={"env": "prod"}),
                  rec("dm2", "b", scope="/dm", metadata={"env": "dev"})])
    assert backend.delete(scope_prefix="/dm", metadata_filter={"env": "prod"}) == 1
    assert backend.get_record("dm2") is not None


def test_delete_older_than(backend):
    old = datetime.now(timezone.utc) - timedelta(days=30)
    backend.save([rec("o1", "old", scope="/age", created_at=old),
                  rec("o2", "new", scope="/age")])
    cutoff = datetime.now(timezone.utc) - timedelta(days=1)
    assert backend.delete(scope_prefix="/age", older_than=cutoff) == 1
    assert backend.get_record("o2") is not None


def test_reset_erases_a_whole_scope(backend):
    backend.save([rec("z1", "a", scope="/wipe/u"), rec("z2", "b", scope="/wipe/u")])
    backend.save([rec("z3", "c", scope="/keep")])
    backend.reset("/wipe")
    assert backend.count("/wipe") == 0
    assert backend.get_record("z3") is not None


@pytest.mark.parametrize("scope", ["nolead", "/trail/", "//double//"])
def test_scope_normalisation(backend, scope):
    backend.save([rec(f"norm{scope}", "x", scope=scope)])
    stored = backend.get_record(f"norm{scope}").scope
    assert stored.startswith("/") and not stored.endswith("/") or stored == "/"


def test_timestamps_are_naive_utc(backend):
    """CrewAI subtracts record timestamps from a naive `utcnow()`.

    Returning an aware datetime raises there, and reading a naive one back as local time
    would shift every stamp by the machine's UTC offset and skew `older_than` sweeps.
    """
    naive = datetime(2026, 1, 2, 3, 4, 5)
    backend.save([rec("tz", "x", scope="/tz", created_at=naive)])
    got = backend.get_record("tz").created_at
    assert got.tzinfo is None, "CrewAI cannot subtract an aware datetime"
    assert got == naive, "a naive stamp must round-trip as UTC, not as local time"


def test_end_to_end_through_crewai_memory(tmp_path):
    """The protocol methods are only half the integration; this drives CrewAI's own layer."""
    import hashlib

    from crewai.memory.unified_memory import Memory

    dim = 64

    def embed(texts):
        return [
            [hashlib.sha256(t.lower().encode()).digest()[i % 32] / 255.0 for i in range(dim)]
            for t in texts
        ]

    b = CitadelBackend(str(tmp_path / "e2e.cdl"), key="pw", dim=dim)
    memory = Memory(storage=b, embedder=embed)
    memory.remember("the deploy failed because the disk was full", scope="/ops")
    memory.remember("the intern reset the staging database", scope="/ops")
    memory.drain_writes()
    assert b.count("/ops") == 2

    hits = memory.recall("the deploy failed because the disk was full", limit=3)
    assert hits, "recall through Memory returned nothing"
    assert hits[0].record.content == "the deploy failed because the disk was full"

    memory.reset("/ops")
    assert b.count("/ops") == 0


def test_use_citadel_claims_the_default_and_declines_deliberate_backends(tmp_path):
    """The hook must not displace a crew that named its own backend.

    CrewAI consults the factory for every spec, so returning a backend unconditionally would
    override an explicit `storage="qdrant-edge"`.
    """
    from crewai.memory.storage.factory import resolve_memory_storage, set_memory_storage_factory

    from citadeldb_crewai import use_citadel

    try:
        b = use_citadel(str(tmp_path / "hook.cdl"), key="pw")
        assert resolve_memory_storage("lancedb") is b, "the default spec must reach Citadel"
        assert resolve_memory_storage("citadel") is b
        assert resolve_memory_storage("qdrant-edge") is None
        assert resolve_memory_storage("./some/lancedb/path") is None
    finally:
        set_memory_storage_factory(None)


def test_async_surface(backend):
    import asyncio

    async def main():
        await backend.asave([rec("a1", "async", scope="/as", embedding=vec(9))])
        hits = await backend.asearch(vec(9), scope_prefix="/as", limit=1)
        assert hits and hits[0][0].id == "a1"
        assert await backend.adelete(record_ids=["a1"]) == 1

    asyncio.run(main())
