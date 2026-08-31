import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from citadeldb_crewai import CitadelBackend
from citadeldb_crewai.backend import KIND, _require_embedder
from crewai.memory.storage.backend import MemoryRecord, ScopeInfo, StorageBackend
from crewai.memory.storage.factory import (
    resolve_memory_storage,
    set_memory_storage_factory,
)

DIM = 1536


class DeterministicEmbedder:
    metric = "cosine"

    def __init__(self, dim: int) -> None:
        self.dim = dim
        self.model_id = f"test-{dim}"

    def embed(self, texts: list[str]) -> list[list[float]]:
        return self.embed_with_cancel(texts, None)

    def embed_with_cancel(
        self, texts: list[str], cancel_token: Any | None
    ) -> list[list[float]]:
        if cancel_token is not None:
            cancel_token.check()
        return [
            [
                hashlib.sha256(text.lower().encode()).digest()[i % 32] / 255.0
                for i in range(self.dim)
            ]
            for text in texts
        ]

    def embed_queries(self, texts: list[str]) -> list[list[float]]:
        return self.embed_queries_with_cancel(texts, None)

    def embed_queries_with_cancel(
        self, texts: list[str], cancel_token: Any | None
    ) -> list[list[float]]:
        return self.embed_with_cancel(texts, cancel_token)

    def __call__(self, texts: list[str]) -> list[list[float]]:
        return self.embed(texts)


EMBEDDER = DeterministicEmbedder(DIM)


def vec(seed: int) -> list[float]:
    r = random.Random(seed)
    return [r.random() for _ in range(DIM)]


@pytest.fixture(scope="module")
def backend(tmp_path_factory):
    # Citadel takes an exclusive lock, so the whole module shares one handle.
    path = tmp_path_factory.mktemp("crew") / "m.cdl"
    return CitadelBackend(str(path), key="test-passphrase", embedder=EMBEDDER)


def test_unscoped_count_does_not_materialize_records(backend, monkeypatch):
    class CountOnly:
        def count(self, region, kind):
            assert region == backend._region and kind == KIND
            return 37

        def fetch(self, *args, **kwargs):
            raise AssertionError("unscoped count must not fetch or decrypt records")

    monkeypatch.setattr(backend, "_mem", CountOnly())
    assert backend.count() == 37


def rec(rid, content, scope="/", **kw):
    return MemoryRecord(id=rid, content=content, scope=scope, **kw)


def test_satisfies_the_protocol(backend):
    assert isinstance(backend, StorageBackend)


def test_required_crewai_storage_api_is_importable():
    assert all(
        callable(symbol)
        for symbol in (
            MemoryRecord,
            ScopeInfo,
            StorageBackend,
            resolve_memory_storage,
            set_memory_storage_factory,
        )
    )


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


def test_a_metadata_only_update_keeps_the_stored_vector(backend):
    """crewai's Memory.update() reads a record back, edits a field and saves it.
    A record read back carries no embedding, so rewriting the atom would replace
    the crew's vector with a text-derived placeholder and the record would stop
    being findable by the vector it was stored under."""
    backend.save([rec("kv", "the deploy failed", scope="/kv", embedding=vec(11))])
    # Querying with the stored vector scores 1.0 only while that vector is the
    # one stored. Asserting rank alone would pass against any vector at all,
    # since the record is the only one in its scope either way.
    exact = backend.search(vec(11), scope_prefix="/kv", limit=1)
    assert [r.id for r, _ in exact] == ["kv"]
    assert exact[0][1] == pytest.approx(1.0, abs=1e-3)

    stored = backend.get_record("kv")
    assert stored.embedding is None, "a record read back carries no vector"
    backend.update(
        stored.model_copy(update={"metadata": {"reviewed": True}, "importance": 0.9})
    )

    assert backend.get_record("kv").metadata == {"reviewed": True}
    assert backend.get_record("kv").importance == pytest.approx(0.9)
    still = backend.search(vec(11), scope_prefix="/kv", limit=1)
    assert [r.id for r, _ in still] == ["kv"]
    assert still[0][1] == pytest.approx(1.0, abs=1e-3), "the stored vector was replaced"


def test_embedder_model_id_is_normalized_without_mutating_the_caller():
    original = DeterministicEmbedder(8)
    original.model_id = "  stable-model  "

    normalized, dim = _require_embedder(original)

    assert dim == 8
    assert normalized is not original
    assert normalized.model_id == "stable-model"
    assert original.model_id == "  stable-model  "
    assert normalized.dim == original.dim
    assert normalized.embed(["delegated"]) == original.embed(["delegated"])


def test_importance_survives_a_ranked_read(backend):
    """Recall relevance remains separate from persisted importance."""
    backend.save([rec("rank", "ranked read", importance=0.25, embedding=vec(12))])
    found = backend.search(vec(12), scope_prefix="/", limit=10)
    got = next(r for r, _ in found if r.id == "rank")
    assert got.importance == pytest.approx(0.25)


def test_storage_search_leaves_importance_for_crewai_to_score(tmp_path):
    b = CitadelBackend(str(tmp_path / "importance.cdl"), key="pw", embedder=EMBEDDER)
    query = [1.0, 0.0] + [0.0] * (DIM - 2)
    nearest = query
    important = [0.9, (1.0 - 0.9**2) ** 0.5] + [0.0] * (DIM - 2)
    distant = [-1.0, 0.0] + [0.0] * (DIM - 2)
    b.save(
        [
            rec("nearest", "nearest", scope="/rank", importance=0.0, embedding=nearest),
            rec(
                "important",
                "important",
                scope="/rank",
                importance=1.0,
                embedding=important,
            ),
            rec("distant", "distant", scope="/rank", importance=0.0, embedding=distant),
        ]
    )

    hits = b.search(query, scope_prefix="/rank", limit=3)
    assert [record.id for record, _ in hits[:2]] == ["nearest", "important"]
    assert hits[0][1] > hits[1][1]
    assert hits[1][0].importance == 1.0


def test_a_filtered_search_finds_a_record_under_a_window_of_others(tmp_path):
    """Category, metadata and score are settled after ranking, so a fixed k
    answers short whenever the only matching record ranks below it."""
    b = CitadelBackend(str(tmp_path / "window.cdl"), key="pw", embedder=EMBEDDER)
    b.save(
        [rec(f"c{i}", f"chaff {i}", scope="/w", embedding=vec(1)) for i in range(400)]
    )
    b.save(
        [
            rec(
                "gold",
                "the needle",
                scope="/w",
                categories=["incident"],
                embedding=vec(2),
            )
        ]
    )
    hits = b.search(vec(1), scope_prefix="/w", categories=["incident"], limit=1)
    assert [r.id for r, _ in hits] == ["gold"]


def test_a_negatively_correlated_record_is_not_dropped_by_the_default_threshold(
    backend,
):
    """crewai always passes min_score=0.0 meaning "no threshold", so a score
    below the documented [0, 1] domain would silently discard real matches."""
    backend.save(
        [rec("neg", "opposite", scope="/neg", embedding=[1.0] + [0.0] * (DIM - 1))]
    )
    found = backend.search(
        [-1.0] + [0.0] * (DIM - 1), scope_prefix="/neg", limit=10, min_score=0.0
    )
    assert [r.id for r, _ in found] == ["neg"]
    assert all(0.0 <= s <= 1.0 for _, s in found)


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
    """A regression would re-embed text into an unrelated vector space."""
    backend.save([rec("e1", "supplied", scope="/emb", embedding=vec(11))])
    ((_, score),) = backend.search(vec(11), scope_prefix="/emb", limit=1)
    assert score == pytest.approx(1.0, abs=1e-3), (
        f"stored vector is not the supplied one ({score})"
    )


def test_records_with_identical_content_keep_their_own_vectors(backend):
    """Vectors were once matched by text, so duplicates could swap them."""
    backend.save(
        [
            rec("dup1", "identical text", scope="/dup", embedding=vec(21)),
            rec("dup2", "identical text", scope="/dup", embedding=vec(22)),
        ]
    )
    ((found, score),) = backend.search(vec(22), scope_prefix="/dup", limit=1)
    assert found.id == "dup2", "a duplicate content record took the wrong vector"
    assert score == pytest.approx(1.0, abs=1e-3)


def test_score_never_exceeds_one_at_a_realistic_width(tmp_path):
    """A score above 1 slips past min_score; narrow widths hide it."""
    wide = 1536
    b = CitadelBackend(
        str(tmp_path / "wide.cdl"), key="pw", embedder=DeterministicEmbedder(wide)
    )
    vectors = []
    for seed in range(10):
        r = random.Random(seed)
        v = [r.random() for _ in range(wide)]
        vectors.append(v)
        b.save([rec(f"w{seed}", "x", scope="/w", embedding=v)])
    for i, v in enumerate(vectors):
        ((_found, score),) = b.search(v, scope_prefix="/w", limit=1)
        assert score <= 1.0, (i, score)


def test_record_without_an_embedding_is_still_saved(backend):
    backend.save([rec("e2", "no vector", scope="/novec")])
    assert backend.get_record("e2").content == "no vector"


def test_bulk_save_and_delete_do_not_repeat_encrypted_scans(tmp_path):
    b = CitadelBackend(str(tmp_path / "bulk.cdl"), key="pw", embedder=EMBEDDER)
    inner = b._mem

    class CountingMemory:
        def __init__(self):
            self.fetches = 0
            self.batches = 0

        def __getattr__(self, name):
            return getattr(inner, name)

        def fetch(self, *args, **kwargs):
            self.fetches += 1
            return inner.fetch(*args, **kwargs)

        def remember_replacing_keyed_batch(self, *args, **kwargs):
            self.batches += 1
            return inner.remember_replacing_keyed_batch(*args, **kwargs)

    counted = CountingMemory()
    b._mem = counted
    b.save(
        [
            rec(f"bulk-{i}", f"body {i}", scope="/bulk", embedding=vec(i))
            for i in range(12)
        ]
    )
    assert counted.fetches == 0
    assert counted.batches == 1

    assert b.delete(record_ids=[f"bulk-{i}" for i in range(12)]) == 12
    assert counted.fetches == 1

    counted.fetches = 0
    b.save([rec(f"generated-{i}", f"body {i}") for i in range(12)])
    assert counted.fetches == 1


def test_a_record_without_a_vector_is_embedded_by_the_backend(backend):
    backend.save([rec("nov", "never embedded", scope="/quiet")])
    backend.save([rec("emb", "really embedded", scope="/quiet", embedding=vec(21))])

    assert backend.get_record("nov").content == "never embedded"
    assert {r.id for r in backend.list_records(scope_prefix="/quiet")} == {"nov", "emb"}

    query = EMBEDDER.embed_queries(["never embedded"])[0]
    hits = backend.search(query, scope_prefix="/quiet", limit=2)
    assert hits[0][0].id == "nov"


def test_dimension_mismatch_names_the_fix(backend):
    with pytest.raises(ValueError, match="3-dimension embedder"):
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
    backend.save(
        [
            rec(
                "c1",
                "x",
                scope="/c",
                categories=["bug"],
                metadata={"env": "prod"},
                embedding=vec(4),
            )
        ]
    )
    backend.save(
        [
            rec(
                "c2",
                "y",
                scope="/c",
                categories=["chore"],
                metadata={"env": "dev"},
                embedding=vec(4),
            )
        ]
    )
    by_cat = {
        r.id
        for r, _ in backend.search(
            vec(4), scope_prefix="/c", categories=["bug"], limit=10
        )
    }
    assert by_cat == {"c1"}
    by_meta = {
        r.id
        for r, _ in backend.search(
            vec(4), scope_prefix="/c", metadata_filter={"env": "dev"}, limit=10
        )
    }
    assert by_meta == {"c2"}


def test_a_scope_buried_under_another_is_still_found(tmp_path):
    """Discarding after the scan spends the budget on the crowded scope."""
    b = CitadelBackend(str(tmp_path / "buried.cdl"), key="pw", embedder=EMBEDDER)
    # Nearest to the query, and enough of them to fill the scan.
    b.save([rec(f"n{i}", "x", scope="/noisy", embedding=vec(7)) for i in range(60)])
    b.save([rec("wanted", "y", scope="/quiet", embedding=vec(7))])
    hits = b.search(vec(7), scope_prefix="/quiet", limit=1)
    assert [r.id for r, _ in hits] == ["wanted"]


def test_min_score_filters(backend):
    backend.save([rec("m1", "z", scope="/min", embedding=vec(5))])
    assert backend.search(vec(5), scope_prefix="/min", limit=5, min_score=1.5) == []


def test_count_and_list_categories(backend):
    backend.save([rec("k1", "a", scope="/cat", categories=["x", "y"])])
    backend.save([rec("k2", "b", scope="/cat", categories=["x"])])
    assert backend.count("/cat") == 2
    assert backend.list_categories("/cat") == {"x": 2, "y": 1}


def test_list_records_is_newest_first_before_pagination(tmp_path):
    b = CitadelBackend(str(tmp_path / "ordered.cdl"), key="pw", embedder=EMBEDDER)
    b.save(
        [
            rec("new", "latest", scope="/ordered", created_at=datetime(2025, 1, 1)),
            rec("old", "earliest", scope="/ordered", created_at=datetime(2020, 1, 1)),
            rec("mid", "middle", scope="/ordered", created_at=datetime(2023, 1, 1)),
        ]
    )
    assert [r.id for r in b.list_records("/ordered")] == ["new", "mid", "old"]
    assert [r.id for r in b.list_records("/ordered", limit=1, offset=1)] == ["mid"]


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


def test_the_root_scope_covers_every_record(tmp_path):
    """An ancestor list starts one level down, so no nested record names root."""
    # Its own file: the shared backend accumulates records from every other test.
    only = CitadelBackend(str(tmp_path / "root.cdl"), key="pw", embedder=EMBEDDER)
    only.save([rec("g1", "a", scope="/")])
    only.save([rec("g2", "b", scope="/deep")])
    only.save([rec("g3", "c", scope="/deep/deeper")])
    assert only.count("/") == 3
    assert only.count() == 3
    assert {r.id for r in only.list_records("/")} == {"g1", "g2", "g3"}
    assert only.get_scope_info("/").record_count == 3


def test_delete_by_record_id(backend):
    backend.save([rec("d1", "a", scope="/del"), rec("d2", "b", scope="/del")])
    assert backend.delete(record_ids=["d1"]) == 1
    assert backend.get_record("d1") is None
    assert backend.get_record("d2") is not None


def test_delete_by_category(backend):
    backend.save(
        [
            rec("dc1", "a", scope="/dc", categories=["drop"]),
            rec("dc2", "b", scope="/dc", categories=["keep"]),
        ]
    )
    assert backend.delete(scope_prefix="/dc", categories=["drop"]) == 1
    assert backend.get_record("dc2") is not None


def test_delete_by_metadata(backend):
    backend.save(
        [
            rec("dm1", "a", scope="/dm", metadata={"env": "prod"}),
            rec("dm2", "b", scope="/dm", metadata={"env": "dev"}),
        ]
    )
    assert backend.delete(scope_prefix="/dm", metadata_filter={"env": "prod"}) == 1
    assert backend.get_record("dm2") is not None


def test_delete_older_than(backend):
    old = datetime.now(timezone.utc) - timedelta(days=30)
    backend.save(
        [rec("o1", "old", scope="/age", created_at=old), rec("o2", "new", scope="/age")]
    )
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
    """CrewAI subtracts record stamps from a naive utcnow()."""
    naive = datetime(2026, 1, 2, 3, 4, 5)
    backend.save([rec("tz", "x", scope="/tz", created_at=naive)])
    got = backend.get_record("tz").created_at
    assert got.tzinfo is None, "CrewAI cannot subtract an aware datetime"
    assert got == naive, "a naive stamp must round-trip as UTC, not as local time"


def test_end_to_end_through_crewai_memory(tmp_path):
    from crewai.memory.unified_memory import Memory

    dim = 64
    embedder = DeterministicEmbedder(dim)
    b = CitadelBackend(str(tmp_path / "e2e.cdl"), key="pw", embedder=embedder)
    memory = Memory(storage=b, embedder=embedder)
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
    """CrewAI consults the factory for every spec, including foreign ones."""
    from citadeldb_crewai import use_citadel

    try:
        b = use_citadel(str(tmp_path / "hook.cdl"), key="pw", embedder=EMBEDDER)
        assert resolve_memory_storage("lancedb") is b, (
            "the default spec must reach Citadel"
        )
        assert resolve_memory_storage("citadel") is b
        assert resolve_memory_storage("qdrant-edge") is None
        assert resolve_memory_storage("./some/lancedb/path") is None
    finally:
        set_memory_storage_factory(None)


def test_a_passphrase_is_required(tmp_path):
    """A default passphrase protects nothing while looking like it does."""
    from citadeldb_crewai import use_citadel

    with pytest.raises(ValueError, match="passphrase"):
        CitadelBackend(str(tmp_path / "nokey.cdl"), embedder=EMBEDDER)
    with pytest.raises(ValueError, match="passphrase"):
        use_citadel(str(tmp_path / "nokey2.cdl"), embedder=EMBEDDER)


def test_an_embedder_is_required_before_a_vault_is_created(tmp_path):
    with pytest.raises(TypeError, match="embedder"):
        CitadelBackend(str(tmp_path / "no-model.cdl"), key="pw")
    partial = type(
        "PartialEmbedder",
        (),
        {
            "dim": 8,
            "metric": "cosine",
            "embed_with_cancel": lambda self, texts, cancel_token: [],
        },
    )()
    with pytest.raises(TypeError, match="model_id"):
        CitadelBackend(str(tmp_path / "invalid-model.cdl"), key="pw", embedder=partial)
    partial.model_id = "default"
    with pytest.raises(TypeError, match="unknown.*default"):
        CitadelBackend(
            str(tmp_path / "placeholder-model.cdl"), key="pw", embedder=partial
        )
    wrong_metric = DeterministicEmbedder(8)
    wrong_metric.metric = "l2"
    with pytest.raises(TypeError, match="metric must be cosine"):
        CitadelBackend(
            str(tmp_path / "wrong-metric.cdl"), key="pw", embedder=wrong_metric
        )
    assert list(tmp_path.iterdir()) == []


def test_it_survives_a_reopen(tmp_path):
    """A region's embedder lives in memory; reattaching is where this fails."""
    import gc

    p = str(tmp_path / "reopen.cdl")
    first = CitadelBackend(p, key="pw", embedder=EMBEDDER)
    first.save([rec("r1", "the disk was full", scope="/ops", embedding=vec(1))])
    del first
    gc.collect()

    again = CitadelBackend(p, key="pw", embedder=EMBEDDER)
    assert again.count("/ops") == 1
    assert again.get_record("r1").content == "the disk was full"
    hits = again.search(vec(1), scope_prefix="/ops", limit=1)
    assert hits and hits[0][0].id == "r1", "recall did not survive the reopen"


def test_a_wrong_passphrase_cannot_reopen(tmp_path):
    import gc

    import citadeldb

    p = str(tmp_path / "enc.cdl")
    first = CitadelBackend(p, key="right", embedder=EMBEDDER)
    first.save([rec("r1", "the disk was full", embedding=vec(1))])
    del first
    gc.collect()

    with pytest.raises(citadeldb.EncryptionError):
        CitadelBackend(p, key="wrong", embedder=EMBEDDER)


def test_concurrent_writes_all_land(tmp_path):
    """Crews run agents in parallel; the engine is shared across threads."""
    import concurrent.futures as cf

    b = CitadelBackend(str(tmp_path / "conc.cdl"), key="pw", embedder=EMBEDDER)
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        list(
            ex.map(
                lambda i: b.save(
                    [rec(f"c{i}", f"body {i}", scope="/c", embedding=vec(i))]
                ),
                range(40),
            )
        )
    assert b.count("/c") == 40


def test_the_event_loop_is_not_blocked(backend):
    """The bindings are sync, so async must run them off the loop."""
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
        await backend.asave(
            [
                rec(f"loop{i}", f"body {i}", scope="/loop", embedding=vec(i))
                for i in range(40)
            ]
        )
        await backend.asearch(vec(0), scope_prefix="/loop", limit=5)
        ticker.cancel()
        assert ticks > 1, "the loop made no progress during a backend call"

    asyncio.run(main())


def test_async_surface(backend):
    import asyncio

    async def main():
        await backend.asave([rec("a1", "async", scope="/as", embedding=vec(9))])
        hits = await backend.asearch(vec(9), scope_prefix="/as", limit=1)
        assert hits and hits[0][0].id == "a1"
        assert await backend.adelete(record_ids=["a1"]) == 1

    asyncio.run(main())
