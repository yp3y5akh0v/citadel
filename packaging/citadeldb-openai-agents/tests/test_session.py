import asyncio

import citadeldb
import pytest
from agents.memory.session import (
    Session,
    _session_accepts_wrapper,
    is_openai_responses_compaction_aware_session,
)
from agents.memory.session_settings import SessionSettings
from citadeldb_openai_agents import CitadelSession, CitadelSessionStore
from citadeldb_openai_agents.session import _require_embedder

MOCK = citadeldb.MockEmbedder(dim=64)


@pytest.fixture(scope="module")
def store(tmp_path_factory):
    # Citadel takes an exclusive lock, so the whole module shares one handle.
    path = tmp_path_factory.mktemp("oa") / "s.cdl"
    return CitadelSessionStore(str(path), key="test-passphrase", embedder=MOCK)


def msg(role, text):
    return {"role": role, "content": text}


def test_satisfies_the_protocol(store):
    assert isinstance(store.session("p"), Session)


@pytest.mark.parametrize(
    ("args", "kwargs", "name"),
    [
        (("other.cdl",), {}, "db_path"),
        (("agent_sessions.cdl",), {}, "db_path"),
        ((), {"key": "other"}, "key"),
        ((), {"key": ""}, "key"),
        ((), {"region": "other"}, "region"),
        ((), {"region": "sessions"}, "region"),
        ((), {"embedder": MOCK}, "embedder"),
        ((), {"embedder": None}, "embedder"),
        ((), {"ttl": 60.0}, "ttl"),
        ((), {"ttl": None}, "ttl"),
    ],
)
def test_prebuilt_store_rejects_inert_storage_options(store, args, kwargs, name):
    with pytest.raises(TypeError, match=name):
        CitadelSession("conflicting-store", *args, store=store, **kwargs)


def test_declares_both_required_attributes(store):
    """The SDK reads session_settings directly, so it must exist as None."""
    s = store.session("attrs")
    assert s.session_id == "attrs"
    assert s.session_settings is None


def test_does_not_claim_the_private_wrapper_contract(store):
    """Declaring the wrapper param would opt into a non-public SDK contract."""
    assert _session_accepts_wrapper(store.session("wrap")) is False


def test_does_not_claim_compaction_support(store):
    assert is_openai_responses_compaction_aware_session(store.session("comp")) is False


async def test_items_round_trip_in_insertion_order(store):
    s = store.session("order")
    await s.add_items([msg("user", "one"), msg("assistant", "two")])
    await s.add_items([msg("user", "three")])
    assert [i["content"] for i in await s.get_items()] == ["one", "two", "three"]


async def test_limit_returns_the_latest_n_chronologically(store):
    """The newest window, oldest of it first: not the first N, not reversed."""
    s = store.session("window")
    await s.add_items([msg("user", str(n)) for n in range(5)])
    assert [i["content"] for i in await s.get_items(limit=2)] == ["3", "4"]


async def test_limit_zero_is_empty_not_everything(store):
    """A tail slice of 0 would return the whole list, so 0 is special."""
    s = store.session("zero")
    await s.add_items([msg("user", "a")])
    assert await s.get_items(limit=0) == []


async def test_limit_larger_than_history_returns_everything(store):
    s = store.session("over")
    await s.add_items([msg("user", "a")])
    assert len(await s.get_items(limit=50)) == 1


async def test_session_settings_supply_the_default_limit(store):
    s = store.session("settings", session_settings=SessionSettings(limit=1))
    await s.add_items([msg("user", "a"), msg("user", "b")])
    assert [i["content"] for i in await s.get_items()] == ["b"]


async def test_explicit_limit_beats_session_settings(store):
    s = store.session("override", session_settings=SessionSettings(limit=1))
    await s.add_items([msg("user", "a"), msg("user", "b")])
    assert len(await s.get_items(limit=2)) == 2


def test_settings_accept_a_plain_dict(store):
    s = store.session("dictset", session_settings={"limit": 3})
    assert s.session_settings.limit == 3


async def test_non_message_items_round_trip_verbatim(store):
    """Items are an opaque union owned by openai; never model their shape."""
    s = store.session("opaque")
    items = [
        {
            "type": "function_call",
            "call_id": "c1",
            "name": "lookup",
            "arguments": '{"q": "berlin"}',
        },
        {"type": "function_call_output", "call_id": "c1", "output": "cloudy"},
        {"role": "user", "content": [{"type": "input_text", "text": "and tomorrow?"}]},
    ]
    await s.add_items(items)
    assert await s.get_items() == items


async def test_empty_add_is_a_no_op(store):
    s = store.session("noop")
    await s.add_items([])
    assert await s.get_items() == []


async def test_pop_returns_and_removes_the_most_recent(store):
    s = store.session("pop")
    await s.add_items([msg("user", "a"), msg("user", "b")])
    assert (await s.pop_item())["content"] == "b"
    assert [i["content"] for i in await s.get_items()] == ["a"]


async def test_pop_on_empty_is_none(store):
    assert await store.session("popempty").pop_item() is None


async def test_pop_then_add_keeps_ordering(store):
    """A rolled-back turn must not sort its replacement before its peers."""
    s = store.session("popadd")
    await s.add_items([msg("user", "a"), msg("user", "wrong")])
    await s.pop_item()
    await s.add_items([msg("user", "right")])
    assert [i["content"] for i in await s.get_items()] == ["a", "right"]


async def test_clear_empties_the_session(store):
    s = store.session("clear")
    await s.add_items([msg("user", "a"), msg("user", "b")])
    await s.clear_session()
    assert await s.get_items() == []


async def test_session_is_reusable_after_clear(store):
    """A cleared session must keep working, with no re-registration."""
    s = store.session("reuse")
    await s.add_items([msg("user", "a")])
    await s.clear_session()
    await s.add_items([msg("user", "b")])
    assert [i["content"] for i in await s.get_items()] == ["b"]


async def test_clear_destroys_keys_rather_than_rows(store):
    s = store.session("erasure")
    await s.add_items([msg("user", "secret")])
    hits = store.items("erasure")
    receipt = store._mem.forget(store._region, [h.id for h in hits])
    assert receipt.cryptographic_erasure is True
    assert receipt.erased_count == 1


async def test_sessions_do_not_see_each_other(store):
    a, b = store.session("iso-a"), store.session("iso-b")
    await a.add_items([msg("user", "mine")])
    await b.add_items([msg("user", "yours")])
    assert [i["content"] for i in await a.get_items()] == ["mine"]
    assert [i["content"] for i in await b.get_items()] == ["yours"]


async def test_clear_is_scoped_to_one_session(store):
    a, b = store.session("scope-a"), store.session("scope-b")
    await a.add_items([msg("user", "a")])
    await b.add_items([msg("user", "b")])
    await a.clear_session()
    assert len(await b.get_items()) == 1


async def test_session_id_is_matched_exactly_not_by_prefix(store):
    """Containment tests the value, so `iso` must not collect `iso-a`."""
    s = store.session("iso")
    await s.add_items([msg("user", "only mine")])
    assert [i["content"] for i in await s.get_items()] == ["only mine"]


async def test_session_ids_lists_populated_sessions(store):
    s = store.session("listed")
    await s.add_items([msg("user", "a")])
    assert "listed" in store.session_ids()


async def test_many_sessions_share_one_file(tmp_path):
    """Citadel allows one handle per file, so sessions must share a store."""
    path = str(tmp_path / "shared.cdl")
    a = CitadelSession("user-1", path, key="pw", embedder=MOCK)
    b = CitadelSession("user-2", path, key="pw", embedder=MOCK)
    await a.add_items([msg("user", "a")])
    await b.add_items([msg("user", "b")])
    assert len(await a.get_items()) == 1
    assert len(await b.get_items()) == 1
    assert set(a._store.session_ids()) == {"user-1", "user-2"}


def test_conflicting_passphrase_is_named_not_ignored(tmp_path):
    """Serving the first caller's passphrase would be a security surprise."""
    import citadeldb

    path = str(tmp_path / "conflict.cdl")
    first = CitadelSession("u1", path, key="pw", embedder=MOCK)
    with pytest.raises(citadeldb.EncryptionError):
        CitadelSession("u2", path, key="other-pw", embedder=MOCK)
    assert first.session_id == "u1"


def test_a_passphrase_is_required(tmp_path):
    with pytest.raises(ValueError, match="passphrase"):
        CitadelSessionStore(str(tmp_path / "nokey.cdl"), key="", embedder=MOCK)


async def test_search_is_semantic_and_session_scoped(store):
    s = store.session("search")
    other = store.session("search-other")
    await s.add_items(
        [
            msg("user", "the deployment failed because the disk was full"),
            msg("user", "lunch plans for friday"),
        ]
    )
    await other.add_items(
        [msg("user", "the deployment failed because the disk was full")]
    )
    hits = await s.search("why did the release break?", limit=1)
    assert hits, "hybrid recall returned nothing"
    assert "disk was full" in hits[0]["content"]


async def test_search_finds_a_session_buried_under_another(store):
    """Discarding after the scan spends the budget on the busy session."""
    quiet = store.session("buried-quiet")
    noisy = store.session("buried-noisy")
    # Ranked above the target for this query, and enough of them to fill the scan.
    await noisy.add_items(
        [msg("user", f"why did the release break run {i}") for i in range(60)]
    )
    await quiet.add_items(
        [msg("user", "the deployment failed because the disk was full")]
    )
    hits = await quiet.search("why did the release break?", limit=1)
    assert hits, "the quiet session's only match was crowded out"
    assert "disk was full" in hits[0]["content"]


async def test_the_sdk_dispatcher_drives_every_protocol_method(store):
    """`Runner` calls sessions through `_call_session_method`, not directly."""
    from agents.memory.session import _call_session_method

    s = store.session("dispatch")
    await _call_session_method(s.add_items, [msg("user", "dispatched")], wrapper=None)
    got = await _call_session_method(s.get_items, wrapper=None)
    assert [i["content"] for i in got] == ["dispatched"]

    popped = await _call_session_method(s.pop_item, wrapper=None)
    assert popped["content"] == "dispatched"
    await _call_session_method(s.clear_session, wrapper=None)
    assert await s.get_items() == []


async def test_the_dispatcher_passes_no_wrapper_to_this_session(store):
    """The wrapper contract is private, so not claiming it is deliberate."""
    from agents.memory.session import _get_session_wrapper

    s = store.session("wrapper")
    assert _get_session_wrapper(s, object()) is None


async def test_it_survives_a_reopen(tmp_path):
    """A region's embedder lives in memory, so search must survive a reopen."""
    import gc

    p = str(tmp_path / "reopen.cdl")
    first = CitadelSessionStore(p, key="pw", embedder=MOCK)
    await first.session("u1").add_items([msg("user", "the disk was full")])
    first.close()
    del first
    gc.collect()

    again = CitadelSessionStore(p, key="pw", embedder=MOCK)
    s = again.session("u1")
    assert [i["content"] for i in await s.get_items()] == ["the disk was full"]
    assert await s.search("why did the release break?", limit=1)


async def test_a_wrong_passphrase_cannot_reopen(tmp_path):
    """Pins the encryption claim rather than inferring it from a reopen."""
    import gc

    import citadeldb

    p = str(tmp_path / "enc.cdl")
    first = CitadelSessionStore(p, key="right", embedder=MOCK)
    await first.session("u1").add_items([msg("user", "secret")])
    first.close()
    del first
    gc.collect()

    with pytest.raises(citadeldb.EncryptionError):
        CitadelSessionStore(p, key="wrong", embedder=MOCK)


async def test_concurrent_turns_all_land(tmp_path):
    """Many in-flight turns mean many worker threads against one engine."""
    import asyncio

    st = CitadelSessionStore(str(tmp_path / "conc.cdl"), key="pw", embedder=MOCK)
    s = st.session("shared")
    await asyncio.gather(*(s.add_items([msg("user", f"m{i}")]) for i in range(40)))
    assert len(await s.get_items()) == 40

    await asyncio.gather(
        *(st.session(f"conc-{i}").add_items([msg("user", "x")]) for i in range(40))
    )
    assert len({i for i in st.session_ids() if i.startswith("conc-")}) == 40


async def test_concurrent_pops_hand_out_each_item_once(tmp_path):
    """The reference pops with one atomic DELETE ... RETURNING. Reading the tail
    and then erasing it lets two in-flight turns return the same item."""
    import asyncio

    st = CitadelSessionStore(str(tmp_path / "pop.cdl"), key="pw", embedder=MOCK)
    s = st.session("shared")
    await s.add_items([msg("user", f"m{i}") for i in range(40)])

    popped = await asyncio.gather(*(s.pop_item() for _ in range(40)))
    got = [p["content"] for p in popped if p is not None]
    assert sorted(got) == sorted(f"m{i}" for i in range(40))
    assert len(got) == len(set(got)), "an item was handed out twice"
    assert await s.get_items() == []


async def test_the_event_loop_is_not_blocked(store):
    """The bindings are sync, so every method must run them off the loop."""
    import asyncio

    s = store.session("loop")
    ticks = 0

    async def tick():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    ticker = asyncio.create_task(tick())
    await asyncio.sleep(0)
    await s.add_items([msg("user", f"turn {i}") for i in range(40)])
    await s.get_items()
    ticker.cancel()
    assert ticks > 1, "the loop made no progress during a session call"


async def test_ttl_expires_items(tmp_path):
    """The engine filters expires_at, so retention needs no sweeper."""
    store = CitadelSessionStore(
        str(tmp_path / "ttl.cdl"), key="pw", ttl=1.0, embedder=MOCK
    )
    s = store.session("ttl")
    await s.add_items([msg("user", "ephemeral")])
    assert len(await s.get_items()) == 1
    await asyncio.sleep(1.3)
    assert await s.get_items() == []


async def test_items_without_ttl_do_not_expire(store):
    s = store.session("nottl")
    await s.add_items([msg("user", "durable")])
    await asyncio.sleep(0.2)
    assert len(await s.get_items()) == 1


def test_an_embedder_is_required(tmp_path):
    with pytest.raises(TypeError, match="embedder"):
        CitadelSessionStore(str(tmp_path / "no-embedder.cdl"), key="pw")
    with pytest.raises(TypeError, match="embedder"):
        CitadelSession("s", str(tmp_path / "no-session-embedder.cdl"), key="pw")
    partial = type(
        "PartialEmbedder",
        (),
        {"dim": 8, "metric": "cosine", "embed": lambda self, texts: []},
    )()
    with pytest.raises(TypeError, match="model_id"):
        CitadelSessionStore(
            str(tmp_path / "invalid-embedder.cdl"), key="pw", embedder=partial
        )
    partial.model_id = "unknown"
    with pytest.raises(TypeError, match="unknown.*default"):
        CitadelSessionStore(
            str(tmp_path / "placeholder-embedder.cdl"), key="pw", embedder=partial
        )
    artifacts = list(tmp_path.iterdir())
    assert artifacts == [], f"invalid construction created vault sidecars: {artifacts}"


def test_embedder_model_id_is_normalized_without_mutating_the_caller(tmp_path):
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

    path = str(tmp_path / "normalized.cdl")
    first = CitadelSessionStore(path, "pw", embedder=embedder)
    first.close()
    embedder.model_id = "stable-model"
    second = CitadelSessionStore(path, "pw", embedder=embedder)
    second.close()
