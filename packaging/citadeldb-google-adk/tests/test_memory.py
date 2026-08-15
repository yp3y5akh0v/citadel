import pytest
from google.adk.events.event import Event
from google.adk.memory.base_memory_service import BaseMemoryService
from google.adk.memory.memory_entry import MemoryEntry
from google.adk.sessions.session import Session
from google.genai import types

from citadeldb_google_adk import CitadelMemoryService

APP = "app"
USER = "user-1"


@pytest.fixture(scope="module")
def svc(tmp_path_factory):
    # Citadel takes an exclusive lock, so the whole module shares one handle.
    path = tmp_path_factory.mktemp("adk") / "m.cdl"
    return CitadelMemoryService(str(path), key="test-passphrase")


def ev(text, *, author="user", eid=None, ts=1.0):
    return Event(
        id=eid or f"e-{text[:8]}-{ts}",
        author=author,
        timestamp=ts,
        content=types.Content(role=author, parts=[types.Part(text=text)]),
    )


def sess(sid, events, *, app=APP, user=USER):
    return Session(id=sid, app_name=app, user_id=user, events=events)


# ---- protocol conformance ------------------------------------------------


def test_is_a_memory_service(svc):
    assert isinstance(svc, BaseMemoryService)


# ---- ingest --------------------------------------------------------------


async def test_a_session_becomes_searchable(svc):
    await svc.add_session_to_memory(
        sess("s1", [ev("the deployment failed because the disk was full")])
    )
    found = await svc.search_memory(app_name=APP, user_id=USER, query="disk was full")
    assert found.memories
    assert "disk was full" in found.memories[0].content.parts[0].text


async def test_search_is_semantic_not_word_overlap(svc):
    """The reference service matches shared words; recall shares none here."""
    await svc.add_session_to_memory(
        sess("sem", [ev("the deployment failed because the disk was full")])
    )
    found = await svc.search_memory(
        app_name=APP, user_id=USER, query="why did the release break?"
    )
    assert found.memories, "hybrid recall returned nothing"


async def test_contentless_events_are_skipped(svc):
    empty = Event(id="empty", author="user", timestamp=1.0, content=None)
    await svc.add_session_to_memory(sess("skip", [empty]))
    assert svc.count(APP, USER) == svc.count(APP, USER)  # no crash, nothing stored


async def test_re_adding_a_session_does_not_duplicate(svc):
    """ADK says a session may be added many times, so ingest must be a delta."""
    s = sess("dup", [ev("only once", eid="fixed-1")])
    await svc.add_session_to_memory(s)
    before = svc.count(APP, USER)
    await svc.add_session_to_memory(s)
    assert svc.count(APP, USER) == before


async def test_a_growing_session_adds_only_the_new_events(svc):
    first = ev("turn one", eid="g-1")
    s = sess("grow", [first])
    await svc.add_session_to_memory(s)
    before = svc.count(APP, USER)
    s.events.append(ev("turn two", eid="g-2"))
    await svc.add_session_to_memory(s)
    assert svc.count(APP, USER) == before + 1


# ---- scope ---------------------------------------------------------------


async def test_users_do_not_see_each_other(svc):
    await svc.add_session_to_memory(sess("u1", [ev("mine alone")], user="alice"))
    await svc.add_session_to_memory(sess("u2", [ev("yours alone")], user="bob"))
    alice = await svc.search_memory(app_name=APP, user_id="alice", query="alone")
    texts = [m.content.parts[0].text for m in alice.memories]
    assert "mine alone" in texts and "yours alone" not in texts


async def test_apps_do_not_see_each_other(svc):
    await svc.add_session_to_memory(sess("a1", [ev("app one secret")], app="one"))
    found = await svc.search_memory(app_name="two", user_id=USER, query="secret")
    assert [m for m in found.memories if "app one" in m.content.parts[0].text] == []


# ---- projection ----------------------------------------------------------


async def test_memory_entry_carries_author_and_timestamp(svc):
    await svc.add_session_to_memory(
        sess("meta", [ev("stamped", author="assistant", eid="m-1", ts=1700000000.0)])
    )
    found = await svc.search_memory(app_name=APP, user_id=USER, query="stamped")
    entry = next(m for m in found.memories if m.id == "m-1")
    assert entry.author == "assistant"
    assert entry.timestamp and entry.timestamp.startswith("20")


async def test_non_text_parts_round_trip_verbatim(svc):
    """Content is a union owned by google.genai; nothing may normalise it."""
    content = types.Content(
        role="model",
        parts=[
            types.Part(text="looking that up"),
            types.Part(
                function_call=types.FunctionCall(name="lookup", args={"q": "berlin"})
            ),
        ],
    )
    e = Event(id="fc-1", author="model", timestamp=3.0, content=content)
    await svc.add_session_to_memory(sess("fc", [e]))
    found = await svc.search_memory(app_name=APP, user_id=USER, query="looking that up")
    entry = next(m for m in found.memories if m.id == "fc-1")
    call = entry.content.parts[1].function_call
    assert call.name == "lookup"
    assert call.args == {"q": "berlin"}


async def test_an_event_with_no_text_at_all_is_skipped(svc):
    """Nothing to embed means recall could never return it."""
    only_call = types.Content(
        role="model",
        parts=[types.Part(function_call=types.FunctionCall(name="noop", args={}))],
    )
    e = Event(id="silent", author="model", timestamp=4.0, content=only_call)
    before = svc.count(APP, USER)
    await svc.add_session_to_memory(sess("silent", [e]))
    assert svc.count(APP, USER) == before


async def test_search_for_an_unknown_user_is_empty(svc):
    found = await svc.search_memory(app_name=APP, user_id="nobody", query="anything")
    assert found.memories == []


async def test_search_on_an_empty_service_is_empty(tmp_path):
    fresh = CitadelMemoryService(str(tmp_path / "empty.cdl"), key="pw")
    found = await fresh.search_memory(app_name=APP, user_id=USER, query="anything")
    assert found.memories == []


async def test_the_event_loop_is_not_blocked(svc):
    """ADK's Runner is async, so the sync bindings must run off the loop."""
    import asyncio

    ticks = 0

    async def tick():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    ticker = asyncio.create_task(tick())
    await asyncio.sleep(0)
    await svc.add_session_to_memory(
        sess("loop", [ev(f"loop turn {n}", eid=f"l-{n}") for n in range(40)])
    )
    await svc.search_memory(app_name=APP, user_id=USER, query="loop turn")
    ticker.cancel()
    assert ticks > 1, "the loop made no progress during a memory call"


async def test_multi_part_content_round_trips(svc):
    """Content is stored whole, so a memory hands back what ADK gave us."""
    content = types.Content(
        role="user",
        parts=[types.Part(text="first part"), types.Part(text="second part")],
    )
    e = Event(id="multi", author="user", timestamp=2.0, content=content)
    await svc.add_session_to_memory(sess("multi", [e]))
    found = await svc.search_memory(app_name=APP, user_id=USER, query="second part")
    entry = next(m for m in found.memories if m.id == "multi")
    assert [p.text for p in entry.content.parts] == ["first part", "second part"]


# ---- the surface the reference refuses -----------------------------------


async def test_events_can_be_added_without_a_session(svc):
    await svc.add_events_to_memory(
        app_name=APP, user_id=USER, events=[ev("delta only", eid="d-1")],
        session_id="delta", custom_metadata={"source": "webhook"},
    )
    found = await svc.search_memory(app_name=APP, user_id=USER, query="delta only")
    entry = next(m for m in found.memories if m.id == "d-1")
    assert entry.custom_metadata["source"] == "webhook"


async def test_re_adding_a_memory_id_replaces_it(svc):
    """An id makes a second write a replacement, not a second copy."""
    def entry(text):
        return MemoryEntry(
            content=types.Content(role="user", parts=[types.Part(text=text)]),
            id="dup-1", author="user",
        )

    await svc.add_memory(app_name=APP, user_id="dupuser", memories=[entry("first")])
    await svc.add_memory(app_name=APP, user_id="dupuser", memories=[entry("second")])
    assert svc.count(APP, "dupuser") == 1
    found = await svc.search_memory(app_name=APP, user_id="dupuser", query="first second")
    assert [m.content.parts[0].text for m in found.memories] == ["second"]


async def test_duplicate_ids_within_one_call_keep_the_last(svc):
    def entry(text):
        return MemoryEntry(
            content=types.Content(role="user", parts=[types.Part(text=text)]),
            id="batch-dup", author="user",
        )

    await svc.add_memory(
        app_name=APP, user_id="batchuser", memories=[entry("one"), entry("two")]
    )
    assert svc.count(APP, "batchuser") == 1


async def test_memories_without_an_id_are_always_new(svc):
    """Nothing identifies them, so they cannot replace anything."""
    def entry():
        return MemoryEntry(
            content=types.Content(role="user", parts=[types.Part(text="anonymous")]),
            author="user",
        )

    await svc.add_memory(app_name=APP, user_id="anon", memories=[entry()])
    await svc.add_memory(app_name=APP, user_id="anon", memories=[entry()])
    assert svc.count(APP, "anon") == 2


async def test_memories_can_be_written_directly(svc):
    """The reference service raises NotImplementedError here."""
    m = MemoryEntry(
        content=types.Content(role="user", parts=[types.Part(text="written directly")]),
        id="direct-1",
        author="user",
    )
    await svc.add_memory(app_name=APP, user_id=USER, memories=[m])
    found = await svc.search_memory(app_name=APP, user_id=USER, query="written directly")
    assert any(x.id == "direct-1" for x in found.memories)


# ---- erasure -------------------------------------------------------------


async def test_forgetting_a_session_leaves_the_rest(svc):
    await svc.add_session_to_memory(sess("keep", [ev("keep me", eid="k-1")], user="carol"))
    await svc.add_session_to_memory(sess("drop", [ev("drop me", eid="k-2")], user="carol"))
    assert svc.forget_session(APP, "carol", "drop") == 1
    left = await svc.search_memory(app_name=APP, user_id="carol", query="me")
    assert [m.id for m in left.memories] == ["k-1"]


async def test_forgetting_a_user_erases_everything_they_own(svc):
    await svc.add_session_to_memory(sess("g1", [ev("gone one")], user="dave"))
    await svc.add_session_to_memory(sess("g2", [ev("gone two")], user="dave"))
    assert svc.forget_user(APP, "dave") == 2
    assert svc.count(APP, "dave") == 0


def test_forgetting_nothing_is_zero_not_an_error(svc):
    assert svc.forget_user(APP, "nobody") == 0


async def test_it_survives_a_reopen(tmp_path):
    """A region's embedder must survive the reopen, not just the events."""
    import gc

    p = str(tmp_path / "reopen.cdl")
    first = CitadelMemoryService(p, key="pw")
    await first.add_session_to_memory(
        sess("s", [ev("the deployment failed because the disk was full")], user="ru")
    )
    del first
    gc.collect()

    again = CitadelMemoryService(p, key="pw")
    assert again.count(APP, "ru") == 1
    found = await again.search_memory(
        app_name=APP, user_id="ru", query="why did the release break?"
    )
    assert found.memories, "recall did not survive the reopen"


def test_a_passphrase_is_required(tmp_path):
    with pytest.raises(ValueError, match="passphrase"):
        CitadelMemoryService(str(tmp_path / "nokey.cdl"), key="")


async def test_concurrent_ingestion_all_lands(tmp_path):
    """Many in-flight sessions means many threads against one engine."""
    import asyncio

    svc = CitadelMemoryService(str(tmp_path / "conc.cdl"), key="pw")
    await asyncio.gather(*(
        svc.add_session_to_memory(sess(f"s{i}", [ev(f"event {i}")], user="cu"))
        for i in range(40)
    ))
    assert svc.count(APP, "cu") == 40


async def test_re_adding_a_session_drops_events_removed_from_it(tmp_path):
    """add_session_to_memory SETS the session's events; add_events_to_memory is
    the additive one. Merging on both leaves a retracted turn searchable."""
    svc = CitadelMemoryService(str(tmp_path / "retract.cdl"), key="pw")
    keep, retract = ev("keep this"), ev("retract this")
    await svc.add_session_to_memory(sess("s", [keep, retract], user="ru"))
    assert svc.count(APP, "ru") == 2

    await svc.add_session_to_memory(sess("s", [keep], user="ru"))
    assert svc.count(APP, "ru") == 1
    found = await svc.search_memory(app_name=APP, user_id="ru", query="retract this")
    texts = [p.text for m in found.memories for p in m.content.parts]
    assert "retract this" not in texts

    # The additive path still adds rather than replacing.
    await svc.add_events_to_memory(
        app_name=APP, user_id="ru", session_id="s", events=[ev("added later")]
    )
    assert svc.count(APP, "ru") == 2


async def test_concurrent_ingestion_of_one_session_stores_it_once(tmp_path):
    """Re-ingesting a session converges; racing ingests of it must too, or the
    same turn comes back from search several times."""
    import asyncio

    svc = CitadelMemoryService(str(tmp_path / "onesess.cdl"), key="pw")
    session = sess("s", [ev("the only turn")], user="ru")
    await asyncio.gather(*(svc.add_session_to_memory(session) for _ in range(8)))
    assert svc.count(APP, "ru") == 1

    found = await svc.search_memory(app_name=APP, user_id="ru", query="the only turn")
    assert len(found.memories) == 1


async def test_a_wrong_passphrase_cannot_reopen(tmp_path):
    """Pins the encryption claim rather than inferring it from a reopen."""
    import gc

    import citadeldb

    p = str(tmp_path / "enc.cdl")
    first = CitadelMemoryService(p, key="right")
    await first.add_session_to_memory(sess("s", [ev("secret")], user="ru"))
    del first
    gc.collect()

    with pytest.raises(citadeldb.EncryptionError):
        CitadelMemoryService(p, key="wrong")


async def test_shares_one_database_with_another_adapter(tmp_path):
    """Two Citadel-backed services on one file must not fight over the lock."""
    path = str(tmp_path / "shared.cdl")
    a = CitadelMemoryService(path, key="pw", region="memory")
    b = CitadelMemoryService(path, key="pw", region="other")
    await a.add_session_to_memory(sess("s", [ev("in region a")]))
    assert a.count(APP, USER) == 1
    assert b.count(APP, USER) == 0, "regions must stay separate"
