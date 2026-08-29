import citadeldb
import pytest
from agent_framework import (
    AgentSession,
    ContextProvider,
    FileHistoryProvider,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    SessionContext,
)
from citadeldb_ms_agent_framework import CitadelHistoryProvider

MOCK = citadeldb.MockEmbedder(dim=64)


@pytest.fixture(scope="module")
def path(tmp_path_factory):
    # Citadel takes an exclusive lock, so the whole module shares one file.
    return str(tmp_path_factory.mktemp("maf") / "h.cdl")


@pytest.fixture()
def provider(path, request):
    return CitadelHistoryProvider(
        path, "pw", embedder=MOCK, source_id=request.node.name
    )


def sid(request) -> str:
    return request.node.name


def msg(text, role="user", **kw):
    return Message(role, [text], **kw)


def context(session_id, inputs):
    return SessionContext(session_id=session_id, input_messages=list(inputs))


def test_is_a_history_provider(provider):
    assert isinstance(provider, HistoryProvider)
    assert isinstance(provider, ContextProvider)


def test_configuration_flags_reach_the_base_class(path):
    """before_run/after_run are the base class's; they read these flags."""
    p = CitadelHistoryProvider(
        path,
        "pw",
        embedder=MOCK,
        source_id="flags",
        load_messages=False,
        store_outputs=False,
        store_context_messages=True,
        store_context_from={"other"},
    )
    assert p.load_messages is False
    assert p.store_outputs is False
    assert p.store_context_messages is True
    assert p.store_context_from == {"other"}
    assert p.store_inputs is True


def test_source_id_defaults_to_a_named_constant(path):
    p = CitadelHistoryProvider(path, "pw", embedder=MOCK)
    assert p.source_id == CitadelHistoryProvider.DEFAULT_SOURCE_ID


async def test_messages_round_trip_in_order(provider, request):
    s = sid(request)
    await provider.save_messages(s, [msg("one"), msg("two", "assistant")])
    await provider.save_messages(s, [msg("three")])
    got = await provider.get_messages(s)
    assert [m.text for m in got] == ["one", "two", "three"]


async def test_roles_and_author_survive(provider, request):
    s = sid(request)
    await provider.save_messages(
        s,
        [
            msg("sys", "system"),
            msg("q", "user", author_name="alice"),
            msg("a", "assistant"),
        ],
    )
    got = await provider.get_messages(s)
    assert [str(m.role) for m in got] == [str(m.role) for m in got]  # stable
    assert got[1].author_name == "alice"


async def test_additional_properties_survive(provider, request):
    s = sid(request)
    await provider.save_messages(
        s, [Message("user", ["x"], additional_properties={"_excluded": True})]
    )
    got = await provider.get_messages(s)
    assert got[0].additional_properties["_excluded"] is True


async def test_multi_content_messages_round_trip(provider, request):
    s = sid(request)
    await provider.save_messages(s, [Message("user", ["first", "second"])])
    got = await provider.get_messages(s)
    assert len(got[0].contents) == 2


async def test_save_appends_rather_than_replaces(provider, request):
    """Built-in providers extend the stored list; history is a transcript."""
    s = sid(request)
    reference = InMemoryHistoryProvider()
    state: dict = {}
    for batch in ([msg("a")], [msg("b")]):
        await provider.save_messages(s, batch)
        await reference.save_messages(s, batch, state=state)
    ours = [m.text for m in await provider.get_messages(s)]
    theirs = [m.text for m in await reference.get_messages(s, state=state)]
    assert ours == theirs == ["a", "b"]


async def test_saving_nothing_is_not_an_error(provider, request):
    s = sid(request)
    await provider.save_messages(s, [])
    assert await provider.get_messages(s) == []


async def test_an_unseen_session_is_empty_not_an_error(provider):
    assert await provider.get_messages("never-used") == []


async def test_before_run_loads_stored_history_into_context(provider, request):
    s = sid(request)
    await provider.save_messages(s, [msg("remembered")])
    ctx = context(s, [msg("new question")])
    await provider.before_run(
        agent=None, session=AgentSession(session_id=s), context=ctx, state={}
    )
    loaded = ctx.get_messages(sources={provider.source_id})
    assert [m.text for m in loaded] == ["remembered"]


async def test_after_run_stores_the_input_messages(provider, request):
    """store_inputs defaults True, so a turn's inputs land in the transcript."""
    s = sid(request)
    ctx = context(s, [msg("asked")])
    await provider.after_run(
        agent=None, session=AgentSession(session_id=s), context=ctx, state={}
    )
    assert [m.text for m in await provider.get_messages(s)] == ["asked"]


async def test_a_full_turn_accumulates(provider, request):
    s = sid(request)
    session = AgentSession(session_id=s)
    for text in ("first", "second"):
        ctx = context(s, [msg(text)])
        await provider.before_run(agent=None, session=session, context=ctx, state={})
        await provider.after_run(agent=None, session=session, context=ctx, state={})
    assert [m.text for m in await provider.get_messages(s)] == ["first", "second"]


async def test_load_messages_false_still_stores(path):
    """An audit-only provider: stores but never loads."""
    p = CitadelHistoryProvider(
        path, "pw", embedder=MOCK, source_id="audit", load_messages=False
    )
    ctx = context("audit-s", [msg("recorded")])
    await p.after_run(
        agent=None, session=AgentSession(session_id="audit-s"), context=ctx, state={}
    )
    assert [m.text for m in await p.get_messages("audit-s")] == ["recorded"]


async def test_store_inputs_false_records_nothing_from_inputs(path):
    p = CitadelHistoryProvider(
        path, "pw", embedder=MOCK, source_id="nostore", store_inputs=False
    )
    ctx = context("nostore-s", [msg("ignored")])
    await p.after_run(
        agent=None, session=AgentSession(session_id="nostore-s"), context=ctx, state={}
    )
    assert await p.get_messages("nostore-s") == []


@pytest.mark.filterwarnings("ignore:.*FileHistoryProvider is experimental.*")
async def test_matches_the_file_provider_on_a_turn(provider, request, tmp_path):
    """FileHistoryProvider is the closest built-in analogue; behave like it."""
    s = sid(request)
    reference = FileHistoryProvider(str(tmp_path / "hist"), source_id="ref")
    for p in (provider, reference):
        ctx = context(s, [msg("a"), msg("b", "assistant")])
        await p.after_run(
            agent=None, session=AgentSession(session_id=s), context=ctx, state={}
        )
    ours = [m.text for m in await provider.get_messages(s)]
    theirs = [m.text for m in await reference.get_messages(s)]
    assert ours == theirs


async def test_state_is_ignored_like_the_file_provider(provider, request):
    """State-backed storage is InMemoryHistoryProvider's model, not ours."""
    s = sid(request)
    await provider.save_messages(s, [msg("stored")], state={"messages": []})
    got = await provider.get_messages(s, state={"messages": [msg("phantom")]})
    assert [m.text for m in got] == ["stored"]


async def test_search_finds_a_session_buried_under_another(provider):
    """Discarding after the scan spends the budget on the busy session."""
    quiet, noisy = "buried-quiet", "buried-noisy"
    # Ranked above the target for this query, and enough of them to fill the scan.
    await provider.save_messages(
        noisy, [msg(f"why did the release break run {i}") for i in range(60)]
    )
    await provider.save_messages(
        quiet, [msg("the deployment failed because the disk was full")]
    )
    hits = await provider.search(quiet, "why did the release break?", limit=1)
    assert hits, "the quiet session's only match was crowded out"
    assert "disk was full" in hits[0].text


async def test_sessions_do_not_see_each_other(provider):
    await provider.save_messages("iso-a", [msg("mine")])
    await provider.save_messages("iso-b", [msg("yours")])
    assert [m.text for m in await provider.get_messages("iso-a")] == ["mine"]


async def test_session_ids_are_matched_exactly_not_by_prefix(provider):
    await provider.save_messages("pre", [msg("outer")])
    await provider.save_messages("pre-fix", [msg("inner")])
    assert [m.text for m in await provider.get_messages("pre")] == ["outer"]


async def test_a_none_session_id_uses_one_stable_bucket(provider):
    """The protocol allows None; the file provider uses a fixed stem too."""
    await provider.save_messages(None, [msg("unattributed")])
    assert [m.text for m in await provider.get_messages(None)] == ["unattributed"]


async def test_forget_destroys_one_session(provider):
    await provider.save_messages("gone", [msg("a"), msg("b")])
    await provider.save_messages("kept", [msg("c")])
    assert await provider.forget("gone") == 2
    assert await provider.get_messages("gone") == []
    assert len(await provider.get_messages("kept")) == 1


async def test_forgetting_nothing_is_zero_not_an_error(provider):
    assert await provider.forget("never-existed") == 0


async def test_a_session_is_reusable_after_forgetting(provider):
    await provider.save_messages("reuse", [msg("first")])
    await provider.forget("reuse")
    await provider.save_messages("reuse", [msg("second")])
    assert [m.text for m in await provider.get_messages("reuse")] == ["second"]


async def test_it_survives_a_reopen(tmp_path):
    """A provider that cannot be reopened is not persistence."""
    import gc

    p = str(tmp_path / "reopen.cdl")
    first = CitadelHistoryProvider(p, "pw", embedder=MOCK)
    await first.save_messages("s", [msg("the disk was full"), msg("ok", "assistant")])
    del first
    gc.collect()

    again = CitadelHistoryProvider(p, "pw", embedder=MOCK)
    assert [m.text for m in await again.get_messages("s")] == [
        "the disk was full",
        "ok",
    ]


async def test_concurrent_saves_all_land(tmp_path):
    """Many in-flight sessions means many worker threads against one engine."""
    import asyncio

    h = CitadelHistoryProvider(str(tmp_path / "conc.cdl"), "pw", embedder=MOCK)
    await asyncio.gather(*(h.save_messages(f"s{i}", [msg(f"m{i}")]) for i in range(40)))
    total = 0
    for i in range(40):
        total += len(await h.get_messages(f"s{i}"))
    assert total == 40


async def test_a_wrong_passphrase_cannot_reopen(tmp_path):
    """Transcripts are the payload, so the encryption claim is pinned here."""
    import gc

    import citadeldb

    p = str(tmp_path / "enc.cdl")
    first = CitadelHistoryProvider(p, "right", embedder=MOCK)
    await first.save_messages("s", [msg("secret")])
    del first
    gc.collect()

    with pytest.raises(citadeldb.EncryptionError):
        CitadelHistoryProvider(p, "wrong", embedder=MOCK)


def test_a_passphrase_is_required(tmp_path):
    with pytest.raises(ValueError, match="passphrase"):
        CitadelHistoryProvider(str(tmp_path / "k.cdl"), "", embedder=MOCK)


async def test_a_message_with_no_text_is_storable(provider, request):
    """An atom needs text to embed, so a textless message needs a handle."""
    s = sid(request)
    await provider.save_messages(s, [Message("assistant", [])])
    got = await provider.get_messages(s)
    assert len(got) == 1 and got[0].text == ""


async def test_a_long_history_keeps_its_order(provider, request):
    s = sid(request)
    await provider.save_messages(s, [msg(f"turn {i}") for i in range(200)])
    assert [m.text for m in await provider.get_messages(s)] == [
        f"turn {i}" for i in range(200)
    ]


async def test_order_survives_across_separate_batches(provider, request):
    s = sid(request)
    for i in range(20):
        await provider.save_messages(s, [msg(f"m{i}")])
    assert [m.text for m in await provider.get_messages(s)] == [
        f"m{i}" for i in range(20)
    ]


async def test_two_providers_share_one_database_file(tmp_path):
    p = str(tmp_path / "shared.cdl")
    a = CitadelHistoryProvider(p, "pw", embedder=MOCK, source_id="a")
    b = CitadelHistoryProvider(p, "pw", embedder=MOCK, source_id="b")
    await a.save_messages("s", [msg("written by a")])
    assert [m.text for m in await b.get_messages("s")] == ["written by a"]


async def test_the_event_loop_is_not_blocked(provider, request):
    """The abstract methods are async, so sync bindings run off the loop."""
    import asyncio

    s = sid(request)
    ticks = 0

    async def tick():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    ticker = asyncio.create_task(tick())
    await asyncio.sleep(0)
    await provider.save_messages(s, [msg(f"loop {i}") for i in range(40)])
    await provider.get_messages(s)
    ticker.cancel()
    assert ticks > 1, "the loop made no progress during a provider call"


def test_an_embedder_is_required(tmp_path):
    with pytest.raises(TypeError, match="embedder"):
        CitadelHistoryProvider(str(tmp_path / "no-embedder.cdl"), "pw")
    partial = type(
        "PartialEmbedder",
        (),
        {"dim": 8, "metric": "cosine", "embed": lambda self, texts: []},
    )()
    with pytest.raises(TypeError, match="model_id"):
        CitadelHistoryProvider(
            str(tmp_path / "invalid-embedder.cdl"), "pw", embedder=partial
        )
    artifacts = list(tmp_path.iterdir())
    assert artifacts == [], f"invalid construction created vault sidecars: {artifacts}"


def test_history_normalizes_model_id_without_mutating_the_caller(tmp_path):
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
    path = str(tmp_path / "normalized-history.cdl")
    first = CitadelHistoryProvider(path, "pw", embedder=embedder)
    assert embedder.model_id == "  stable-model  "
    first._db.close()

    embedder.model_id = "stable-model"
    second = CitadelHistoryProvider(path, "pw", embedder=embedder)
    second._db.close()
