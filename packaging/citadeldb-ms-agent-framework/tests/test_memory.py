import pytest
from agent_framework import AgentSession, ContextProvider, Message, SessionContext

from citadeldb_ms_agent_framework import CitadelContextProvider


@pytest.fixture(scope="module")
def path(tmp_path_factory):
    # Citadel takes an exclusive lock, so the whole module shares one file.
    return str(tmp_path_factory.mktemp("mem") / "m.cdl")


@pytest.fixture()
def provider(path, request):
    return CitadelContextProvider(
        path, "pw", source_id=request.node.name, scope=request.node.name
    )


def msg(text, role="user"):
    return Message(role, [text])


def context(inputs, session_id="s"):
    return SessionContext(session_id=session_id, input_messages=list(inputs))


async def turn(provider, texts, session_id="s"):
    ctx = context([msg(t) for t in texts], session_id)
    await provider.after_run(
        agent=None, session=AgentSession(session_id=session_id), context=ctx, state={}
    )


# ---- conformance ---------------------------------------------------------


def test_is_a_context_provider(provider):
    assert isinstance(provider, ContextProvider)
    assert provider.source_id


def test_a_passphrase_is_required(tmp_path):
    with pytest.raises(ValueError, match="passphrase"):
        CitadelContextProvider(str(tmp_path / "k.cdl"), "")


# ---- recall --------------------------------------------------------------


async def test_a_restated_fact_does_not_crowd_out_the_rest_of_the_scope(provider):
    """after_run stores every turn verbatim, so a fact the user restates is
    stored once per turn. Those copies are one memory to the model, so asking
    the engine for `limit` rows spends the whole budget on them."""
    for _ in range(40):
        await turn(provider, ["the deploy failed because the disk was full"])
    for n in range(4):
        await turn(provider, [f"unrelated note {n}"])

    ctx = context([msg("why did the release break?")])
    await provider.before_run(
        agent=None, session=AgentSession(session_id="s"), context=ctx, state={}
    )
    delivered = "\n".join(m.text for m in ctx.get_messages(sources={provider.source_id}))
    assert delivered.count("the disk was full") == 1, "duplicates reached the model"
    for n in range(4):
        assert f"unrelated note {n}" in delivered, (
            f"note {n} was crowded out by 40 copies of one fact"
        )


async def test_a_memory_is_recalled_into_the_context(provider):
    await turn(provider, ["the deploy failed because the disk was full"])
    ctx = context([msg("why did the release break?")])
    await provider.before_run(
        agent=None, session=AgentSession(session_id="s"), context=ctx, state={}
    )
    added = ctx.get_messages(sources={provider.source_id})
    assert added, "nothing was recalled"
    assert "disk was full" in added[0].text


async def test_a_scope_buried_under_another_is_still_recalled(path, request):
    """Discarding after the scan spends the budget on the crowded scope."""
    name = request.node.name
    noisy = CitadelContextProvider(path, "pw", source_id=f"{name}-n", scope=f"{name}-n")
    quiet = CitadelContextProvider(path, "pw", source_id=name, scope=name)
    # Ranked above the target for this query, and enough of them to fill the scan.
    await turn(noisy, [f"why did the release break run {i}" for i in range(60)])
    await turn(quiet, ["the deploy failed because the disk was full"])

    ctx = context([msg("why did the release break?")])
    await quiet.before_run(
        agent=None, session=AgentSession(session_id="s"), context=ctx, state={}
    )
    added = ctx.get_messages(sources={quiet.source_id})
    assert added, "the quiet scope's only memory was crowded out"
    assert "disk was full" in added[0].text


async def test_the_context_prompt_frames_the_memories(provider):
    await turn(provider, ["remembered fact"])
    ctx = context([msg("recall")])
    await provider.before_run(
        agent=None, session=AgentSession(session_id="s"), context=ctx, state={}
    )
    added = ctx.get_messages(sources={provider.source_id})
    assert added[0].text.startswith(CitadelContextProvider.DEFAULT_CONTEXT_PROMPT)


async def test_recall_is_capped_by_limit(path):
    p = CitadelContextProvider(path, "pw", source_id="cap", scope="cap", limit=2)
    await turn(p, [f"fact number {i}" for i in range(6)])
    ctx = context([msg("fact")])
    await p.before_run(
        agent=None, session=AgentSession(session_id="s"), context=ctx, state={}
    )
    added = ctx.get_messages(sources={p.source_id})
    recalled = added[0].text.removeprefix(
        CitadelContextProvider.DEFAULT_CONTEXT_PROMPT
    ).strip().splitlines()
    assert len(recalled) == 2, recalled


async def test_a_repeated_fact_does_not_spend_the_whole_budget(path):
    """A fact repeated across turns is stored each time, and it is one fact."""
    p = CitadelContextProvider(path, "pw", source_id="dedup", scope="dedup", limit=5)
    for _ in range(5):
        await turn(p, ["my dog is called Mochi"])
    await turn(p, ["I live in Berlin"])

    ctx = context([msg("what do you know about me?")])
    await p.before_run(
        agent=None, session=AgentSession(session_id="s"), context=ctx, state={}
    )
    recalled = ctx.get_messages(sources={p.source_id})[0].text.removeprefix(
        CitadelContextProvider.DEFAULT_CONTEXT_PROMPT
    ).strip().splitlines()
    assert len(recalled) == len(set(recalled)), recalled
    assert any("Mochi" in r for r in recalled)
    assert any("Berlin" in r for r in recalled)


async def test_nothing_is_added_when_there_is_nothing_to_recall(provider):
    ctx = context([msg("a question with no history behind it")])
    await provider.before_run(
        agent=None, session=AgentSession(session_id="s"), context=ctx, state={}
    )
    assert ctx.get_messages(sources={provider.source_id}) == []


async def test_an_empty_input_recalls_nothing(provider):
    await turn(provider, ["something"])
    ctx = context([Message("user", [])])
    await provider.before_run(
        agent=None, session=AgentSession(session_id="s"), context=ctx, state={}
    )
    assert ctx.get_messages(sources={provider.source_id}) == []


# ---- what gets remembered ------------------------------------------------


async def test_memories_outlive_the_session_that_made_them(provider):
    """Cross-session recall is this provider's job, not the history one's."""
    await turn(provider, ["learned in session one"], session_id="one")
    ctx = context([msg("learned")], session_id="two")
    await provider.before_run(
        agent=None, session=AgentSession(session_id="two"), context=ctx, state={}
    )
    added = ctx.get_messages(sources={provider.source_id})
    assert added and "session one" in added[0].text


async def test_scopes_do_not_see_each_other(path):
    a = CitadelContextProvider(path, "pw", source_id="sa", scope="alice")
    b = CitadelContextProvider(path, "pw", source_id="sb", scope="bob")
    await turn(a, ["alice's private note"])
    ctx = context([msg("private note")])
    await b.before_run(
        agent=None, session=AgentSession(session_id="s"), context=ctx, state={}
    )
    assert ctx.get_messages(sources={b.source_id}) == []


async def test_a_turn_with_no_text_remembers_nothing(provider):
    ctx = context([Message("user", [])])
    await provider.after_run(
        agent=None, session=AgentSession(session_id="s"), context=ctx, state={}
    )
    assert await provider.forget() == 0


# ---- erasure -------------------------------------------------------------


async def test_forget_destroys_the_scope(provider):
    await turn(provider, ["a", "b"])
    assert await provider.forget() == 2
    ctx = context([msg("a")])
    await provider.before_run(
        agent=None, session=AgentSession(session_id="s"), context=ctx, state={}
    )
    assert ctx.get_messages(sources={provider.source_id}) == []


async def test_it_survives_a_reopen(tmp_path):
    """A region's embedder lives in memory, so recall must survive reopen."""
    import gc

    p = str(tmp_path / "reopen.cdl")
    first = CitadelContextProvider(p, "pw", scope="u")
    await turn(first, ["the deployment failed because the disk was full"])
    del first
    gc.collect()

    again = CitadelContextProvider(p, "pw", scope="u")
    ctx = context([msg("why did the release break?")])
    await again.before_run(
        agent=None, session=AgentSession(session_id="s"), context=ctx, state={}
    )
    assert ctx.get_messages(sources={again.source_id}), "recall did not survive reopen"


async def test_forgetting_nothing_is_zero_not_an_error(provider):
    assert await provider.forget() == 0


# ---- the two providers together ------------------------------------------


async def test_it_shares_a_database_with_the_history_provider(tmp_path):
    """The pairing the framework's own Redis integration ships, on one file."""
    from citadeldb_ms_agent_framework import CitadelHistoryProvider

    p = str(tmp_path / "both.cdl")
    memory = CitadelContextProvider(p, "pw", scope="u1")
    history = CitadelHistoryProvider(p, "pw")
    await turn(memory, ["remembered across sessions"])
    await history.save_messages("s", [msg("this exact turn")])
    assert len(await history.get_messages("s")) == 1
    assert await memory.forget() == 1


async def test_the_event_loop_is_not_blocked(provider):
    import asyncio

    ticks = 0

    async def tick():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    ticker = asyncio.create_task(tick())
    await asyncio.sleep(0)
    await turn(provider, [f"fact {i}" for i in range(40)])
    ctx = context([msg("fact")])
    await provider.before_run(
        agent=None, session=AgentSession(session_id="s"), context=ctx, state={}
    )
    ticker.cancel()
    assert ticks > 1, "the loop made no progress during a provider call"
