import citadeldb
import pytest
from citadeldb_langchain import CitadelChatMessageHistory
from citadeldb_langchain.chat_history import _require_embedder
from langchain_core.chat_history import BaseChatMessageHistory
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

MOCK = citadeldb.MockEmbedder(dim=64)


@pytest.fixture(scope="module")
def path(tmp_path_factory):
    # Citadel takes an exclusive lock, so the whole module shares one file.
    return str(tmp_path_factory.mktemp("hist") / "h.cdl")


@pytest.fixture()
def history(path, request):
    return CitadelChatMessageHistory(request.node.name, path, key="pw", embedder=MOCK)


def test_is_a_chat_message_history(history):
    assert isinstance(history, BaseChatMessageHistory)


def test_messages_round_trip_in_order(history):
    history.add_messages([HumanMessage("hello"), AIMessage("hi there")])
    history.add_messages([HumanMessage("and again")])
    assert [m.content for m in history.messages] == ["hello", "hi there", "and again"]


def test_message_types_survive(history):
    history.add_messages([SystemMessage("be brief"), HumanMessage("q"), AIMessage("a")])
    assert [m.type for m in history.messages] == ["system", "human", "ai"]


def test_add_message_singular_works(history):
    """The base class routes add_message through add_messages."""
    history.add_message(HumanMessage("just one"))
    assert [m.content for m in history.messages] == ["just one"]


def test_convenience_helpers_work(history):
    history.add_user_message("from the user")
    history.add_ai_message("from the model")
    assert [m.type for m in history.messages] == ["human", "ai"]


def test_tool_messages_round_trip(history):
    """A tool result carries fields a plain text store would drop."""
    history.add_messages([ToolMessage(content="42", tool_call_id="call-1")])
    got = history.messages[0]
    assert got.type == "tool" and got.tool_call_id == "call-1"


def test_block_content_round_trips(history):
    """Content can be a list of blocks rather than a string."""
    history.add_messages([HumanMessage(content=[{"type": "text", "text": "blocky"}])])
    assert history.messages[0].content == [{"type": "text", "text": "blocky"}]


def test_additional_kwargs_survive(history):
    history.add_messages([AIMessage("x", additional_kwargs={"finish": "stop"})])
    assert history.messages[0].additional_kwargs["finish"] == "stop"


def test_empty_history_is_an_empty_list(history):
    assert history.messages == []


def test_adding_nothing_is_not_an_error(history):
    history.add_messages([])
    assert history.messages == []


def test_clear_empties_only_this_session(path):
    a = CitadelChatMessageHistory("iso-a", path, key="pw", embedder=MOCK)
    b = CitadelChatMessageHistory("iso-b", path, key="pw", embedder=MOCK)
    a.add_messages([HumanMessage("mine")])
    b.add_messages([HumanMessage("yours")])
    a.clear()
    assert a.messages == []
    assert [m.content for m in b.messages] == ["yours"]


def test_history_is_reusable_after_clear(history):
    history.add_messages([HumanMessage("first")])
    history.clear()
    history.add_messages([HumanMessage("second")])
    assert [m.content for m in history.messages] == ["second"]


def test_forget_reports_how_many_keys_were_destroyed(history):
    history.add_messages([HumanMessage("a"), HumanMessage("b")])
    assert history.forget() == 2
    assert history.messages == []


def test_session_ids_are_matched_exactly_not_by_prefix(path):
    outer = CitadelChatMessageHistory("pre", path, key="pw", embedder=MOCK)
    inner = CitadelChatMessageHistory("pre-fix", path, key="pw", embedder=MOCK)
    outer.add_messages([HumanMessage("outer only")])
    inner.add_messages([HumanMessage("inner only")])
    assert [m.content for m in outer.messages] == ["outer only"]


def test_a_passphrase_is_required(tmp_path):
    with pytest.raises(ValueError, match="passphrase"):
        CitadelChatMessageHistory("s", str(tmp_path / "k.cdl"), key="", embedder=MOCK)


def test_a_wrong_passphrase_cannot_reopen(tmp_path):
    """Transcripts are the payload, so the encryption claim is pinned here."""
    import gc

    import citadeldb

    p = str(tmp_path / "enc.cdl")
    first = CitadelChatMessageHistory("s", p, key="right", embedder=MOCK)
    first.add_messages([HumanMessage("secret")])
    del first
    gc.collect()

    with pytest.raises(citadeldb.EncryptionError):
        CitadelChatMessageHistory("s", p, key="wrong", embedder=MOCK)


async def test_async_surface_round_trips(history):
    await history.aadd_messages([HumanMessage("async")])
    assert [m.content for m in await history.aget_messages()] == ["async"]
    await history.aclear()
    assert await history.aget_messages() == []


async def test_the_event_loop_is_not_blocked(history):
    import asyncio

    ticks = 0

    async def tick():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    ticker = asyncio.create_task(tick())
    await asyncio.sleep(0)
    await history.aadd_messages([HumanMessage(f"turn {i}") for i in range(40)])
    await history.aget_messages()
    ticker.cancel()
    assert ticks > 1, "the loop made no progress during a history call"


def test_an_embedder_is_required(tmp_path):
    with pytest.raises(TypeError, match="embedder"):
        CitadelChatMessageHistory("s", str(tmp_path / "no-embedder.cdl"), key="pw")
    partial = type(
        "PartialEmbedder",
        (),
        {"dim": 8, "metric": "cosine", "embed": lambda self, texts: []},
    )()
    with pytest.raises(TypeError, match="model_id"):
        CitadelChatMessageHistory(
            "s", str(tmp_path / "invalid-embedder.cdl"), key="pw", embedder=partial
        )
    partial.model_id = "default"
    with pytest.raises(TypeError, match="unknown.*default"):
        CitadelChatMessageHistory(
            "s", str(tmp_path / "placeholder-embedder.cdl"), key="pw", embedder=partial
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
