"""Tests for CitadelSessionManager, asserted against FileSessionManager."""

import concurrent.futures as cf
import gc
import os
import tempfile

import citadeldb
import pytest
from citadeldb_strands_agents import CitadelSessionManager
from citadeldb_strands_agents.session import _require_embedder
from strands.session.file_session_manager import FileSessionManager
from strands.types.exceptions import SessionException
from strands.types.session import Session, SessionAgent, SessionMessage, SessionType

MOCK = citadeldb.MockEmbedder(dim=64)

SID = "s1"
AID = "a1"


def path(name="s.cdl"):
    return os.path.join(tempfile.mkdtemp(), name)


def mgr(p=None, session_id=SID):
    return CitadelSessionManager(session_id, p or path(), "pw", embedder=MOCK)


def ref(session_id=SID):
    return FileSessionManager(session_id, storage_dir=tempfile.mkdtemp())


def msg(i, text="hello"):
    return SessionMessage(
        message={"role": "user", "content": [{"text": text}]}, message_id=i
    )


def agent(aid=AID):
    return SessionAgent(agent_id=aid, state={}, conversation_manager_state={})


def seeded(m, n=3):
    """A session with an agent and n messages."""
    m.create_agent(SID, agent())
    for i in range(n):
        m.create_message(SID, AID, msg(i, f"turn {i}"))
    return m


def test_it_is_both_a_repository_and_a_manager():
    from strands.session.session_manager import SessionManager
    from strands.session.session_repository import SessionRepository

    m = mgr()
    assert isinstance(m, SessionRepository)
    assert isinstance(m, SessionManager)


def test_constructing_it_creates_the_session_like_the_reference():
    """RepositorySessionManager creates the session on construction."""
    assert mgr().read_session(SID) is not None
    assert ref().read_session(SID) is not None


def test_a_passphrase_is_required():
    with pytest.raises(ValueError, match="passphrase"):
        CitadelSessionManager(SID, path(), "", embedder=MOCK)


def test_creating_a_duplicate_session_raises_like_the_reference():
    m, r = mgr(), ref()
    dup = Session(session_id=SID, session_type=SessionType.AGENT)
    for store in (m, r):
        with pytest.raises(SessionException, match="already exists"):
            store.create_session(dup)


def test_reading_an_unknown_session_is_none_like_the_reference():
    assert mgr().read_session("nope") is None
    assert ref().read_session("nope") is None


def test_reading_an_unknown_agent_is_none_like_the_reference():
    assert mgr().read_agent(SID, "nope") is None
    assert ref().read_agent(SID, "nope") is None


def test_updating_an_unknown_agent_raises_like_the_reference():
    m, r = mgr(), ref()
    for store in (m, r):
        with pytest.raises(SessionException, match="does not exist"):
            store.update_agent(SID, agent("ghost"))


def test_reading_an_unknown_message_is_none_like_the_reference():
    m, r = seeded(mgr()), seeded(ref())
    assert m.read_message(SID, AID, 99) is None
    assert r.read_message(SID, AID, 99) is None


def test_updating_an_unknown_message_raises_like_the_reference():
    m, r = seeded(mgr()), seeded(ref())
    for store in (m, r):
        with pytest.raises(SessionException, match="does not exist"):
            store.update_message(SID, AID, msg(99))


def test_listing_messages_for_an_unknown_agent_raises_like_the_reference():
    m, r = mgr(), ref()
    for store in (m, r):
        with pytest.raises(SessionException):
            store.list_messages(SID, "ghost")


def test_messages_come_back_in_conversation_order():
    m = seeded(mgr(), 5)
    assert [x.message_id for x in m.list_messages(SID, AID)] == [0, 1, 2, 3, 4]


def test_order_follows_the_index_not_the_write_time():
    """Updating an early message must not move it to the end."""
    m = seeded(mgr(), 5)
    m.update_message(SID, AID, msg(1, "edited"))
    assert [x.message_id for x in m.list_messages(SID, AID)] == [0, 1, 2, 3, 4]


def test_pagination_matches_the_reference():
    m, r = seeded(mgr(), 6), seeded(ref(), 6)
    for limit, offset in ((2, 0), (2, 2), (None, 3), (10, 0), (2, 99)):
        ours = [x.message_id for x in m.list_messages(SID, AID, limit, offset)]
        theirs = [x.message_id for x in r.list_messages(SID, AID, limit, offset)]
        assert ours == theirs, (limit, offset, ours, theirs)


def test_updating_a_message_does_not_duplicate_it():
    m = seeded(mgr(), 3)
    for text in ("one", "two", "three"):
        m.update_message(SID, AID, msg(1, text))
    assert len(m.list_messages(SID, AID)) == 3
    assert m.read_message(SID, AID, 1).message["content"][0]["text"] == "three"


def test_updating_an_agent_does_not_duplicate_it():
    m = seeded(mgr())
    for _ in range(5):
        a = agent()
        a.state = {"n": 1}
        m.update_agent(SID, a)
    assert m.read_agent(SID, AID).state == {"n": 1}


def test_the_reference_keeps_the_redacted_text_on_disk():
    """Established as the baseline this integration improves on."""
    r = seeded(ref(), 1)
    original = "my card number is 4111 1111 1111 1111"
    r.create_message(SID, AID, msg(1, original))
    m = r.read_message(SID, AID, 1)
    m.redact_message = {"role": "user", "content": [{"text": "[REDACTED]"}]}
    r.update_message(SID, AID, m)

    stored = r.read_message(SID, AID, 1)
    assert stored.redact_message is not None
    assert original in str(stored.message), "the reference keeps the original beside it"


def test_a_redaction_removes_the_original_from_the_store():
    """Keeping the original would leave the redacted content in the store."""
    m = seeded(mgr(), 0)
    secret = "my card number is 4111 1111 1111 1111"
    m.create_message(SID, AID, msg(0, secret))

    stored = m.read_message(SID, AID, 0)
    stored.redact_message = {"role": "user", "content": [{"text": "[REDACTED]"}]}
    m.update_message(SID, AID, stored)

    after = m.read_message(SID, AID, 0)
    assert after.to_message() == {"role": "user", "content": [{"text": "[REDACTED]"}]}
    assert secret not in str(after.message), "the original survived the redaction"
    assert secret not in str(after.to_dict()), "the original survived the redaction"


def test_a_redaction_at_creation_time_also_drops_the_original():
    m = seeded(mgr(), 0)
    secret = "another secret"
    sm = msg(0, secret)
    sm.redact_message = {"role": "user", "content": [{"text": "[REDACTED]"}]}
    m.create_message(SID, AID, sm)
    assert secret not in str(m.read_message(SID, AID, 0).to_dict())


@pytest.mark.parametrize("operation", ["create", "update"])
@pytest.mark.parametrize(
    ("redaction", "expected_text"),
    [
        pytest.param({}, "message 0", id="empty-dict"),
        pytest.param({"role": "user", "content": []}, "message 0", id="empty-content"),
        pytest.param(
            {"role": "user", "content": [{"text": "[REDACTED]"}]},
            "[REDACTED]",
            id="text",
        ),
        pytest.param(
            {
                "role": "user",
                "content": [
                    {"image": {"format": "png", "source": {"bytes": b"\x89PNG"}}}
                ],
            },
            "message 0",
            id="binary-content",
        ),
    ],
)
def test_redaction_replaces_payload_and_searchable_text(
    tmp_path, operation, redaction, expected_text
):
    m = seeded(mgr(str(tmp_path / "redaction.cdl")), 0)
    secret = "secret that must not survive redaction"
    incoming = msg(0, secret)
    if operation == "update":
        m.create_message(SID, AID, incoming)
        incoming = m.read_message(SID, AID, 0)
        assert incoming.message["content"][0]["text"] == secret

    incoming.redact_message = redaction
    if operation == "create":
        m.create_message(SID, AID, incoming)
    else:
        m.update_message(SID, AID, incoming)

    restored = m.read_message(SID, AID, 0)
    assert restored.message == redaction
    assert restored.to_message() == redaction
    hits = m._mem.fetch(
        m._region, "message", payload_filter={"sid": SID, "aid": AID, "mid": 0}
    )
    assert len(hits) == 1
    assert secret not in str(hits[0].payload)
    assert hits[0].text == expected_text


def test_an_unredacted_message_keeps_its_content():
    """Only a redaction drops the original; an ordinary update must not."""
    m = seeded(mgr(), 0)
    m.create_message(SID, AID, msg(0, "ordinary content"))
    m.update_message(SID, AID, msg(0, "edited content"))
    after = m.read_message(SID, AID, 0)
    assert after.to_message()["content"][0]["text"] == "edited content"
    assert after.redact_message is None
    hits = m._mem.fetch(
        m._region, "message", payload_filter={"sid": SID, "aid": AID, "mid": 0}
    )
    assert len(hits) == 1
    assert hits[0].text == "edited content"


def test_a_redaction_replaces_the_stored_record():
    m = seeded(mgr(), 1)
    original = "my card number is 4111 1111 1111 1111"
    m.create_message(SID, AID, msg(1, original))

    stored = m.read_message(SID, AID, 1)
    stored.redact_message = {"role": "user", "content": [{"text": "[REDACTED]"}]}
    m.update_message(SID, AID, stored)

    # One record for the index, carrying the redaction.
    assert len([x for x in m.list_messages(SID, AID) if x.message_id == 1]) == 1
    assert m.read_message(SID, AID, 1).redact_message is not None


def test_created_at_survives_an_update_like_the_reference():
    m, r = seeded(mgr(), 1), seeded(ref(), 1)
    for store in (m, r):
        first = store.read_message(SID, AID, 0)
        edited = msg(0, "edited")
        store.update_message(SID, AID, edited)
        assert store.read_message(SID, AID, 0).created_at == first.created_at


def test_forget_session_destroys_everything_it_owns():
    p = path()
    m = seeded(mgr(p), 3)
    assert m.forget_session(SID) == 5  # session + agent + 3 messages
    assert m.read_session(SID) is None
    assert m.read_agent(SID, AID) is None


def test_forgetting_an_unknown_session_is_zero():
    assert mgr().forget_session("never") == 0


def test_it_survives_a_reopen():
    p = path()
    first = seeded(mgr(p), 3)
    first.update_agent(
        SID, SessionAgent(agent_id=AID, state={"k": "v"}, conversation_manager_state={})
    )
    del first
    gc.collect()

    again = CitadelSessionManager(SID, p, "pw", embedder=MOCK)
    assert again.read_session(SID) is not None
    assert again.read_agent(SID, AID).state == {"k": "v"}
    assert [x.message_id for x in again.list_messages(SID, AID)] == [0, 1, 2]


def test_a_wrong_passphrase_cannot_reopen():
    import citadeldb

    p = path()
    first = seeded(mgr(p))
    del first
    gc.collect()
    with pytest.raises(citadeldb.EncryptionError):
        CitadelSessionManager(SID, p, "wrong", embedder=MOCK)


def test_concurrent_message_writes_all_land():
    m = seeded(mgr(), 0)
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(lambda i: m.create_message(SID, AID, msg(i, f"t{i}")), range(40)))
    assert len(m.list_messages(SID, AID)) == 40


def test_a_message_id_is_stored_once_however_often_it_is_written():
    """message_id is the conversation index, so a second write of one id
    replaces it. Two request handlers over one store is the normal shape."""
    m = seeded(mgr(), 0)
    m.create_message(SID, AID, msg(7, "from handler one"))
    m.create_message(SID, AID, msg(7, "from handler two"))
    stored = m.list_messages(SID, AID)
    assert [s.message_id for s in stored] == [7]
    assert (
        m.read_message(SID, AID, 7).message["content"][0]["text"] == "from handler two"
    )


def test_concurrent_writes_of_one_message_id_leave_one_record():
    m = seeded(mgr(), 0)
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(lambda i: m.create_message(SID, AID, msg(3, f"t{i}")), range(32)))
    assert [s.message_id for s in m.list_messages(SID, AID)] == [3]


def test_sessions_and_agents_are_scoped_exactly():
    p = path()
    m = CitadelSessionManager("sess", p, "pw", embedder=MOCK)
    other = CitadelSessionManager("sess-2", p, "pw", embedder=MOCK)
    m.create_agent("sess", agent())
    other.create_agent("sess-2", agent())
    m.create_message("sess", AID, msg(0, "mine"))
    assert len(m.list_messages("sess", AID)) == 1
    assert other.list_messages("sess-2", AID) == []


def test_agents_within_a_session_are_scoped_exactly():
    m = mgr()
    m.create_agent(SID, agent("a"))
    m.create_agent(SID, agent("a-2"))
    m.create_message(SID, "a", msg(0, "mine"))
    assert len(m.list_messages(SID, "a")) == 1
    assert m.list_messages(SID, "a-2") == []


def test_unicode_round_trips():
    m = seeded(mgr(), 0)
    text = "ünïcode — 中文 — emoji 🎉"
    m.create_message(SID, AID, msg(0, text))
    assert m.read_message(SID, AID, 0).message["content"][0]["text"] == text


def test_agent_state_named_like_internal_fields_round_trips():
    m = seeded(mgr(), 0)
    hostile = {"sid": "x", "aid": "x", "mid": "x", "message": "x", "agent": "x"}
    a = agent()
    a.state = dict(hostile)
    m.update_agent(SID, a)
    assert m.read_agent(SID, AID).state == hostile


def test_a_message_with_no_text_is_storable():
    m = seeded(mgr(), 0)
    m.create_message(
        SID,
        AID,
        SessionMessage(message={"role": "assistant", "content": []}, message_id=0),
    )
    assert len(m.list_messages(SID, AID)) == 1


def test_two_managers_share_one_database_file():
    p = path()
    a = CitadelSessionManager("sa", p, "pw", embedder=MOCK)
    b = CitadelSessionManager("sb", p, "pw", embedder=MOCK)
    a.create_agent("sa", agent())
    a.create_message("sa", AID, msg(0, "in a"))
    assert a.read_session("sa") is not None and b.read_session("sb") is not None
    assert b.read_session("sa") is not None, "one file, both sessions visible"


def test_an_embedder_is_required(tmp_path):
    with pytest.raises(TypeError, match="embedder"):
        CitadelSessionManager(SID, str(tmp_path / "no-embedder.cdl"), "pw")
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
        CitadelSessionManager(
            SID, str(tmp_path / "invalid-embedder.cdl"), "pw", embedder=partial
        )
    partial.model_id = "default"
    with pytest.raises(TypeError, match="unknown.*default"):
        CitadelSessionManager(
            SID, str(tmp_path / "placeholder-embedder.cdl"), "pw", embedder=partial
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
            "embed_with_cancel": lambda self, texts, cancel_token: [
                [0.0] * 8 for _ in texts
            ],
        },
    )()

    normalized = _require_embedder(embedder)

    assert normalized.model_id == "stable-model"
    assert embedder.model_id == "  stable-model  "
    assert len(normalized.embed_with_cancel(["probe"], None)[0]) == 8

    path = str(tmp_path / "normalized.cdl")
    first = CitadelSessionManager("s", path, "pw", embedder=embedder)
    first._db.close()
    embedder.model_id = "stable-model"
    second = CitadelSessionManager("s", path, "pw", embedder=embedder)
    second._db.close()
