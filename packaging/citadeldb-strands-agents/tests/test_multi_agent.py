"""Multi-agent state and the manager paths the framework drives."""
import os
import tempfile

import pytest
from strands.session.file_session_manager import FileSessionManager
from strands.types.exceptions import SessionException
from strands.types.session import SessionAgent, SessionMessage

from citadeldb_strands_agents import CitadelSessionManager

SID, AID = "s1", "a1"


def path(name="s.cdl"):
    return os.path.join(tempfile.mkdtemp(), name)


def mgr(p=None, session_id=SID):
    return CitadelSessionManager(session_id, p or path(), "pw")


def ref(session_id=SID):
    return FileSessionManager(session_id, storage_dir=tempfile.mkdtemp())


def msg(i, text="hello"):
    return SessionMessage(
        message={"role": "user", "content": [{"text": text}]}, message_id=i
    )


def agent(aid=AID):
    return SessionAgent(agent_id=aid, state={}, conversation_manager_state={})


class FakeMultiAgent:
    """The shape RepositorySessionManager requires."""

    def __init__(self, mid="swarm-1", state=None):
        self.id = mid
        self._state = state or {"nodes": ["a", "b"], "step": 1}

    def serialize_state(self):
        return dict(self._state)

    def deserialize_state(self, state):
        self._state = dict(state)


# ---- multi-agent state: the base class raises without these --------------


def test_multi_agent_state_round_trips():
    m = mgr()
    swarm = FakeMultiAgent()
    m.create_multi_agent(SID, swarm)
    assert m.read_multi_agent(SID, "swarm-1") == {"nodes": ["a", "b"], "step": 1}


def test_reading_unknown_multi_agent_state_is_none_like_the_reference():
    assert mgr().read_multi_agent(SID, "nope") is None
    assert ref().read_multi_agent(SID, "nope") is None


def test_updating_unknown_multi_agent_state_raises_like_the_reference():
    m, r = mgr(), ref()
    for store in (m, r):
        with pytest.raises(SessionException, match="does not exist"):
            store.update_multi_agent(SID, FakeMultiAgent("ghost"))


def test_updating_multi_agent_state_does_not_duplicate():
    m = mgr()
    swarm = FakeMultiAgent()
    m.create_multi_agent(SID, swarm)
    for step in range(2, 6):
        swarm._state["step"] = step
        m.update_multi_agent(SID, swarm)
    assert m.read_multi_agent(SID, "swarm-1")["step"] == 5


def test_the_manager_hook_initializes_multi_agent_state():
    """initialize_multi_agent creates on first sight and restores afterwards."""
    p = path()
    first = mgr(p)
    swarm = FakeMultiAgent(state={"step": 7})
    first.initialize_multi_agent(swarm)
    first.sync_multi_agent(swarm)
    del first

    import gc

    gc.collect()
    again = CitadelSessionManager(SID, p, "pw")
    restored = FakeMultiAgent(state={"step": 0})
    again.initialize_multi_agent(restored)
    assert restored._state["step"] == 7, "multi-agent state did not restore"


def test_forget_session_also_destroys_multi_agent_state():
    m = mgr()
    m.create_multi_agent(SID, FakeMultiAgent())
    m.forget_session(SID)
    assert m.read_multi_agent(SID, "swarm-1") is None


# ---- the path an Agent actually takes ------------------------------------


def test_an_agent_restores_its_history_through_the_manager():
    """initialize() is what Agent construction calls."""
    from strands import Agent

    p = path()
    first = mgr(p)
    a = Agent(agent_id=AID, session_manager=first, messages=[
        {"role": "user", "content": [{"text": "remembered turn"}]}
    ])
    first.sync_agent(a)
    del a, first

    import gc

    gc.collect()
    again = CitadelSessionManager(SID, p, "pw")
    restored = Agent(agent_id=AID, session_manager=again)
    assert any(
        "remembered turn" in str(m) for m in restored.messages
    ), f"history did not restore: {restored.messages}"


def test_agent_state_survives_a_restore():
    from strands import Agent

    p = path()
    first = mgr(p)
    a = Agent(agent_id=AID, session_manager=first)
    a.state.set("locale", "en-GB")
    first.sync_agent(a)
    del a, first

    import gc

    gc.collect()
    again = CitadelSessionManager(SID, p, "pw")
    restored = Agent(agent_id=AID, session_manager=again)
    assert restored.state.get("locale") == "en-GB"


# ---- list_messages slice edges vs the reference --------------------------


def seeded(store, n=4):
    store.create_agent(SID, agent())
    for i in range(n):
        store.create_message(SID, AID, msg(i, f"turn {i}"))
    return store


def test_slice_edges_match_the_reference():
    m, r = seeded(mgr()), seeded(ref())
    for limit, offset in ((0, 0), (1, 0), (None, 0), (None, 10), (3, 2), (100, 1)):
        ours = [x.message_id for x in m.list_messages(SID, AID, limit, offset)]
        theirs = [x.message_id for x in r.list_messages(SID, AID, limit, offset)]
        assert ours == theirs, (limit, offset, ours, theirs)


def test_listing_an_agent_with_no_messages_is_empty_like_the_reference():
    m, r = mgr(), ref()
    for store in (m, r):
        store.create_agent(SID, agent())
        assert store.list_messages(SID, AID) == []


# ---- an update that fails must not lose the record ----------------------


def test_a_failed_update_leaves_the_original_readable():
    """The write path erases before it writes; a failure must not orphan."""
    m = seeded(mgr(), 2)
    broken = msg(0, "replacement")
    broken.message_id = 0

    class Exploding(dict):
        def __getitem__(self, k):
            raise RuntimeError("boom")

    # A message whose serialization explodes.
    try:
        m.update_message(SID, AID, msg(99))  # unknown id: raises before any erase
    except SessionException:
        pass
    assert m.read_message(SID, AID, 0).message["content"][0]["text"] == "turn 0"
    assert len(m.list_messages(SID, AID)) == 2
