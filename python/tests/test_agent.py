"""Agent runtime binding: LLM clients, belief graph, tools, verifier, agent loop."""

import pytest

import citadeldb
from citadeldb import agent as ag


class ScriptLLM:
    """Deterministic LLM callback that records requests and replies with plain text."""

    model_id = "script"

    def __init__(self):
        self.calls = []

    def complete(self, request):
        self.calls.append(request)
        return {"content": "Done.", "finish_reason": "stop"}


def _region(name="agent", dim=64):
    db = citadeldb.connect(key="k")
    mem = db.memory()
    mem.create_region(name, citadeldb.MockEmbedder(dim))
    return mem


# ---- LLM client ------------------------------------------------------------


def test_mock_client_model_id():
    llm = ag.LLMClient.mock()
    assert llm.model_id == "mock"


def test_mock_client_completes():
    llm = ag.LLMClient.mock()
    out = llm.complete({"messages": [{"role": "user", "content": "hi"}]})
    assert isinstance(out, dict)
    assert "content" in out and "finish_reason" in out


def test_unknown_provider_errors():
    with pytest.raises(citadeldb.LlmError):
        ag.LLMClient.provider("not-a-provider", "x")


def test_provider_without_key_errors():
    # claude is compiled in but needs ANTHROPIC_API_KEY; absent -> a clear error.
    import os

    if os.environ.get("ANTHROPIC_API_KEY"):
        pytest.skip("ANTHROPIC_API_KEY is set")
    with pytest.raises(citadeldb.LlmError):
        ag.LLMClient.provider("claude", "claude-opus-4-8")


def test_llm_replay_requires_traces():
    mem = _region()
    g = citadeldb.BeliefGraph(mem, "agent")
    with pytest.raises(citadeldb.ProgrammingError):  # no recorded llm_trace chain
        ag.LLMClient.replay(g)


def test_llm_replay_from_recorded_run():
    mem = _region()
    g = citadeldb.BeliefGraph(mem, "agent")
    resp = ag.LLMClient.mock().complete({"messages": [{"role": "user", "content": "hi"}]})
    g.record_llm_call("h", "mock", resp)
    replay = ag.LLMClient.replay(g)
    assert replay.replay_misses == 0
    assert isinstance(replay.model_id, str)


def test_prompt_library_from_region():
    mem = _region("prompts")
    mem.remember(
        "prompts",
        {"kind": "prompt", "text": "custom planner", "payload": {"name": "planner", "version": 99}},
    )
    lib = ag.PromptLibrary.from_region(mem, "prompts")
    assert lib.resolve("planner") == "custom planner"


# ---- belief graph (no LLM) -------------------------------------------------


def test_belief_graph_drive_and_verify():
    mem = _region()
    g = citadeldb.BeliefGraph(mem, "agent")

    goal_id = g.add_goal(citadeldb.Goal("solve it", acceptance_criteria=["done"]))
    sm_id = g.set_self_model(ag.SelfModel("solver", goal_ref=goal_id))
    task_id = g.add_task(ag.Task("step one"), [], goal_id)

    assert g.get_goal(goal_id).prompt == "solve it"
    assert g.get_task(task_id).status == "pending"

    g.set_task_status(task_id, "done")
    assert g.get_task(task_id).status == "done"
    assert g.next_unblocked_tasks() == []  # the only task is done

    assert g.current_self_model().identity == "solver"
    assert g.has_provenance(task_id, goal_id)

    check = ag.CoInstantiationCheck("action-1", goal_id, sm_id, True, True, 0, 5)
    assert check.verdict == "pass"
    g.record_check(check, task_id)

    report = g.verify_chain()
    assert report.valid
    assert report.total_checks == 1
    assert report.breaches == []

    trail = g.export_audit_trail()
    assert len(trail) == 1 and trail[0].action_id == "action-1"


def test_belief_graph_goal_status():
    mem = _region()
    g = citadeldb.BeliefGraph(mem, "agent")
    goal_id = g.add_goal(citadeldb.Goal("g"))
    assert g.get_goal_status(goal_id) is None
    g.set_goal_status(goal_id, "achieved")
    assert g.get_goal_status(goal_id) == "achieved"


def test_verified_export_kind_is_short_form():
    mem = _region()
    g = citadeldb.BeliefGraph(mem, "agent")
    cand = g.add_candidate('{"v": 1}', 0.9)
    atom = g.add_verified_artifact(cand, "construction", "checker-x", "1.0", 0.95)
    exp = g.export_verified_artifact(atom)
    assert exp is not None and exp.kind == "construction"  # short form, as minted
    cand2 = g.add_candidate('{"v": 2}', 0.8)
    atom2 = g.add_verified_artifact(cand2, "lemma", "checker-x", "1.0", 0.9)
    assert g.export_verified_artifact(atom2).kind == "lemma"


# ---- tools -----------------------------------------------------------------


class EchoTool:
    name = "echo"
    description = "echo the text argument"
    input_schema = {
        "type": "object",
        "properties": {"text": {"type": "string"}},
        "required": ["text"],
    }

    def call(self, args):
        return args.get("text", "")


def test_tool_registry_python_and_builtin():
    mem = _region()
    tools = ag.ToolRegistry()
    tools.register(EchoTool())
    tools.add_mem_recall(mem, "agent")
    tools.add_mem_remember(mem, "agent")

    names = tools.names()
    assert "echo" in names
    assert len(names) >= 3  # echo + the two built-ins

    specs = tools.specs()
    echo = next(s for s in specs if s["name"] == "echo")
    assert echo["description"] == "echo the text argument"
    assert echo["input_schema"]["type"] == "object"

    assert tools.contains("echo")
    assert tools.permissions("echo") is not None


def test_tool_registry_file_tool_permissions(tmp_path):
    tools = ag.ToolRegistry()
    tools.add_file_read([str(tmp_path)])
    perms = tools.permissions("file_read")
    assert perms is not None
    assert perms["filesystem"] is not None  # an allowlisted read path


# ---- config + budget -------------------------------------------------------


def test_config_getters_and_setters():
    cfg = ag.AgentConfig()
    assert cfg.drift_bound == 5
    assert cfg.max_react_steps == 6
    assert cfg.max_repairs == 2
    cfg.drift_bound = 9
    cfg.max_react_steps = 3
    cfg.max_repairs = 5
    cfg.temperature = 0.5
    assert cfg.drift_bound == 9
    assert cfg.max_react_steps == 3
    assert cfg.max_repairs == 5
    assert cfg.temperature == pytest.approx(0.5)


def test_recall_context_config_methods():
    cfg = ag.AgentConfig()
    cfg.set_recall_context_weights(0.5, 0.25, 0.0, 0.25)
    cfg.set_recall_context_graph_expand(1, ["derived_from"])
    cfg.clear_recall_context_graph_expand()
    with pytest.raises(ValueError):
        cfg.set_recall_context_graph_expand(1, ["not_an_edge"])


def test_budget_defaults_and_overrides():
    b = ag.AgentBudget(max_steps=10, max_tokens=5000)
    assert b.max_steps == 10
    assert b.max_tokens == 5000
    assert b.max_wall_secs == 600  # default
    assert b.max_cost_usd is None


def test_prompt_library_override():
    lib = ag.PromptLibrary()
    lib.set("planner", 99, "custom planner prompt")
    assert lib.resolve("planner") == "custom planner prompt"
    with pytest.raises(ValueError):
        lib.set("not-a-prompt", 1, "x")


# ---- the agent loop --------------------------------------------------------


def test_agent_run_invokes_callback_and_reports():
    mem = _region("a")
    llm = ScriptLLM()
    agent = citadeldb.Agent(mem, "a", llm, budget=ag.AgentBudget(max_steps=4))

    report = agent.run("Say hello")

    assert llm.calls, "the LLM callback was invoked"
    assert "messages" in llm.calls[0]
    assert report.terminated_by in {
        "success",
        "incomplete",
        "drift_exceeded",
        "budget_exceeded",
    }
    assert isinstance(report.chain_valid, bool)

    # The agent's graph is inspectable after the run.
    chain = agent.graph().verify_chain()
    assert isinstance(chain.valid, bool)


def test_discovery_runs_with_verifier():
    mem = _region("d")
    llm = ScriptLLM()

    class Checker:
        # checker_id + checker_version make this an attested checker (may mint).
        checker_id = "test-checker"
        checker_version = "1.0"

        def verify(self, request):
            return {"satisfied": True, "reason": "ok"}

        def score(self, request):
            return {"satisfied": True, "score": 1.0, "reason": "ok"}

    cfg = ag.AgentConfig()
    cfg.set_proposal_operator(ag.LlmProposer())
    cfg.set_verifier(Checker())

    agent = citadeldb.Agent(
        mem,
        "d",
        llm,
        config=cfg,
        budget=ag.AgentBudget(max_proposals=2, max_steps=4),
    )
    goal = ag.DiscoveryGoal(citadeldb.Goal("find a thing"), max_idle_rounds=1, max_mints=1)

    report = agent.run_discovery(goal)
    assert isinstance(report.minted, list)
    assert report.terminated_by in {
        "success",
        "incomplete",
        "drift_exceeded",
        "budget_exceeded",
    }


class _Checker:
    # An attested deterministic checker (may mint).
    checker_id = "test-checker"
    checker_version = "1.0"

    def verify(self, request):
        return {"satisfied": True, "reason": "ok"}

    def score(self, request):
        return {"satisfied": True, "score": 1.0, "reason": "ok"}


def test_python_proposal_operator_drives_discovery():
    """A custom Python ProposalOperator drives discovery via the owned LLM channel."""
    import json

    mem = _region("po")

    class ArtifactLLM:
        model_id = "artifact"

        def complete(self, request):
            assert "messages" in request
            return {"content": '{"value": 7}', "finish_reason": "stop"}

    class MyProposer:
        def __init__(self):
            self.rounds = 0

        def propose(self, ctx, llm):
            self.rounds += 1
            assert "goal" in ctx and "elites" in ctx and "system" in ctx
            resp = llm.complete({"messages": [{"role": "user", "content": "propose"}]})
            return [json.loads(resp["content"])]

    proposer = MyProposer()
    cfg = ag.AgentConfig()
    cfg.set_proposal_operator(proposer)  # a Python operator, not the built-in
    cfg.set_verifier(_Checker())

    agent = citadeldb.Agent(
        mem,
        "po",
        ArtifactLLM(),
        config=cfg,
        budget=ag.AgentBudget(max_proposals=3, max_steps=4),
    )
    report = agent.run_discovery(
        ag.DiscoveryGoal(citadeldb.Goal("find a value"), max_idle_rounds=1, max_mints=1)
    )

    assert proposer.rounds >= 1, "the Python operator was invoked"
    assert isinstance(report.minted, list)
    assert report.terminated_by in {
        "success",
        "incomplete",
        "drift_exceeded",
        "budget_exceeded",
    }


def test_completer_is_poisoned_after_propose():
    """A channel stashed beyond its propose() call must refuse further use."""
    mem = _region("poison")
    stash = {}

    class StashingProposer:
        def propose(self, ctx, llm):
            stash["llm"] = llm
            return []

    class LLM:
        model_id = "x"

        def complete(self, request):
            return {"content": "{}", "finish_reason": "stop"}

    cfg = ag.AgentConfig()
    cfg.set_proposal_operator(StashingProposer())
    cfg.set_verifier(_Checker())

    agent = citadeldb.Agent(
        mem,
        "poison",
        LLM(),
        config=cfg,
        budget=ag.AgentBudget(max_proposals=1, max_steps=2),
    )
    agent.run_discovery(
        ag.DiscoveryGoal(citadeldb.Goal("g"), max_idle_rounds=1, max_mints=1)
    )

    assert "llm" in stash, "the operator ran and stashed the channel"
    with pytest.raises(citadeldb.ProgrammingError):
        stash["llm"].complete({"messages": []})


def test_python_operator_multi_call_and_multi_candidate():
    """A Python operator that drives the channel multiple times and traces each call."""
    import json

    mem = _region("multi")

    class LLM:
        model_id = "m"

        def __init__(self):
            self.calls = 0

        def complete(self, request):
            self.calls += 1
            return {"content": '{"x": %d}' % self.calls, "finish_reason": "stop"}

    llm = LLM()

    class MultiProposer:
        def propose(self, ctx, channel):
            a = json.loads(
                channel.complete({"messages": [{"role": "user", "content": "a"}]})["content"]
            )
            b = json.loads(
                channel.complete({"messages": [{"role": "user", "content": "b"}]})["content"]
            )
            return [a, b]

    cfg = ag.AgentConfig()
    cfg.set_proposal_operator(MultiProposer())
    cfg.set_verifier(_Checker())
    agent = citadeldb.Agent(
        mem, "multi", llm, config=cfg, budget=ag.AgentBudget(max_proposals=2, max_steps=4)
    )
    report = agent.run_discovery(
        ag.DiscoveryGoal(citadeldb.Goal("g"), max_idle_rounds=1, max_mints=2)
    )

    assert llm.calls >= 2, "the operator made multiple channel calls"
    assert isinstance(report.minted, list)
    traces = agent.graph().load_llm_traces()
    assert len(traces) >= 2, "every channel call was traced"


def test_python_operator_exception_propagates():
    """An exception inside a Python operator surfaces as an error from run_discovery."""
    mem = _region("operr")

    class LLM:
        model_id = "m"

        def complete(self, request):
            return {"content": "{}", "finish_reason": "stop"}

    class BadProposer:
        def propose(self, ctx, channel):
            raise ValueError("operator boom")

    cfg = ag.AgentConfig()
    cfg.set_proposal_operator(BadProposer())
    cfg.set_verifier(_Checker())
    agent = citadeldb.Agent(
        mem, "operr", LLM(), config=cfg, budget=ag.AgentBudget(max_proposals=1, max_steps=2)
    )
    with pytest.raises(citadeldb.CitadelError):
        agent.run_discovery(
            ag.DiscoveryGoal(citadeldb.Goal("g"), max_idle_rounds=1, max_mints=1)
        )


def test_agent_run_records_llm_traces():
    """The cognition loop traces its LLM calls; the binding exposes them via the graph."""
    mem = _region("traces")
    llm = ScriptLLM()
    agent = citadeldb.Agent(mem, "traces", llm, budget=ag.AgentBudget(max_steps=4))
    agent.run("do a small task")
    traces = agent.graph().load_llm_traces()
    assert len(traces) >= 1, "the cognition loop recorded its LLM calls"
