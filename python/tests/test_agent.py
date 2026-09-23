"""Agent runtime binding: LLM clients, belief graph, tools, verifier, agent loop."""

import pytest

import citadeldb
from citadeldb import agent as ag


def _reported_reply(content):
    # These deterministic in-process replies have measured zero provider usage.
    return {
        "content": content,
        "finish_reason": "stop",
        "usage": {"input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0},
    }


class ScriptLLM:
    """Deterministic LLM callback that records requests and replies with plain text."""

    model_id = "script"

    def __init__(self):
        self.calls = []

    def complete(self, request):
        self.calls.append(request)
        return _reported_reply("Done.")


def _region(name="agent", dim=64):
    db = citadeldb.connect(key="k")
    mem = db.memory()
    mem.create_region(name, citadeldb.MockEmbedder(dim))
    return mem


class _FailTraceEmbedder:
    dim = 64
    metric = "cosine"
    model_id = "python-trace-failure-fixture"

    def __init__(self, fail_on):
        self.inner = citadeldb.MockEmbedder(self.dim)
        self.fail_on = fail_on
        self.traces = 0

    def embed_with_cancel(self, texts, cancel_token):
        for text in texts:
            if len(text) == 64 and all(c in "0123456789abcdef" for c in text):
                self.traces += 1
                if self.traces == self.fail_on:
                    raise RuntimeError("private storage cause")
        return self.inner.embed_with_cancel(texts, cancel_token)


def _failing_trace_region(fail_on):
    mem = citadeldb.connect(key="k").memory()
    embedder = _FailTraceEmbedder(fail_on)
    mem.create_region("trace", embedder)
    return mem, embedder


@pytest.mark.parametrize("known", [False, True])
def test_trace_storage_failure_retains_callback_response_without_running_tools(known):
    import traceback

    mem, embedder = _failing_trace_region(2)

    class Tool(EchoTool):
        calls = 0

        def call(self, args):
            self.calls += 1
            return "should not run"

    class LLM:
        model_id = "retained-callback"

        def __init__(self):
            self.calls = []

        def complete(self, request):
            self.calls.append(request)
            if len(self.calls) == 1:
                return {
                    "content": "",
                    "finish_reason": "tool_use",
                    "usage": {"input_tokens": 3, "output_tokens": 1, "cost_usd": 0.01},
                    "tool_calls": [{"id": "plan", "name": "submit_plan", "arguments": {
                        "goal": {"prompt": "private prompt", "acceptance_criteria": [], "constraints": []},
                        "tasks": [{"description": "use echo", "deps": []}],
                    }}],
                }
            assert len(self.calls) == 2
            return {
                "content": "private answer",
                "finish_reason": "tool_use",
                "usage": {"input_tokens": 5, "output_tokens": 2, "cost_usd": 0.02} if known else None,
                "tool_calls": [{"id": "echo", "name": "echo", "arguments": {"text": "private tool input"}}],
            }

    llm, tool = LLM(), Tool()
    tools = ag.ToolRegistry()
    tools.register(tool)
    agent = citadeldb.Agent(mem, "trace", llm, tools=tools)
    with pytest.raises(citadeldb.AgentError) as raised:
        agent.run("private prompt")
    error = raised.value
    assert len(llm.calls) == 2 and embedder.traces == 2 and tool.calls == 0
    assert len(agent.graph().load_llm_traces()) == 1
    assert isinstance(error.storage_error, citadeldb.OperationalError)
    assert "private storage cause" in str(error.storage_error)
    assert error.__cause__ is None
    rendered = "".join(traceback.format_exception(error))
    # Only the summary is formatted. Recovery and the storage cause are opt-in.
    for private in ("private answer", "private storage cause", "private tool input"):
        assert private not in str(error) and private not in repr(error) and private not in rendered
    recovery = error.recovery
    assert recovery["confirmed_persisted"] == 0
    assert recovery["usage"]["tokens"] == (11 if known else None)
    assert recovery["usage"]["cost_usd"] == (pytest.approx(0.03) if known else None)
    assert len(recovery["calls"]) == 1
    call = recovery["calls"][0]
    assert call["request"] == llm.calls[1]
    assert call["request"]["seed"] == 1
    assert call["attempt"] == 1 and call["model_id"] == llm.model_id
    assert len(call["request_hash"]) == 64
    assert call["client"]["provider"] == "in-process"
    assert len(call["client"]["endpoint_sha256"]) == 64
    assert len(call["client"]["wire_defaults_sha256"]) == 64
    assert call["prompt"]["id"] == "execute"
    assert call["prompt"]["text"] == call["request"]["messages"][0]["content"]
    assert len(call["prompt"]["hash"]) == 64
    response = call["outcome"]["response"]
    assert call["outcome"]["kind"] == "response"
    assert response["content"] == "private answer" and response["finish_reason"] == "tool_use"
    assert response["tool_calls"][0]["arguments"] == {"text": "private tool input"}
    assert response["usage"] == ({"input_tokens": 5, "output_tokens": 2, "cost_usd": 0.02} if known else None)


def test_trace_storage_failure_retains_provider_failure_and_unknown_usage():
    mem, embedder = _failing_trace_region(1)

    class LLM:
        model_id = "failing-callback"
        calls = 0

        def complete(self, request):
            self.calls += 1
            raise RuntimeError("private provider failure")

    llm = LLM()
    with pytest.raises(citadeldb.AgentError) as raised:
        citadeldb.Agent(mem, "trace", llm).run("private prompt")
    error = raised.value
    assert llm.calls == 1 and embedder.traces == 1
    assert error.recovery["usage"]["tokens"] is None
    assert error.recovery["usage"]["cost_usd"] is None
    failure = error.recovery["calls"][0]["outcome"]
    assert failure["kind"] == "error"
    assert failure["error"] == {"kind": "backend", "message": "RuntimeError: private provider failure", "pre_dispatch": False, "retryable": False}
    assert "private provider failure" not in str(error)


def test_ordinary_agent_errors_have_no_shared_recovery_payload():
    first, second = citadeldb.AgentError("first"), citadeldb.AgentError("second")
    assert first.recovery is None and first.storage_error is None
    assert second.recovery is None and second.storage_error is None
    first.recovery = {"calls": []}
    assert second.recovery is None


# ---- LLM client ------------------------------------------------------------


def test_mock_client_model_id():
    llm = ag.LLMClient.mock()
    assert llm.model_id == "mock"


def test_mock_client_completes():
    llm = ag.LLMClient.mock()
    out = llm.complete({"messages": [{"role": "user", "content": "hi"}]})
    assert isinstance(out, dict)
    assert "content" in out and "finish_reason" in out
    assert out["usage"] is None


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


def test_seed_is_pinned_by_default_and_can_be_released():
    """Temperature 0 alone does not make a control call reproducible."""
    cfg = ag.AgentConfig()
    assert cfg.seed == 1
    cfg.seed = 7
    assert cfg.seed == 7
    cfg.seed = None
    assert cfg.seed is None


def test_recall_context_config_methods():
    cfg = ag.AgentConfig()
    cfg.set_recall_context_weights(0.5, 0.25, 0.0, 0.25)
    cfg.set_recall_context_graph_expand(1, ["derived_from"])
    cfg.clear_recall_context_graph_expand()
    cfg.set_recall_context_kinds(["fact", "evidence"])
    cfg.set_recall_context_as_of(1_700_000_000_000_000)
    cfg.set_recall_context_as_of(None)  # None = no as-of pin
    cfg.set_recall_context_payload_filter({"topic": "x"})
    cfg.set_recall_context_payload_filter(None)  # None = no filter
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


@pytest.mark.parametrize("cost_cap", [-1.0, float("nan"), float("inf"), -float("inf")])
def test_invalid_cost_limit_stops_before_a_callback(cost_cap):
    mem = _region("invalid-cost")
    llm = ScriptLLM()
    agent = citadeldb.Agent(
        mem, "invalid-cost", llm, budget=ag.AgentBudget(max_cost_usd=cost_cap)
    )
    report = agent.run("Say hello")
    assert report.terminated_by == "invalid_cost_limit"
    assert report.budget_exceeded is None
    assert llm.calls == []


def test_zero_cost_limit_is_a_valid_cap():
    mem = _region("zero-cost")
    llm = ScriptLLM()
    agent = citadeldb.Agent(
        mem, "zero-cost", llm, budget=ag.AgentBudget(max_cost_usd=0.0)
    )
    report = agent.run("Say hello")
    assert report.terminated_by == "budget_exceeded"
    assert report.budget_exceeded == "cost"
    assert llm.calls == []


@pytest.mark.parametrize(
    "usage, cost_cap, expected",
    [
        (None, None, "token_usage_unavailable"),
        ({"input_tokens": 3, "output_tokens": 1, "cost_usd": None}, 1.0,
         "cost_usage_unavailable"),
    ],
)
def test_unavailable_usage_stops_after_preserving_the_response(usage, cost_cap, expected):
    mem = _region("unavailable")

    class LLM:
        model_id = "unreported"

        def __init__(self):
            self.calls = 0

        def complete(self, request):
            self.calls += 1
            return {"content": "retained answer", "finish_reason": "stop", "usage": usage}

    llm = LLM()
    agent = citadeldb.Agent(
        mem, "unavailable", llm,
        budget=ag.AgentBudget(max_steps=4, max_cost_usd=cost_cap),
    )
    report = agent.run("Say hello")
    assert report.terminated_by == expected
    assert llm.calls == 1
    traces = agent.graph().load_llm_traces()
    assert len(traces) == 1
    assert traces[0][1]["content"] == "retained answer"


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
            return _reported_reply('{"value": 7}')

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
            return _reported_reply("{}")

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
            return _reported_reply('{"x": %d}' % self.calls)

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
            return _reported_reply("{}")

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
