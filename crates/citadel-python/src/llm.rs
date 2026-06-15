//! The LLM client bridge: built-in or Python-backed client adapters.

use std::sync::Arc;

use citadel_ai::factory;
use citadel_ai::{
    AssistantMessage, CompletionRequest, CompletionResponse, Effort, FinishReason, LLMClient,
    LlmError, Message, TokenUsage, ToolCall, ToolChoice, ToolSpec,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;
use serde_json::Value as Json;

use crate::errors::llm_build_err;
use crate::graph::PyBeliefGraph;
use crate::{dict_item, json_to_py, py_to_json, to_pyerr};

// ---- Rust -> Python --------------------------------------------------------

fn message_to_py(py: Python<'_>, m: &Message) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    match m {
        Message::System(s) => {
            d.set_item("role", "system")?;
            d.set_item("content", s)?;
        }
        Message::User(s) => {
            d.set_item("role", "user")?;
            d.set_item("content", s)?;
        }
        Message::Assistant(am) => {
            d.set_item("role", "assistant")?;
            d.set_item("content", &am.content)?;
            d.set_item("tool_calls", tool_calls_to_py(py, &am.tool_calls)?)?;
        }
        Message::Tool {
            call_id,
            content,
            is_error,
        } => {
            d.set_item("role", "tool")?;
            d.set_item("call_id", call_id)?;
            d.set_item("content", content)?;
            d.set_item("is_error", is_error)?;
        }
    }
    d.into_py_any(py)
}

fn tool_calls_to_py(py: Python<'_>, calls: &[ToolCall]) -> PyResult<Vec<Py<PyAny>>> {
    calls.iter().map(|c| tool_call_to_py(py, c)).collect()
}

pub(crate) fn tool_call_to_py(py: Python<'_>, c: &ToolCall) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("id", &c.id)?;
    d.set_item("name", &c.name)?;
    d.set_item("arguments", json_to_py(py, &c.arguments)?)?;
    d.into_py_any(py)
}

pub(crate) fn tool_spec_to_py(py: Python<'_>, t: &ToolSpec) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("name", &t.name)?;
    d.set_item("description", &t.description)?;
    d.set_item("input_schema", json_to_py(py, &t.input_schema)?)?;
    d.into_py_any(py)
}

fn tool_choice_to_py(py: Python<'_>, tc: &ToolChoice) -> PyResult<Py<PyAny>> {
    match tc {
        ToolChoice::Auto => "auto".into_py_any(py),
        ToolChoice::Any => "any".into_py_any(py),
        ToolChoice::Tool(name) => {
            let d = PyDict::new(py);
            d.set_item("type", "tool")?;
            d.set_item("name", name)?;
            d.into_py_any(py)
        }
    }
}

/// Render a request as the dict handed to a Python `complete`.
fn request_to_py<'py>(
    py: Python<'py>,
    req: &CompletionRequest,
    model_id: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    let messages = req
        .messages
        .iter()
        .map(|m| message_to_py(py, m))
        .collect::<PyResult<Vec<_>>>()?;
    d.set_item("messages", messages)?;
    let tools = req
        .tools
        .iter()
        .map(|t| tool_spec_to_py(py, t))
        .collect::<PyResult<Vec<_>>>()?;
    d.set_item("tools", tools)?;
    d.set_item("tool_choice", tool_choice_to_py(py, &req.tool_choice)?)?;
    d.set_item("max_tokens", req.max_tokens)?;
    d.set_item("temperature", req.temperature)?;
    d.set_item("effort", req.effort.map(Effort::as_str))?;
    d.set_item(
        "output_schema",
        match &req.output_schema {
            Some(v) => json_to_py(py, v)?,
            None => py.None(),
        },
    )?;
    d.set_item("stop", req.stop.clone())?;
    d.set_item("model_id", model_id)?;
    Ok(d)
}

pub(crate) fn response_to_py(py: Python<'_>, resp: &CompletionResponse) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("content", &resp.message.content)?;
    d.set_item(
        "tool_calls",
        tool_calls_to_py(py, &resp.message.tool_calls)?,
    )?;
    let usage = PyDict::new(py);
    usage.set_item("input_tokens", resp.usage.input_tokens)?;
    usage.set_item("output_tokens", resp.usage.output_tokens)?;
    usage.set_item("cost_usd", resp.usage.cost_usd)?;
    d.set_item("usage", usage)?;
    d.set_item("finish_reason", finish_reason_str(resp.finish_reason))?;
    d.into_py_any(py)
}

fn finish_reason_str(f: FinishReason) -> &'static str {
    match f {
        FinishReason::Stop => "stop",
        FinishReason::Length => "length",
        FinishReason::ToolUse => "tool_use",
        FinishReason::Error => "error",
    }
}

// ---- Python -> Rust --------------------------------------------------------

fn as_dict<'py>(obj: &Bound<'py, PyAny>, what: &str) -> PyResult<Bound<'py, PyDict>> {
    obj.extract::<Bound<'py, PyDict>>()
        .map_err(|_| PyValueError::new_err(format!("{what} must be a dict")))
}

fn message_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Message> {
    let d = as_dict(obj, "message")?;
    let role: String = dict_item(&d, "role")?
        .ok_or_else(|| PyValueError::new_err("message missing 'role'"))?
        .extract()?;
    match role.as_str() {
        "system" => Ok(Message::System(msg_content(&d)?)),
        "user" => Ok(Message::User(msg_content(&d)?)),
        "assistant" => Ok(Message::Assistant(AssistantMessage {
            content: msg_content(&d)?,
            tool_calls: tool_calls_from_py(&d)?,
        })),
        "tool" => Ok(Message::Tool {
            call_id: dict_item(&d, "call_id")?
                .ok_or_else(|| PyValueError::new_err("tool message requires 'call_id'"))?
                .extract()?,
            content: msg_content(&d)?,
            is_error: dict_item(&d, "is_error")?
                .map(|v| v.extract())
                .transpose()?
                .unwrap_or(false),
        }),
        other => Err(PyValueError::new_err(format!(
            "unknown message role '{other}' (system|user|assistant|tool)"
        ))),
    }
}

fn msg_content(d: &Bound<'_, PyDict>) -> PyResult<String> {
    Ok(dict_item(d, "content")?
        .map(|v| v.extract())
        .transpose()?
        .unwrap_or_default())
}

fn tool_calls_from_py(d: &Bound<'_, PyDict>) -> PyResult<Vec<ToolCall>> {
    match dict_item(d, "tool_calls")? {
        Some(v) => v
            .extract::<Vec<Bound<'_, PyAny>>>()?
            .iter()
            .map(tool_call_from_py)
            .collect(),
        None => Ok(Vec::new()),
    }
}

pub(crate) fn tool_call_from_py(obj: &Bound<'_, PyAny>) -> PyResult<ToolCall> {
    let d = as_dict(obj, "tool call")?;
    Ok(ToolCall {
        id: dict_item(&d, "id")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or_default(),
        name: dict_item(&d, "name")?
            .ok_or_else(|| PyValueError::new_err("tool call missing 'name'"))?
            .extract()?,
        arguments: match dict_item(&d, "arguments")? {
            Some(v) => py_to_json(d.py(), &v)?,
            None => Json::Object(serde_json::Map::new()),
        },
    })
}

fn tool_spec_from_py(obj: &Bound<'_, PyAny>) -> PyResult<ToolSpec> {
    let d = as_dict(obj, "tool spec")?;
    Ok(ToolSpec {
        name: dict_item(&d, "name")?
            .ok_or_else(|| PyValueError::new_err("tool spec missing 'name'"))?
            .extract()?,
        description: dict_item(&d, "description")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or_default(),
        input_schema: match dict_item(&d, "input_schema")? {
            Some(v) => py_to_json(d.py(), &v)?,
            None => Json::Object(serde_json::Map::new()),
        },
    })
}

fn tool_choice_from_py(obj: &Bound<'_, PyAny>) -> PyResult<ToolChoice> {
    if let Ok(s) = obj.extract::<String>() {
        return match s.to_ascii_lowercase().as_str() {
            "auto" => Ok(ToolChoice::Auto),
            "any" => Ok(ToolChoice::Any),
            other => Err(PyValueError::new_err(format!(
                "unknown tool_choice '{other}' (auto|any|{{type:tool,name}})"
            ))),
        };
    }
    let d = as_dict(obj, "tool_choice")?;
    Ok(ToolChoice::Tool(
        dict_item(&d, "name")?
            .ok_or_else(|| PyValueError::new_err("tool_choice tool requires 'name'"))?
            .extract()?,
    ))
}

fn usage_from_py(obj: &Bound<'_, PyAny>) -> PyResult<TokenUsage> {
    let d = as_dict(obj, "usage")?;
    Ok(TokenUsage {
        input_tokens: dict_item(&d, "input_tokens")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(0),
        output_tokens: dict_item(&d, "output_tokens")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(0),
        cost_usd: dict_item(&d, "cost_usd")?
            .map(|v| v.extract())
            .transpose()?,
    })
}

fn parse_effort(s: &str) -> PyResult<Effort> {
    match s.to_ascii_lowercase().as_str() {
        "low" => Ok(Effort::Low),
        "medium" | "med" => Ok(Effort::Medium),
        "high" => Ok(Effort::High),
        "max" => Ok(Effort::Max),
        other => Err(PyValueError::new_err(format!(
            "unknown effort '{other}' (low|medium|high|max)"
        ))),
    }
}

fn parse_finish_reason(s: &str) -> PyResult<FinishReason> {
    match s.to_ascii_lowercase().as_str() {
        "stop" => Ok(FinishReason::Stop),
        "length" => Ok(FinishReason::Length),
        "tool_use" | "tooluse" => Ok(FinishReason::ToolUse),
        "error" => Ok(FinishReason::Error),
        other => Err(PyValueError::new_err(format!(
            "unknown finish_reason '{other}' (stop|length|tool_use|error)"
        ))),
    }
}

/// Parse a `complete` result: a bare `str` is plain text, else a dict with
/// `content` / `tool_calls` / `usage` / `finish_reason`.
fn response_from_py(obj: &Bound<'_, PyAny>) -> PyResult<CompletionResponse> {
    if let Ok(text) = obj.extract::<String>() {
        return Ok(CompletionResponse::text(text));
    }
    let d = as_dict(obj, "complete() result (str or dict)")?;
    let content = msg_content(&d)?;
    let tool_calls = tool_calls_from_py(&d)?;
    let usage = match dict_item(&d, "usage")? {
        Some(v) => usage_from_py(&v)?,
        None => TokenUsage::default(),
    };
    let finish_reason = match dict_item(&d, "finish_reason")? {
        Some(v) => parse_finish_reason(&v.extract::<String>()?)?,
        None if tool_calls.is_empty() => FinishReason::Stop,
        None => FinishReason::ToolUse,
    };
    Ok(CompletionResponse {
        message: AssistantMessage {
            content,
            tool_calls,
        },
        usage,
        finish_reason,
    })
}

/// Parse a request dict (inverse of [`request_to_py`]).
pub(crate) fn request_from_py(obj: &Bound<'_, PyAny>) -> PyResult<CompletionRequest> {
    let d = as_dict(obj, "request")?;
    let messages = dict_item(&d, "messages")?
        .ok_or_else(|| PyValueError::new_err("request missing 'messages'"))?
        .extract::<Vec<Bound<'_, PyAny>>>()?
        .iter()
        .map(message_from_py)
        .collect::<PyResult<Vec<_>>>()?;
    let tools = match dict_item(&d, "tools")? {
        Some(v) => v
            .extract::<Vec<Bound<'_, PyAny>>>()?
            .iter()
            .map(tool_spec_from_py)
            .collect::<PyResult<Vec<_>>>()?,
        None => Vec::new(),
    };
    Ok(CompletionRequest {
        messages,
        tools,
        tool_choice: match dict_item(&d, "tool_choice")? {
            Some(v) => tool_choice_from_py(&v)?,
            None => ToolChoice::Auto,
        },
        max_tokens: dict_item(&d, "max_tokens")?
            .map(|v| v.extract())
            .transpose()?,
        temperature: dict_item(&d, "temperature")?
            .map(|v| v.extract())
            .transpose()?,
        effort: match dict_item(&d, "effort")? {
            Some(v) => Some(parse_effort(&v.extract::<String>()?)?),
            None => None,
        },
        output_schema: match dict_item(&d, "output_schema")? {
            Some(v) => Some(py_to_json(d.py(), &v)?),
            None => None,
        },
        stop: dict_item(&d, "stop")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or_default(),
    })
}

// ---- the callback bridge (Python object -> LLMClient) ----------------------

/// Adapts a Python LLM object to [`LLMClient`]. `model_id` is read once;
/// `complete` and the optional `count_tokens` call back into Python.
struct PyLlmCallback {
    callable: Py<PyAny>,
    model_id: String,
}

impl PyLlmCallback {
    fn from_object(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            model_id: obj.getattr("model_id")?.extract()?,
            callable: obj.clone().unbind(),
        })
    }
}

impl LLMClient for PyLlmCallback {
    fn complete(&self, req: &CompletionRequest) -> Result<CompletionResponse, LlmError> {
        Python::attach(|py| {
            let dict = request_to_py(py, req, &self.model_id)
                .map_err(|e| LlmError::Backend(e.to_string()))?;
            let out = self
                .callable
                .bind(py)
                .call_method1("complete", (dict,))
                .map_err(|e| LlmError::Backend(e.to_string()))?;
            response_from_py(&out).map_err(|e| LlmError::Backend(e.to_string()))
        })
    }

    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn count_tokens(&self, messages: &[Message]) -> usize {
        Python::attach(|py| python_count_tokens(py, self.callable.bind(py), messages))
            .unwrap_or_else(|| estimate_tokens(messages))
    }
}

/// Call the object's optional `count_tokens(messages) -> int`; `None` if it is
/// absent or fails, so the caller falls back to the local estimate.
fn python_count_tokens(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    messages: &[Message],
) -> Option<usize> {
    let method = obj.getattr("count_tokens").ok()?;
    if method.is_none() {
        return None;
    }
    let list = messages
        .iter()
        .map(|m| message_to_py(py, m))
        .collect::<PyResult<Vec<_>>>()
        .ok()?;
    method.call1((list,)).ok()?.extract::<usize>().ok()
}

/// ~4 chars per token, floored at message count; matches the mock client's estimate.
fn estimate_tokens(messages: &[Message]) -> usize {
    let chars: usize = messages.iter().map(message_chars).sum();
    (chars / 4).max(messages.len())
}

fn message_chars(m: &Message) -> usize {
    match m {
        Message::System(s) | Message::User(s) => s.len(),
        Message::Assistant(am) => {
            am.content.len()
                + am.tool_calls
                    .iter()
                    .map(|c| c.name.len() + c.arguments.to_string().len())
                    .sum::<usize>()
        }
        Message::Tool { content, .. } => content.len(),
    }
}

// ---- the client handle (built-in providers) --------------------------------

/// A ready LLM client: a built-in provider (claude/openai/gemini/ollama) or the
/// canned `mock`. Pass it - or any object with `model_id` + `complete` - to `Agent`.
#[pyclass(name = "LLMClient")]
pub(crate) struct PyLlmHandle {
    pub(crate) inner: Arc<dyn LLMClient>,
    /// Present only for replay handles; exposes the cache-miss count.
    replay: Option<factory::Replay>,
}

#[pymethods]
impl PyLlmHandle {
    /// Build a provider client. API keys come from the environment
    /// (`ANTHROPIC_API_KEY` / `OPENAI_API_KEY` / `GEMINI_API_KEY`; ollama needs none).
    #[staticmethod]
    fn provider(provider: &str, model: &str) -> PyResult<Self> {
        Ok(Self {
            inner: factory::build(provider, model).map_err(llm_build_err)?,
            replay: None,
        })
    }

    /// Select a client from the environment: `{prefix}_PROVIDER` / `{prefix}_MODEL`,
    /// each falling back to the given default.
    #[staticmethod]
    fn from_env(prefix: &str, default_provider: &str, default_model: &str) -> PyResult<Self> {
        Ok(Self {
            inner: factory::from_env(prefix, default_provider, default_model)
                .map_err(llm_build_err)?,
            replay: None,
        })
    }

    /// A canned-response client (no key, no network); for quickstarts and tests.
    #[staticmethod]
    fn mock() -> PyResult<Self> {
        Ok(Self {
            inner: factory::build("mock", "mock").map_err(llm_build_err)?,
            replay: None,
        })
    }

    /// A deterministic-replay client seeded from a graph's recorded `llm_trace`
    /// chain: replays recorded responses by request hash with zero live calls.
    /// Pass it to `Agent` to reproduce/audit a recorded run; `replay_misses` is 0
    /// on a faithful replay. Errors if the graph has no traces.
    #[staticmethod]
    fn replay(graph: &PyBeliefGraph) -> PyResult<Self> {
        let r = factory::replay_from_graph(graph.belief_graph()).map_err(to_pyerr)?;
        Ok(Self {
            inner: r.client(),
            replay: Some(r),
        })
    }

    #[getter]
    fn model_id(&self) -> String {
        self.inner.model_id().to_string()
    }

    /// Cache misses during replay (0 on a faithful replay); `None` otherwise.
    #[getter]
    fn replay_misses(&self) -> Option<u32> {
        self.replay.as_ref().map(|r| r.misses())
    }

    /// Best-effort token count for a list of message dicts.
    fn count_tokens(&self, messages: &Bound<'_, PyAny>) -> PyResult<usize> {
        let msgs = messages
            .extract::<Vec<Bound<'_, PyAny>>>()?
            .iter()
            .map(message_from_py)
            .collect::<PyResult<Vec<_>>>()?;
        Ok(self.inner.count_tokens(&msgs))
    }

    /// Run one completion. `request` is a dict (messages/tools/...); returns a dict
    /// (content/tool_calls/usage/finish_reason). Releases the GIL during the call.
    fn complete(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let req = request_from_py(request)?;
        let inner = Arc::clone(&self.inner);
        let resp = py.detach(move || inner.complete(&req)).map_err(to_pyerr)?;
        response_to_py(py, &resp)
    }
}

// ---- dispatch --------------------------------------------------------------

/// Resolve the `llm` argument: a built-in handle is used directly; any other
/// object is adapted as a Python callback.
pub(crate) fn build_llm(obj: &Bound<'_, PyAny>) -> PyResult<Arc<dyn LLMClient>> {
    if let Ok(handle) = obj.extract::<PyRef<'_, PyLlmHandle>>() {
        return Ok(Arc::clone(&handle.inner));
    }
    Ok(Arc::new(PyLlmCallback::from_object(obj)?))
}
