//! Tool registry: built-in and Python-defined tool dispatch.

use std::path::PathBuf;
use std::time::Duration;

use citadel_ai::{
    ExecPolicy, FileReadTool, FileWriteTool, FsPolicy, ListDirTool, MemRecallTool, MemRememberTool,
    NetworkPolicy, RunCommandTool, Tool, ToolError, ToolPermissions, ToolRegistry, ToolSpec,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;
use serde_json::Value as Json;

use crate::llm::tool_spec_to_py;
use crate::mem::PyMemory;
use crate::{json_to_py, py_to_json, to_pyerr};

// ---- the Python tool bridge ------------------------------------------------

/// Adapts a Python tool object to [`Tool`]; non-str `call` returns are JSON-encoded.
struct PyToolBridge {
    callable: Py<PyAny>,
    spec: ToolSpec,
}

impl PyToolBridge {
    fn from_object(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let name: String = obj.getattr("name")?.extract()?;
        let description: String = obj.getattr("description")?.extract()?;
        let input_schema = match obj.getattr("input_schema") {
            Ok(s) if !s.is_none() => py_to_json(obj.py(), &s)?,
            _ => Json::Object(serde_json::Map::new()),
        };
        Ok(Self {
            callable: obj.clone().unbind(),
            spec: ToolSpec {
                name,
                description,
                input_schema,
            },
        })
    }
}

fn tool_failed(tool: &str, e: PyErr) -> ToolError {
    ToolError::Failed {
        tool: tool.to_string(),
        reason: e.to_string(),
    }
}

impl Tool for PyToolBridge {
    fn spec(&self) -> ToolSpec {
        self.spec.clone()
    }

    fn call(&self, args: &Json) -> Result<String, ToolError> {
        Python::attach(|py| {
            let py_args = json_to_py(py, args).map_err(|e| tool_failed(&self.spec.name, e))?;
            let out = self
                .callable
                .bind(py)
                .call_method1("call", (py_args,))
                .map_err(|e| tool_failed(&self.spec.name, e))?;
            match out.extract::<String>() {
                Ok(s) => Ok(s),
                Err(_) => Ok(py_to_json(py, &out)
                    .map_err(|e| tool_failed(&self.spec.name, e))?
                    .to_string()),
            }
        })
    }
}

// ---- permission inspection -------------------------------------------------

fn paths_to_str(paths: &[PathBuf]) -> Vec<String> {
    paths.iter().map(|p| p.display().to_string()).collect()
}

/// Render a tool's declared permissions as a read-only dict.
fn permissions_to_py(py: Python<'_>, perms: &ToolPermissions) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item(
        "network",
        match &perms.network {
            NetworkPolicy::None => py.None(),
            NetworkPolicy::AllowDomains(domains) => {
                let n = PyDict::new(py);
                n.set_item("allow_domains", domains.clone())?;
                n.into_py_any(py)?
            }
        },
    )?;
    d.set_item(
        "filesystem",
        match &perms.filesystem {
            FsPolicy::None => py.None(),
            FsPolicy::AllowPaths { read, write } => {
                let f = PyDict::new(py);
                f.set_item("read", paths_to_str(read))?;
                f.set_item("write", paths_to_str(write))?;
                f.into_py_any(py)?
            }
        },
    )?;
    d.set_item(
        "exec",
        match &perms.exec {
            ExecPolicy::None => py.None(),
            ExecPolicy::AllowPrograms {
                programs,
                working_dir,
                timeout_ms,
            } => {
                let e = PyDict::new(py);
                e.set_item("programs", programs.clone())?;
                e.set_item("working_dir", working_dir.display().to_string())?;
                e.set_item("timeout_ms", *timeout_ms)?;
                e.into_py_any(py)?
            }
        },
    )?;
    d.into_py_any(py)
}

// ---- the registry ----------------------------------------------------------

/// Tools to hand an `Agent`; the agent takes ownership, leaving this empty.
#[pyclass(name = "ToolRegistry")]
#[derive(Default)]
pub(crate) struct PyToolRegistry {
    inner: ToolRegistry,
}

impl PyToolRegistry {
    /// Move the built registry out for the agent (leaves this one empty).
    pub(crate) fn take(slf: &Bound<'_, Self>) -> ToolRegistry {
        std::mem::take(&mut slf.borrow_mut().inner)
    }
}

#[pymethods]
impl PyToolRegistry {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    /// Register a Python tool: any object with `name`, `description`,
    /// `input_schema` (a JSON-Schema dict) and `call(args: dict) -> str`.
    fn register(&mut self, tool: &Bound<'_, PyAny>) -> PyResult<()> {
        self.inner
            .register(Box::new(PyToolBridge::from_object(tool)?));
        Ok(())
    }

    /// Register the built-in semantic-recall tool over a memory region.
    fn add_mem_recall(&mut self, memory: &PyMemory, region: &str) {
        self.inner
            .register(Box::new(MemRecallTool::new(memory.engine(), region)));
    }

    /// Register the built-in remember tool over a memory region.
    fn add_mem_remember(&mut self, memory: &PyMemory, region: &str) {
        self.inner
            .register(Box::new(MemRememberTool::new(memory.engine(), region)));
    }

    /// Register an allowlisted file-read tool (paths canonicalized at build time).
    fn add_file_read(&mut self, allowed: Vec<PathBuf>) -> PyResult<()> {
        self.inner
            .register(Box::new(FileReadTool::new(allowed).map_err(to_pyerr)?));
        Ok(())
    }

    /// Register an allowlisted file-write tool.
    fn add_file_write(&mut self, allowed: Vec<PathBuf>) -> PyResult<()> {
        self.inner
            .register(Box::new(FileWriteTool::new(allowed).map_err(to_pyerr)?));
        Ok(())
    }

    /// Register an allowlisted directory-listing tool.
    fn add_list_dir(&mut self, allowed: Vec<PathBuf>) -> PyResult<()> {
        self.inner
            .register(Box::new(ListDirTool::new(allowed).map_err(to_pyerr)?));
        Ok(())
    }

    /// Register an allowlisted command runner (`allowed` program basenames; each
    /// call is killed after `timeout_ms`). No shell; args are literal argv.
    fn add_run_command(
        &mut self,
        allowed: Vec<String>,
        workdir: PathBuf,
        timeout_ms: u64,
    ) -> PyResult<()> {
        self.inner.register(Box::new(
            RunCommandTool::new(allowed, workdir, Duration::from_millis(timeout_ms))
                .map_err(to_pyerr)?,
        ));
        Ok(())
    }

    /// The registered tool names.
    fn names(&self) -> Vec<String> {
        self.inner.names().into_iter().map(String::from).collect()
    }

    fn contains(&self, name: &str) -> bool {
        self.inner.contains(name)
    }

    /// The tool specs (`name`/`description`/`input_schema`) the agent advertises.
    fn specs(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.inner
            .specs()
            .iter()
            .map(|s| tool_spec_to_py(py, s))
            .collect()
    }

    /// A named tool's declared permissions as a dict, if it is registered.
    fn permissions(&self, py: Python<'_>, name: &str) -> PyResult<Option<Py<PyAny>>> {
        self.inner
            .permissions(name)
            .map(|p| permissions_to_py(py, &p))
            .transpose()
    }
}
