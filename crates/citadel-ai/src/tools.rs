//! Tool registry, the `Tool` trait, and the built-in memory tools.
//!
//! Each tool declares a JSON-Schema [`ToolSpec`] and [`ToolPermissions`] (least-
//! privilege). `mem_recall`/`mem_remember` need no network/fs/process. The opt-in
//! `file-tools`/`command-tool` carry the teeth, each enforcing its allowlist inside
//! `call`; [`structural_constraints_ok`] is post-dispatch defense-in-depth.

use std::path::PathBuf;
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::{json, Value};

use citadel_mem::{AtomInput, MemoryEngine, RecallQuery};

use crate::llm::{ToolCall, ToolSpec};

#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    #[error("unknown tool: {0}")]
    Unknown(String),
    #[error("bad arguments for {tool}: {reason}")]
    BadArgs { tool: String, reason: String },
    #[error("tool {tool} failed: {reason}")]
    Failed { tool: String, reason: String },
}

#[derive(Debug, Clone, Default)]
pub enum NetworkPolicy {
    #[default]
    None,
    AllowDomains(Vec<String>),
}

#[derive(Debug, Clone, Default)]
pub enum FsPolicy {
    #[default]
    None,
    AllowPaths {
        read: Vec<PathBuf>,
        write: Vec<PathBuf>,
    },
}

/// What programs a tool may spawn. Default `None`: no subprocesses. `RunCommandTool`
/// declares `AllowPrograms` so the read-only gate can forbid it (a run is a mutation).
#[derive(Debug, Clone, Default)]
pub enum ExecPolicy {
    #[default]
    None,
    AllowPrograms {
        programs: Vec<String>,
        working_dir: PathBuf,
        timeout_ms: u64,
    },
}

/// What a tool may touch. The default is least privilege: no network, no fs,
/// no subprocesses.
#[derive(Debug, Clone, Default)]
pub struct ToolPermissions {
    pub network: NetworkPolicy,
    pub filesystem: FsPolicy,
    pub exec: ExecPolicy,
}

/// A callable tool. `call` receives the model's JSON arguments and returns a
/// string result fed back to the model as a tool message.
pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    fn permissions(&self) -> ToolPermissions {
        ToolPermissions::default()
    }
    fn call(&self, args: &Value) -> Result<String, ToolError>;
}

/// The set of tools an agent may invoke, keyed by name.
#[derive(Default)]
pub struct ToolRegistry {
    tools: FxHashMap<String, Box<dyn Tool>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, tool: Box<dyn Tool>) {
        self.tools.insert(tool.spec().name, tool);
    }

    /// Specs for every registered tool (the model's tool list).
    pub fn specs(&self) -> Vec<ToolSpec> {
        self.tools.values().map(|t| t.spec()).collect()
    }

    pub fn contains(&self, name: &str) -> bool {
        self.tools.contains_key(name)
    }

    /// Names of all registered tools.
    pub fn names(&self) -> Vec<&str> {
        self.tools.keys().map(String::as_str).collect()
    }

    /// The declared permissions of a registered tool, or `None` if not registered.
    pub fn permissions(&self, name: &str) -> Option<ToolPermissions> {
        self.tools.get(name).map(|t| t.permissions())
    }

    /// Run the named tool, or `Unknown` if unregistered. Arguments pass RAW; the tool
    /// validates and coerces them.
    ///
    /// Structured-arg coercion is PER-TOOL by design: `run_command.args` is the only
    /// structured argument any tool declares and needs tool-specific handling. Lift a
    /// shared pass here only when a SECOND tool declares a top-level array/object arg.
    pub fn dispatch(&self, call: &ToolCall) -> Result<String, ToolError> {
        let tool = self
            .tools
            .get(&call.name)
            .ok_or_else(|| ToolError::Unknown(call.name.clone()))?;
        tool.call(&call.arguments)
    }
}

fn str_arg<'a>(args: &'a Value, key: &str, tool: &str) -> Result<&'a str, ToolError> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::BadArgs {
            tool: tool.into(),
            reason: format!("missing string '{key}'"),
        })
}

/// Deterministic structural constraint check: every dispatched call must be a
/// registered tool, satisfy an "only use ..." whitelist parsed from `constraints`,
/// and honor a read-only rule (no `mem_remember`, no fs-write/exec tool). Phrases it
/// can't map fall through to the `Verifier`/critic (heuristic by design).
pub(crate) fn structural_constraints_ok(
    reg: &ToolRegistry,
    constraints: &[String],
    calls: &[ToolCall],
) -> bool {
    let mut whitelist: Option<FxHashSet<String>> = None;
    let mut read_only = false;
    for c in constraints {
        let lc = c.to_ascii_lowercase();
        if lc.contains("read-only")
            || lc.contains("read only")
            || lc.contains("readonly")
            || lc.contains("no write")
            || lc.contains("no_write")
            || lc.contains("no mem_remember")
        {
            read_only = true;
        }
        if lc.contains("only use") || lc.contains("only the") {
            let set = whitelist.get_or_insert_with(FxHashSet::default);
            for name in reg.names() {
                if lc.contains(&name.to_ascii_lowercase()) {
                    set.insert(name.to_string());
                }
            }
        }
    }
    calls.iter().all(|call| {
        if !reg.contains(&call.name) {
            return false;
        }
        if let Some(set) = &whitelist {
            if !set.contains(&call.name) {
                return false;
            }
        }
        if read_only {
            if call.name == "mem_remember" {
                return false;
            }
            if let Some(perms) = reg.permissions(&call.name) {
                if let FsPolicy::AllowPaths { write, .. } = &perms.filesystem {
                    if !write.is_empty() {
                        return false;
                    }
                }
                // A command run can mutate (write files, hit the network), so
                // read-only forbids any exec-capable tool outright.
                if matches!(perms.exec, ExecPolicy::AllowPrograms { .. }) {
                    return false;
                }
            }
        }
        true
    })
}

/// Recall the most relevant memories for a query in a fixed region.
pub struct MemRecallTool {
    mem: Arc<MemoryEngine>,
    region: String,
}

impl MemRecallTool {
    pub fn new(mem: Arc<MemoryEngine>, region: impl Into<String>) -> Self {
        Self {
            mem,
            region: region.into(),
        }
    }
}

impl Tool for MemRecallTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "mem_recall".into(),
            description: "Recall the most relevant memories for a query.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "what to recall"},
                    "k": {"type": "integer", "description": "max results (default 5)"}
                },
                "required": ["query"]
            }),
        }
    }

    fn call(&self, args: &Value) -> Result<String, ToolError> {
        let query = str_arg(args, "query", "mem_recall")?;
        let k = args.get("k").and_then(Value::as_u64).unwrap_or(5) as usize;
        let hits = self
            .mem
            .recall(&self.region, RecallQuery::by_text(query, k))
            .map_err(|e| ToolError::Failed {
                tool: "mem_recall".into(),
                reason: e.to_string(),
            })?;
        let rows: Vec<Value> = hits
            .iter()
            .map(|h| json!({"id": h.id, "kind": h.kind, "text": h.text, "score": h.score}))
            .collect();
        Ok(Value::Array(rows).to_string())
    }
}

/// Store a new memory atom in a fixed region.
pub struct MemRememberTool {
    mem: Arc<MemoryEngine>,
    region: String,
}

impl MemRememberTool {
    pub fn new(mem: Arc<MemoryEngine>, region: impl Into<String>) -> Self {
        Self {
            mem,
            region: region.into(),
        }
    }
}

impl Tool for MemRememberTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "mem_remember".into(),
            description: "Store a memory for later recall.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "text": {"type": "string", "description": "the content to remember"},
                    "kind": {"type": "string", "description": "atom kind (default 'fact')"}
                },
                "required": ["text"]
            }),
        }
    }

    fn call(&self, args: &Value) -> Result<String, ToolError> {
        let text = str_arg(args, "text", "mem_remember")?;
        let kind = args.get("kind").and_then(Value::as_str).unwrap_or("fact");
        let id = self
            .mem
            .remember(&self.region, AtomInput::new(kind, text))
            .map_err(|e| ToolError::Failed {
                tool: "mem_remember".into(),
                reason: e.to_string(),
            })?;
        Ok(json!({"id": id, "status": "stored"}).to_string())
    }
}

/// Opt-in, native-only file tools (`file-tools` feature): read/write within an
/// allowlist of canonical roots.
///
/// Security: each tool enforces its allowlist at call time - the SOLE preventive
/// control (`structural_constraints_ok` runs post-dispatch, audit/drift only).
/// Paths are canonicalized (symlinks resolved) BEFORE a path-COMPONENT containment
/// check, defeating the CVE-2025-53109/53110 symlink-escape class and the `/root` vs
/// `/root-sibling` prefix trick; canonicalizing both sides also makes containment
/// correct on case-insensitive filesystems. NOT an OS sandbox (a TOCTOU window
/// exists - use a container). A path may be absolute or relative to a root; the
/// canonicalize+containment gate always decides, so relatives never widen access.
#[cfg(all(feature = "file-tools", not(target_arch = "wasm32")))]
mod fs_tools {
    use std::fs;
    use std::path::{Component, Path, PathBuf};

    use serde_json::{json, Value};

    use super::{str_arg, FsPolicy, Tool, ToolError, ToolPermissions, ToolSpec};

    /// Largest file `file_read` returns, to bound a single tool result.
    const MAX_READ_BYTES: u64 = 1 << 20;

    /// Canonicalize each allowed root once; an unresolvable root is a loud error
    /// (no silent deny-all). Empty input = deny-all by construction.
    fn canonical_roots(
        roots: impl IntoIterator<Item = PathBuf>,
        tool: &str,
    ) -> Result<Vec<PathBuf>, ToolError> {
        roots
            .into_iter()
            .map(|r| {
                fs::canonicalize(&r).map_err(|e| ToolError::Failed {
                    tool: tool.into(),
                    reason: format!("allowed root {} is not resolvable: {e}", r.display()),
                })
            })
            .collect()
    }

    /// Whether `real` (already canonical) lies within a canonical root, matched
    /// by path component - never a string prefix.
    fn contained(real: &Path, roots: &[PathBuf]) -> bool {
        roots.iter().any(|root| real.starts_with(root))
    }

    /// Human-readable primary root for a tool's spec; falls back to a generic
    /// phrase for a deny-all tool with no roots.
    fn primary_root(roots: &[PathBuf]) -> String {
        roots
            .first()
            .map(|r| r.display().to_string())
            .unwrap_or_else(|| "an allowed directory".into())
    }

    /// Absolute candidates for `req`: absolute as-is; relative joined against each
    /// root in order. Containment is checked per candidate, so relatives never widen.
    fn candidates(req: &Path, roots: &[PathBuf]) -> Vec<PathBuf> {
        if req.is_absolute() {
            vec![req.to_path_buf()]
        } else {
            roots.iter().map(|root| root.join(req)).collect()
        }
    }

    /// Reject a path that names no file (empty or only `.`/`..`), fail-closed before
    /// any filesystem access.
    fn require_named(req: &Path, tool: &str) -> Result<(), ToolError> {
        if req.components().any(|c| matches!(c, Component::Normal(_))) {
            Ok(())
        } else {
            Err(ToolError::BadArgs {
                tool: tool.into(),
                reason: "path must name a file (it was empty or only '.'/'..')".into(),
            })
        }
    }

    /// No candidate resolved inside the roots: "outside allowed roots" if one resolved
    /// out, else name the roots to guide a corrected path.
    fn not_contained(
        tool: &str,
        req: &Path,
        roots: &[PathBuf],
        resolved_outside: bool,
    ) -> ToolError {
        let reason = if resolved_outside {
            "path outside allowed roots".to_string()
        } else {
            format!(
                "{} did not resolve inside an allowed root; give an absolute path or \
                 one relative to {:?}",
                req.display(),
                roots
            )
        };
        ToolError::Failed {
            tool: tool.into(),
            reason,
        }
    }

    /// Resolve a read target: canonicalize each candidate (symlinks + `..` collapse)
    /// and return the first whose real path is contained.
    fn resolve_read(req: &Path, roots: &[PathBuf]) -> Result<PathBuf, ToolError> {
        require_named(req, "file_read")?;
        let mut resolved_outside = false;
        for cand in candidates(req, roots) {
            match fs::canonicalize(&cand) {
                Ok(real) if contained(&real, roots) => return Ok(real),
                Ok(_) => resolved_outside = true,
                Err(_) => {}
            }
        }
        Err(not_contained("file_read", req, roots, resolved_outside))
    }

    /// Resolve a write target (leaf may not exist): canonicalize the PARENT and
    /// re-append the Normal leaf; an existing symlink leaf is resolved so its real
    /// target is containment-checked. First candidate inside the roots wins.
    fn resolve_write(req: &Path, roots: &[PathBuf]) -> Result<PathBuf, ToolError> {
        require_named(req, "file_write")?;
        let Some(Component::Normal(leaf)) = req.components().next_back() else {
            return Err(ToolError::BadArgs {
                tool: "file_write".into(),
                reason: "path must end in a normal file name".into(),
            });
        };
        let mut resolved_outside = false;
        for cand in candidates(req, roots) {
            let Some(parent) = cand.parent() else {
                continue;
            };
            let Ok(real_parent) = fs::canonicalize(parent) else {
                continue;
            };
            let candidate = real_parent.join(leaf);
            let final_path = match fs::symlink_metadata(&candidate) {
                Ok(m) if m.file_type().is_symlink() => match fs::canonicalize(&candidate) {
                    Ok(real) => real,
                    Err(_) => continue,
                },
                _ => candidate,
            };
            if contained(&final_path, roots) {
                return Ok(final_path);
            }
            resolved_outside = true;
        }
        Err(not_contained("file_write", req, roots, resolved_outside))
    }

    /// Reads UTF-8 files within an allowlist of canonical roots.
    pub struct FileReadTool {
        roots: Vec<PathBuf>,
    }

    impl FileReadTool {
        /// Canonicalizes `allowed` now; errors on an unresolvable root. Empty
        /// `allowed` denies every read.
        pub fn new(allowed: impl IntoIterator<Item = PathBuf>) -> Result<Self, ToolError> {
            Ok(Self {
                roots: canonical_roots(allowed, "file_read")?,
            })
        }
    }

    impl Tool for FileReadTool {
        fn spec(&self) -> ToolSpec {
            let where_ = primary_root(&self.roots);
            ToolSpec {
                name: "file_read".into(),
                description: format!(
                    "Read a UTF-8 text file inside {where_}. The path may be absolute or \
                     relative to that directory."
                ),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": format!("absolute path, or a path relative to {where_}")}
                    },
                    "required": ["path"]
                }),
            }
        }

        fn permissions(&self) -> ToolPermissions {
            ToolPermissions {
                filesystem: FsPolicy::AllowPaths {
                    read: self.roots.clone(),
                    write: Vec::new(),
                },
                ..Default::default()
            }
        }

        fn call(&self, args: &Value) -> Result<String, ToolError> {
            let path = resolve_read(Path::new(str_arg(args, "path", "file_read")?), &self.roots)?;
            let md = fs::metadata(&path).map_err(|e| ToolError::Failed {
                tool: "file_read".into(),
                reason: format!("stat: {e}"),
            })?;
            // A directory reports len 0 (slipping past the cap) and then read_to_string
            // fails with a misleading "access denied"; reject it clearly instead.
            if md.is_dir() {
                return Err(ToolError::BadArgs {
                    tool: "file_read".into(),
                    reason: "path is a directory, not a file".into(),
                });
            }
            let len = md.len();
            if len > MAX_READ_BYTES {
                return Err(ToolError::Failed {
                    tool: "file_read".into(),
                    reason: format!("file is {len} bytes, over the {MAX_READ_BYTES}-byte cap"),
                });
            }
            fs::read_to_string(&path).map_err(|e| ToolError::Failed {
                tool: "file_read".into(),
                reason: format!("read: {e}"),
            })
        }
    }

    /// Writes (overwrites) text files within an allowlist of canonical roots.
    pub struct FileWriteTool {
        roots: Vec<PathBuf>,
    }

    impl FileWriteTool {
        /// Canonicalizes `allowed` now; errors on an unresolvable root. Empty
        /// `allowed` denies every write.
        pub fn new(allowed: impl IntoIterator<Item = PathBuf>) -> Result<Self, ToolError> {
            Ok(Self {
                roots: canonical_roots(allowed, "file_write")?,
            })
        }
    }

    impl Tool for FileWriteTool {
        fn spec(&self) -> ToolSpec {
            let where_ = primary_root(&self.roots);
            ToolSpec {
                name: "file_write".into(),
                description: format!(
                    "Write (overwrite) a text file inside {where_}. The path may be absolute \
                     or relative to that directory."
                ),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": format!("absolute path, or a path relative to {where_}")},
                        "contents": {"type": "string", "description": "the text to write"}
                    },
                    "required": ["path", "contents"]
                }),
            }
        }

        fn permissions(&self) -> ToolPermissions {
            ToolPermissions {
                filesystem: FsPolicy::AllowPaths {
                    read: Vec::new(),
                    write: self.roots.clone(),
                },
                ..Default::default()
            }
        }

        fn call(&self, args: &Value) -> Result<String, ToolError> {
            let path = resolve_write(Path::new(str_arg(args, "path", "file_write")?), &self.roots)?;
            // Block overwriting an existing directory (false for a not-yet-created file).
            if path.is_dir() {
                return Err(ToolError::BadArgs {
                    tool: "file_write".into(),
                    reason: "path is a directory, not a file".into(),
                });
            }
            let contents = str_arg(args, "contents", "file_write")?;
            fs::write(&path, contents).map_err(|e| ToolError::Failed {
                tool: "file_write".into(),
                reason: format!("write: {e}"),
            })?;
            Ok(json!({
                "path": path.display().to_string(),
                "bytes": contents.len(),
                "status": "written"
            })
            .to_string())
        }
    }

    /// Lists one directory level inside the allowlisted roots (name + is_dir),
    /// reusing file_read's canonicalize+containment gate. Uses `DirEntry::file_type`,
    /// which does NOT follow symlinks/junctions, so a reparse point reports is_dir=false
    /// and is never traversed (entry.metadata would leak out-of-root targets).
    pub struct ListDirTool {
        roots: Vec<PathBuf>,
    }

    impl ListDirTool {
        /// Canonicalizes `allowed` now; errors on an unresolvable root. Empty
        /// `allowed` denies every listing.
        pub fn new(allowed: impl IntoIterator<Item = PathBuf>) -> Result<Self, ToolError> {
            Ok(Self {
                roots: canonical_roots(allowed, "list_dir")?,
            })
        }
    }

    impl Tool for ListDirTool {
        fn spec(&self) -> ToolSpec {
            let where_ = primary_root(&self.roots);
            ToolSpec {
                name: "list_dir".into(),
                description: format!(
                    "List one directory level inside {where_}, returning each entry's name and \
                     whether it is a directory. The path may be absolute, relative to that \
                     directory, or omitted (or \".\") for the directory itself."
                ),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": format!("directory to list: absolute, relative to {where_}, or omitted/\".\" for the root")}
                    },
                    "required": []
                }),
            }
        }

        fn permissions(&self) -> ToolPermissions {
            ToolPermissions {
                filesystem: FsPolicy::AllowPaths {
                    read: self.roots.clone(),
                    write: Vec::new(),
                },
                ..Default::default()
            }
        }

        fn call(&self, args: &Value) -> Result<String, ToolError> {
            // Optional arg, read like the other tools' optional args (str_arg would
            // hard-error on a missing key, which "required: []" forbids).
            let raw = args
                .get("path")
                .and_then(Value::as_str)
                .map(str::trim)
                .unwrap_or("");
            // require_named rejects ""/"."/"..", so listing the root must resolve to
            // the primary canonical root directly (trivially contained). first()
            // guards the empty-roots case - indexing roots[0] would be a new panic.
            let dir = if raw.is_empty() || raw == "." {
                self.roots
                    .first()
                    .cloned()
                    .ok_or_else(|| ToolError::Failed {
                        tool: "list_dir".into(),
                        reason: "no allowed roots".into(),
                    })?
            } else {
                resolve_read(Path::new(raw), &self.roots)?
            };
            if !fs::metadata(&dir)
                .map_err(|e| ToolError::Failed {
                    tool: "list_dir".into(),
                    reason: format!("stat: {e}"),
                })?
                .is_dir()
            {
                return Err(ToolError::BadArgs {
                    tool: "list_dir".into(),
                    reason: "path is not a directory".into(),
                });
            }
            let mut entries: Vec<(String, bool)> = Vec::new();
            for entry in fs::read_dir(&dir).map_err(|e| ToolError::Failed {
                tool: "list_dir".into(),
                reason: format!("read_dir: {e}"),
            })? {
                let entry = entry.map_err(|e| ToolError::Failed {
                    tool: "list_dir".into(),
                    reason: format!("entry: {e}"),
                })?;
                // file_type does NOT follow a symlink/junction; entry.metadata WOULD
                // and could leak an out-of-root target, so it must never be used here.
                let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
                entries.push((entry.file_name().to_string_lossy().into_owned(), is_dir));
            }
            entries.sort(); // read_dir order is OS-dependent; emit a stable order
            let rows: Vec<Value> = entries
                .into_iter()
                .map(|(name, is_dir)| json!({ "name": name, "is_dir": is_dir }))
                .collect();
            Ok(json!({ "path": dir.display().to_string(), "entries": rows }).to_string())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::llm::ToolCall;
        use crate::tools::{structural_constraints_ok, ToolRegistry};

        fn write_file(path: &Path, body: &str) {
            std::fs::write(path, body).unwrap();
        }

        #[test]
        fn read_and_write_within_root() {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().to_path_buf();
            write_file(&root.join("a.txt"), "hello");

            let reader = FileReadTool::new([root.clone()]).unwrap();
            let out = reader
                .call(&json!({ "path": root.join("a.txt").to_str().unwrap() }))
                .unwrap();
            assert_eq!(out, "hello");

            // Write a brand-new file inside the root.
            let writer = FileWriteTool::new([root.clone()]).unwrap();
            let res = writer
                .call(
                    &json!({ "path": root.join("new.txt").to_str().unwrap(), "contents": "data" }),
                )
                .unwrap();
            assert!(res.contains("\"status\":\"written\""));
            assert_eq!(
                std::fs::read_to_string(root.join("new.txt")).unwrap(),
                "data"
            );
        }

        #[test]
        fn read_and_write_outside_root_denied() {
            let allowed = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            write_file(&outside.path().join("secret.txt"), "s");

            let reader = FileReadTool::new([allowed.path().to_path_buf()]).unwrap();
            let err = reader
                .call(&json!({ "path": outside.path().join("secret.txt").to_str().unwrap() }))
                .unwrap_err();
            assert!(format!("{err}").contains("outside allowed roots"));

            let writer = FileWriteTool::new([allowed.path().to_path_buf()]).unwrap();
            let err = writer
                .call(&json!({ "path": outside.path().join("x.txt").to_str().unwrap(), "contents": "x" }))
                .unwrap_err();
            assert!(format!("{err}").contains("outside allowed roots"));
        }

        #[test]
        fn empty_roots_deny_all() {
            let dir = tempfile::tempdir().unwrap();
            write_file(&dir.path().join("a.txt"), "x");
            let reader = FileReadTool::new(std::iter::empty()).unwrap();
            assert!(reader
                .call(&json!({ "path": dir.path().join("a.txt").to_str().unwrap() }))
                .is_err());
        }

        #[test]
        fn relative_path_resolves_within_root() {
            // A path relative to the allowed root now resolves (instead of being
            // bounced), so the agent can use the natural "src/lib.rs" form.
            let dir = tempfile::tempdir().unwrap();
            let root = std::fs::canonicalize(dir.path()).unwrap();
            std::fs::create_dir(root.join("src")).unwrap();
            write_file(&root.join("src").join("lib.rs"), "fn main() {}");

            let reader = FileReadTool::new([root.clone()]).unwrap();
            assert_eq!(
                reader.call(&json!({ "path": "src/lib.rs" })).unwrap(),
                "fn main() {}"
            );

            let writer = FileWriteTool::new([root.join("src")]).unwrap();
            let res = writer
                .call(&json!({ "path": "lib.rs", "contents": "fn updated() {}" }))
                .unwrap();
            assert!(res.contains("\"status\":\"written\""));
            assert_eq!(
                std::fs::read_to_string(root.join("src").join("lib.rs")).unwrap(),
                "fn updated() {}"
            );
        }

        #[test]
        fn relative_escape_denied() {
            // A relative path that climbs out of the root is still rejected: the
            // canonicalize + containment gate runs after the root join, so the
            // relative form can never widen the sandbox.
            let allowed = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            write_file(&outside.path().join("secret.txt"), "s");
            // Reach the sibling tempdir by a relative path out of the allowed root.
            let escape = Path::new("..")
                .join(outside.path().file_name().unwrap())
                .join("secret.txt");
            let escape = escape.to_str().unwrap();

            let reader = FileReadTool::new([allowed.path().to_path_buf()]).unwrap();
            assert!(reader.call(&json!({ "path": escape })).is_err());

            let writer = FileWriteTool::new([allowed.path().to_path_buf()]).unwrap();
            assert!(writer
                .call(&json!({ "path": escape, "contents": "pwn" }))
                .is_err());
            // The out-of-root file was not touched.
            assert_eq!(
                std::fs::read_to_string(outside.path().join("secret.txt")).unwrap(),
                "s"
            );
        }

        #[test]
        fn degenerate_path_rejected() {
            // Empty and dot-only paths name no file; reject them before any
            // filesystem access (fail-closed).
            let dir = tempfile::tempdir().unwrap();
            let reader = FileReadTool::new([dir.path().to_path_buf()]).unwrap();
            let writer = FileWriteTool::new([dir.path().to_path_buf()]).unwrap();
            for p in ["", ".", ".."] {
                assert!(reader.call(&json!({ "path": p })).is_err(), "read {p:?}");
                assert!(
                    writer.call(&json!({ "path": p, "contents": "x" })).is_err(),
                    "write {p:?}"
                );
            }
        }

        #[test]
        fn read_or_write_directory_rejected() {
            // A directory target gets a clear error, not the OS's "access denied".
            let dir = tempfile::tempdir().unwrap();
            std::fs::create_dir(dir.path().join("sub")).unwrap();
            let reader = FileReadTool::new([dir.path().to_path_buf()]).unwrap();
            assert!(
                format!("{}", reader.call(&json!({ "path": "sub" })).unwrap_err())
                    .contains("is a directory")
            );
            let writer = FileWriteTool::new([dir.path().to_path_buf()]).unwrap();
            assert!(format!(
                "{}",
                writer
                    .call(&json!({ "path": "sub", "contents": "x" }))
                    .unwrap_err()
            )
            .contains("is a directory"));
        }

        #[test]
        fn list_dir_lists_root_and_denies_escape() {
            let allowed = tempfile::tempdir().unwrap();
            let root = std::fs::canonicalize(allowed.path()).unwrap();
            std::fs::create_dir(root.join("src")).unwrap();
            write_file(&root.join("a.txt"), "x");
            let outside = tempfile::tempdir().unwrap();
            write_file(&outside.path().join("secret.txt"), "s");

            let lister = ListDirTool::new([root.clone()]).unwrap();
            // The root lists via an omitted path and via ".", with is_dir per entry.
            for args in [json!({}), json!({ "path": "." })] {
                let v: Value = serde_json::from_str(&lister.call(&args).unwrap()).unwrap();
                let entries = v["entries"].as_array().unwrap();
                let is_dir = |name: &str| {
                    entries
                        .iter()
                        .find(|e| e["name"] == name)
                        .map(|e| e["is_dir"].as_bool().unwrap())
                };
                assert_eq!(is_dir("a.txt"), Some(false));
                assert_eq!(is_dir("src"), Some(true));
            }
            // An absolute-outside dir and a relative escape are both denied.
            assert!(lister
                .call(&json!({ "path": outside.path().to_str().unwrap() }))
                .is_err());
            let escape = Path::new("..").join(outside.path().file_name().unwrap());
            assert!(lister
                .call(&json!({ "path": escape.to_str().unwrap() }))
                .is_err());
        }

        #[test]
        fn list_dir_on_a_file_rejected() {
            let dir = tempfile::tempdir().unwrap();
            write_file(&dir.path().join("a.txt"), "x");
            let lister = ListDirTool::new([dir.path().to_path_buf()]).unwrap();
            let err = lister.call(&json!({ "path": "a.txt" })).unwrap_err();
            assert!(format!("{err}").contains("not a directory"));
        }

        #[test]
        fn list_dir_empty_roots_no_panic() {
            // The "."/root default must not index roots[0] when there are no roots.
            let lister = ListDirTool::new(std::iter::empty()).unwrap();
            assert!(lister.call(&json!({})).is_err());
            assert!(lister.call(&json!({ "path": "." })).is_err());
        }

        #[test]
        fn list_dir_allowed_under_read_only() {
            let dir = tempfile::tempdir().unwrap();
            let mut reg = ToolRegistry::new();
            reg.register(Box::new(
                ListDirTool::new([dir.path().to_path_buf()]).unwrap(),
            ));
            let call = ToolCall {
                id: "1".into(),
                name: "list_dir".into(),
                arguments: json!({}),
            };
            assert!(structural_constraints_ok(
                &reg,
                &["read-only".to_string()],
                std::slice::from_ref(&call)
            ));
        }

        #[test]
        fn write_to_dotdot_leaf_rejected() {
            let dir = tempfile::tempdir().unwrap();
            let writer = FileWriteTool::new([dir.path().to_path_buf()]).unwrap();
            let bad = dir.path().join("..");
            let err = writer
                .call(&json!({ "path": bad.to_str().unwrap(), "contents": "x" }))
                .unwrap_err();
            assert!(format!("{err}").contains("normal file name"));
        }

        #[test]
        fn read_size_cap_enforced() {
            let dir = tempfile::tempdir().unwrap();
            let big = dir.path().join("big.txt");
            write_file(&big, &"a".repeat((MAX_READ_BYTES + 1) as usize));
            let reader = FileReadTool::new([dir.path().to_path_buf()]).unwrap();
            let err = reader
                .call(&json!({ "path": big.to_str().unwrap() }))
                .unwrap_err();
            assert!(format!("{err}").contains("cap"));
        }

        #[test]
        fn structural_gate_blocks_write_allows_read_under_read_only() {
            let dir = tempfile::tempdir().unwrap();
            let mut reg = ToolRegistry::new();
            reg.register(Box::new(
                FileReadTool::new([dir.path().to_path_buf()]).unwrap(),
            ));
            reg.register(Box::new(
                FileWriteTool::new([dir.path().to_path_buf()]).unwrap(),
            ));
            let constraints = vec!["read-only".to_string()];
            let read_call = ToolCall {
                id: "1".into(),
                name: "file_read".into(),
                arguments: json!({}),
            };
            let write_call = ToolCall {
                id: "2".into(),
                name: "file_write".into(),
                arguments: json!({}),
            };
            assert!(structural_constraints_ok(
                &reg,
                &constraints,
                std::slice::from_ref(&read_call)
            ));
            assert!(!structural_constraints_ok(
                &reg,
                &constraints,
                std::slice::from_ref(&write_call)
            ));
        }

        // Symlink-escape guards (Unix: symlink creation is std::os::unix).
        #[cfg(unix)]
        #[test]
        fn read_symlink_escape_denied() {
            use std::os::unix::fs::symlink;
            let allowed = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            write_file(&outside.path().join("secret.txt"), "s");
            let link = allowed.path().join("link.txt");
            symlink(outside.path().join("secret.txt"), &link).unwrap();

            let reader = FileReadTool::new([allowed.path().to_path_buf()]).unwrap();
            let err = reader
                .call(&json!({ "path": link.to_str().unwrap() }))
                .unwrap_err();
            // Denied by containment after resolving the symlink - NOT a missing-file error.
            assert!(format!("{err}").contains("outside allowed roots"));
        }

        #[cfg(unix)]
        #[test]
        fn write_via_existing_symlink_leaf_denied() {
            use std::os::unix::fs::symlink;
            let allowed = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            // EXISTING out-of-root target so the deny is by containment, not a
            // dangling-symlink canonicalize error (guards the symlink branch).
            write_file(&outside.path().join("target.txt"), "orig");
            let link = allowed.path().join("evil.txt");
            symlink(outside.path().join("target.txt"), &link).unwrap();

            let writer = FileWriteTool::new([allowed.path().to_path_buf()]).unwrap();
            let err = writer
                .call(&json!({ "path": link.to_str().unwrap(), "contents": "pwned" }))
                .unwrap_err();
            assert!(format!("{err}").contains("outside allowed roots"));
            // The out-of-root target was not modified.
            assert_eq!(
                std::fs::read_to_string(outside.path().join("target.txt")).unwrap(),
                "orig"
            );
        }

        #[cfg(unix)]
        #[test]
        fn symlinked_root_resolves_for_in_root_access() {
            use std::os::unix::fs::symlink;
            let real = tempfile::tempdir().unwrap();
            write_file(&real.path().join("a.txt"), "ok");
            let link_dir = tempfile::tempdir().unwrap();
            let root_link = link_dir.path().join("root");
            symlink(real.path(), &root_link).unwrap();

            // Root is a symlink; canonicalized at construction to the real dir,
            // so a real-path read inside it is allowed.
            let reader = FileReadTool::new([root_link]).unwrap();
            let out = reader
                .call(&json!({ "path": real.path().join("a.txt").to_str().unwrap() }))
                .unwrap();
            assert_eq!(out, "ok");
        }
    }
}

#[cfg(all(feature = "file-tools", not(target_arch = "wasm32")))]
pub use fs_tools::{FileReadTool, FileWriteTool, ListDirTool};

/// Opt-in, native-only command/test-runner tool (`command-tool` feature): runs one
/// allowlisted program in a fixed canonical working dir, captures capped output, and
/// kills the child past a deadline.
///
/// Security, enforced in `call()` before every spawn (the SOLE preventive control;
/// `structural_constraints_ok` runs post-dispatch):
///   - ARG-VECTOR, NEVER A SHELL: `Command::new(prog).args(vec)`, no `sh -c`, so the
///     CWE-78 shell-injection class is structurally gone (`; rm -rf /` is a literal
///     token). `args` may be a JSON array or a whitespace-split string; both yield
///     verbatim argv tokens, neither can re-introduce a shell.
///   - PROGRAM ALLOWLIST (basename): no path separator, no `..`, no `.bat`/`.cmd`/
///     `.com`/script wrapper (sidesteps the Windows BatBadBut CVE-2024-24576, unfixed
///     before Rust 1.77.2 > our 1.75 MSRV), and in the configured set; empty = deny-all.
///   - WORKING DIR canonicalized at construction; only the START dir (a child can `cd`,
///     open absolute paths, or fork) - weak confinement, not a jail.
///   - OUTPUT CAP per stream via drain threads (no full-pipe block); `truncated: true`.
///   - TIMEOUT via `wait-timeout`, then kill+reap. `timed_out` is the authoritative kill
///     flag, NOT `exit_code == null` (a Windows kill yields a non-null code).
///   - SECRET STRIP: the child inherits the real env (the toolchain needs it), minus
///     every secret-NAMED var (`is_secret_env_name`) - best-effort defense, not a boundary.
///
/// A non-zero exit is NOT an error (a failing test is a valid observation); only
/// spawn/timeout/policy failures are `ToolError`. NOT an OS sandbox (no network/syscall
/// isolation, TOCTOU) - real isolation is the operator's container.
#[cfg(all(feature = "command-tool", not(target_arch = "wasm32")))]
mod cmd_tools {
    use std::io::Read;
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    use rustc_hash::FxHashSet;
    use serde_json::{json, Value};
    use wait_timeout::ChildExt;

    use super::{str_arg, ExecPolicy, Tool, ToolError, ToolPermissions, ToolSpec};

    /// Largest amount captured per stream; the rest is drained and discarded.
    const MAX_OUTPUT_BYTES: usize = 1 << 20;

    /// After the child resolves, how long to wait for the drain threads' buffered
    /// output - so `call` always returns within `timeout + DRAIN_GRACE` even if a
    /// forked grandchild holds a pipe open (killing it is the container's job).
    const DRAIN_GRACE: Duration = Duration::from_secs(2);

    /// Windows script/batch wrappers that run THROUGH `cmd.exe` and re-expose the
    /// BatBadBut CVE-2024-24576 argv injection on pre-1.77.2 std; refused by extension.
    const BLOCKED_EXTS: &[&str] = &[
        ".bat", ".cmd", ".com", ".vbs", ".vbe", ".js", ".jse", ".wsf", ".wsh",
    ];

    /// A program name must be a bare basename: non-empty, no path separator, no `..`,
    /// no whitespace/control char (a trailing space/dot would slip a `cmd.exe `-style
    /// name past the extension check, since Windows strips them), and not a wrapper.
    fn is_rejected_program(p: &str) -> bool {
        if p.is_empty() || p.contains('/') || p.contains('\\') || p.contains("..") {
            return true;
        }
        if p.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return true;
        }
        let trimmed = p.trim_end_matches('.');
        if trimmed.is_empty() {
            return true;
        }
        let lower = trimmed.to_ascii_lowercase();
        BLOCKED_EXTS.iter().any(|ext| lower.ends_with(ext))
    }

    /// Whether an env var NAME looks like a secret, so the child never inherits it.
    /// Name-based (not value), OS-agnostic; best-effort defense, not a guarantee.
    fn is_secret_env_name(name: &str) -> bool {
        let n = name.to_ascii_uppercase();
        const EXACT: &[&str] = &[
            "ANTHROPIC_API_KEY",
            "OPENAI_API_KEY",
            "GH_TOKEN",
            "GITHUB_TOKEN",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "GOOGLE_APPLICATION_CREDENTIALS",
        ];
        const NEEDLES: &[&str] = &[
            "API_KEY",
            "APIKEY",
            "_TOKEN",
            "SECRET",
            "PASSWORD",
            "PASSWD",
            "CREDENTIAL",
            "PRIVATE_KEY",
        ];
        EXACT.contains(&n.as_str()) || NEEDLES.iter().any(|&needle| n.contains(needle))
    }

    /// Read a child pipe to EOF, keeping at most `cap` bytes (keep reading past it so
    /// the child never blocks). Returns the bytes and whether it exceeded `cap`.
    fn drain_capped<R: Read>(mut r: R, cap: usize) -> (Vec<u8>, bool) {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        let mut truncated = false;
        loop {
            match r.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    if buf.len() < cap {
                        let take = (cap - buf.len()).min(n);
                        buf.extend_from_slice(&chunk[..take]);
                        if take < n {
                            truncated = true;
                        }
                    } else {
                        truncated = true;
                    }
                }
                Err(_) => break,
            }
        }
        (buf, truncated)
    }

    /// Runs one allowlisted program in a fixed working directory with a deadline.
    #[derive(Debug)]
    pub struct RunCommandTool {
        allowed: Vec<String>,
        workdir: PathBuf,
        timeout: Duration,
        cap: usize,
    }

    impl RunCommandTool {
        /// Canonicalizes `workdir` now (loud error if unresolvable); dedups and
        /// validates the allowlist. Empty `allowed` denies every program.
        pub fn new(
            allowed: impl IntoIterator<Item = String>,
            workdir: PathBuf,
            timeout: Duration,
        ) -> Result<Self, ToolError> {
            Self::with_cap(allowed, workdir, timeout, MAX_OUTPUT_BYTES)
        }

        fn with_cap(
            allowed: impl IntoIterator<Item = String>,
            workdir: PathBuf,
            timeout: Duration,
            cap: usize,
        ) -> Result<Self, ToolError> {
            let workdir = std::fs::canonicalize(&workdir).map_err(|e| ToolError::Failed {
                tool: "run_command".into(),
                reason: format!("working dir {} is not resolvable: {e}", workdir.display()),
            })?;
            let mut seen = FxHashSet::default();
            let mut programs = Vec::new();
            for p in allowed {
                if is_rejected_program(&p) {
                    return Err(ToolError::Failed {
                        tool: "run_command".into(),
                        reason: format!("allowed program {p:?} must be a bare basename"),
                    });
                }
                if seen.insert(p.clone()) {
                    programs.push(p);
                }
            }
            Ok(Self {
                allowed: programs,
                workdir,
                timeout,
                cap,
            })
        }
    }

    impl Tool for RunCommandTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "run_command".into(),
                description: format!(
                    "Run an allowlisted program (argument vector, no shell) in the working \
                     directory {} and capture its output. The program already starts in that \
                     directory. Not an OS sandbox: that path is only the starting directory \
                     (the program may cd elsewhere, open absolute paths, or fork).",
                    self.workdir.display()
                ),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "program": {"type": "string", "description": "executable basename; must be in the allowlist"},
                        "args": {
                            "type": ["array", "string"],
                            "items": {"type": "string"},
                            "description": "Argument vector, passed verbatim (no shell, no metacharacter or quote interpretation). Prefer an array, e.g. [\"test\", \"--release\"]. A plain string is also accepted and is split on whitespace into literal tokens, so \"test --release\" becomes [\"test\", \"--release\"] (NOT a shell line: \"; rm -rf /\" would just pass the literal tokens \";\" \"rm\" \"-rf\" \"/\" to the program). Use the array form when an argument legitimately contains spaces."
                        }
                    },
                    "required": ["program"]
                }),
            }
        }

        fn permissions(&self) -> ToolPermissions {
            ToolPermissions {
                exec: ExecPolicy::AllowPrograms {
                    programs: self.allowed.clone(),
                    working_dir: self.workdir.clone(),
                    timeout_ms: self.timeout.as_millis() as u64,
                },
                ..Default::default()
            }
        }

        fn call(&self, args: &Value) -> Result<String, ToolError> {
            let program = str_arg(args, "program", "run_command")?;
            if is_rejected_program(program) {
                return Err(ToolError::Failed {
                    tool: "run_command".into(),
                    reason: "program must be a bare basename (no path separator, no '..', no \
                        whitespace, and not a .bat/.cmd/.com/script wrapper)"
                        .into(),
                });
            }
            if !self.allowed.iter().any(|a| a == program) {
                return Err(ToolError::Failed {
                    tool: "run_command".into(),
                    reason: format!(
                        "program {program:?} not in allowlist; allowed programs: {:?}",
                        self.allowed
                    ),
                });
            }

            // `args` is the argument VECTOR, never a shell line. All accepted shapes
            // produce LITERAL argv tokens via `Command::args` (no `sh -c`): a JSON
            // array, a stringified JSON array ("[\"test\"]", parsed not split), or a
            // plain string (whitespace-split). No quote/glob/metachar handling, so
            // "; rm -rf /" becomes harmless literal tokens. Coercing (vs rejecting)
            // lets the agent run its verifying `cargo test`.
            let arg_vec = match args.get("args") {
                None | Some(Value::Null) => Vec::new(),
                Some(Value::Array(items)) => {
                    let mut v = Vec::with_capacity(items.len());
                    for it in items {
                        match it.as_str() {
                            Some(s) => v.push(s.to_string()),
                            None => {
                                return Err(ToolError::BadArgs {
                                    tool: "run_command".into(),
                                    reason: "every element of the 'args' array must be a string, \
                                        e.g. [\"test\", \"--release\"]"
                                        .into(),
                                })
                            }
                        }
                    }
                    v
                }
                Some(Value::String(s)) => {
                    // A string is either a STRINGIFIED JSON array ("[\"test\"]", the
                    // common LLM mistake) or a whitespace token list ("test --release").
                    // Parse the former, split the latter; both yield literal argv tokens.
                    let trimmed = s.trim();
                    match serde_json::from_str::<Vec<String>>(trimmed) {
                        Ok(parsed) => parsed,
                        // Looks like a JSON array but doesn't parse: splitting would
                        // yield garbage like `["test",`. Reject with guidance - a plain
                        // arg never starts with '['.
                        Err(_) if trimmed.starts_with('[') => {
                            return Err(ToolError::BadArgs {
                                tool: "run_command".into(),
                                reason: "'args' looks like a malformed JSON array; send a real \
                                    array of strings, e.g. [\"test\", \"--release\"]"
                                    .into(),
                            })
                        }
                        Err(_) => trimmed.split_whitespace().map(str::to_string).collect(),
                    }
                }
                Some(_) => {
                    return Err(ToolError::BadArgs {
                        tool: "run_command".into(),
                        reason: "'args' must be an array of strings (e.g. [\"test\", \
                            \"--release\"]) or a single string split on whitespace into \
                            literal tokens (e.g. \"test --release\")"
                            .into(),
                    })
                }
            };

            // Inherit the real env so the toolchain self-locates, then env_remove the
            // secret-NAMED vars (is_secret_env_name) + CARGO_TARGET_DIR (isolate the
            // child target dir). env_clear + an allowlist starved MSVC linking.
            let mut cmd = Command::new(program);
            cmd.args(&arg_vec)
                .current_dir(&self.workdir)
                .env_remove("CARGO_TARGET_DIR")
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            for (name, _) in std::env::vars() {
                if is_secret_env_name(&name) {
                    cmd.env_remove(&name);
                }
            }
            let mut child = cmd.spawn().map_err(|e| ToolError::Failed {
                tool: "run_command".into(),
                reason: format!("spawn {program}: {e}"),
            })?;

            // Drain both pipes concurrently so the child never blocks (`.output()`
            // would block to EOF and ignore the timeout). Channel, not a blocking join,
            // so a forked grandchild holding a pipe can't make `call` hang past the deadline.
            let cap = self.cap;
            let (tx, rx) = mpsc::channel::<(usize, Vec<u8>, bool)>();
            let mut expected = 0usize;
            if let Some(s) = child.stdout.take() {
                let tx = tx.clone();
                expected += 1;
                thread::spawn(move || {
                    let (b, t) = drain_capped(s, cap);
                    let _ = tx.send((0, b, t));
                });
            }
            if let Some(s) = child.stderr.take() {
                let tx = tx.clone();
                expected += 1;
                thread::spawn(move || {
                    let (b, t) = drain_capped(s, cap);
                    let _ = tx.send((1, b, t));
                });
            }
            drop(tx);

            // Resolve within the deadline. On timeout OR wait error, kill+reap so no
            // zombie/orphan is left on any path (std's Child::drop neither kills nor waits).
            let (exit_code, timed_out, wait_failed) = match child.wait_timeout(self.timeout) {
                Ok(Some(st)) => (st.code(), false, false),
                Ok(None) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    (None, true, false)
                }
                Err(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    (None, false, true)
                }
            };

            // Collect whatever the drain threads captured, bounded by DRAIN_GRACE.
            let mut streams: [(Vec<u8>, bool); 2] = [(Vec::new(), false), (Vec::new(), false)];
            let mut got = 0usize;
            let grace_deadline = Instant::now() + DRAIN_GRACE;
            while got < expected {
                let Some(rem) = grace_deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                match rx.recv_timeout(rem) {
                    Ok((idx, bytes, trunc)) if idx < 2 => {
                        streams[idx] = (bytes, trunc);
                        got += 1;
                    }
                    Ok(_) => got += 1,
                    Err(_) => break,
                }
            }

            if wait_failed {
                return Err(ToolError::Failed {
                    tool: "run_command".into(),
                    reason: "waiting on the child process failed".into(),
                });
            }

            // `exit_code` is null only on a Unix signal-kill (a Windows kill reports a
            // non-null code), so `timed_out` is the authoritative kill flag.
            Ok(json!({
                "exit_code": exit_code,
                "stdout": String::from_utf8_lossy(&streams[0].0).into_owned(),
                "stderr": String::from_utf8_lossy(&streams[1].0).into_owned(),
                "timed_out": timed_out,
                "truncated": streams[0].1 || streams[1].1,
            })
            .to_string())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::llm::ToolCall;
        use crate::tools::{structural_constraints_ok, ToolRegistry};
        use std::time::Instant;

        // No single benign program is present and arg-vector-pure on both Unix
        // and Windows (echo is a cmd builtin), so the spawning tests split per
        // OS, like the fs_tools symlink tests.
        #[cfg(unix)]
        mod plat {
            pub const PRINT: &str = "echo";
            pub fn print_args(s: &str) -> Vec<String> {
                vec![s.to_string()]
            }
            pub const FAIL: &str = "false"; // exits 1, no output
            pub fn fail_args() -> Vec<String> {
                Vec::new()
            }
            pub const SLEEPER: &str = "sleep";
            pub fn sleeper_args() -> Vec<String> {
                vec!["5".to_string()]
            }
        }
        #[cfg(windows)]
        mod plat {
            // ping.exe is on PATH, arg-vector pure, and waits ~1s between echoes.
            pub const PRINT: &str = "ping";
            pub fn print_args(_s: &str) -> Vec<String> {
                vec!["-n".into(), "1".into(), "127.0.0.1".into()]
            }
            pub const FAIL: &str = "ping"; // no args -> usage, non-zero exit
            pub fn fail_args() -> Vec<String> {
                Vec::new()
            }
            pub const SLEEPER: &str = "ping";
            pub fn sleeper_args() -> Vec<String> {
                vec!["-n".into(), "10".into(), "127.0.0.1".into()]
            }
        }

        fn tool(allowed: &[&str], dir: &std::path::Path, timeout: Duration) -> RunCommandTool {
            RunCommandTool::new(
                allowed.iter().map(|s| s.to_string()),
                dir.to_path_buf(),
                timeout,
            )
            .unwrap()
        }

        fn parse(out: &str) -> Value {
            serde_json::from_str(out).unwrap()
        }

        #[test]
        fn runs_and_captures_output() {
            let dir = tempfile::tempdir().unwrap();
            let t = tool(&[plat::PRINT], dir.path(), Duration::from_secs(30));
            let out = t
                .call(&json!({"program": plat::PRINT, "args": plat::print_args("citadel-marker")}))
                .unwrap();
            let v = parse(&out);
            assert_eq!(v["exit_code"], json!(0), "got {out}");
            assert_eq!(v["timed_out"], json!(false));
            // Unix echo prints the marker; Windows ping prints to localhost.
            #[cfg(unix)]
            assert!(v["stdout"].as_str().unwrap().contains("citadel-marker"));
            #[cfg(windows)]
            assert!(!v["stdout"].as_str().unwrap().is_empty());
        }

        #[test]
        fn allowlist_denies_unlisted_program() {
            let dir = tempfile::tempdir().unwrap();
            let t = tool(&["cargo"], dir.path(), Duration::from_secs(5));
            let err = t.call(&json!({"program": "git"})).unwrap_err();
            let msg = format!("{err}");
            assert!(msg.contains("not in allowlist"));
            // The denial surfaces the allowlist so the agent stops guessing programs.
            assert!(msg.contains("cargo"), "should list allowlist: {msg}");
        }

        // A bare string `args` is split on whitespace into literal argv tokens and
        // runs exactly like the equivalent array, for a model that sends a string.
        #[test]
        fn string_args_split_on_whitespace_and_run() {
            let dir = tempfile::tempdir().unwrap();
            let t = tool(&[plat::PRINT], dir.path(), Duration::from_secs(30));
            #[cfg(unix)]
            let string_args = "citadel-marker";
            #[cfg(windows)]
            let string_args = "-n 1 127.0.0.1";
            let out = t
                .call(&json!({"program": plat::PRINT, "args": string_args}))
                .unwrap();
            let v = parse(&out);
            assert_eq!(v["exit_code"], json!(0), "got {out}");
            assert_eq!(v["timed_out"], json!(false));
            #[cfg(unix)]
            assert!(v["stdout"].as_str().unwrap().contains("citadel-marker"));
            #[cfg(windows)]
            assert!(!v["stdout"].as_str().unwrap().is_empty());
        }

        // A STRINGIFIED JSON array like "[\"test\"]" must be PARSED back to its
        // elements, NOT whitespace-split into garbage tokens like `["test",`
        // (which fed cargo `cargo '["test",'` -> "no such command"). Tokens stay
        // literal argv either way.
        #[test]
        fn stringified_json_array_args_are_parsed_not_split() {
            let dir = tempfile::tempdir().unwrap();
            let t = tool(&[plat::PRINT], dir.path(), Duration::from_secs(30));
            #[cfg(unix)]
            {
                // echo "citadel-marker": parsed -> one clean token; split would
                // leave the literal bracketed token "[\"citadel-marker\"]".
                let out = t
                    .call(&json!({"program": plat::PRINT, "args": "[\"citadel-marker\"]"}))
                    .unwrap();
                let v = parse(&out);
                assert_eq!(v["exit_code"], json!(0), "got {out}");
                let so = v["stdout"].as_str().unwrap();
                assert!(so.contains("citadel-marker"), "got {out}");
                assert!(
                    !so.contains('['),
                    "must be parsed to elements, not a literal array token: {out}"
                );
            }
            #[cfg(windows)]
            {
                // ping -n 1 127.0.0.1: parsed -> exits 0; split would feed the
                // garbage tokens `["-n",` etc. and fail.
                let out = t
                    .call(
                        &json!({"program": plat::PRINT, "args": "[\"-n\", \"1\", \"127.0.0.1\"]"}),
                    )
                    .unwrap();
                let v = parse(&out);
                assert_eq!(
                    v["exit_code"],
                    json!(0),
                    "parsed array must run clean: {out}"
                );
            }
        }

        // A string that LOOKS like a JSON array but is MALFORMED is rejected with
        // guidance, NOT whitespace-split into garbage tokens like `["test",` that
        // cargo would read as a bogus subcommand.
        #[test]
        fn malformed_json_array_string_args_rejected_with_guidance() {
            let dir = tempfile::tempdir().unwrap();
            let t = tool(&["cargo"], dir.path(), Duration::from_secs(5));
            let err = t
                .call(&json!({"program": "cargo", "args": "[\"test\","}))
                .unwrap_err();
            let msg = format!("{err}");
            assert!(msg.contains("malformed JSON array"), "{msg}");
        }

        // SECURITY: a string with shell metacharacters is NOT a shell line. Each
        // whitespace token is passed verbatim to the allowlisted program; there is
        // no `sh -c`, so `;` and `rm` are just literal argv tokens handed to
        // `echo`/`ping`, never a command split. The string runs WITHOUT error
        // precisely because nothing interprets the metacharacters (CWE-78 stays
        // structurally closed).
        #[test]
        fn string_args_metacharacters_are_literal_tokens_not_shell() {
            let dir = tempfile::tempdir().unwrap();
            let t = tool(&[plat::PRINT], dir.path(), Duration::from_secs(30));
            // On unix `echo` happily prints these literal tokens; on Windows we
            // keep ping's real args first so it still exits 0, with a trailing
            // metacharacter token proving it was passed as data, not parsed.
            #[cfg(unix)]
            let injection = "; rm -rf / | cat";
            #[cfg(windows)]
            let injection = "-n 1 127.0.0.1 & del";
            let out = t
                .call(&json!({"program": plat::PRINT, "args": injection}))
                .unwrap();
            let v = parse(&out);
            assert_eq!(v["timed_out"], json!(false), "got {out}");
            // No shell ran the second "command": on unix echo prints the tokens
            // verbatim (so the literal ';' and 'rm' appear in stdout, proving
            // they were data); the directory is untouched either way.
            #[cfg(unix)]
            {
                let so = v["stdout"].as_str().unwrap();
                assert!(
                    so.contains(';') && so.contains("rm"),
                    "echoed literally: {out}"
                );
            }
            assert!(dir.path().exists(), "workdir must be untouched");
        }

        // An empty / whitespace-only string yields an empty argv, never a
        // spurious empty token.
        #[test]
        fn empty_string_args_is_empty_vector() {
            let dir = tempfile::tempdir().unwrap();
            let t = tool(&[plat::FAIL], dir.path(), Duration::from_secs(30));
            let out = t
                .call(&json!({"program": plat::FAIL, "args": "   "}))
                .unwrap();
            let v = parse(&out);
            assert_eq!(v["timed_out"], json!(false), "got {out}");
        }

        // A non-string / non-array `args` (e.g. a number) still errors, and the
        // message now teaches both accepted shapes.
        #[test]
        fn non_string_non_array_args_rejected_with_guidance() {
            let dir = tempfile::tempdir().unwrap();
            let t = tool(&["cargo"], dir.path(), Duration::from_secs(5));
            let err = t
                .call(&json!({"program": "cargo", "args": 42}))
                .unwrap_err();
            let msg = format!("{err}");
            assert!(msg.contains("array of strings"), "{msg}");
            assert!(msg.contains("single string"), "{msg}");
        }

        #[test]
        fn empty_allowlist_denies_all() {
            let dir = tempfile::tempdir().unwrap();
            let t = tool(&[], dir.path(), Duration::from_secs(5));
            let err = t.call(&json!({"program": "cargo"})).unwrap_err();
            assert!(format!("{err}").contains("not in allowlist"));
        }

        #[test]
        fn non_basename_program_rejected() {
            let dir = tempfile::tempdir().unwrap();
            let t = tool(&["cargo"], dir.path(), Duration::from_secs(5));
            // Includes the BatBadBut bypass vectors: trailing space and trailing
            // dot (Windows strips both before resolving the file), embedded
            // whitespace, and the wider script/wrapper extension set.
            for bad in [
                "../evil",
                "/usr/bin/sh",
                "sub/dir/x",
                "evil.bat",
                "x.CMD",
                "run.cmd ",
                "run.bat.",
                "a b",
                "evil.vbs",
                "evil.js",
            ] {
                let err = t.call(&json!({"program": bad})).unwrap_err();
                assert!(
                    format!("{err}").contains("bare basename"),
                    "{bad:?} should be rejected: {err}"
                );
            }
        }

        #[test]
        fn batch_program_in_allowlist_rejected_at_construction() {
            let dir = tempfile::tempdir().unwrap();
            // A trailing-space batch name must be refused at construction too.
            for bad in ["run.bat", "run.cmd "] {
                let err = RunCommandTool::new(
                    [bad.to_string()],
                    dir.path().to_path_buf(),
                    Duration::from_secs(5),
                )
                .unwrap_err();
                assert!(format!("{err}").contains("bare basename"), "{bad:?}");
            }
        }

        #[test]
        fn unresolvable_workdir_errors_loud() {
            let missing = std::env::temp_dir().join("citadel-no-such-dir-9f3c1");
            let err = RunCommandTool::new(["cargo".to_string()], missing, Duration::from_secs(5))
                .unwrap_err();
            assert!(format!("{err}").contains("not resolvable"));
        }

        #[test]
        fn nonzero_exit_is_observation_not_error() {
            let dir = tempfile::tempdir().unwrap();
            let t = tool(&[plat::FAIL], dir.path(), Duration::from_secs(30));
            let out = t
                .call(&json!({"program": plat::FAIL, "args": plat::fail_args()}))
                .unwrap();
            let v = parse(&out);
            assert_eq!(v["timed_out"], json!(false));
            assert_ne!(v["exit_code"], json!(0), "expected non-zero exit: {out}");
            assert_ne!(v["exit_code"], Value::Null);
        }

        #[test]
        fn timeout_fires_and_reaps() {
            let dir = tempfile::tempdir().unwrap();
            let t = tool(&[plat::SLEEPER], dir.path(), Duration::from_millis(200));
            let start = Instant::now();
            let out = t
                .call(&json!({"program": plat::SLEEPER, "args": plat::sleeper_args()}))
                .unwrap();
            let elapsed = start.elapsed();
            let v = parse(&out);
            assert_eq!(v["timed_out"], json!(true), "got {out}");
            // The kill must land well before the child's natural ~5s+ runtime.
            assert!(elapsed < Duration::from_secs(4), "took {elapsed:?}");
        }

        // Regression: a child that exits immediately but backgrounds a long-lived
        // grandchild inheriting stdout keeps the pipe open. A blocking drain join
        // would hang until the grandchild dies (~5s); the bounded collection must
        // return within ~DRAIN_GRACE regardless.
        #[cfg(unix)]
        #[test]
        fn surviving_grandchild_does_not_hang_call() {
            let dir = tempfile::tempdir().unwrap();
            let t = tool(&["sh"], dir.path(), Duration::from_secs(30));
            let start = Instant::now();
            let out = t
                .call(&json!({"program": "sh", "args": ["-c", "sleep 5 & exit 0"]}))
                .unwrap();
            let elapsed = start.elapsed();
            let v = parse(&out);
            assert_eq!(v["exit_code"], json!(0), "got {out}");
            assert_eq!(v["timed_out"], json!(false));
            // Bounded by DRAIN_GRACE (2s) + slack, NOT the grandchild's lifetime.
            assert!(elapsed < Duration::from_secs(4), "call hung: {elapsed:?}");
        }

        #[test]
        fn output_cap_truncates() {
            let dir = tempfile::tempdir().unwrap();
            let t = RunCommandTool::with_cap(
                [plat::PRINT.to_string()],
                dir.path().to_path_buf(),
                Duration::from_secs(30),
                8,
            )
            .unwrap();
            let out = t
                .call(&json!({"program": plat::PRINT, "args": plat::print_args("aaaaaaaaaaaaaaaaaaaa")}))
                .unwrap();
            let v = parse(&out);
            assert_eq!(v["truncated"], json!(true), "got {out}");
            assert!(v["stdout"].as_str().unwrap().len() <= 8);
        }

        #[test]
        fn read_only_gate_blocks_run_command() {
            let dir = tempfile::tempdir().unwrap();
            let mut reg = ToolRegistry::new();
            reg.register(Box::new(tool(
                &["cargo"],
                dir.path(),
                Duration::from_secs(5),
            )));
            let call = ToolCall {
                id: "1".into(),
                name: "run_command".into(),
                arguments: json!({}),
            };
            assert!(structural_constraints_ok(
                &reg,
                &[],
                std::slice::from_ref(&call)
            ));
            assert!(!structural_constraints_ok(
                &reg,
                &["read-only".to_string()],
                std::slice::from_ref(&call)
            ));
        }
    }
}

#[cfg(all(feature = "command-tool", not(target_arch = "wasm32")))]
pub use cmd_tools::RunCommandTool;

#[cfg(test)]
mod tests {
    use super::*;
    use citadel::{Argon2Profile, DatabaseBuilder};
    use citadel_mem::MockEmbedder;

    struct Echo;
    impl Tool for Echo {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "echo".into(),
                description: "echo args".into(),
                input_schema: json!({"type": "object"}),
            }
        }
        fn call(&self, args: &Value) -> Result<String, ToolError> {
            Ok(args.to_string())
        }
    }

    fn call_of(name: &str, args: Value) -> ToolCall {
        ToolCall {
            id: "c".into(),
            name: name.into(),
            arguments: args,
        }
    }

    #[test]
    fn registry_dispatches_and_rejects_unknown() {
        let mut reg = ToolRegistry::new();
        reg.register(Box::new(Echo));
        assert!(reg.contains("echo"));
        assert_eq!(reg.specs().len(), 1);
        let out = reg.dispatch(&call_of("echo", json!({"a": 1}))).unwrap();
        assert!(out.contains("\"a\":1"));
        assert!(matches!(
            reg.dispatch(&call_of("ghost", json!({}))),
            Err(ToolError::Unknown(_))
        ));
    }

    #[test]
    fn default_permissions_are_least_privilege() {
        let perms = Echo.permissions();
        assert!(matches!(perms.network, NetworkPolicy::None));
        assert!(matches!(perms.filesystem, FsPolicy::None));
        assert!(matches!(perms.exec, ExecPolicy::None));
    }

    fn engine() -> (tempfile::TempDir, Arc<MemoryEngine>) {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let eng = Arc::new(MemoryEngine::open(Arc::new(db)).unwrap());
        eng.create_region("r", Arc::new(MockEmbedder::new(64)))
            .unwrap();
        (dir, eng)
    }

    #[test]
    fn mem_tools_remember_then_recall() {
        let (_dir, eng) = engine();
        let mut reg = ToolRegistry::new();
        reg.register(Box::new(MemRememberTool::new(Arc::clone(&eng), "r")));
        reg.register(Box::new(MemRecallTool::new(Arc::clone(&eng), "r")));

        let stored = reg
            .dispatch(&call_of(
                "mem_remember",
                json!({"text": "the sky is blue today"}),
            ))
            .unwrap();
        assert!(stored.contains("\"status\":\"stored\""));

        let recalled = reg
            .dispatch(&call_of("mem_recall", json!({"query": "sky", "k": 5})))
            .unwrap();
        assert!(recalled.contains("the sky is blue today"), "got {recalled}");
    }

    #[test]
    fn mem_recall_rejects_missing_query() {
        let (_dir, eng) = engine();
        let tool = MemRecallTool::new(eng, "r");
        assert!(matches!(
            tool.call(&json!({"nope": 1})),
            Err(ToolError::BadArgs { .. })
        ));
    }

    #[test]
    fn structural_constraints_whitelist_and_readonly() {
        let (_dir, eng) = engine();
        let mut reg = ToolRegistry::new();
        reg.register(Box::new(Echo));
        reg.register(Box::new(MemRememberTool::new(Arc::clone(&eng), "r")));

        let echo = call_of("echo", json!({}));
        let remember = call_of("mem_remember", json!({"text": "x"}));
        let ghost = call_of("ghost", json!({}));

        // No constraints: registered calls pass, unregistered is rejected.
        assert!(structural_constraints_ok(
            &reg,
            &[],
            std::slice::from_ref(&echo)
        ));
        assert!(!structural_constraints_ok(
            &reg,
            &[],
            std::slice::from_ref(&ghost)
        ));

        // "only use echo" forbids mem_remember.
        let only_echo = ["only use echo".to_string()];
        assert!(structural_constraints_ok(
            &reg,
            &only_echo,
            std::slice::from_ref(&echo)
        ));
        assert!(!structural_constraints_ok(
            &reg,
            &only_echo,
            std::slice::from_ref(&remember)
        ));

        // read-only forbids mem_remember.
        let read_only = ["read-only".to_string()];
        assert!(structural_constraints_ok(
            &reg,
            &read_only,
            std::slice::from_ref(&echo)
        ));
        assert!(!structural_constraints_ok(
            &reg,
            &read_only,
            std::slice::from_ref(&remember)
        ));
    }
}
