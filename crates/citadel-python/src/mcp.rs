//! MCP server binding: forwards argv to `citadel_mcp::run` with the GIL released.

use pyo3::prelude::*;

/// Run the MCP CLI with `argv`; returns the exit code. Serving blocks until the
/// client closes stdin. Passphrase from `CITADEL_KEY`.
#[pyfunction]
pub(crate) fn mcp_main(py: Python<'_>, argv: Vec<String>) -> i32 {
    match py.detach(|| citadel_mcp::run(&argv)) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("citadel-mcp: {e}");
            1
        }
    }
}
