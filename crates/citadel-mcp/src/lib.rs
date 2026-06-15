//! Model Context Protocol (MCP) server for Citadel: exposes a [`citadel_mem`] region as
//! MCP tools over a synchronous JSON-RPC 2.0 stdio transport (no tokio, no SDK).

pub mod protocol;
pub mod serve;
pub mod server;
mod types;

pub use serve::{run, serve_with_config, ServeConfig};
pub use server::serve_stdio;

#[cfg(feature = "hub")]
pub use serve::pull_model;
