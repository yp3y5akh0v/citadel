//! Model Context Protocol (MCP) server for Citadel: exposes a [`citadel_mem`] region as
//! MCP tools over a synchronous JSON-RPC 2.0 stdio transport (no tokio, no SDK).

pub mod protocol;
pub mod server;
mod types;

pub use server::serve_stdio;
