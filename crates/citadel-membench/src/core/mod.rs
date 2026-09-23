//! Dataset-agnostic benchmark engine shared by every benchmark plugin: error
//! type, rate-limit pacing, and the reader/judge eval machinery.

pub mod agentic;
pub mod benchmark;
pub mod civil;
pub mod config;
pub mod db;
pub mod error;
pub mod eval;
pub mod hash;
pub(crate) mod progress;
pub mod ratelimit;
pub mod retrieval;
pub mod temporal;
