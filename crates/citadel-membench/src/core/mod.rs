//! Dataset-agnostic benchmark engine shared by every benchmark plugin: error type,
//! rate-limit pacing, and the reader/judge eval machinery.

pub mod benchmark;
pub mod civil;
pub mod error;
pub mod eval;
pub mod hash;
pub mod ratelimit;
