//! Benchmark plugins. Each benchmark lives in its own submodule with its own
//! dataset/ingest/prompts; the shared engine in `core` drives them.

pub mod locomo;
pub mod longmemeval;
