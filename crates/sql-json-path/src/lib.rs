// Copyright 2023 RisingWave Labs
// Modifications Copyright (c) Citadel contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Vendored SQL/JSON Path implementation for Citadel.
//!
//! Forked from `sql-json-path` v0.1.1 (RisingWave Labs). Citadel uses only
//! the `serde_json` backend; the upstream `simd-json` / `jsonbb` backends
//! were dropped during vendoring. See `NOTICE` for full attribution.

mod ast;
mod datetime;
mod eval;
pub mod json;
mod parser;

pub use ast::JsonPath;
pub use eval::Error as EvalError;
pub use parser::Error as ParseError;
