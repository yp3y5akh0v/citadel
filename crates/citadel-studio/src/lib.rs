//! Citadel Studio: native desktop client for the CitadelDB engine.
//!
//! Screens read `State` and emit `Action`s. `effects` and `engine` perform external work
//! off the UI thread, while `state::apply` remains the sole state-mutation boundary.

pub mod app;
pub mod cloud;
pub mod effects;
pub mod engine;
pub mod fonts;
pub mod gpu;
pub mod grid;
pub mod icon;
pub mod model;
pub mod plan;
pub mod rail;
pub mod screens;
pub mod sql;
pub mod sqlite;
pub mod state;
pub mod theme;
pub mod ui;
pub mod visible;
pub mod widgets;
pub mod window;
