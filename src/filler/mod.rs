//! # Filler Orchestration
//!
//! This module implements the DAG execution and materialization orchestration logic.
//! It uses a state store to track model versions and avoid redundant work.

pub mod dag;
pub mod grace_period;
pub mod state;
pub mod table_guard;

pub use dag::{Filler, ModelTask};
pub use state::{ModelMetadata, StateStore};
pub use table_guard::TableGuard;
