//! # Filler Orchestration
//! 
//! This module implements the DAG execution and materialization orchestration logic.
//! It uses a state store to track model versions and avoid redundant work.

pub mod state;
pub mod dag;
pub mod table_guard;

pub use state::{StateStore, ModelMetadata};
pub use dag::{Filler, ModelTask};
pub use table_guard::TableGuard;
