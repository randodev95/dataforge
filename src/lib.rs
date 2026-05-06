//! # Titan Engine
//!
//! Titan is a high-performance, vectorized SQL execution engine designed for reliable
//! data materialization. It implements a "Serialized Pipe" architecture to bypass
//! the complexities of AST sharing across polyglot SQL environments.
//!
//! ## Core Principles
//! - **Modularity**: Clearly separated layers for execution, parsing, and orchestration.
//! - **Reliability**: Integrated data contracts and schema evolution.
//! - **Performance**: Vectorized execution via DataFusion and Delta Lake.

pub mod artifacts;
pub mod circuit_breaker;
pub mod cli;
pub mod connectors;
pub mod core;
pub mod cost;
pub mod error;
pub mod execution;
pub mod filler;
pub mod fingerprint;
pub mod hooks;
pub mod intern;
pub mod mapper;
pub mod materialize;
pub mod metrics;
pub mod optimize;
pub mod project;
pub mod quality;
pub mod telemetry;
pub mod utils;

pub use cli::Cli;
pub use core::TitanSQL;
pub use error::{Result, TitanError};
pub use execution::Muscle;
pub use filler::{Filler, ModelMetadata, ModelTask, StateStore};
pub use fingerprint::{Fingerprinter, LogicHash};
pub use mapper::Mapper;
pub use materialize::VDE;
pub use project::Project;
