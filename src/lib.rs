//! # Titan Engine
//! 
//! Titan is a high-performance, vectorized SQL execution engine designed for reliable 
//! data materialization. It implements a "Serialized Pipe" architecture to bypass 
//! the complexities of AST sharing across polyglot SQL environments.

pub mod core;
pub mod fingerprint;
pub mod filler;
pub mod execution;
pub mod mapper;
pub mod materialize;
pub mod project;
pub mod cli;
pub mod error;
pub mod hooks;
pub mod quality;

pub use core::TitanSQL;
pub use fingerprint::{Fingerprinter, LogicHash};
pub use filler::{Filler, StateStore, ModelMetadata};
pub use execution::Muscle;
pub use mapper::Mapper;
pub use materialize::VDE;
pub use project::Project;
pub use cli::Cli;
pub use error::{TitanError, Result};
