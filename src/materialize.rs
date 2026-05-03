// Materialization core module

pub mod view;
pub mod table;
pub mod incremental;
pub mod vde;

pub use vde::VDE;

use anyhow::Result;
use std::sync::Arc;
use async_trait::async_trait;

/// Enum describing the supported materializations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Materialization {
    View,
    Table,
    Incremental,
    Ephemeral,
}

impl Materialization {
    /// Parse a string from model config into a Materialization variant.
    pub fn from_str(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "view" => Materialization::View,
            "table" => Materialization::Table,
            "incremental" => Materialization::Incremental,
            "ephemeral" => Materialization::Ephemeral,
            _ => Materialization::View, // default like dbt
        }
    }
}

/// Trait implemented by each concrete materializer.
#[async_trait]
pub trait Materializer {
    async fn materialize(&self, env: &str, model_name: &str, hash: &crate::fingerprint::LogicHash, sql: &str) -> Result<()>;
}

/// Factory that returns a boxed materializer for a given enum.
pub fn get_materializer(
    mat: &Materialization,
    muscle: Arc<crate::execution::Muscle>,
    vde: Arc<crate::materialize::VDE>,
) -> Box<dyn Materializer + Send + Sync> {
    match mat {
        Materialization::View => Box::new(view::ViewMaterializer { vde }),
        Materialization::Table => Box::new(table::TableMaterializer { muscle }),
        Materialization::Incremental => Box::new(incremental::IncrementalMaterializer { muscle }),
        Materialization::Ephemeral => Box::new(view::ViewMaterializer { vde }), // Ephemeral behaves like a view but not persisted
    }
}
