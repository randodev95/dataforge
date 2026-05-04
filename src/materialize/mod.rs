//! # Materialization Strategies
//! 
//! This module defines the different ways SQL models can be persisted 
//! (e.g. Views, Tables, Incremental models, SCD2 Snapshots).
//! It uses a factory pattern to select the appropriate strategy at runtime.

pub mod view;
pub mod table;
pub mod incremental;
pub mod snapshot;
pub mod adbc;
pub mod vde;

pub use vde::VDE;
use crate::error::Result;
use async_trait::async_trait;
use crate::fingerprint::LogicHash;
use uuid::Uuid;
use std::sync::Arc;
use crate::project::OnSchemaChange;
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Materialization {
    View,
    Table,
    Incremental,
    Snapshot,
    Ephemeral,
}

impl FromStr for Materialization {
    type Err = crate::error::TitanError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "view" => Ok(Materialization::View),
            "table" => Ok(Materialization::Table),
            "incremental" => Ok(Materialization::Incremental),
            "snapshot" => Ok(Materialization::Snapshot),
            "ephemeral" => Ok(Materialization::Ephemeral),
            _ => Ok(Materialization::View), // Default to View
        }
    }
}

#[async_trait]
pub trait Materializer: Send + Sync {
    async fn materialize(&self, env: &str, model_name: &str, hash: &LogicHash, exec_id: &Uuid, rendered_sql: &str) -> Result<()>;
}

pub enum MaterializerStrategy {
    View(view::ViewMaterializer),
    Table(table::TableMaterializer),
    Incremental(incremental::IncrementalMaterializer),
    Snapshot(snapshot::SnapshotMaterializer),
    Adbc(adbc::AdbcMaterializer),
}

#[async_trait]
impl Materializer for MaterializerStrategy {
    async fn materialize(&self, env: &str, model_name: &str, hash: &LogicHash, exec_id: &Uuid, rendered_sql: &str) -> Result<()> {
        match self {
            MaterializerStrategy::View(m) => m.materialize(env, model_name, hash, exec_id, rendered_sql).await,
            MaterializerStrategy::Table(m) => m.materialize(env, model_name, hash, exec_id, rendered_sql).await,
            MaterializerStrategy::Incremental(m) => m.materialize(env, model_name, hash, exec_id, rendered_sql).await,
            MaterializerStrategy::Snapshot(m) => m.materialize(env, model_name, hash, exec_id, rendered_sql).await,
            MaterializerStrategy::Adbc(m) => m.materialize(env, model_name, hash, exec_id, rendered_sql).await,
        }
    }
}

pub fn get_materializer(
    mat: &Materialization,
    muscle: Arc<crate::execution::Muscle>,
    vde: Arc<VDE>,
    unique_key: Option<String>,
    target_type: &str,
    retention: Option<crate::project::RetentionConfig>,
    on_schema_change: OnSchemaChange,
    project_root: PathBuf,
) -> MaterializerStrategy {
    if target_type == "adbc" && matches!(mat, Materialization::Table | Materialization::Incremental) {
        return MaterializerStrategy::Adbc(crate::materialize::adbc::AdbcMaterializer { muscle });
    }

    match mat {
        Materialization::View => MaterializerStrategy::View(view::ViewMaterializer { vde }),
        Materialization::Table => MaterializerStrategy::Table(table::TableMaterializer { muscle }),
        Materialization::Incremental => MaterializerStrategy::Incremental(incremental::IncrementalMaterializer { 
            muscle, 
            unique_key, 
            on_schema_change, 
            base_path: project_root 
        }),
        Materialization::Snapshot => MaterializerStrategy::Snapshot(snapshot::SnapshotMaterializer::new(
            muscle, 
            vde, 
            unique_key, 
            retention.map(|r| r.snapshots_days), 
            project_root
        )),
        Materialization::Ephemeral => MaterializerStrategy::View(view::ViewMaterializer { vde }),
    }
}
