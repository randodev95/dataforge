//! # Materialization Strategies
//!
//! This module defines the different ways SQL models can be persisted
//! (e.g. Views, Tables, Incremental models, SCD2 Snapshots).
//! It uses a factory pattern to select the appropriate strategy at runtime.

pub mod adbc;
pub mod incremental;
pub mod snapshot;
pub mod table;
pub mod vde;
pub mod view;

use crate::error::Result;
use crate::fingerprint::LogicHash;
use crate::project::OnSchemaChange;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;
pub use vde::VDE;

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
    async fn materialize(
        &self,
        env: &str,
        model_name: &str,
        target_name: &str,
        hash: &LogicHash,
        exec_id: &Uuid,
        rendered_sql: &str,
    ) -> Result<()>;
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
    async fn materialize(
        &self,
        env: &str,
        model_name: &str,
        target_name: &str,
        hash: &LogicHash,
        exec_id: &Uuid,
        rendered_sql: &str,
    ) -> Result<()> {
        match self {
            MaterializerStrategy::View(m) => {
                m.materialize(env, model_name, target_name, hash, exec_id, rendered_sql)
                    .await
            }
            MaterializerStrategy::Table(m) => {
                m.materialize(env, model_name, target_name, hash, exec_id, rendered_sql)
                    .await
            }
            MaterializerStrategy::Incremental(m) => {
                m.materialize(env, model_name, target_name, hash, exec_id, rendered_sql)
                    .await
            }
            MaterializerStrategy::Snapshot(m) => {
                m.materialize(env, model_name, target_name, hash, exec_id, rendered_sql)
                    .await
            }
            MaterializerStrategy::Adbc(m) => {
                m.materialize(env, model_name, target_name, hash, exec_id, rendered_sql)
                    .await
            }
        }
    }
}

pub fn get_materializer(
    mat: &Materialization,
    muscle: Arc<crate::execution::Muscle>,
    vde: Arc<VDE>,
    unique_key: Option<String>,
    partition_by: Option<String>,
    target_type: &str,
    retention: Option<crate::project::RetentionConfig>,
    on_schema_change: OnSchemaChange,
    project_root: PathBuf,
    columns: Vec<crate::project::ModelColumn>,
) -> MaterializerStrategy {
    let dialect = crate::core::get_dialect(target_type);

    if target_type == "adbc" && matches!(mat, Materialization::Table | Materialization::Incremental)
    {
        return MaterializerStrategy::Adbc(crate::materialize::adbc::AdbcMaterializer { muscle });
    }

    let mut column_map = crate::core::column_map::ColumnMap::new();
    for col in columns {
        if let Some(dt_str) = col.data_type {
            // Simplified mapping for now
            let dt = match dt_str.to_lowercase().as_str() {
                "int64" | "bigint" => datafusion::arrow::datatypes::DataType::Int64,
                "float64" | "double" => datafusion::arrow::datatypes::DataType::Float64,
                _ => datafusion::arrow::datatypes::DataType::Utf8,
            };
            column_map.insert(col.name.clone(), col.name, dt);
        }
    }

    match mat {
        Materialization::View => MaterializerStrategy::View(view::ViewMaterializer { vde }),
        Materialization::Table => MaterializerStrategy::Table(table::TableMaterializer { muscle }),
        Materialization::Incremental => {
            MaterializerStrategy::Incremental(incremental::IncrementalMaterializer {
                muscle,
                unique_key,
                partition_by,
                on_schema_change,
                base_path: project_root,
                dialect,
                column_map,
            })
        }
        Materialization::Snapshot => {
            MaterializerStrategy::Snapshot(snapshot::SnapshotMaterializer::new(
                muscle,
                vde,
                unique_key,
                retention.map(|r| r.snapshots_days),
                project_root,
            ))
        }
        Materialization::Ephemeral => MaterializerStrategy::View(view::ViewMaterializer { vde }),
    }
}
