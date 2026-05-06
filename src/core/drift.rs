//! # Schema Drift Detection
//!
//! This module identifies differences between the source SQL schema and the
//! physical table in the warehouse.

use crate::core::dialect::SqlDialect;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DriftAction {
    /// No changes needed.
    Ignore,
    /// Only safe type changes (widening) or new columns.
    AlterTable,
    /// Unsafe changes requiring a table recreation.
    DropAndRecreate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftResult {
    pub action: DriftAction,
    pub added_columns: Vec<ColumnInfo>,
    pub changed_columns: Vec<ChangedColumn>,
    pub dropped_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangedColumn {
    pub name: String,
    pub source_type: String,
    pub target_type: String,
    pub is_safe: bool,
}

pub struct DriftDetector;

impl DriftDetector {
    pub fn detect(
        source_columns: &[ColumnInfo],
        target_columns: &[ColumnInfo],
        dialect: &dyn SqlDialect,
    ) -> DriftResult {
        let mut added_columns = Vec::new();
        let mut changed_columns = Vec::new();
        let mut dropped_columns = Vec::new();

        let target_map: std::collections::HashMap<String, String> = target_columns
            .iter()
            .map(|c| (c.name.to_lowercase(), c.data_type.clone()))
            .collect();

        let source_names: std::collections::HashSet<String> = source_columns
            .iter()
            .map(|c| c.name.to_lowercase())
            .collect();

        // Detect added and changed columns
        for source_col in source_columns {
            let name_lower = source_col.name.to_lowercase();
            if let Some(target_type) = target_map.get(&name_lower) {
                if source_col.data_type.to_lowercase() != target_type.to_lowercase() {
                    let is_safe = dialect.is_safe_widening(target_type, &source_col.data_type);
                    changed_columns.push(ChangedColumn {
                        name: source_col.name.clone(),
                        source_type: source_col.data_type.clone(),
                        target_type: target_type.clone(),
                        is_safe,
                    });
                }
            } else {
                added_columns.push(source_col.clone());
            }
        }

        // Detect dropped columns
        for target_col in target_columns {
            if !source_names.contains(&target_col.name.to_lowercase()) {
                dropped_columns.push(target_col.name.clone());
            }
        }

        let mut action = DriftAction::Ignore;
        if !added_columns.is_empty() || !changed_columns.is_empty() {
            action = DriftAction::AlterTable;
            if changed_columns.iter().any(|c| !c.is_safe) {
                action = DriftAction::DropAndRecreate;
            }
        }

        DriftResult {
            action,
            added_columns,
            changed_columns,
            dropped_columns,
        }
    }

    pub fn generate_alter_sql(
        table_name: &str,
        drift: &DriftResult,
        dialect: &dyn SqlDialect,
    ) -> Vec<String> {
        let mut sql = Vec::new();

        for col in &drift.added_columns {
            sql.push(dialect.add_column_sql(table_name, &col.name, &col.data_type));
        }

        for col in &drift.changed_columns {
            if col.is_safe {
                sql.push(dialect.alter_column_type_sql(table_name, &col.name, &col.source_type));
            }
        }

        sql
    }
}
