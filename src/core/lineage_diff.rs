//! # Lineage Diffing
//!
//! This module computes the difference between two lineage graphs to detect
//! breaking changes or logic shifts.

use crate::core::lineage::{ColumnLineage, LineageResult, TransformKind};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageDiff {
    pub added_edges: Vec<ColumnLineage>,
    pub removed_edges: Vec<ColumnLineage>,
    pub transform_changes: Vec<TransformChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformChange {
    pub model: String,
    pub column: String,
    pub old_transform: TransformKind,
    pub new_transform: TransformKind,
}

pub struct LineageDiffer;

impl LineageDiffer {
    pub fn diff(old: &LineageResult, new: &LineageResult) -> LineageDiff {
        let mut added_edges = Vec::new();
        let mut removed_edges = Vec::new();
        let mut transform_changes = Vec::new();

        let old_map: HashMap<(String, String, String), TransformKind> = old
            .columns
            .iter()
            .map(|c| {
                (
                    (
                        c.source.model.to_string(),
                        c.source.column.to_string(),
                        c.target_column.clone(),
                    ),
                    c.transform.clone(),
                )
            })
            .collect();

        let new_map: HashMap<(String, String, String), TransformKind> = new
            .columns
            .iter()
            .map(|c| {
                (
                    (
                        c.source.model.to_string(),
                        c.source.column.to_string(),
                        c.target_column.clone(),
                    ),
                    c.transform.clone(),
                )
            })
            .collect();

        // Detect added and changed
        for (key, new_transform) in &new_map {
            if let Some(old_transform) = old_map.get(key) {
                if old_transform != new_transform {
                    transform_changes.push(TransformChange {
                        model: key.0.clone(),
                        column: key.2.clone(),
                        old_transform: old_transform.clone(),
                        new_transform: new_transform.clone(),
                    });
                }
            } else {
                // Find the original ColumnLineage to push to added_edges
                if let Some(edge) = new.columns.iter().find(|c| {
                    c.source.model.as_ref() == key.0
                        && c.source.column.as_ref() == key.1
                        && c.target_column == key.2
                }) {
                    added_edges.push(edge.clone());
                }
            }
        }

        // Detect removed
        for key in old_map.keys() {
            if !new_map.contains_key(key)
                && let Some(edge) = old.columns.iter().find(|c| {
                    c.source.model.as_ref() == key.0
                        && c.source.column.as_ref() == key.1
                        && c.target_column == key.2
                })
            {
                removed_edges.push(edge.clone());
            }
        }

        LineageDiff {
            added_edges,
            removed_edges,
            transform_changes,
        }
    }
}
