//! Cost attribution and FinOps for DataForge pipelines.
//!
//! Inspired by Rocky RS and adapted for the Titan Engine.

use crate::filler::dag::ModelTask;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Confidence {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEstimate {
    pub estimated_rows: u64,
    pub estimated_bytes: u64,
    pub estimated_compute_cost_usd: f64,
    pub confidence: Confidence,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum WarehouseType {
    Databricks,
    Snowflake,
    BigQuery,
    DuckDb,
}

pub struct WarehouseCostModel {
    pub per_row_scan_cost: f64,
    pub per_row_compute_cost: f64,
    pub per_byte_io_cost: f64,
}

impl WarehouseCostModel {
    pub fn for_warehouse(w: WarehouseType) -> Self {
        match w {
            WarehouseType::Databricks => Self {
                per_row_scan_cost: 1.0e-9,
                per_row_compute_cost: 5.0e-9,
                per_byte_io_cost: 5.0e-12,
            },
            WarehouseType::Snowflake => Self {
                per_row_scan_cost: 1.2e-9,
                per_row_compute_cost: 6.0e-9,
                per_byte_io_cost: 6.0e-12,
            },
            WarehouseType::BigQuery => Self {
                per_row_scan_cost: 0.8e-9,
                per_row_compute_cost: 4.0e-9,
                per_byte_io_cost: 6.25e-12,
            },
            WarehouseType::DuckDb => Self {
                per_row_scan_cost: 0.0,
                per_row_compute_cost: 0.0,
                per_byte_io_cost: 0.0,
            },
        }
    }
}

/// Simple cost propagation across a list of tasks.
pub fn estimate_project_cost(
    tasks: &[ModelTask],
    warehouse: WarehouseType,
) -> HashMap<String, CostEstimate> {
    let mut estimates = HashMap::new();
    let model = WarehouseCostModel::for_warehouse(warehouse);

    // In a real implementation, we would use topological sort and propagate
    // row counts. For now, we use baseline heuristics per model.
    for task in tasks {
        let est = CostEstimate {
            estimated_rows: 1000, // Baseline heuristic
            estimated_bytes: 1024 * 1024,
            estimated_compute_cost_usd: model.per_row_compute_cost * 1000.0,
            confidence: Confidence::Low,
        };
        estimates.insert(task.name.clone(), est);
    }
    estimates
}
