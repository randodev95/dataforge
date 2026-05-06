use crate::Muscle;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelDiffStatus {
    Unchanged,
    Modified,
    Added,
    Removed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffResult {
    pub model_name: String,
    pub status: ModelDiffStatus,
    pub row_count_before: Option<u64>,
    pub row_count_after: Option<u64>,
    pub column_changes: Vec<String>,
}

pub struct CIDiffer {
    pub muscle: Arc<Muscle>,
}

impl CIDiffer {
    pub fn new(muscle: Arc<Muscle>) -> Self {
        Self { muscle }
    }

    pub async fn compare(
        &self,
        model_name: &str,
        base_table: &str,
        target_table: &str,
    ) -> Result<DiffResult> {
        let count_base = self.get_row_count(base_table).await?;
        let count_target = self.get_row_count(target_table).await?;

        let schema_base = self.get_schema(base_table).await?;
        let schema_target = self.get_schema(target_table).await?;

        let mut column_changes = Vec::new();
        let mut base_cols = HashSet::new();
        for f in schema_base.fields() {
            base_cols.insert(f.name().clone());
        }

        let mut target_cols = HashSet::new();
        for f in schema_target.fields() {
            target_cols.insert(f.name().clone());
            if !base_cols.contains(f.name()) {
                column_changes.push(format!("+ {}", f.name()));
            }
        }

        for name in base_cols {
            if !target_cols.contains(&name) {
                column_changes.push(format!("- {name}"));
            }
        }

        let status = if count_base == count_target && column_changes.is_empty() {
            ModelDiffStatus::Unchanged
        } else {
            ModelDiffStatus::Modified
        };

        Ok(DiffResult {
            model_name: model_name.to_string(),
            status,
            row_count_before: Some(count_base),
            row_count_after: Some(count_target),
            column_changes,
        })
    }

    async fn get_schema(&self, table: &str) -> Result<datafusion::arrow::datatypes::SchemaRef> {
        let sql = format!("SELECT * FROM {table} LIMIT 0");
        let df = self
            .muscle
            .ctx
            .sql(&sql)
            .await
            .map_err(|e| crate::error::TitanError::ExecutionError(e.to_string()))?;
        let schema: &datafusion::arrow::datatypes::Schema = df.schema().as_ref();
        Ok(std::sync::Arc::new(schema.clone()))
    }

    async fn get_row_count(&self, table: &str) -> Result<u64> {
        let sql = format!("SELECT COUNT(*) FROM {table}");
        let df = self
            .muscle
            .ctx
            .sql(&sql)
            .await
            .map_err(|e| crate::error::TitanError::ExecutionError(e.to_string()))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| crate::error::TitanError::ExecutionError(e.to_string()))?;
        if batches.is_empty() {
            return Ok(0);
        }
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        Ok(col.value(0) as u64)
    }
}
