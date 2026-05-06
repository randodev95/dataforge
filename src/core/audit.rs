use anyhow::Result;
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::open_table;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp: u64,
    pub env: String,
    pub model: String,
    pub hash: String,
    pub status: String,
    pub rows_affected: usize,
    pub duration_ms: u64,
}

pub struct AuditLogger;

impl AuditLogger {
    pub async fn log(project_root: &Path, entry: AuditEntry) -> Result<()> {
        let audit_path = project_root.join("target/titan_audit");
        std::fs::create_dir_all(&audit_path)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::UInt64, false),
            Field::new("env", DataType::Utf8, false),
            Field::new("model", DataType::Utf8, false),
            Field::new("hash", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("rows_affected", DataType::UInt64, false),
            Field::new("duration_ms", DataType::UInt64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(datafusion::arrow::array::UInt64Array::from(vec![
                    entry.timestamp,
                ])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec![entry.env])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec![
                    entry.model,
                ])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec![
                    entry.hash,
                ])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec![
                    entry.status,
                ])),
                Arc::new(datafusion::arrow::array::UInt64Array::from(vec![
                    entry.rows_affected as u64,
                ])),
                Arc::new(datafusion::arrow::array::UInt64Array::from(vec![
                    entry.duration_ms,
                ])),
            ],
        )?;

        let log_path = audit_path.join("_delta_log");
        if log_path.exists() {
            let abs_path = std::fs::canonicalize(&audit_path)?;
            let url = url::Url::from_directory_path(abs_path)
                .map_err(|()| anyhow::anyhow!("Invalid audit log path"))?;
            let table = open_table(url).await?;
            table
                .write(vec![batch])
                .with_save_mode(deltalake::protocol::SaveMode::Append)
                .await?;
        } else {
            info!("Creating audit log table");
            let abs_path = std::fs::canonicalize(&audit_path)?;
            let url = url::Url::from_directory_path(abs_path)
                .map_err(|()| anyhow::anyhow!("Invalid audit log path"))?;
            let log_store = deltalake::logstore::logstore_for(&url, Default::default())?;

            deltalake::operations::write::WriteBuilder::new(log_store, None)
                .with_input_batches(vec![batch])
                .with_save_mode(deltalake::protocol::SaveMode::ErrorIfExists)
                .await?;
        }

        Ok(())
    }
}
