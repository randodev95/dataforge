use crate::error::{Result, TitanError};
use crate::execution::Muscle;
use crate::fingerprint::LogicHash;
use crate::materialize::{Materializer, VDE};
use async_trait::async_trait;
use chrono::Utc;
use deltalake::open_table;
use futures::StreamExt;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::debug;

/// Materializer for SCD-2 (Slowly Changing Dimensions Type 2) Snapshots in Delta Lake.
///
/// This materializer tracks historical changes by maintaining a valid_from and valid_to
/// window for each row, keyed by a unique identifier and logic hash.
pub struct SnapshotMaterializer {
    muscle: Arc<Muscle>,
    unique_key: Option<String>,
    retention_days: Option<i64>,
    base_path: PathBuf,
}

impl SnapshotMaterializer {
    pub fn new(
        muscle: Arc<Muscle>,
        _vde: Arc<VDE>,
        unique_key: Option<String>,
        retention_days: Option<i64>,
        base_path: PathBuf,
    ) -> Self {
        Self {
            muscle,
            unique_key,
            retention_days,
            base_path,
        }
    }
}

#[async_trait]
impl Materializer for SnapshotMaterializer {
    async fn materialize(
        &self,
        env: &str,
        name: &str,
        target_name: &str,
        hash: &LogicHash,
        exec_id: &uuid::Uuid,
        rendered_sql: &str,
    ) -> Result<()> {
        use crate::metrics::{MATERIALIZATION_LATENCY_SECONDS, MATERIALIZATIONS_TOTAL};
        let _timer = MATERIALIZATION_LATENCY_SECONDS
            .with_label_values(&["snapshot"])
            .start_timer();

        let result = self
            .materialize_internal(env, name, target_name, hash, exec_id, rendered_sql)
            .await;

        if result.is_ok() {
            MATERIALIZATIONS_TOTAL
                .with_label_values(&["snapshot", "success"])
                .inc();
        } else {
            MATERIALIZATIONS_TOTAL
                .with_label_values(&["snapshot", "fail"])
                .inc();
        }

        result
    }
}

impl SnapshotMaterializer {
    async fn materialize_internal(
        &self,
        env: &str,
        name: &str,
        target_name: &str,
        hash: &LogicHash,
        exec_id: &uuid::Uuid,
        rendered_sql: &str,
    ) -> Result<()> {
        let unique_key = self.unique_key.as_ref().ok_or_else(|| {
            TitanError::ValidationError("unique_key is required for snapshots".to_string())
        })?;

        let table_path = self
            .base_path
            .join(format!("snapshots/{env}/{target_name}"));
        std::fs::create_dir_all(&table_path)?;

        let now = Utc::now().timestamp_millis();

        let source_view = format!("source_{name}_{hash}_{exec_id}");
        let _source_guard = crate::filler::TableGuard::new(self.muscle.clone(), &source_view);

        self.muscle
            .ctx
            .register_table(
                &source_view,
                self.muscle
                    .ctx
                    .sql(rendered_sql)
                    .await
                    .map_err(|e| TitanError::SqlParseError(e.to_string()))?
                    .into_view(),
            )
            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

        let source_df = self
            .muscle
            .ctx
            .table(&source_view)
            .await
            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

        let log_path = table_path.join("_delta_log");
        let table_exists = log_path.exists();

        if table_exists {
            debug!(model = %name, "Performing SCD-2 Merge");
            let abs_path = std::fs::canonicalize(&table_path)?;
            let url = url::Url::from_directory_path(abs_path)
                .map_err(|()| TitanError::ValidationError("Invalid table path".to_string()))?;
            let delta_table = open_table(url.clone())
                .await
                .map_err(|e| TitanError::DatabaseError(e.to_string()))?;

            let log_store = delta_table.log_store();
            let eager_snapshot = deltalake::kernel::EagerSnapshot::try_new(
                log_store.as_ref(),
                Default::default(),
                None,
            )
            .await
            .map_err(|e| TitanError::DatabaseError(e.to_string()))?;
            let table_provider = deltalake::delta_datafusion::DeltaTableProvider::try_new(
                eager_snapshot,
                log_store.clone(),
                Default::default(),
            )
            .map_err(|e| TitanError::DatabaseError(e.to_string()))?;

            let snapshot_table_name = format!("snapshot_{name}_{hash}_{exec_id}");
            let _snapshot_guard =
                crate::filler::TableGuard::new(self.muscle.clone(), &snapshot_table_name);

            self.muscle
                .ctx
                .register_table(&snapshot_table_name, Arc::new(table_provider))
                .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

            let current_df = self
                .muscle
                .ctx
                .table(&snapshot_table_name)
                .await
                .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

            let mut new_fields = Vec::new();
            for f in source_df.schema().fields() {
                if current_df.schema().field_with_name(None, f.name()).is_err() {
                    new_fields.push(f.clone());
                }
            }

            let all_cols: Vec<String> = current_df
                .schema()
                .fields()
                .iter()
                .map(|f| format!("c.\"{}\"", f.name()))
                .collect();
            let mut all_with_new = all_cols.clone();
            for f in &new_fields {
                let dt_sql = match f.data_type() {
                    deltalake::arrow::datatypes::DataType::Utf8 => "VARCHAR",
                    deltalake::arrow::datatypes::DataType::Int64 => "BIGINT",
                    deltalake::arrow::datatypes::DataType::Float64 => "DOUBLE",
                    deltalake::arrow::datatypes::DataType::Boolean => "BOOLEAN",
                    _ => "VARCHAR",
                };
                all_with_new.push(format!("CAST(NULL AS {}) as \"{}\"", dt_sql, f.name()));
            }
            let all_cols_str = all_with_new.join(", ");

            let expiring_cols: Vec<String> = current_df
                .schema()
                .fields()
                .iter()
                .map(|f| {
                    let name = f.name();
                    if name == "titan_valid_to" {
                        format!("CAST({now} AS BIGINT) as \"titan_valid_to\"")
                    } else {
                        format!("c.\"{name}\"")
                    }
                })
                .collect();
            let mut expiring_with_new = expiring_cols;
            for f in &new_fields {
                let dt_sql = match f.data_type() {
                    deltalake::arrow::datatypes::DataType::Utf8 => "VARCHAR",
                    deltalake::arrow::datatypes::DataType::Int64 => "BIGINT",
                    deltalake::arrow::datatypes::DataType::Float64 => "DOUBLE",
                    deltalake::arrow::datatypes::DataType::Boolean => "BOOLEAN",
                    _ => "VARCHAR",
                };
                expiring_with_new.push(format!("CAST(NULL AS {}) as \"{}\"", dt_sql, f.name()));
            }
            let expiring_cols_str = expiring_with_new.join(", ");

            let new_cols: Vec<String> = current_df
                .schema()
                .fields()
                .iter()
                .map(|f| {
                    let name = f.name();
                    if name == "titan_logic_hash" {
                        format!(
                            "CAST('{}' AS VARCHAR) as \"titan_logic_hash\"",
                            hash.as_str()
                        )
                    } else if name == "titan_valid_from" {
                        format!("CAST({now} AS BIGINT) as \"titan_valid_from\"")
                    } else if name == "titan_valid_to" {
                        "CAST(NULL AS BIGINT) as \"titan_valid_to\"".to_string()
                    } else {
                        format!("s.\"{name}\" as \"{name}\"")
                    }
                })
                .collect();
            let mut new_with_new = new_cols;
            for f in &new_fields {
                new_with_new.push(format!("s.\"{}\" as \"{}\"", f.name(), f.name()));
            }
            let new_cols_str = new_with_new.join(", ");

            let merge_sql = format!(
                "
                -- 1. Unchanged or already expired rows: Keep as is
                SELECT {all} FROM \"{snapshot}\" c
                WHERE c.titan_valid_to IS NOT NULL 
                OR (c.titan_valid_to IS NULL AND c.titan_logic_hash = '{hash}')
                UNION ALL
                -- 2. Active records that need expiring (hash changed or row deleted)
                SELECT {expiring} FROM \"{snapshot}\" c
                WHERE c.titan_valid_to IS NULL AND c.titan_logic_hash != '{hash}'
                UNION ALL
                -- 3. New records from source that don't match any active row hash
                SELECT {new_rows} FROM \"{source}\" s
                WHERE NOT EXISTS (
                    SELECT 1 FROM \"{snapshot}\" c 
                    WHERE s.\"{pk}\" = c.\"{pk}\" AND c.titan_valid_to IS NULL AND c.titan_logic_hash = '{hash}'
                )
                ",
                source = source_view,
                snapshot = snapshot_table_name,
                pk = unique_key,
                hash = hash.as_str(),
                all = all_cols_str,
                expiring = expiring_cols_str,
                new_rows = new_cols_str
            );

            let df = self
                .muscle
                .ctx
                .sql(&merge_sql)
                .await
                .map_err(|e| TitanError::SqlParseError(e.to_string()))?;
            let mut stream = df
                .execute_stream()
                .await
                .map_err(|e| TitanError::ExecutionError(e.to_string()))?;
            let mut row_count = 0;

            let mut write_op = delta_table
                .write(Vec::new())
                .with_save_mode(deltalake::protocol::SaveMode::Overwrite);

            if !new_fields.is_empty() {
                write_op =
                    write_op.with_schema_mode(deltalake::operations::write::SchemaMode::Merge);
            }

            while let Some(batch) = stream.next().await {
                let batch = batch.map_err(|e| TitanError::ExecutionError(e.to_string()))?;
                row_count += batch.num_rows();
                write_op = write_op.with_input_batches(vec![batch]);
            }

            write_op
                .await
                .map_err(|e: deltalake::errors::DeltaTableError| {
                    TitanError::DatabaseError(e.to_string())
                })?;
            crate::metrics::ROWS_WRITTEN_TOTAL
                .with_label_values(&["snapshot"])
                .inc_by(row_count as f64);

            if let Some(days) = self.retention_days {
                let final_table = open_table(url.clone())
                    .await
                    .map_err(|e| TitanError::DatabaseError(e.to_string()))?;
                final_table
                    .vacuum()
                    .with_retention_period(chrono::Duration::try_days(days).ok_or_else(|| {
                        TitanError::ValidationError("Invalid retention days".to_string())
                    })?)
                    .await
                    .map_err(|e| TitanError::DatabaseError(e.to_string()))?;
            }
        } else {
            debug!(model = %name, "Creating initial snapshot");
            let schema = source_df.schema();
            let cols: Vec<_> = schema
                .fields()
                .iter()
                .map(|f| format!("\"{}\"", f.name()))
                .collect();
            let cols_str = cols.join(", ");

            let init_sql = format!(
                "SELECT {}, '{}' as titan_logic_hash, {} as titan_valid_from, CAST(NULL AS BIGINT) as titan_valid_to FROM \"{}\"",
                cols_str,
                hash.as_str(),
                now,
                source_view
            );

            let abs_path = std::fs::canonicalize(&table_path)?;
            let df = self
                .muscle
                .ctx
                .sql(&init_sql)
                .await
                .map_err(|e| TitanError::SqlParseError(e.to_string()))?;
            let mut stream = df
                .execute_stream()
                .await
                .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

            let url = url::Url::from_directory_path(abs_path)
                .map_err(|()| TitanError::ValidationError("Invalid table path".to_string()))?;
            let log_store = deltalake::logstore::logstore_for(&url, Default::default())
                .map_err(|e| TitanError::DatabaseError(e.to_string()))?;

            let mut writer = deltalake::operations::write::WriteBuilder::new(log_store, None)
                .with_save_mode(deltalake::protocol::SaveMode::Overwrite);

            while let Some(batch) = stream.next().await {
                let batch = batch.map_err(|e| TitanError::ExecutionError(e.to_string()))?;
                writer = writer.with_input_batches(vec![batch]);
            }

            writer
                .await
                .map_err(|e| TitanError::DatabaseError(e.to_string()))?;
        }

        Ok(())
    }
}
