use crate::error::{Result, TitanError};
use crate::execution::Muscle;
use crate::fingerprint::LogicHash;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use deltalake::arrow::datatypes::{Field, Schema};
use deltalake::open_table;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info};

use crate::core::SqlDialect;
use crate::project::OnSchemaChange;
use futures::StreamExt;

/// Materializer for Incremental models in Delta Lake.
///
/// Supports merging new data based on a unique key or appending if no key is provided.
pub struct IncrementalMaterializer {
    pub muscle: Arc<Muscle>,
    pub unique_key: Option<String>,
    pub partition_by: Option<String>,
    pub on_schema_change: OnSchemaChange,
    pub base_path: PathBuf,
    pub dialect: Arc<dyn SqlDialect>,
    pub column_map: crate::core::column_map::ColumnMap,
}

#[async_trait]
impl super::Materializer for IncrementalMaterializer {
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
            .with_label_values(&["incremental"])
            .start_timer();

        let result = self
            .materialize_internal(env, name, target_name, hash, exec_id, rendered_sql)
            .await;

        if result.is_ok() {
            MATERIALIZATIONS_TOTAL
                .with_label_values(&["incremental", "success"])
                .inc();
        } else {
            MATERIALIZATIONS_TOTAL
                .with_label_values(&["incremental", "fail"])
                .inc();
        }

        result
    }
}
impl IncrementalMaterializer {
    async fn materialize_internal(
        &self,
        env: &str,
        name: &str,
        target_name: &str,
        hash: &LogicHash,
        exec_id: &uuid::Uuid,
        rendered_sql: &str,
    ) -> Result<()> {
        let table_path = self.base_path.join(format!("tables/{env}/{target_name}"));
        std::fs::create_dir_all(&table_path)?;

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
            info!(model = %name, "Performing incremental update for {:?}", table_path);
            let abs_path = std::fs::canonicalize(&table_path)?;
            let url = url::Url::from_directory_path(abs_path)
                .map_err(|()| TitanError::ValidationError("Invalid path".to_string()))?;
            let delta_table = open_table(url.clone())
                .await
                .map_err(|e| TitanError::DatabaseError(e.to_string()))?;

            if let Some(pk) = &self.unique_key {
                debug!(model = %name, pk = %pk, "Merging incremental update via atomic overwrite");

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

                let target_schema = table_provider.schema();
                let target_table_name = format!("target_{name}_{hash}_{exec_id}");
                let _target_guard =
                    crate::filler::TableGuard::new(self.muscle.clone(), &target_table_name);

                self.muscle
                    .ctx
                    .register_table(&target_table_name, Arc::new(table_provider))
                    .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

                let mut new_fields = Vec::new();
                for f in source_df.schema().fields() {
                    if target_schema.field_with_name(f.name()).is_err() {
                        new_fields.push(f.clone());
                    }
                }

                let mut combined_fields: Vec<Field> = target_schema
                    .fields()
                    .iter()
                    .map(|f| (**f).clone())
                    .collect();
                for f in &new_fields {
                    combined_fields.push((**f).clone());
                }
                let new_target_schema = Arc::new(Schema::new(combined_fields));

                let resulting_columns: Vec<String> = new_target_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();

                let merge_sql = if let Some(p) = &self.partition_by {
                    self.dialect.partition_overwrite(
                        &self.dialect.quote_identifier(&target_table_name),
                        &self.dialect.quote_identifier(&source_view),
                        p,
                        &resulting_columns,
                    )
                } else {
                    self.dialect.merge_upsert(
                        &self.dialect.quote_identifier(&target_table_name),
                        &self.dialect.quote_identifier(&source_view),
                        pk.split(',')
                            .map(|s| s.trim().to_string())
                            .collect::<Vec<_>>()
                            .as_slice(),
                        &resulting_columns,
                    )
                };

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
                    .with_label_values(&["incremental"])
                    .inc_by(row_count as f64);
            } else {
                debug!(model = %name, "Appending incremental update");
                let mut stream = source_df
                    .execute_stream()
                    .await
                    .map_err(|e| TitanError::ExecutionError(e.to_string()))?;
                let mut row_count = 0;

                let mut write_op = delta_table
                    .write(Vec::new())
                    .with_save_mode(deltalake::protocol::SaveMode::Append);

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
                    .with_label_values(&["incremental"])
                    .inc_by(row_count as f64);
            }
        } else {
            info!(model = %name, "Creating initial incremental table at {:?}", table_path);
            let mut stream = source_df
                .execute_stream()
                .await
                .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

            let abs_path = std::fs::canonicalize(&table_path)?;
            let url = url::Url::from_directory_path(abs_path)
                .map_err(|()| TitanError::ValidationError("Invalid path".to_string()))?;

            let log_store = deltalake::logstore::logstore_for(&url, Default::default())
                .map_err(|e| TitanError::DatabaseError(e.to_string()))?;

            let mut writer = deltalake::operations::write::WriteBuilder::new(log_store, None)
                .with_save_mode(deltalake::protocol::SaveMode::ErrorIfExists);

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
