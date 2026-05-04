use crate::error::{TitanError, Result};
use async_trait::async_trait;
use crate::execution::Muscle;
use crate::fingerprint::LogicHash;
use std::sync::Arc;
use deltalake::open_table;
use deltalake::arrow::record_batch::RecordBatch;
use tracing::{info, debug};
use std::path::PathBuf;
use datafusion::datasource::TableProvider;
use deltalake::arrow::datatypes::{Schema, Field};

use crate::project::OnSchemaChange;

/// Materializer for Incremental models in Delta Lake.
/// 
/// Supports merging new data based on a unique key or appending if no key is provided.
pub struct IncrementalMaterializer {
    pub muscle: Arc<Muscle>,
    pub unique_key: Option<String>,
    pub on_schema_change: OnSchemaChange,
    pub base_path: PathBuf,
}

#[async_trait]
impl super::Materializer for IncrementalMaterializer {
    async fn materialize(&self, env: &str, name: &str, hash: &LogicHash, exec_id: &uuid::Uuid, rendered_sql: &str) -> Result<()>{
        use crate::metrics::{MATERIALIZATIONS_TOTAL, MATERIALIZATION_LATENCY_SECONDS};
        let _timer = MATERIALIZATION_LATENCY_SECONDS.with_label_values(&["incremental"]).start_timer();
        
        let result = self.materialize_internal(env, name, hash, exec_id, rendered_sql).await;
        
        if result.is_ok() {
            MATERIALIZATIONS_TOTAL.with_label_values(&["incremental", "success"]).inc();
        } else {
            MATERIALIZATIONS_TOTAL.with_label_values(&["incremental", "fail"]).inc();
        }
        
        result
    }
}
impl IncrementalMaterializer {
    async fn materialize_internal(&self, env: &str, name: &str, hash: &LogicHash, exec_id: &uuid::Uuid, rendered_sql: &str) -> Result<()> {
        let table_path = self.base_path.join(format!("tables/{}/{}", env, name));
        std::fs::create_dir_all(&table_path)?;

        let source_view = format!("source_{}_{}_{}", name, hash, exec_id);
        let _source_guard = crate::filler::TableGuard::new(self.muscle.clone(), &source_view);
        
        self.muscle.ctx.register_table(
            &source_view, 
            self.muscle.ctx.sql(rendered_sql).await
                .map_err(|e| TitanError::SqlParseError(e.to_string()))?.into_view()
        ).map_err(|e| TitanError::ExecutionError(e.to_string()))?;
        
        let source_df = self.muscle.ctx.table(&source_view).await
            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

        let log_path = table_path.join("_delta_log");
        let table_exists = log_path.exists();

        if !table_exists {
            info!(model = %name, "Creating initial incremental table at {:?}", table_path);
            let batches = source_df.collect().await.map_err(|e| TitanError::ExecutionError(e.to_string()))?;
            if batches.is_empty() { return Ok(()); }

            let abs_path = std::fs::canonicalize(&table_path)?;
            let url = url::Url::from_directory_path(abs_path).map_err(|_| TitanError::ValidationError("Invalid path".to_string()))?;
            
            let log_store = deltalake::logstore::logstore_for(&url, Default::default())
                .map_err(|e| TitanError::DatabaseError(e.to_string()))?;
            
            deltalake::operations::write::WriteBuilder::new(log_store, None)
                .with_input_batches(batches)
                .with_save_mode(deltalake::protocol::SaveMode::ErrorIfExists)
                .await.map_err(|e| TitanError::DatabaseError(e.to_string()))?;
        } else {
            info!(model = %name, "Performing incremental update for {:?}", table_path);
            let abs_path = std::fs::canonicalize(&table_path)?;
            let url = url::Url::from_directory_path(abs_path).map_err(|_| TitanError::ValidationError("Invalid path".to_string()))?;
            let delta_table = open_table(url.clone()).await.map_err(|e| TitanError::DatabaseError(e.to_string()))?;
            
            if let Some(pk) = &self.unique_key {
                debug!(model = %name, pk = %pk, "Merging incremental update via atomic overwrite");
                
                let log_store = delta_table.log_store();
                let eager_snapshot = deltalake::kernel::EagerSnapshot::try_new(log_store.as_ref(), Default::default(), None).await
                    .map_err(|e| TitanError::DatabaseError(e.to_string()))?;
                let table_provider = deltalake::delta_datafusion::DeltaTableProvider::try_new(
                    eager_snapshot,
                    log_store.clone(),
                    Default::default()
                ).map_err(|e| TitanError::DatabaseError(e.to_string()))?;
                
                let target_schema = table_provider.schema();
                let target_table_name = format!("target_{}_{}_{}", name, hash, exec_id);
                let _target_guard = crate::filler::TableGuard::new(self.muscle.clone(), &target_table_name);

                self.muscle.ctx.register_table(&target_table_name, Arc::new(table_provider))
                    .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

                let mut new_fields = Vec::new();
                for f in source_df.schema().fields() {
                    if target_schema.field_with_name(f.name()).is_err() {
                        new_fields.push(f.clone());
                    }
                }

                let mut combined_fields: Vec<Field> = target_schema.fields().iter().map(|f| (**f).clone()).collect();
                for f in &new_fields {
                    combined_fields.push((**f).clone());
                }
                let new_target_schema = Arc::new(Schema::new(combined_fields));

                let col_names: Vec<String> = target_schema.fields().iter().map(|f| format!("\"{}\"", f.name())).collect();
                let mut all_with_new = col_names.clone();
                for f in &new_fields {
                    all_with_new.push(format!("s.\"{}\" as \"{}\"", f.name(), f.name()));
                }
                let cols_str = all_with_new.join(", ");

                let mut target_cols: Vec<String> = col_names.clone();
                for f in &new_fields {
                    let dt_sql = match f.data_type() {
                        deltalake::arrow::datatypes::DataType::Utf8 => "VARCHAR",
                        deltalake::arrow::datatypes::DataType::Int64 => "BIGINT",
                        deltalake::arrow::datatypes::DataType::Float64 => "DOUBLE",
                        deltalake::arrow::datatypes::DataType::Boolean => "BOOLEAN",
                        _ => "VARCHAR"
                    };
                    target_cols.push(format!("CAST(NULL AS {}) as \"{}\"", dt_sql, f.name()));
                }
                let target_cols_str = target_cols.join(", ");

                let merge_sql = format!(
                    "
                    SELECT {cols} FROM {source} s
                    UNION ALL
                    SELECT {t_cols} FROM {target} t
                    WHERE NOT EXISTS (SELECT 1 FROM {source} s WHERE s.\"{pk}\" = t.\"{pk}\")
                    ",
                    cols = cols_str,
                    t_cols = target_cols_str,
                    source = source_view,
                    target = target_table_name,
                    pk = pk
                );

                let df = self.muscle.ctx.sql(&merge_sql).await.map_err(|e| TitanError::SqlParseError(e.to_string()))?;
                let batches = df.collect().await.map_err(|e| TitanError::ExecutionError(e.to_string()))?;
                
                if batches.is_empty() { return Ok(()); }

                let aligned_batches: Vec<RecordBatch> = batches.into_iter().map(|b| {
                    let mut columns = Vec::new();
                    let b_schema = b.schema();
                    for i in 0..new_target_schema.fields().len() {
                        let target_field = new_target_schema.field(i);
                        let name = target_field.name();
                        let idx = b_schema.index_of(name).expect("Column not found in source");
                        let col = b.column(idx);
                        if col.data_type() != target_field.data_type() {
                            columns.push(deltalake::arrow::compute::cast(col, target_field.data_type()).expect("Cast failed"));
                        } else {
                            columns.push(col.clone());
                        }
                    }
                    RecordBatch::try_new(new_target_schema.clone(), columns).expect("Align failed")
                }).collect();

                let row_count: usize = aligned_batches.iter().map(|b| b.num_rows()).sum();
                let mut write_op = deltalake::DeltaOps(delta_table).write(aligned_batches)
                    .with_save_mode(deltalake::protocol::SaveMode::Overwrite);
                
                if !new_fields.is_empty() {
                    write_op = write_op.with_schema_mode(deltalake::operations::write::SchemaMode::Merge);
                }

                let _: deltalake::DeltaTable = write_op.await.map_err(|e: deltalake::errors::DeltaTableError| TitanError::DatabaseError(e.to_string()))?;
                crate::metrics::ROWS_WRITTEN_TOTAL.with_label_values(&["incremental"]).inc_by(row_count as f64);
            } else {
                debug!(model = %name, "Appending incremental update");
                let batches = source_df.collect().await.map_err(|e| TitanError::ExecutionError(e.to_string()))?;
                if batches.is_empty() { return Ok(()); }
                
                let _: deltalake::DeltaTable = deltalake::DeltaOps(delta_table).write(batches)
                    .with_save_mode(deltalake::protocol::SaveMode::Append)
                    .await.map_err(|e: deltalake::errors::DeltaTableError| TitanError::DatabaseError(e.to_string()))?;
            }
        }

        Ok(())
    }
}
