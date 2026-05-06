//! # Pipeline Execution DAG
//!
//! This module implements the core execution engine for Titan pipelines,
//! using a task-based DAG approach with concurrency control and state persistence.

use crate::core::drift::ColumnInfo;
use crate::error::{Result, TitanError};
use crate::filler::TableGuard;
use crate::filler::grace_period::GracePeriodManager;
use crate::fingerprint::{Fingerprinter, LogicHash};
use crate::materialize::{Materialization, Materializer};
use crate::{ModelMetadata, Muscle, StateStore, VDE};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use minijinja::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Clone)]
pub struct ModelTask {
    pub name: String,
    pub env: String,
    pub raw_sql: String,
    pub config: HashMap<String, Value>,
    pub fingerprinter: Arc<Fingerprinter>,
    pub state_store: Arc<StateStore>,
    pub muscle: Arc<Muscle>,
    pub vde: Arc<VDE>,
    pub parent_names: Vec<String>,
    pub materialization: crate::materialize::Materialization,
    pub unique_key: Option<String>,
    pub partition_by: Option<String>,
    pub target_type: String,
    pub retention: Option<crate::project::RetentionConfig>,
    pub on_schema_change: crate::project::OnSchemaChange,
    pub plan_only: bool,
    pub semaphore: Arc<Semaphore>,
    pub project_root: std::path::PathBuf,
    pub contract_enforced: bool,
    pub columns: Vec<crate::project::ModelColumn>,
    pub vars: HashMap<String, serde_yml::Value>,
    pub exec_id: Uuid,
    pub cancellation_token: CancellationToken,
    pub shadow_name: Option<String>,
    pub allow_drift: bool,
    pub grace_period_days: i64,
    pub quarantine_mode: Option<crate::quality::QuarantineMode>,
}

impl ModelTask {
    pub async fn resolve_parents(&self) -> Result<Vec<LogicHash>> {
        let mut parent_hashes = Vec::with_capacity(self.parent_names.len());

        let mut futures = Vec::with_capacity(self.parent_names.len());
        for name in &self.parent_names {
            let state_store = self.state_store.clone();
            let env = self.env.clone();
            let name = name.clone();
            let plan_only = self.plan_only;
            let muscle = self.muscle.clone();
            futures.push(tokio::task::spawn(async move {
                match state_store.get_hash_by_name(&env, &name) {
                    Ok(Some(hash)) => Ok(hash),
                    Ok(None) if plan_only => Ok(LogicHash::new("planned_parent".to_string())),
                    Ok(None) => {
                        // Check if it's a source or seed registered in DataFusion
                        if muscle.ctx.table_exist(&name).unwrap_or(false) {
                            Ok(LogicHash::new(format!("source_{name}")))
                        } else {
                            Err(TitanError::DependencyNotFound(name, env))
                        }
                    }
                    Err(e) => Err(TitanError::StateError(e.to_string())),
                }
            }));
        }

        for f in futures {
            parent_hashes.push(
                f.await
                    .map_err(|e| TitanError::ExecutionError(e.to_string()))??,
            );
        }

        Ok(parent_hashes)
    }

    pub fn save_metadata(&self, hash: &LogicHash, metadata: &ModelMetadata) -> Result<()> {
        self.state_store
            .put_metadata(&self.env, &self.name, hash, metadata)
            .map_err(|e| TitanError::StateError(e.to_string()))
    }

    fn validate_contract(&self, df_schema: &datafusion::common::DFSchema) -> Result<()> {
        if !self.contract_enforced {
            return Ok(());
        }

        for col in &self.columns {
            let field = df_schema.field_with_name(None, &col.name).map_err(|_| {
                TitanError::ValidationError(format!(
                    "Contract violation: model {} is missing column {}",
                    self.name, col.name
                ))
            })?;

            if let Some(expected_type) = &col.data_type {
                let actual_type = format!("{:?}", field.data_type()).to_lowercase();
                if !actual_type.contains(&expected_type.to_lowercase()) {
                    return Err(TitanError::ValidationError(format!(
                        "Contract violation: model {} column {} has type {}, expected {}",
                        self.name, col.name, actual_type, expected_type
                    )));
                }
            }
        }
        Ok(())
    }

    pub async fn execute(&self) -> Result<(LogicHash, u128)> {
        let start_time = std::time::Instant::now();

        tokio::select! {
            () = self.cancellation_token.cancelled() => {
                return Err(TitanError::ExecutionError("Task cancelled".to_string()));
            }
            permit = self.semaphore.acquire() => {
                let _permit = permit.map_err(|e| TitanError::ExecutionError(e.to_string()))?;
                self.execute_internal(start_time).await
            }
        }
    }

    async fn execute_internal(&self, start_time: std::time::Instant) -> Result<(LogicHash, u128)> {
        let parent_hashes = self.resolve_parents().await?;
        let is_inc_run = self.materialization == Materialization::Incremental
            && self
                .state_store
                .get_metadata_by_name(&self.env, &self.name)
                .unwrap_or(None)
                .is_some();

        let (titan_sql, logic_hash) = self.fingerprinter.fingerprint(
            &self.raw_sql,
            &self.env,
            &self.config,
            &parent_hashes,
            &self.name,
            is_inc_run,
            &self.vars,
        )?;

        let df = self
            .muscle
            .ctx
            .sql(titan_sql.as_str())
            .await
            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

        self.validate_contract(df.schema())?;

        if self.plan_only {
            info!(model = %self.name, hash = %logic_hash.as_str(), "Planned");
            return Ok((logic_hash, 0));
        }

        // Reliability: Schema Drift & Grace Period
        if !self.plan_only {
            self.handle_reliability(df.schema().as_ref()).await?;
        }

        let strategy = crate::materialize::get_materializer(
            &self.materialization,
            self.muscle.clone(),
            self.vde.clone(),
            self.unique_key.clone(),
            self.partition_by.clone(),
            &self.target_type,
            self.retention.clone(),
            self.on_schema_change,
            self.project_root.clone(),
            self.columns.clone(),
        );

        let target_name = self.shadow_name.as_ref().unwrap_or(&self.name);

        let mut final_sql = titan_sql.clone();
        if let Some(mode) = self.quarantine_mode {
            let source_view = format!("q_source_{}_{}", self.name, self.exec_id);
            self.muscle
                .ctx
                .register_table(
                    &source_view,
                    self.muscle
                        .ctx
                        .sql(final_sql.as_str())
                        .await
                        .map_err(|e| TitanError::ExecutionError(e.to_string()))?
                        .into_view(),
                )
                .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

            if let Some(plan) = crate::quality::quarantine::QuarantineCompiler::compile(
                &self.name,
                &self.columns,
                mode,
                source_view,
            )? {
                info!(model = %self.name, mode = ?mode, "Applying row quarantine");
                for stmt in plan.statements {
                    if stmt.role == "quarantine" {
                        // In a real warehouse, this would be a CTAS
                        // Here we'll just register it as a view for demonstration
                        let df = self
                            .muscle
                            .ctx
                            .sql(&stmt.sql)
                            .await
                            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;
                        info!(model = %self.name, "Isolated bad rows to {}", plan.quarantine_table);
                        self.muscle
                            .ctx
                            .register_table(&plan.quarantine_table, df.into_view())
                            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;
                    } else if stmt.role == "valid" {
                        final_sql = crate::core::sql::TitanSQL::new(stmt.sql);
                    }
                }
            }
        }

        // Apply masking
        let masked_cols: Vec<String> = self
            .columns
            .iter()
            .map(|c| {
                if let Some(strategy) = c.masking {
                    format!("{} AS {}", strategy.apply(&c.name), c.name)
                } else {
                    c.name.clone()
                }
            })
            .collect();

        if self.columns.iter().any(|c| c.masking.is_some()) {
            info!(model = %self.name, "Applying PII masking policies");
            let masking_sql = format!(
                "SELECT {} FROM ({})",
                masked_cols.join(", "),
                final_sql.as_str()
            );
            final_sql = crate::core::sql::TitanSQL::new(masking_sql);
        }

        strategy
            .materialize(
                &self.env,
                &self.name,
                target_name,
                &logic_hash,
                &self.exec_id,
                final_sql.as_str(),
            )
            .await?;

        let target_name = format!("{}_{}_{}", self.env, self.name, self.exec_id);
        let _guard = TableGuard::new(self.muscle.clone(), &target_name);

        match self.target_type.as_str() {
            "delta" => {
                let table_path = if self.materialization == Materialization::Incremental {
                    self.project_root
                        .join(format!("tables/{}/{}", self.env, self.name))
                } else {
                    self.project_root
                        .join(format!("snapshots/{}/{}", self.env, self.name))
                };
                if table_path.exists() {
                    self.muscle
                        .register_delta(
                            &target_name,
                            table_path.to_str().ok_or_else(|| {
                                TitanError::ValidationError("Invalid path".to_string())
                            })?,
                        )
                        .await?;
                }
            }
            "parquet" => {
                let table_path = self
                    .project_root
                    .join(format!("tables/{}/{}.parquet", self.env, self.name));
                if table_path.exists() {
                    self.muscle
                        .register_parquet(
                            &target_name,
                            table_path.to_str().ok_or_else(|| {
                                TitanError::ValidationError("Invalid path".to_string())
                            })?,
                        )
                        .await?;
                }
            }
            "csv" => {
                let table_path = self
                    .project_root
                    .join(format!("tables/{}/{}.csv", self.env, self.name));
                if table_path.exists() {
                    self.muscle
                        .register_csv(
                            &target_name,
                            table_path.to_str().ok_or_else(|| {
                                TitanError::ValidationError("Invalid path".to_string())
                            })?,
                        )
                        .await?;
                }
            }
            _ => {
                self.muscle
                    .ctx
                    .register_table(&target_name, df.into_view())
                    .map_err(|e| TitanError::ExecutionError(e.to_string()))?;
            }
        }

        // Also register a "stable" name for downstream tasks if they are in the same context
        let stable_name = format!("{}_{}", self.env, self.name);
        let df = self
            .muscle
            .ctx
            .table(&target_name)
            .await
            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;
        self.muscle
            .ctx
            .register_table(&stable_name, df.into_view())
            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

        let metadata = ModelMetadata {
            status: "success".to_string(),
            materialization_path: format!("{}/{}", self.env, self.name),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        self.save_metadata(&logic_hash, &metadata)?;
        self.state_store
            .put_value(&logic_hash, titan_sql.as_str().to_string())
            .map_err(|e| TitanError::StateError(e.to_string()))?;

        let duration = start_time.elapsed().as_millis();
        info!(model = %self.name, hash = %logic_hash.as_str(), duration_ms = %duration, "Executed");

        // Audit Logging
        let audit_entry = crate::core::audit::AuditEntry {
            timestamp: metadata.created_at,
            env: self.env.clone(),
            model: self.name.clone(),
            hash: logic_hash.as_str().to_string(),
            status: "success".to_string(),
            rows_affected: 0,
            duration_ms: duration as u64,
        };
        let _ = crate::core::audit::AuditLogger::log(&self.project_root, audit_entry).await;

        Ok((logic_hash, duration))
    }
}

pub struct Filler {
    pub state_store: Arc<StateStore>,
    pub fingerprinter: Arc<Fingerprinter>,
    pub semaphore: Arc<Semaphore>,
    pub project_root: std::path::PathBuf,
    pub vars: HashMap<String, serde_yml::Value>,
    pub cancellation_token: CancellationToken,
}

impl Filler {
    pub fn new(
        state_store: StateStore,
        project_root: &std::path::Path,
        concurrency: usize,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            state_store: Arc::new(state_store),
            fingerprinter: Arc::new(Fingerprinter::new(project_root)),
            semaphore: Arc::new(Semaphore::new(concurrency)),
            project_root: project_root.to_path_buf(),
            vars: HashMap::new(),
            cancellation_token,
        }
    }

    pub fn with_vars(mut self, vars: HashMap<String, serde_yml::Value>) -> Self {
        self.vars = vars;
        self
    }

    pub async fn run_dag(&self, tasks: Vec<ModelTask>) -> Result<crate::artifacts::RunResults> {
        let mut completed = HashSet::new();
        let mut in_progress = FuturesUnordered::new();
        let mut results = Vec::new();
        let mut pending: HashSet<&str> = tasks.iter().map(|t| t.name.as_str()).collect();
        let name_to_task: HashMap<&str, &ModelTask> =
            tasks.iter().map(|t| (t.name.as_str(), t)).collect();

        loop {
            let ready_tasks: Vec<&str> = pending
                .iter()
                .filter(|name| {
                    let task = name_to_task[**name];
                    task.parent_names.iter().all(|p| {
                        !name_to_task.contains_key(p.as_str()) || completed.contains(p.as_str())
                    })
                })
                .copied()
                .collect();

            for name in ready_tasks {
                pending.remove(name);
                let task = name_to_task[name];
                in_progress.push(async move { (name, task.execute().await) });
            }

            if in_progress.is_empty() {
                if !pending.is_empty() {
                    return Err(TitanError::CircularDependency(
                        "Unresolved dependencies remained".to_string(),
                    ));
                }
                break;
            }

            tokio::select! {
                () = self.cancellation_token.cancelled() => {
                    info!("Cancellation received, shutting down DAG runner");
                    return Err(TitanError::ExecutionError("Run cancelled by user".to_string()));
                }
                next = in_progress.next() => {
                    if let Some((name, result)) = next {
                        match result {
                            Ok((_hash, duration)) => {
                                completed.insert(name.to_string());
                                results.push(crate::artifacts::run_results::RunResult {
                                    name: name.to_string(),
                                    status: "success".to_string(),
                                    duration_ms: duration,
                                    rows_affected: None,
                                });
                            }
                            Err(e) => {
                                error!(model = %name, error = %e, "Task failed");
                                return Err(e);
                            }
                        }
                    }
                }
            }
        }

        Ok(crate::artifacts::RunResults {
            generated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            results,
        })
    }
}

impl ModelTask {
    async fn handle_reliability(
        &self,
        source_schema: &datafusion::arrow::datatypes::Schema,
    ) -> Result<()> {
        let dialect = crate::core::get_dialect(&self.target_type);
        let _target_name = self.shadow_name.as_ref().unwrap_or(&self.name);

        // 1. Convert source schema to ColumnInfo
        let _source_columns: Vec<ColumnInfo> = source_schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().clone(),
                data_type: dialect.map_type(f.data_type()),
            })
            .collect();

        // 2. Fetch existing grace period records
        let existing_grace = self.state_store.get_grace_periods(&self.env, &self.name)?;

        // 3. Detect drift if the table exists
        if self.shadow_name.is_none() {
            let dropped_columns = Vec::new();

            let result = GracePeriodManager::compute_drops(
                &self.name,
                &dropped_columns,
                &existing_grace,
                self.grace_period_days,
                chrono::Utc::now(),
            );

            // Save new grace period records
            let mut final_grace = result.still_in_grace;
            final_grace.extend(result.new_records);
            self.state_store
                .put_grace_periods(&self.env, &self.name, final_grace)?;

            if !result.expired.is_empty() {
                info!(model = %self.name, expired = ?result.expired, "Grace period expired for columns, they will be physically dropped in next run (if supported)");
            }
        }

        Ok(())
    }
}
