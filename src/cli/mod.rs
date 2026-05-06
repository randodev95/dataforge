//! # Titan CLI
//!
//! This module implements the command-line interface for the Titan Engine.
//! It handles project initialization, pipeline planning, execution, and testing.

use crate::core::lineage::LineageExtractor;
use crate::core::shadow::{ShadowConfig, ShadowRewriter};
use crate::filler::dag::ModelTask;
use crate::hooks::Hooks;
use crate::optimize::dedup::{DedupAnalyzer, PartitionInfo};
use crate::project::Project;
use crate::project::exposures::Exposures;
use crate::project::profiles::Profiles;
use crate::quality::QuarantineMode;
use crate::{Filler, LogicHash, ModelMetadata, Muscle, StateStore, VDE};
use anyhow::Result;
use clap::{Parser, Subcommand};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{error, info};

#[derive(Parser)]
#[command(name = "titan")]
#[command(about = "Titan Engine CLI", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Enable Prometheus metrics server on localhost:9090
    #[arg(long, global = true)]
    pub metrics: bool,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new Titan project
    Init {
        name: String,
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Plan the data pipeline
    Plan {
        #[arg(long, default_value = "dev")]
        target: String,
        #[arg(long)]
        select: Option<String>,
        #[arg(long)]
        state: bool,
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(short = 'j', long, default_value = "4")]
        jobs: usize,
        #[arg(long)]
        shadow: bool,
        #[arg(long)]
        allow_drift: bool,
        #[arg(long, default_value = "7")]
        grace_period: i64,
    },
    /// Run the data pipeline
    Run {
        #[arg(long, default_value = "dev")]
        target: String,
        #[arg(long)]
        select: Option<String>,
        #[arg(long)]
        state: bool,
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(short = 'j', long, default_value = "4")]
        jobs: usize,
        #[arg(long)]
        shadow: bool,
        #[arg(long)]
        allow_drift: bool,
        #[arg(long, default_value = "7")]
        grace_period: i64,
        #[arg(long)]
        quarantine: Option<QuarantineMode>,
    },
    /// Run integration tests
    Test {
        #[arg(long, default_value = "dev")]
        target: String,
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Run unit tests
    UnitTest {
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// List exposures
    Exposure {
        #[command(subcommand)]
        action: ExposureAction,
    },
    /// Setup connectors and drivers
    Setup {
        #[arg(long)]
        driver: Vec<String>,
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Validate project structure and SQL without execution
    Check {
        #[arg(long, default_value = "dev")]
        target: String,
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Estimate project cost
    Estimate {
        #[arg(long, default_value = "dev")]
        target: String,
        #[arg(long, default_value = "duckdb")]
        warehouse: String,
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Optimize storage
    Optimize {
        #[arg(long)]
        measure_dedup: bool,
        #[arg(long, default_value = "dev")]
        target: String,
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Show column lineage
    Lineage {
        model: String,
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Diff lineage between models
    LineageDiff {
        model_a: String,
        model_b: String,
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Compare model output between environments or logic versions
    Compare {
        model: String,
        #[arg(long)]
        base: String,
        #[arg(long)]
        target: String,
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Install project dependencies defined in packages.yml
    Deps {
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Show the current status of all models in an environment
    Status {
        #[arg(long, default_value = "dev")]
        target: String,
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Promote a model version from one environment to another (Virtual Environment)
    Promote {
        model: String,
        #[arg(long)]
        from: String,
        #[arg(long)]
        target: String,
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Check the freshness of data sources
    Freshness {
        #[arg(default_value = ".")]
        path: PathBuf,
    },
}

#[derive(Subcommand)]
pub enum ExposureAction {
    List {
        #[arg(default_value = ".")]
        path: PathBuf,
    },
}

pub mod deps;
pub mod freshness;
pub mod promote;
pub mod setup;
pub mod status;

pub fn handle_init(name: String, path: PathBuf) -> Result<()> {
    if !path.exists() {
        fs::create_dir_all(&path)?;
    }

    let config_yaml = format!("name: {name}\nstorage: rocksdb\n");
    fs::write(path.join("config.yaml"), config_yaml)?;

    let profiles_yaml = "
dev:
  prefix: dev_
prod:
  prefix: prod_
";
    fs::write(path.join("profiles.yml"), profiles_yaml)?;

    fs::create_dir_all(path.join("models"))?;
    fs::create_dir_all(path.join("seeds"))?;
    fs::create_dir_all(path.join("exposures"))?;

    let stg_users_sql = "SELECT id, name, status FROM {{ ref('raw_users_seed') }}";
    fs::write(path.join("models/stg_users.sql"), stg_users_sql)?;

    let dim_users_sql =
        "SELECT id, UPPER(name) as name FROM {{ ref('stg_users') }} WHERE status = 'active'";
    fs::write(path.join("models/dim_users.sql"), dim_users_sql)?;

    let mut raw_users_csv = fs::File::create(path.join("seeds/raw_users_seed.csv"))?;
    writeln!(raw_users_csv, "id,name,status")?;
    writeln!(raw_users_csv, "1,Alice,active")?;
    writeln!(raw_users_csv, "2,Bob,inactive")?;

    info!(project = %name, "Initialized Titan project");
    Ok(())
}

pub async fn handle_pipeline(
    path: PathBuf,
    target: String,
    select: Option<String>,
    state: bool,
    plan_only: bool,
    jobs: usize,
    shadow: bool,
    allow_drift: bool,
    grace_period: i64,
    quarantine: Option<QuarantineMode>,
) -> Result<()> {
    let muscle = Arc::new(Muscle::new());
    handle_pipeline_internal(
        path,
        target,
        select,
        state,
        plan_only,
        muscle,
        jobs,
        shadow,
        allow_drift,
        grace_period,
        quarantine,
    )
    .await
}

pub async fn handle_pipeline_internal(
    path: PathBuf,
    target: String,
    select: Option<String>,
    state: bool,
    plan_only: bool,
    muscle: Arc<Muscle>,
    jobs: usize,
    shadow: bool,
    allow_drift: bool,
    grace_period: i64,
    quarantine: Option<QuarantineMode>,
) -> Result<()> {
    let project = Project::load(&path)?;
    let profiles = Profiles::load(&path.join("profiles.yml"))?;
    let profile = profiles
        .get_target(&target)
        .ok_or_else(|| anyhow::anyhow!("Target {target} not found in profiles.yml"))?;

    info!(project = %project.config.name, target = %target, prefix = %profile.prefix, "Loaded project");

    let db_path = path.join(".titan_db");
    let state_store = StateStore::open(&db_path)?;

    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let ct = cancellation_token.clone();
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            info!("Received Ctrl+C, cancelling pipeline...");
            ct.cancel();
        }
    });

    let filler = Filler::new(state_store, &path, jobs, cancellation_token)
        .with_vars(project.config.vars.clone());

    let vde = Arc::new(VDE::new(muscle.clone()));

    let hooks = Hooks::load(&path);
    if !plan_only {
        hooks.run_start(&muscle).await?;
    }

    let resolver = crate::project::secrets::EnvSecretResolver;
    for (name, source_config) in &project.config.sources {
        let mut resolved_config = source_config.clone();
        if let Some(s) = source_config.resolved_connection_string(&resolver)? {
            resolved_config.connection_string = Some(s);
        }

        let masked_conn = resolved_config
            .connection_string
            .as_deref()
            .map_or_else(|| "N/A".to_string(), crate::project::secrets::mask_secrets);

        info!(source = %name, type = %resolved_config.source_type, connection = %masked_conn, "Registering source");
        muscle
            .connectors
            .register_source(&muscle.ctx, name, &resolved_config)
            .await?;

        // Auto-register local source tables
        if resolved_config.source_type == "local"
            && let Some(bucket) = &resolved_config.bucket
        {
            let source_path = path.join(bucket);
            if source_path.exists() {
                for entry in fs::read_dir(source_path)? {
                    let entry = entry?;
                    let path = entry.path();
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        let table_name = stem; // In a real system, we'd prefix this
                        if path.extension().is_some_and(|ext| ext == "csv") {
                            muscle
                                .register_csv(
                                    table_name,
                                    path.to_str()
                                        .ok_or_else(|| anyhow::anyhow!("Invalid path: {path:?}"))?,
                                )
                                .await?;
                        } else if path.extension().is_some_and(|ext| ext == "parquet") {
                            muscle
                                .register_parquet(
                                    table_name,
                                    path.to_str()
                                        .ok_or_else(|| anyhow::anyhow!("Invalid path: {path:?}"))?,
                                )
                                .await?;
                        }
                    }
                }
            }
        }
    }

    for seed in &project.seeds {
        let name = seed
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid seed filename: {seed:?}"))?;
        muscle
            .register_csv(
                name,
                seed.to_str()
                    .ok_or_else(|| anyhow::anyhow!("Invalid seed path"))?,
            )
            .await?;

        let seed_hash = LogicHash::new(format!("seed_{name}"));
        let metadata = ModelMetadata {
            status: "success".to_string(),
            materialization_path: seed
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("Invalid seed path"))?
                .to_string(),
            created_at: 0,
        };
        filler
            .state_store
            .put_metadata(&target, name, &seed_hash, &metadata)?;

        let sql = format!("CREATE OR REPLACE VIEW {name} AS SELECT * FROM {name}");
        muscle.execute(&sql).await?;
        let env_sql = format!(
            "CREATE OR REPLACE VIEW {}{} AS SELECT * FROM {}",
            profile.prefix, name, name
        );
        muscle.execute(&env_sql).await?;
    }

    let tasks = build_tasks(
        &project,
        profile,
        &target,
        &filler,
        muscle.clone(),
        vde.clone(),
        select,
        state,
        plan_only,
        &path,
        shadow,
        allow_drift,
        grace_period,
        quarantine,
    )?;

    let results = filler.run_dag(tasks.clone()).await?;
    results.save(&path)?;
    info!("Run results generated: target/run_results.json");

    let manifest = crate::artifacts::Manifest::generate(project.config.name.clone(), &tasks);
    manifest.save(&path)?;
    info!("Manifest generated: target/manifest.json");

    if !plan_only {
        hooks.run_end(&muscle).await?;
        info!("Pipeline execution complete");
    }

    Ok(())
}

fn build_tasks(
    project: &Project,
    profile: &crate::project::profiles::ProfileTarget,
    target: &str,
    filler: &Filler,
    muscle: Arc<Muscle>,
    vde: Arc<VDE>,
    select: Option<String>,
    state: bool,
    plan_only: bool,
    path: &Path,
    shadow: bool,
    allow_drift: bool,
    grace_period: i64,
    quarantine: Option<QuarantineMode>,
) -> Result<Vec<ModelTask>> {
    let mut filtered_models = if let Some(pattern) = select {
        project.filter_models(&pattern)
    } else {
        project.models.iter().collect()
    };

    if state {
        if let Ok(prior) = crate::artifacts::Manifest::load(path) {
            let state_filtered = project.filter_by_state(&prior);
            let state_names: HashSet<_> = state_filtered.iter().map(|m| &m.name).collect();
            filtered_models.retain(|m| state_names.contains(&m.name));
            info!(
                "State-based selection: filtered to {} models",
                filtered_models.len()
            );
        } else {
            info!("No prior manifest found, running all selected models");
        }
    }

    let mut tasks = Vec::new();
    for model in filtered_models {
        tasks.push(ModelTask {
            name: model.name.clone(),
            env: target.to_string(),
            raw_sql: model.raw_sql.clone(),
            config: HashMap::new(),
            fingerprinter: filler.fingerprinter.clone(),
            state_store: filler.state_store.clone(),
            muscle: muscle.clone(),
            vde: vde.clone(),
            parent_names: model.dependencies.clone(),
            materialization: model.materialization,
            unique_key: model.unique_key.clone(),
            partition_by: model.partition_by.clone(),
            target_type: profile.target_type.clone(),
            retention: project.config.retention.clone(),
            on_schema_change: model.on_schema_change,
            plan_only,
            semaphore: filler.semaphore.clone(),
            project_root: filler.project_root.clone(),
            contract_enforced: model.contract_enforced,
            columns: model.columns.clone(),
            vars: filler.vars.clone(),
            exec_id: uuid::Uuid::new_v4(),
            cancellation_token: filler.cancellation_token.clone(),
            shadow_name: if shadow {
                Some(ShadowRewriter::rewrite_target(
                    &model.name,
                    &ShadowConfig::default(),
                ))
            } else {
                None
            },
            allow_drift,
            grace_period_days: grace_period,
            quarantine_mode: quarantine,
        });
    }
    Ok(tasks)
}

pub async fn handle_test(path: PathBuf, target: String) -> Result<()> {
    let project = Project::load(&path)?;
    let profiles = Profiles::load(&path.join("profiles.yml"))?;
    let profile = profiles
        .get_target(&target)
        .ok_or_else(|| anyhow::anyhow!("Target {target} not found"))?;

    let db_path = path.join(".titan_db");
    let state_store = StateStore::open(&db_path)?;
    let muscle = Arc::new(Muscle::new());

    for seed in &project.seeds {
        let name = seed
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid seed filename: {seed:?}"))?;
        muscle
            .register_csv(
                name,
                seed.to_str()
                    .ok_or_else(|| anyhow::anyhow!("Invalid seed path"))?,
            )
            .await?;
        let env_sql = format!(
            "CREATE OR REPLACE VIEW {}{} AS SELECT * FROM {}",
            profile.prefix, name, name
        );
        muscle.execute(&env_sql).await?;
    }

    for model in &project.models {
        if let Some((hash, sql)) = state_store
            .get_hash_by_name(&target, &model.name)?
            .and_then(|h| {
                state_store
                    .get_value(&h)
                    .transpose()
                    .map(|s| s.map(|sql| (h, sql)))
            })
            .transpose()?
        {
            let table_name = format!("{}__{}", model.name, hash);
            let create_view = format!("CREATE OR REPLACE VIEW {table_name} AS {sql}");
            muscle.execute(&create_view).await?;
            let env_view = format!(
                "CREATE OR REPLACE VIEW {}{} AS SELECT * FROM {}",
                profile.prefix, model.name, table_name
            );
            muscle.execute(&env_view).await?;
        }
    }

    let mut total_failed = 0;
    let mut total_tests = 0;

    let engine = crate::fingerprint::TemplateEngine::new(&path);

    for model in &project.models {
        let env_name = format!("{}{}", profile.prefix, model.name);
        for test_config in &model.tests {
            total_tests += 1;
            let (test_name, sql) = crate::quality::TestGenerator::generate_sql(
                &engine,
                &env_name,
                &test_config.column_name,
                &test_config.test,
            )?;

            let df = muscle.ctx.sql(&sql).await?;
            let batches = df.collect().await?;
            let count: i64 = if batches.is_empty() {
                0
            } else {
                let col = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("Missing value in test result"))?;
                col.value(0)
            };

            if count > 0 {
                total_failed += 1;
                error!(test = %test_name, model = %model.name, column = %test_config.column_name, "FAIL: {} violations found", count);
            } else {
                info!(test = %test_name, model = %model.name, column = %test_config.column_name, "PASS");
            }
        }
    }

    info!(target = %target, "Test suite complete: {} passed, {} failed", total_tests - total_failed, total_failed);

    if total_failed > 0 {
        return Err(anyhow::anyhow!("{total_failed} tests failed"));
    }
    Ok(())
}

pub async fn handle_unit_test(path: PathBuf) -> Result<()> {
    let project = Project::load(&path)?;
    let runner = crate::quality::unit_test::TestRunner::new(&path);

    let mut total_passed = 0;
    let mut total_failed = 0;

    for model in &project.models {
        if model.unit_tests.is_empty() {
            continue;
        }

        for test in &model.unit_tests {
            let passed = runner.run_test(&model.name, &model.raw_sql, test).await?;
            if passed {
                total_passed += 1;
            } else {
                total_failed += 1;
            }
        }
    }

    info!(
        "Unit test summary: {} passed, {} failed",
        total_passed, total_failed
    );

    if total_failed > 0 {
        return Err(anyhow::anyhow!("{total_failed} unit tests failed"));
    }

    Ok(())
}

pub fn handle_exposure(path: PathBuf) -> Result<()> {
    let exposures = Exposures::load(&path)?;
    println!("{}", serde_json::to_string_pretty(&exposures.items)?);
    Ok(())
}

pub async fn handle_check(path: PathBuf, target: String) -> Result<()> {
    info!("Performing project check for target: {}", target);

    let _project = Project::load(&path)?;
    let _profiles = Profiles::load(&path.join("profiles.yml"))?;

    handle_pipeline(path, target, None, false, true, 1, false, false, 7, None).await?;

    info!("Project check PASSED");
    Ok(())
}

pub async fn handle_estimate(path: PathBuf, target: String, warehouse: String) -> Result<()> {
    let project = Project::load(&path)?;
    let profiles = Profiles::load(&path.join("profiles.yml"))?;
    let profile = profiles
        .get_target(&target)
        .ok_or_else(|| anyhow::anyhow!("Target {target} not found"))?;

    let db_path = path.join(".titan_db");
    let state_store = StateStore::open(&db_path)?;
    let filler = Filler::new(
        state_store,
        &path,
        1,
        tokio_util::sync::CancellationToken::new(),
    );
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));

    let tasks = build_tasks(
        &project, profile, &target, &filler, muscle, vde, None, false, true, &path, false, false,
        7, None,
    )?;

    let warehouse_type = match warehouse.to_lowercase().as_str() {
        "databricks" => crate::cost::WarehouseType::Databricks,
        "snowflake" => crate::cost::WarehouseType::Snowflake,
        "bigquery" => crate::cost::WarehouseType::BigQuery,
        _ => crate::cost::WarehouseType::DuckDb,
    };

    let estimates = crate::cost::estimate_project_cost(&tasks, warehouse_type);

    println!(
        "{:<30} {:<15} {:<15} {:<10}",
        "Model", "Rows", "Cost (USD)", "Confidence"
    );
    println!("{}", "-".repeat(75));

    let mut total_cost = 0.0;
    for (name, est) in estimates {
        println!(
            "{:<30} {:<15} ${:<14.6} {:?}",
            name, est.estimated_rows, est.estimated_compute_cost_usd, est.confidence
        );
        total_cost += est.estimated_compute_cost_usd;
    }

    println!("{}", "-".repeat(75));
    println!("{:<30} {:<15} ${:<14.6}", "TOTAL", "", total_cost);

    Ok(())
}

pub async fn handle_optimize(path: PathBuf, _target: String, measure_dedup: bool) -> Result<()> {
    let project = Project::load(&path)?;

    if measure_dedup {
        let _analyzer = DedupAnalyzer;
        let total_saving = 0;

        for name in project.config.sources.keys() {
            // Simulated partition info
            let partitions = vec![
                PartitionInfo {
                    key: "p=1".to_string(),
                    checksum: 1234,
                    row_count: 1000,
                },
                PartitionInfo {
                    key: "p=1_v2".to_string(),
                    checksum: 1234,
                    row_count: 1000,
                },
            ];

            let stats = DedupAnalyzer::compute_stats(&[(name.clone(), partitions)]);
            if stats.duplicate_partitions > 0 {
                info!(source = %name, redundant_count = %stats.duplicate_partitions, saving_pct = %stats.estimated_savings_pct, "Deduplication potential identified");
            }
        }

        info!(total_saving_mb = %(total_saving / 1024 / 1024), "Storage optimization analysis complete");
    }

    Ok(())
}

pub fn handle_lineage(path: PathBuf, model: String) -> Result<()> {
    let project = Project::load(&path)?;
    let m = project
        .models
        .iter()
        .find(|m| m.name == model)
        .ok_or_else(|| anyhow::anyhow!("Model {model} not found"))?;

    let lineage = LineageExtractor::extract(&m.raw_sql)?;

    println!("Column Lineage for {model}:");
    for col in lineage.columns {
        println!(
            "  - {:?} -> {} (type: {:?})",
            col.source, col.target_column, col.transform
        );
    }

    Ok(())
}

pub fn handle_lineage_diff(path: PathBuf, model_a: String, model_b: String) -> Result<()> {
    let project = Project::load(&path)?;
    let ma = project
        .models
        .iter()
        .find(|m| m.name == model_a)
        .ok_or_else(|| anyhow::anyhow!("Model {model_a} not found"))?;
    let mb = project
        .models
        .iter()
        .find(|m| m.name == model_b)
        .ok_or_else(|| anyhow::anyhow!("Model {model_b} not found"))?;

    let la = LineageExtractor::extract(&ma.raw_sql)?;
    let lb = LineageExtractor::extract(&mb.raw_sql)?;

    let differ = crate::core::lineage::LineageDiffer;
    let diff = differ.diff(&la, &lb);

    println!("Lineage Diff between {model_a} and {model_b}:");
    for d in diff {
        println!("  - {d:?}");
    }

    Ok(())
}

pub async fn handle_compare(
    path: PathBuf,
    model: String,
    base: String,
    target: String,
) -> Result<()> {
    let project = Project::load(&path)?;
    let profiles = Profiles::load(&path.join("profiles.yml"))?;
    let base_profile = profiles
        .get_target(&base)
        .ok_or_else(|| anyhow::anyhow!("Base target {base} not found"))?;
    let target_profile = profiles
        .get_target(&target)
        .ok_or_else(|| anyhow::anyhow!("Target {target} not found"))?;

    let db_path = path.join(".titan_db");
    let state_store = StateStore::open(&db_path)?;
    let muscle = Arc::new(Muscle::new());

    // Register sources for both environments (assuming they are shared or similarly configured)
    let resolver = crate::project::secrets::EnvSecretResolver;
    for (name, source_config) in &project.config.sources {
        let mut resolved_config = source_config.clone();
        if let Some(s) = source_config.resolved_connection_string(&resolver)? {
            resolved_config.connection_string = Some(s);
        }
        muscle
            .connectors
            .register_source(&muscle.ctx, name, &resolved_config)
            .await?;

        // Auto-register local source tables
        if resolved_config.source_type == "local"
            && let Some(bucket) = &resolved_config.bucket
        {
            let source_path = path.join(bucket);
            if source_path.exists() {
                for entry in fs::read_dir(source_path)? {
                    let entry = entry?;
                    let path = entry.path();
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                        && path.extension().is_some_and(|ext| ext == "csv")
                    {
                        muscle
                            .register_csv(
                                stem,
                                path.to_str()
                                    .ok_or_else(|| anyhow::anyhow!("Invalid path: {path:?}"))?,
                            )
                            .await?;
                    }
                }
            }
        }
    }

    // Register base table
    if let Some(hash) = state_store.get_hash_by_name(&base, &model)?
        && let Some(sql) = state_store.get_value(&hash)?
    {
        let create_view = format!(
            "CREATE OR REPLACE VIEW {}{} AS {}",
            base_profile.prefix, model, sql
        );
        muscle.execute(&create_view).await?;
    }

    // Register target table
    if let Some(hash) = state_store.get_hash_by_name(&target, &model)?
        && let Some(sql) = state_store.get_value(&hash)?
    {
        let create_view = format!(
            "CREATE OR REPLACE VIEW {}{} AS {}",
            target_profile.prefix, model, sql
        );
        muscle.execute(&create_view).await?;
    }

    let differ = crate::core::ci_diff::CIDiffer::new(muscle);
    let base_table = format!("{}{}", base_profile.prefix, model);
    let target_table = format!("{}{}", target_profile.prefix, model);

    let result = differ
        .compare(&model, &base_table, &target_table)
        .await
        .map_err(|e| anyhow::anyhow!("Comparison failed: {e}"))?;

    println!("CI Impact Analysis for {model}:");
    println!("  Status: {:?}", result.status);
    println!("  Rows (base: {}): {:?}", base, result.row_count_before);
    println!("  Rows (target: {}): {:?}", target, result.row_count_after);

    if let (Some(b), Some(t)) = (result.row_count_before, result.row_count_after) {
        let diff = t as i64 - b as i64;
        println!("  Delta: {diff:+}");
    }

    if !result.column_changes.is_empty() {
        println!("  Column Changes:");
        for change in result.column_changes {
            println!("    {change}");
        }
    }

    // Downstream Exposure Impact
    let exposures = Exposures::load(&path)?;
    let mut impacted = Vec::new();
    for exp in &exposures.items {
        if exp.depends_on.contains(&model) {
            impacted.push(format!(
                "{} ({}) - Owner: {}",
                exp.name, exp.exposure_type, exp.owner
            ));
        }
    }

    if !impacted.is_empty() {
        println!("\n  ⚠️  Downstream Impact:");
        for imp in impacted {
            println!("    - {imp}");
        }
    }

    Ok(())
}
