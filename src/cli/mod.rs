//! # Titan CLI
//! 
//! This module implements the command-line interface for the Titan Engine.
//! It handles project initialization, pipeline planning, execution, and testing.

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use anyhow::Result;
use std::fs;
use std::io::Write;
use crate::project::Project;
use crate::project::profiles::Profiles;
use crate::project::exposures::Exposures;
use crate::hooks::Hooks;
use crate::{Filler, StateStore, Muscle, VDE, LogicHash, ModelMetadata};
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use crate::filler::dag::ModelTask;
use tracing::{info, error};

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
    },
    /// Run tests
    Test {
        #[arg(long, default_value = "dev")]
        target: String,
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
}

#[derive(Subcommand)]
pub enum ExposureAction {
    List {
        #[arg(default_value = ".")]
        path: PathBuf,
    },
}

pub mod setup;

pub fn handle_init(name: String, path: PathBuf) -> Result<()> {
    if !path.exists() {
        fs::create_dir_all(&path)?;
    }

    let config_yaml = format!("name: {}\nstorage: rocksdb\n", name);
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

    let dim_users_sql = "SELECT id, UPPER(name) as name FROM {{ ref('stg_users') }} WHERE status = 'active'";
    fs::write(path.join("models/dim_users.sql"), dim_users_sql)?;

    let mut raw_users_csv = fs::File::create(path.join("seeds/raw_users_seed.csv"))?;
    writeln!(raw_users_csv, "id,name,status")?;
    writeln!(raw_users_csv, "1,Alice,active")?;
    writeln!(raw_users_csv, "2,Bob,inactive")?;
    
    info!(project = %name, "Initialized Titan project");
    Ok(())
}

pub async fn handle_pipeline(path: PathBuf, target: String, select: Option<String>, state: bool, plan_only: bool, jobs: usize) -> Result<()> {
    let muscle = Arc::new(Muscle::new());
    handle_pipeline_internal(path, target, select, state, plan_only, muscle, jobs).await
}

pub async fn handle_pipeline_internal(path: PathBuf, target: String, select: Option<String>, state: bool, plan_only: bool, muscle: Arc<Muscle>, jobs: usize) -> Result<()> {
    let project = Project::load(&path)?;
    let profiles = Profiles::load(&path.join("profiles.yml"))?;
    let profile = profiles.get_target(&target).ok_or_else(|| anyhow::anyhow!("Target {} not found in profiles.yml", target))?;
    
    info!(project = %project.config.name, target = %target, prefix = %profile.prefix, "Loaded project");

    let db_path = path.join(".titan_db");
    let state_store = StateStore::open(&db_path)?;
    
    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let ct = cancellation_token.clone();
    tokio::spawn(async move {
        if let Ok(_) = tokio::signal::ctrl_c().await {
            info!("Received Ctrl+C, cancelling pipeline...");
            ct.cancel();
        }
    });

    let filler = Filler::new(state_store, &path, jobs, cancellation_token)
        .with_vars(project.config.vars.clone()); // Default concurrency of 4
    
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
        
        let masked_conn = resolved_config.connection_string.as_deref()
            .map(crate::project::secrets::mask_secrets)
            .unwrap_or_else(|| "N/A".to_string());
        
        info!(source = %name, type = %resolved_config.source_type, connection = %masked_conn, "Registering source");
        muscle.connectors.register_source(&muscle.ctx, name, &resolved_config).await?;
    }

    for seed in &project.seeds {
        let name = seed.file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid seed filename: {:?}", seed))?;
        muscle.register_csv(name, seed.to_str().ok_or_else(|| anyhow::anyhow!("Invalid seed path"))?).await?;
        
        let seed_hash = LogicHash::new(format!("seed_{}", name));
        let metadata = ModelMetadata {
            status: "success".to_string(),
            materialization_path: seed.to_str().unwrap().to_string(),
            created_at: 0,
        };
        filler.state_store.put_metadata(&target, name, &seed_hash, &metadata)?;
        
        let sql = format!("CREATE OR REPLACE VIEW {} AS SELECT * FROM {}", name, name);
        muscle.execute(&sql).await?;
        let env_sql = format!("CREATE OR REPLACE VIEW {}{} AS SELECT * FROM {}", profile.prefix, name, name);
        muscle.execute(&env_sql).await?;
    }

    let mut filtered_models = if let Some(pattern) = select {
        project.filter_models(&pattern)
    } else {
        project.models.iter().collect()
    };

    if state {
        if let Ok(prior) = crate::artifacts::Manifest::load(&path) {
            let state_filtered = project.filter_by_state(&prior);
            let state_names: HashSet<_> = state_filtered.iter().map(|m| &m.name).collect();
            filtered_models.retain(|m| state_names.contains(&m.name));
            info!("State-based selection: filtered to {} models", filtered_models.len());
        } else {
            info!("No prior manifest found, running all selected models");
        }
    }

    let mut tasks = Vec::new();
    for model in filtered_models {
        tasks.push(ModelTask {
            name: model.name.clone(),
            env: target.clone(),
            raw_sql: model.raw_sql.clone(),
            config: HashMap::new(),
            fingerprinter: filler.fingerprinter.clone(),
            state_store: filler.state_store.clone(),
            muscle: muscle.clone(),
            vde: vde.clone(),
            parent_names: model.dependencies.clone(),
            materialization: model.materialization,
            unique_key: model.unique_key.clone(),
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
        });
    }

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

pub async fn handle_test(path: PathBuf, target: String) -> Result<()> {
    let project = Project::load(&path)?;
    let profiles = Profiles::load(&path.join("profiles.yml"))?;
    let profile = profiles.get_target(&target).ok_or_else(|| anyhow::anyhow!("Target {} not found", target))?;
    
    let db_path = path.join(".titan_db");
    let state_store = StateStore::open(&db_path)?;
    let muscle = Arc::new(Muscle::new());

    for seed in &project.seeds {
        let name = seed.file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid seed filename: {:?}", seed))?;
        muscle.register_csv(name, seed.to_str().ok_or_else(|| anyhow::anyhow!("Invalid seed path"))?).await?;
        let env_sql = format!("CREATE OR REPLACE VIEW {}{} AS SELECT * FROM {}", profile.prefix, name, name);
        muscle.execute(&env_sql).await?;
    }

    for model in &project.models {
        if let Some((hash, sql)) = state_store.get_hash_by_name(&target, &model.name)?
            .and_then(|h| state_store.get_value(&h).transpose().map(|s| s.map(|sql| (h, sql))))
            .transpose()? {
            let table_name = format!("{}__{}", model.name, hash);
            let create_view = format!("CREATE OR REPLACE VIEW {} AS {}", table_name, sql);
            muscle.execute(&create_view).await?;
            let env_view = format!("CREATE OR REPLACE VIEW {}{} AS SELECT * FROM {}", profile.prefix, model.name, table_name);
            muscle.execute(&env_view).await?;
        }
    }

    let mut total_failed = 0;
    let mut total_tests = 0;

    for model in &project.models {
        let env_name = format!("{}{}", profile.prefix, model.name);
        for test_config in &model.tests {
            total_tests += 1;
            let (test_name, sql) = crate::quality::TestGenerator::generate_sql(&env_name, &test_config.column_name, &test_config.test)?;
            
            let df = muscle.ctx.sql(&sql).await?;
            let batches = df.collect().await?;
            let count: i64 = if batches.is_empty() {
                0
            } else {
                let col = batches[0].column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap();
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
        return Err(anyhow::anyhow!("{} tests failed", total_failed));
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
    
    handle_pipeline(path, target, None, false, true, 1).await?;
    
    info!("Project check PASSED");
    Ok(())
}
