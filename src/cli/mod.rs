use clap::{Parser, Subcommand};
use std::path::PathBuf;
use anyhow::Result;
use std::fs;
use std::io::Write;
use crate::project::Project;
use crate::{Filler, StateStore, Muscle, VDE, LogicHash, ModelMetadata};
use std::sync::Arc;
use std::collections::HashMap;
use crate::filler::dag::ModelTask;
use tracing::{info, debug};

#[derive(Parser)]
#[command(name = "titan")]
#[command(about = "Titan Engine CLI", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new Titan project (SQLMesh-style)
    Init {
        /// Project name
        name: String,
        /// Path to initialize
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Plan the data pipeline (dry run)
    Plan {
        /// Environment (e.g. prod, dev)
        #[arg(long, default_value = "prod")]
        env: String,
        /// Model selection pattern (e.g. +model, model+)
        #[arg(long)]
        select: Option<String>,
        /// Project root
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Run the data pipeline
    Run {
        /// Environment (e.g. prod, dev)
        #[arg(long, default_value = "prod")]
        env: String,
        /// Model selection pattern (e.g. +model, model+)
        #[arg(long)]
        select: Option<String>,
        /// Project root
        #[arg(default_value = ".")]
        path: PathBuf,
    },
}

pub async fn handle_init(name: String, path: PathBuf) -> Result<()> {
    if !path.exists() {
        fs::create_dir_all(&path)?;
    }

    let config_yaml = format!(
        "name: {}\nstorage: rocksdb\n",
        name
    );
    fs::write(path.join("config.yaml"), config_yaml)?;

    let external_models_yaml = "
- name: raw_users
  columns:
    id: INT
    name: TEXT
    status: TEXT
";
    fs::write(path.join("external_models.yaml"), external_models_yaml)?;

    fs::create_dir_all(path.join("models"))?;
    fs::create_dir_all(path.join("seeds"))?;

    let stg_users_sql = "SELECT id, name, status FROM {{ ref('raw_users_seed') }}";
    fs::write(path.join("models/stg_users.sql"), stg_users_sql)?;

    let dim_users_sql = "SELECT id, UPPER(name) as name FROM {{ ref('stg_users') }} WHERE status = 'active'";
    fs::write(path.join("models/dim_users.sql"), dim_users_sql)?;

    let mut raw_users_csv = fs::File::create(path.join("seeds/raw_users_seed.csv"))?;
    writeln!(raw_users_csv, "id,name,status")?;
    writeln!(raw_users_csv, "1,Alice,active")?;
    writeln!(raw_users_csv, "2,Bob,inactive")?;
    writeln!(raw_users_csv, "3,Charlie,active")?;

    info!(project = %name, "Initialized Titan project");
    Ok(())
}

pub async fn handle_pipeline(path: PathBuf, env: String, select: Option<String>, plan_only: bool) -> Result<()> {
    let project = Project::load(&path)?;
    info!(project = %project.config.name, environment = %env, "Loaded project");

    let db_path = path.join(".titan_db");
    let state_store = StateStore::open(&db_path)?;
    let filler = Filler::new(state_store);
    
    // Execution engine setup
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));

    // For DataFusion demo, we'll use a prefix instead of a real schema to avoid catalog management overhead
    // So env.model becomes env__model
    let env_prefix = format!("{}_", env);

    for seed in &project.seeds {
        let name = seed.file_stem().unwrap().to_str().unwrap();
        muscle.register_csv(name, seed.to_str().unwrap()).await?;
        
        let seed_hash = LogicHash::new(format!("seed_{}", name));
        let metadata = ModelMetadata {
            status: "success".to_string(),
            materialization_path: seed.to_str().unwrap().to_string(),
            created_at: 0,
        };
        filler.state_store.put_metadata(&env, name, &seed_hash, &metadata)?;
        
        // Register seed as a view
        let sql = format!("CREATE OR REPLACE VIEW {} AS SELECT * FROM {}", name, name);
        muscle.execute(&sql).await?;
        
        // Register in environment prefix
        let env_sql = format!("CREATE OR REPLACE VIEW {}{} AS SELECT * FROM {}", env_prefix, name, name);
        muscle.execute(&env_sql).await?;
    }

    let filtered_models = if let Some(pattern) = select {
        debug!(selection = %pattern, "Applying selection filter");
        project.filter_models(&pattern)
    } else {
        project.models.clone()
    };

    let mut tasks = Vec::new();
    for model in filtered_models {
        tasks.push(ModelTask {
            name: model.name,
            env: env.clone(),
            raw_sql: model.raw_sql,
            config: HashMap::new(),
            fingerprinter: filler.fingerprinter.clone(),
            state_store: filler.state_store.clone(),
            muscle: muscle.clone(),
            vde: vde.clone(),
            parent_names: model.dependencies,
            plan_only,
        });
    }

    if plan_only {
        info!("--- Titan Plan ---");
    } else {
        info!("--- Titan Run ---");
    }
    
    filler.run_dag(tasks).await?;
    
    if !plan_only {
        info!("Pipeline execution complete");
        
        let final_view = format!("{}_dim_users", env);
        let check_sql = format!("SELECT * FROM {} LIMIT 5", final_view);
        let batches = muscle.execute_and_fetch(&check_sql).await?;
        info!("Verification query results for {}:", final_view);
        for batch in batches {
            use datafusion::arrow::util::pretty::print_batches;
            print_batches(&[batch]).map_err(|e| anyhow::anyhow!(e))?;
        }
    }

    Ok(())
}
