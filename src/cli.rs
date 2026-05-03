use clap::{Parser, Subcommand};
use std::path::PathBuf;
use anyhow::Result;
use std::fs;
use std::io::Write;
use crate::project::Project;
use crate::project::profiles::Profiles;
use crate::project::exposures::Exposures;
use crate::hooks::Hooks;
use crate::quality::{Test, UniqueTest, NotNullTest};
use crate::{Filler, StateStore, Muscle, VDE, LogicHash, ModelMetadata};
use std::sync::Arc;
use std::collections::HashMap;
use crate::filler::dag::ModelTask;
use crate::materialize::Materialization;
use tracing::{info, error};

#[derive(Parser)]
#[command(name = "titan")]
#[command(about = "Titan Engine CLI", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
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
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Run the data pipeline
    Run {
        #[arg(long, default_value = "dev")]
        target: String,
        #[arg(long)]
        select: Option<String>,
        #[arg(default_value = ".")]
        path: PathBuf,
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
}

#[derive(Subcommand)]
pub enum ExposureAction {
    List {
        #[arg(default_value = ".")]
        path: PathBuf,
    },
}

pub async fn handle_init(name: String, path: PathBuf) -> Result<()> {
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

pub async fn handle_pipeline(path: PathBuf, target: String, select: Option<String>, plan_only: bool) -> Result<()> {
    let project = Project::load(&path)?;
    let profiles = Profiles::load(&path.join("profiles.yml"))?;
    let profile = profiles.get_target(&target).ok_or_else(|| anyhow::anyhow!("Target {} not found in profiles.yml", target))?;
    
    info!(project = %project.config.name, target = %target, prefix = %profile.prefix, "Loaded project");

    let db_path = path.join(".titan_db");
    let state_store = StateStore::open(&db_path)?;
    let filler = Filler::new(state_store, 4); // Default concurrency of 4
    
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));

    // Run-start hooks
    let hooks = Hooks::load(&path);
    if !plan_only {
        hooks.run_start(&muscle).await?;
    }

    for seed in &project.seeds {
        let name = seed.file_stem().unwrap().to_str().unwrap();
        muscle.register_csv(name, seed.to_str().unwrap()).await?;
        
        let seed_hash = LogicHash::new(format!("seed_{}", name));
        let metadata = ModelMetadata {
            status: "success".to_string(),
            materialization_path: seed.to_str().unwrap().to_string(),
            created_at: 0,
        };
        filler.state_store.put_metadata(&target, name, &seed_hash, &metadata)?;
        
        // Register seed
        let sql = format!("CREATE OR REPLACE VIEW {} AS SELECT * FROM {}", name, name);
        muscle.execute(&sql).await?;
        let env_sql = format!("CREATE OR REPLACE VIEW {}{} AS SELECT * FROM {}", profile.prefix, name, name);
        muscle.execute(&env_sql).await?;
    }

    let filtered_models = if let Some(pattern) = select {
        project.filter_models(&pattern)
    } else {
        project.models.clone()
    };

    let mut tasks = Vec::new();
    for model in filtered_models {
        tasks.push(ModelTask {
            name: model.name,
            env: target.clone(),
            raw_sql: model.raw_sql,
            config: HashMap::new(),
            fingerprinter: filler.fingerprinter.clone(),
            state_store: filler.state_store.clone(),
            muscle: muscle.clone(),
            vde: vde.clone(),
            parent_names: model.dependencies,
            materialization: Materialization::View, // Default for now
            plan_only,
            semaphore: filler.semaphore.clone(),
        });
    }

    filler.run_dag(tasks).await?;

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

    // Register seeds
    for seed in &project.seeds {
        let name = seed.file_stem().unwrap().to_str().unwrap();
        muscle.register_csv(name, seed.to_str().unwrap()).await?;
        let env_sql = format!("CREATE OR REPLACE VIEW {}{} AS SELECT * FROM {}", profile.prefix, name, name);
        muscle.execute(&env_sql).await?;
    }

    // Register models from state
    for model in &project.models {
        if let Some(hash) = state_store.get_hash_by_name(&target, &model.name)? {
            if let Some(sql) = state_store.get_value(&hash)? {
                let table_name = format!("{}__{}", model.name, hash);
                let create_view = format!("CREATE OR REPLACE VIEW {} AS {}", table_name, sql);
                muscle.execute(&create_view).await?;
                let env_view = format!("CREATE OR REPLACE VIEW {}{} AS SELECT * FROM {}", profile.prefix, model.name, table_name);
                muscle.execute(&env_view).await?;
            }
        }
    }

    // Simple dbt-style test discovery (hardcoded for demo)
    let tests: Vec<Box<dyn Test + Send + Sync>> = vec![
        Box::new(UniqueTest { model: format!("{}dim_users", profile.prefix), column: "id".to_string() }),
        Box::new(NotNullTest { model: format!("{}dim_users", profile.prefix), column: "id".to_string() }),
    ];

    info!(target = %target, "Running {} tests", tests.len());
    for test in tests {
        match test.run(&muscle).await {
            Ok(_) => info!(test = %test.name(), "PASS"),
            Err(e) => {
                error!(test = %test.name(), "FAIL: {}", e);
                return Err(e);
            }
        }
    }
    Ok(())
}

pub async fn handle_exposure(path: PathBuf) -> Result<()> {
    let exposures = Exposures::load(&path)?;
    println!("{}", serde_json::to_string_pretty(&exposures.items)?);
    Ok(())
}
