mod common;
use common::TestEnv;
use titan_engine::{Filler, Muscle, VDE};
use titan_engine::filler::dag::ModelTask;
use titan_engine::materialize::Materialization;
use titan_engine::fingerprint::normalize::Normalizer;
use polyglot_sql::{parse_one, generate, DialectType};
use std::collections::HashMap;
use std::sync::Arc;
use std::path::{Path, PathBuf};
use tokio_util::sync::CancellationToken;

fn create_test_task(
    name: &str,
    env: &str,
    raw_sql: &str,
    filler: &Filler,
    muscle: Arc<Muscle>,
    vde: Arc<VDE>,
    parent_names: Vec<String>,
) -> ModelTask {
    ModelTask {
        name: name.to_string(),
        env: env.to_string(),
        raw_sql: raw_sql.to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle,
        vde,
        parent_names,
        unique_key: None,
        materialization: Materialization::View,
        target_type: "datafusion".to_string(),
        retention: None,
        on_schema_change: titan_engine::project::OnSchemaChange::default(),
        plan_only: false,
        semaphore: filler.semaphore.clone(),
        project_root: PathBuf::from("."),
        contract_enforced: false,
        columns: vec![],
        vars: HashMap::new(),
        exec_id: uuid::Uuid::new_v4(),
        cancellation_token: CancellationToken::new(),
    }
}

#[tokio::test]
async fn test_engine_dag_lifecycle() {
    let env = TestEnv::new();
    let token = CancellationToken::new();
    let filler = Filler::new(env.state_store, Path::new("."), 4, token);
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));
    let env_name = "prod";

    // 1. Root Task
    let task1 = create_test_task("stg_users", env_name, "SELECT 1 as id", &filler, muscle.clone(), vde.clone(), vec![]);

    // 2. Dependent Task
    let task2 = create_test_task("dim_users", env_name, "SELECT * FROM {{ ref('stg_users') }}", &filler, muscle.clone(), vde.clone(), vec!["stg_users".to_string()]);

    // Run DAG
    let _ = filler.run_dag(vec![task1.clone(), task2.clone()]).await.unwrap();

    // Verify persistence
    let hash1 = filler.state_store.get_hash_by_name(env_name, "stg_users").unwrap().unwrap();
    let hash2 = filler.state_store.get_hash_by_name(env_name, "dim_users").unwrap().unwrap();
    assert_ne!(hash1, hash2);

    // Smart Skip Test (Simulate a new CLI run)
    let muscle2 = Arc::new(Muscle::new());
    let vde2 = Arc::new(VDE::new(muscle2.clone()));
    let _ = filler.run_dag(vec![
        create_test_task("stg_users", env_name, "SELECT 1 as id", &filler, muscle2.clone(), vde2.clone(), vec![]),
        create_test_task("dim_users", env_name, "SELECT * FROM {{ ref('stg_users') }}", &filler, muscle2.clone(), vde2.clone(), vec!["stg_users".to_string()]),
    ]).await.unwrap();
    let hash1_retry = filler.state_store.get_hash_by_name(env_name, "stg_users").unwrap().unwrap();
    assert_eq!(hash1, hash1_retry);
}

#[tokio::test]
async fn test_parallel_scheduling() {
    let env = TestEnv::new();
    let token = CancellationToken::new();
    let filler = Filler::new(env.state_store, Path::new("."), 4, token);
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));
    let env_name = "prod";
    
    let tasks = vec!["a", "b", "c"].into_iter().map(|n| {
        create_test_task(n, env_name, &format!("SELECT '{}'", n), &filler, muscle.clone(), vde.clone(), vec![])
    }).collect();

    let _ = filler.run_dag(tasks).await.unwrap();
    assert!(filler.state_store.get_hash_by_name(env_name, "c").unwrap().is_some());
}

#[test]
fn test_sql_canonical_normalization() {
    let sql = "SELECT id, name FROM users WHERE id > 10 LIMIT 100";
    let ast = parse_one(sql, DialectType::PostgreSQL).unwrap();
    
    let mysql = generate(&ast, DialectType::MySQL).unwrap();
    let snowflake = generate(&ast, DialectType::Snowflake).unwrap();

    let norm_mysql = Normalizer::normalize(&mysql).unwrap();
    let norm_snowflake = Normalizer::normalize(&snowflake).unwrap();
    
    assert_eq!(norm_mysql.as_str(), norm_snowflake.as_str());
}

#[tokio::test]
async fn test_failure_isolation() {
    let env = TestEnv::new();
    let token = CancellationToken::new();
    let filler = Filler::new(env.state_store, Path::new("."), 4, token);
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));
    
    let bad_task = create_test_task("bad", "prod", "SELECT * FROM missing", &filler, muscle.clone(), vde.clone(), vec!["missing".to_string()]);

    assert!(filler.run_dag(vec![bad_task]).await.is_err());
    assert!(filler.state_store.get_hash_by_name("prod", "bad").unwrap().is_none());
}
