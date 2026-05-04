mod common;
use common::TestEnv;
use titan_engine::{Filler, Muscle, VDE};
use titan_engine::filler::dag::ModelTask;
use titan_engine::materialize::Materialization;
use titan_engine::fingerprint::normalize::Normalizer;
use polyglot_sql::{parse_one, generate, DialectType};
use std::collections::HashMap;
use std::sync::Arc;
use std::path::Path;

#[tokio::test]
async fn test_engine_dag_lifecycle() {
    let env = TestEnv::new();
    let filler = Filler::new(env.state_store, Path::new("."), 4);
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));
    let env_name = "prod";

    // 1. Root Task
    let task1 = ModelTask {
        name: "stg_users".to_string(),
        env: env_name.to_string(),
        raw_sql: "SELECT 1 as id".to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec![],
        unique_key: None,
        materialization: Materialization::View,
        target_type: "datafusion".to_string(),
        retention: None,
        on_schema_change: titan_engine::project::OnSchemaChange::default(),
        plan_only: false,
        semaphore: filler.semaphore.clone(),
    };

    // 2. Dependent Task
    let task2 = ModelTask {
        name: "dim_users".to_string(),
        env: env_name.to_string(),
        raw_sql: "SELECT * FROM {{ ref('stg_users') }}".to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec!["stg_users".to_string()],
        unique_key: None,
        materialization: Materialization::View,
        target_type: "datafusion".to_string(),
        retention: None,
        on_schema_change: titan_engine::project::OnSchemaChange::default(),
        plan_only: false,
        semaphore: filler.semaphore.clone(),
    };

    // Run DAG
    let _ = filler.run_dag(vec![task1.clone(), task2.clone()]).await.unwrap();

    // Verify persistence
    let hash1 = filler.state_store.get_hash_by_name(env_name, "stg_users").unwrap().unwrap();
    let hash2 = filler.state_store.get_hash_by_name(env_name, "dim_users").unwrap().unwrap();
    assert_ne!(hash1, hash2);

    // Smart Skip Test
    let _ = filler.run_dag(vec![task1, task2]).await.unwrap();
    let hash1_retry = filler.state_store.get_hash_by_name(env_name, "stg_users").unwrap().unwrap();
    assert_eq!(hash1, hash1_retry);
}

#[tokio::test]
async fn test_parallel_scheduling() {
    let env = TestEnv::new();
    let filler = Filler::new(env.state_store, Path::new("."), 4);
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));
    let env_name = "prod";
    
    let tasks = vec!["a", "b", "c"].into_iter().map(|n| ModelTask {
        name: n.to_string(),
        env: env_name.to_string(),
        raw_sql: format!("SELECT '{}'", n),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec![],
        unique_key: None,
        materialization: Materialization::View,
        target_type: "datafusion".to_string(),
        retention: None,
        on_schema_change: titan_engine::project::OnSchemaChange::default(),
        plan_only: false,
        semaphore: filler.semaphore.clone(),
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
    let filler = Filler::new(env.state_store, Path::new("."), 4);
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));
    
    let bad_task = ModelTask {
        name: "bad".to_string(),
        env: "prod".to_string(),
        raw_sql: "SELECT * FROM missing".to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec!["missing".to_string()],
        unique_key: None,
        materialization: Materialization::View,
        target_type: "datafusion".to_string(),
        retention: None,
        on_schema_change: titan_engine::project::OnSchemaChange::default(),
        plan_only: false,
        semaphore: filler.semaphore.clone(),
    };

    assert!(filler.run_dag(vec![bad_task]).await.is_err());
    assert!(filler.state_store.get_hash_by_name("prod", "bad").unwrap().is_none());
}
