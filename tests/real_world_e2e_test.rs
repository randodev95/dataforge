mod common;
use common::TestEnv;
use titan_engine::{Filler, LogicHash, Muscle, VDE};
use titan_engine::filler::dag::ModelTask;
use std::collections::HashMap;
use std::sync::Arc;
use std::io::Write;
use tempfile::Builder;

#[tokio::test]
async fn test_real_world_pipeline() {
    let env = TestEnv::new();
    let filler = Filler::new(env.state_store);
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));
    let env_name = "prod";
    
    // 1. Create a mock CSV representing the raw source
    let mut raw_users_csv = Builder::new().suffix(".csv").tempfile().expect("Failed to create temp file");
    writeln!(raw_users_csv, "id,name,status").unwrap();
    writeln!(raw_users_csv, "1,Alice,active").unwrap();
    writeln!(raw_users_csv, "2,Bob,inactive").unwrap();
    writeln!(raw_users_csv, "3,Charlie,active").unwrap();
    
    // Register the CSV in Muscle
    muscle.register_csv("raw_users", raw_users_csv.path().to_str().unwrap()).await.unwrap();

    // 2. Define the Pipeline
    let stg_task = ModelTask {
        name: "stg_users".to_string(),
        env: env_name.to_string(),
        raw_sql: "SELECT id, name FROM raw_users WHERE status = 'active'".to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec![],
        plan_only: false,
    };

    let dim_task = ModelTask {
        name: "dim_users".to_string(),
        env: env_name.to_string(),
        raw_sql: "SELECT id, UPPER(name) as name FROM {{ ref('stg_users') }}".to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec!["stg_users".to_string()],
        plan_only: false,
    };

    // --- TEST A: Initial Run ---
    // Now filler handles everything including physical execution
    filler.run_dag(vec![stg_task.clone(), dim_task.clone()]).await.unwrap();
    
    let stg_hash = filler.state_store.get_hash_by_name(env_name, "stg_users").unwrap().unwrap();
    let dim_hash = filler.state_store.get_hash_by_name(env_name, "dim_users").unwrap().unwrap();

    // Verify Results
    // The view name in VDE for prod is prod_dim_users (due to our underscore prefix change)
    let results = muscle.execute_and_fetch("SELECT * FROM prod_dim_users ORDER BY id").await.unwrap();
    assert_eq!(results.len(), 1, "Should have 1 batch");
    assert_eq!(results[0].num_rows(), 2, "Should have 2 active users");
    
    // --- TEST B: Smart Skip ---
    filler.run_dag(vec![stg_task.clone(), dim_task.clone()]).await.unwrap();
    
    let stg_hash_2 = filler.state_store.get_hash_by_name(env_name, "stg_users").unwrap().unwrap();
    let dim_hash_2 = filler.state_store.get_hash_by_name(env_name, "dim_users").unwrap().unwrap();
    
    assert_eq!(stg_hash, stg_hash_2);
    assert_eq!(dim_hash, dim_hash_2);

    // --- TEST C: Logic Change Cascade ---
    let stg_task_new = ModelTask {
        name: "stg_users".to_string(),
        env: env_name.to_string(),
        raw_sql: "SELECT id, name FROM raw_users WHERE status = 'inactive'".to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec![],
        plan_only: false,
    };

    filler.run_dag(vec![stg_task_new.clone()]).await.unwrap();
    let stg_hash_new = filler.state_store.get_hash_by_name(env_name, "stg_users").unwrap().unwrap();
    
    assert_ne!(stg_hash, stg_hash_new, "Hash should change with new logic");
    
    // Dim task should now get a new hash because its parent hash changed
    filler.run_dag(vec![stg_task_new, dim_task.clone()]).await.unwrap();
    let dim_hash_new = filler.state_store.get_hash_by_name(env_name, "dim_users").unwrap().unwrap();
    
    assert_ne!(dim_hash, dim_hash_new, "Child hash should change when parent hash changes");
}

#[tokio::test]
async fn test_failure_isolation() {
    let env = TestEnv::new();
    let filler = Filler::new(env.state_store);
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));
    let env_name = "prod";
    
    let bad_task = ModelTask {
        name: "bad_model".to_string(),
        env: env_name.to_string(),
        raw_sql: "SELECT * FROM missing_parent".to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec!["missing_parent".to_string()],
        plan_only: false,
    };

    let result = filler.run_dag(vec![bad_task]).await;
    assert!(result.is_err(), "Should fail due to missing dependency");
    
    let stored_hash = filler.state_store.get_hash_by_name(env_name, "bad_model").unwrap();
    assert!(stored_hash.is_none(), "Failed model should not be recorded in state store");
}
