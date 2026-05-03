mod common;
use common::TestEnv;
use titan_engine::{Filler, LogicHash, Muscle, VDE};
use titan_engine::filler::dag::ModelTask;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_full_dag_execution() {
    let env = TestEnv::new();
    let filler = Filler::new(env.state_store);
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));
    
    let fingerprinter = filler.fingerprinter.clone();
    let store = filler.state_store.clone();
    let env_name = "prod";

    // 1. Task 1 (Root)
    let task1 = ModelTask {
        name: "stg_users".to_string(),
        env: env_name.to_string(),
        raw_sql: "SELECT 1 as id".to_string(),
        config: HashMap::new(),
        fingerprinter: fingerprinter.clone(),
        state_store: store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec![],
        plan_only: false,
    };

    // 2. Run first time
    filler.run_dag(vec![task1]).await.unwrap();

    // 3. Verify it was stored
    let hash = store.get_hash_by_name(env_name, "stg_users").unwrap().expect("Hash should exist");
    let metadata = store.get_metadata(&hash).unwrap().expect("Metadata should exist");
    assert_eq!(metadata.status, "success");

    // 4. Task 2 (Dependent)
    let task2 = ModelTask {
        name: "dim_users".to_string(),
        env: env_name.to_string(),
        raw_sql: "SELECT * FROM {{ ref('stg_users') }}".to_string(),
        config: HashMap::new(),
        fingerprinter: fingerprinter.clone(),
        state_store: store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec!["stg_users".to_string()],
        plan_only: false,
    };

    // 5. Run second time (should process task2)
    filler.run_dag(vec![task2]).await.unwrap();

    // 6. Verify task2 stored
    let hash2 = store.get_hash_by_name(env_name, "dim_users").unwrap().expect("Hash should exist");
    assert_ne!(hash, hash2);
}

#[tokio::test]
async fn test_smart_skip() {
    let env = TestEnv::new();
    let filler = Filler::new(env.state_store);
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));
    let env_name = "prod";
    
    let task = ModelTask {
        name: "test".to_string(),
        env: env_name.to_string(),
        raw_sql: "SELECT 1".to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec![],
        plan_only: false,
    };

    // Run 1
    filler.run_dag(vec![task]).await.unwrap();
    
    // Run 2 (Should skip)
    let task_redo = ModelTask {
        name: "test".to_string(),
        env: env_name.to_string(),
        raw_sql: "SELECT 1".to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec![],
        plan_only: false,
    };
    
    filler.run_dag(vec![task_redo]).await.unwrap();
}
