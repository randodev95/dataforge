mod common;
use common::TestEnv;
use titan_engine::{Filler, Muscle, VDE};
use titan_engine::filler::dag::ModelTask;
use titan_engine::materialize::Materialization;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_parallel_vs_series_execution() {
    let env = TestEnv::new();
    let filler = Filler::new(env.state_store, 4);
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));
    let env_name = "prod";
    
    let task_a = ModelTask {
        name: "task_a".to_string(),
        env: env_name.to_string(),
        raw_sql: "SELECT 1".to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec![],
        plan_only: false,
        semaphore: filler.semaphore.clone(),
        materialization: Materialization::View,
    };

    let task_b = ModelTask {
        name: "task_b".to_string(),
        env: env_name.to_string(),
        raw_sql: "SELECT 2".to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec![],
        plan_only: false,
        semaphore: filler.semaphore.clone(),
        materialization: Materialization::View,
    };

    // Run two independent tasks - they should run in parallel in the DAG
    filler.run_dag(vec![task_a, task_b]).await.unwrap();
    
    // Now test series: Task C depends on A and B
    let task_c = ModelTask {
        name: "task_c".to_string(),
        env: env_name.to_string(),
        raw_sql: "SELECT 3".to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec!["task_a".to_string(), "task_b".to_string()],
        plan_only: false,
        semaphore: filler.semaphore.clone(),
        materialization: Materialization::View,
    };

    filler.run_dag(vec![task_c]).await.unwrap();
    
    assert!(filler.state_store.get_hash_by_name(env_name, "task_c").unwrap().is_some());
}
