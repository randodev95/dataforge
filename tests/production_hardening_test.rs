use titan_engine::{Filler, StateStore, Muscle, VDE, ModelTask, LogicHash};
use titan_engine::materialize::{Materialization};
use titan_engine::project::{OnSchemaChange, ModelColumn};
use std::sync::Arc;
use tempfile::TempDir;
use std::collections::HashMap;
use std::fs;

#[tokio::test]
async fn test_incremental_and_contracts() {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path();
    
    let db_path = root.join(".titan_db");
    let state_store = StateStore::open(&db_path).unwrap();
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));
    let filler = Filler::new(state_store, root, 1);

    // 1. Setup Source Data
    let csv1_path = root.join("raw1.csv");
    fs::write(&csv1_path, "id,val\n1,A\n2,B").unwrap();
    muscle.register_csv("raw_data", csv1_path.to_str().unwrap()).await.unwrap();
    muscle.execute("CREATE VIEW raw_view AS SELECT * FROM raw_data").await.unwrap();

    // 2. Incremental Run (First run - should create table)
    let task1 = ModelTask {
        name: "inc_model".to_string(),
        env: "dev".to_string(),
        raw_sql: "SELECT * FROM raw_view".to_string(),
        config: HashMap::new(),
        fingerprinter: filler.fingerprinter.clone(),
        state_store: filler.state_store.clone(),
        muscle: muscle.clone(),
        vde: vde.clone(),
        parent_names: vec![],
        materialization: Materialization::Incremental,
        unique_key: Some("id".to_string()),
        target_type: "delta".to_string(),
        retention: None,
        on_schema_change: OnSchemaChange::Fail,
        plan_only: false,
        semaphore: filler.semaphore.clone(),
        project_root: root.to_path_buf(),
        contract_enforced: true,
        columns: vec![
            ModelColumn { name: "id".to_string(), data_type: Some("int64".to_string()) },
            ModelColumn { name: "val".to_string(), data_type: Some("utf8".to_string()) },
        ],
        vars: HashMap::new(),
    };

    let (hash1, _) = task1.execute().await.expect("First run failed");

    // Verify first run (queried via dev_inc_model which was auto-registered)
    let df = muscle.ctx.sql("SELECT * FROM dev_inc_model").await.unwrap();
    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);

    // 3. Incremental Run (Second run - should merge)
    let csv2_path = root.join("raw2.csv");
    fs::write(&csv2_path, "id,val\n1,A_updated\n3,C").unwrap();
    muscle.register_csv("raw_data_v2", csv2_path.to_str().unwrap()).await.unwrap();
    muscle.execute("CREATE OR REPLACE VIEW raw_view AS SELECT * FROM raw_data_v2").await.unwrap();

    let (hash2, _) = task1.execute().await.expect("Second run failed");
    assert_ne!(hash1, hash2);

    // Verify merge result
    let df = muscle.ctx.sql("SELECT * FROM dev_inc_model ORDER BY id").await.unwrap();
    let res = df.collect().await.unwrap();
    let total_rows: usize = res.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    // 4. Test Contract Violation
    let task_bad = ModelTask {
        name: "bad_contract".to_string(),
        raw_sql: "SELECT id FROM raw_view".to_string(), // Missing 'val' column
        ..task1.clone()
    };
    
    let res = task_bad.execute().await;
    assert!(res.is_err(), "Should have failed contract validation");
    let err_msg = res.unwrap_err().to_string();
    assert!(err_msg.contains("missing column val"), "Error message should mention missing column val, got: {}", err_msg);

    // 5. Test Variable Resolution
    let mut vars = HashMap::new();
    vars.insert("my_val".to_string(), serde_yml::to_value("hello").unwrap());
    
    let task_vars = ModelTask {
        name: "var_model".to_string(),
        raw_sql: "SELECT '{{ var('my_val') }}' as msg".to_string(),
        vars,
        contract_enforced: false,
        ..task1.clone()
    };

    task_vars.execute().await.expect("Variable resolution failed");
    let df = muscle.ctx.sql("SELECT * FROM dev_var_model").await.unwrap();
    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty(), "Result batches should not be empty");
    let batch = &batches[0];
    let col = batch.column(0);
    let val = if let Some(a) = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>() {
        a.value(0).to_string()
    } else if let Some(a) = col.as_any().downcast_ref::<datafusion::arrow::array::StringViewArray>() {
        a.value(0).to_string()
    } else {
        panic!("Column 0 is not a string array, got: {:?}", col.data_type());
    };
    assert_eq!(val, "hello");
}
