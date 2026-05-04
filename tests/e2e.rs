use titan_engine::cli::handle_pipeline_internal;
use titan_engine::execution::Muscle;
use std::sync::Arc;
use std::path::{Path, PathBuf};
use datafusion::arrow::array::Int64Array;
use std::fs;

/// Bootstraps the test projects if they were deleted.
fn bootstrap_jaffle_shop(root: &Path) {
    let p = root.join("jaffle_shop");
    if p.exists() { return; }
    
    fs::create_dir_all(p.join("seeds")).unwrap();
    fs::create_dir_all(p.join("models")).unwrap();
    
    fs::write(p.join("config.yaml"), "name: jaffle_shop\nstorage: rocksdb").unwrap();
    fs::write(p.join("profiles.yml"), "dev:\n  prefix: dev_").unwrap();
    
    // 1000 customers for speed
    let mut cust = String::from("id,first_name,last_name\n");
    for i in 1..=1000 { cust.push_str(&format!("{},First{},Last{}\n", i, i, i)); }
    fs::write(p.join("seeds/raw_customers.csv"), cust).unwrap();

    fs::write(p.join("models/stg_customers.sql"), "{{ config(materialized='view') }}\nSELECT id as customer_id, first_name FROM {{ ref('raw_customers') }}").unwrap();
    fs::write(p.join("models/customers.sql"), "{{ config(materialized='table') }}\nSELECT * FROM {{ ref('stg_customers') }}").unwrap();
}

#[tokio::test]
async fn test_e2e_jaffle_shop_lifecycle() {
    let _ = tracing_subscriber::fmt::try_init();
    let root = PathBuf::from("test_projects");
    bootstrap_jaffle_shop(&root);
    
    let project_path = root.join("jaffle_shop");
    let muscle = Arc::new(Muscle::new());
    
    // Cleanup state
    let _ = fs::remove_dir_all(project_path.join(".titan_db"));
    
    handle_pipeline_internal(project_path, "dev".to_string(), None, false, false, muscle.clone()).await.unwrap();
    
    let results = muscle.execute_and_fetch("SELECT count(*) FROM dev_customers").await.unwrap();
    let count = results[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(count, 1000);
}
