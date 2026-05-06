mod common;
use std::fs;
use std::path::PathBuf;
use titan_engine::cli::handle_test;

#[tokio::test]
async fn test_yaml_data_contracts() {
    let root = PathBuf::from("test_contracts");
    if root.exists() {
        fs::remove_dir_all(&root).unwrap();
    }
    fs::create_dir_all(&root).unwrap();

    // 1. Setup project
    fs::write(
        root.join("config.yaml"),
        "name: contract_test\nstorage: rocksdb\n",
    )
    .unwrap();
    fs::write(root.join("profiles.yml"), "dev:\n  prefix: dev_").unwrap();
    fs::create_dir_all(root.join("models")).unwrap();
    fs::create_dir_all(root.join("seeds")).unwrap();

    // 2. Create seed with duplicates and NULLs
    let seed_content = "id,name\n1,Alice\n1,Bob\n,Charlie\n";
    fs::write(root.join("seeds/users.csv"), seed_content).unwrap();

    // 3. Create model
    let model_sql = "SELECT * FROM {{ ref('users') }}";
    fs::write(root.join("models/stg_users.sql"), model_sql).unwrap();

    // 4. Create schema.yml with tests
    let schema_yml = "
version: 2
models:
  - name: stg_users
    columns:
      - name: id
        tests:
          - unique
          - not_null
";
    fs::write(root.join("models/schema.yml"), schema_yml).unwrap();

    // 5. Run pipeline to materialize
    titan_engine::cli::handle_pipeline(root.clone(), "dev".to_string(), None, false, false)
        .await
        .unwrap();

    // 6. Run tests - should FAIL
    let result = handle_test(root.clone(), "dev".to_string()).await;
    assert!(
        result.is_err(),
        "Expected tests to fail due to unique and not_null violations"
    );

    let err_msg = result.err().unwrap().to_string();
    println!("Actual error: {}", err_msg);
    assert!(err_msg.contains("tests failed"));
}
