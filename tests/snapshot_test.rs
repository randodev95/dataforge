use titan_engine::materialize::snapshot::SnapshotMaterializer;
use titan_engine::materialize::{Materializer, VDE};
use titan_engine::execution::Muscle;
use titan_engine::fingerprint::LogicHash;
use std::sync::Arc;
use deltalake::open_table;

#[tokio::test]
async fn test_snapshot_scd2_logic() {
    let muscle = Arc::new(Muscle::new());
    let vde = Arc::new(VDE::new(muscle.clone()));
    
    let temp_dir = tempfile::tempdir().unwrap();
    let env = "test_env";
    let model = "scd2_model";
    let project_root = temp_dir.path().to_path_buf();
    
    let materializer = SnapshotMaterializer::new(
        muscle.clone(), 
        vde.clone(), 
        Some("id".to_string()), 
        None, 
        project_root.clone()
    );

    // 1. Initial Snapshot
    let hash1 = LogicHash::new("hash1".to_string());
    let sql1 = "SELECT 1 as id, 'A' as val";
    let exec_id1 = uuid::Uuid::new_v4();
    materializer.materialize(env, model, &hash1, &exec_id1, sql1).await.expect("Initial snapshot failed");

    // Verify 1
    {
        let table_path = project_root.join(format!("snapshots/{}/{}", env, model));
        let abs_path = std::fs::canonicalize(&table_path).unwrap();
        let url = url::Url::from_directory_path(abs_path).unwrap();
        let delta_table = open_table(url).await.unwrap();
        let log_store = delta_table.log_store();
        let eager_snapshot = deltalake::kernel::EagerSnapshot::try_new(log_store.as_ref(), Default::default(), None).await.unwrap();
        let table_provider = deltalake::delta_datafusion::DeltaTableProvider::try_new(
            eager_snapshot,
            log_store.clone(),
            Default::default()
        ).unwrap();
        muscle.ctx.register_table("verify_init", Arc::new(table_provider)).unwrap();
        let count_df = muscle.ctx.sql("SELECT COUNT(*) FROM verify_init").await.unwrap();
        let count_results = count_df.collect().await.unwrap();
        let count = count_results[0].column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap().value(0);
        assert_eq!(count, 1, "Initial count should be 1");
        muscle.ctx.deregister_table("verify_init").unwrap();
    }

    // 2. Second Snapshot (Value changed)
    let hash2 = LogicHash::new("hash2".to_string());
    let sql2 = "SELECT 1 as id, 'B' as val";
    let exec_id2 = uuid::Uuid::new_v4();
    materializer.materialize(env, model, &hash2, &exec_id2, sql2).await.expect("Second snapshot failed");

    // 3. Verify SCD-2 state
    let table_path = project_root.join(format!("snapshots/{}/{}", env, model));
    let abs_path = std::fs::canonicalize(&table_path).unwrap();
    let url = url::Url::from_directory_path(abs_path).unwrap();
    
    // Open the table fresh to see latest version
    let delta_table = open_table(url).await.unwrap();
    let log_store = delta_table.log_store();
    let eager_snapshot = deltalake::kernel::EagerSnapshot::try_new(log_store.as_ref(), Default::default(), None).await.unwrap();
    let table_provider = deltalake::delta_datafusion::DeltaTableProvider::try_new(
        eager_snapshot,
        log_store.clone(),
        Default::default()
    ).unwrap();
    
    muscle.ctx.register_table("verify_scd2", Arc::new(table_provider)).unwrap();
    
    let df = muscle.ctx.sql("SELECT * FROM verify_scd2").await.unwrap();
    let results = df.collect().await.unwrap();
    datafusion::arrow::util::pretty::print_batches(&results).unwrap();
    
    let count_df = muscle.ctx.sql("SELECT COUNT(*) as count FROM verify_scd2").await.unwrap();
    let count_results = count_df.collect().await.unwrap();
    let count = count_results[0].column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap().value(0);
    
    // We expect 2 rows: 1 expired ('A') and 1 active ('B')
    assert_eq!(count, 2);
}
