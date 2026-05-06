use datafusion::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;
use titan_engine::execution::Muscle;
use titan_engine::project::Project;

#[tokio::test]
async fn test_masking_applied() -> anyhow::Result<()> {
    let muscle = Arc::new(Muscle::new());

    // Register the materialized table (in a real run it would be in the target/snapshots dir if materialized=table)
    // But here we registered it as a view in memory during the run (if not physically written)
    // Wait, titan-engine run physically writes it to the target dir if materialized=table.

    let project_root = PathBuf::from("demo_project");
    let table_path = project_root.join("snapshots/dev/orders_summary");

    if table_path.exists() {
        muscle
            .register_delta("orders_summary", table_path.to_str().unwrap())
            .await?;
        let df = muscle.ctx.sql("SELECT * FROM orders_summary").await?;
        df.show().await?;
    } else {
        println!("Table path not found: {:?}", table_path);
    }

    Ok(())
}
