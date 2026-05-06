use crate::error::Result;
use crate::execution::Muscle;
use crate::fingerprint::LogicHash;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::info;

pub struct AdbcMaterializer {
    pub muscle: Arc<Muscle>,
}

#[async_trait]
impl super::Materializer for AdbcMaterializer {
    async fn materialize(
        &self,
        _env: &str,
        _model_name: &str,
        target_name: &str,
        _hash: &LogicHash,
        _exec_id: &uuid::Uuid,
        sql: &str,
    ) -> Result<()> {
        info!(model = %target_name, "Materializing to ADBC destination");

        // 1. Execute query in DataFusion to get RecordBatch stream
        let results = self.muscle.execute_and_fetch(sql).await?;

        // 2. Stream results to ADBC destination
        // In a real implementation:
        // let mut adbc_conn = ...
        // adbc_conn.ingest(model_name, results)?;

        info!(
            rows = results
                .iter()
                .map(datafusion::arrow::array::RecordBatch::num_rows)
                .sum::<usize>(),
            "Injected batches into ADBC destination (simulated)"
        );

        Ok(())
    }
}
