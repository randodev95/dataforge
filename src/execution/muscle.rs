use crate::error::{Result, TitanError};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use deltalake::open_table;
use tracing::{debug, info};
use url::Url;

pub struct Muscle {
    pub ctx: SessionContext,
    pub connectors: crate::connectors::ConnectorRegistry,
    pub circuit_breaker: std::sync::Arc<crate::circuit_breaker::CircuitBreaker>,
}

impl Muscle {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
            connectors: crate::connectors::ConnectorRegistry::new(),
            circuit_breaker: std::sync::Arc::new(crate::circuit_breaker::CircuitBreaker::new(
                5,
                std::time::Duration::from_secs(60),
            )),
        }
    }

    pub async fn execute(&self, sql: &str) -> Result<()> {
        self.circuit_breaker
            .check()
            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

        debug!(sql = %sql, "Executing SQL");
        let df = self.ctx.sql(sql).await.map_err(|e| {
            self.circuit_breaker.record_failure();
            info!(sql = %sql, error = %e, "SQL Planning Failed");
            TitanError::SqlParseError(e.to_string())
        })?;

        let _ = df.collect().await.map_err(|e| {
            self.circuit_breaker.record_failure();
            TitanError::ExecutionError(e.to_string())
        })?;

        self.circuit_breaker.record_success();
        Ok(())
    }

    pub async fn execute_and_fetch(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        self.circuit_breaker
            .check()
            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

        debug!(sql = %sql, "Executing and fetching SQL");
        let df = self.ctx.sql(sql).await.map_err(|e| {
            self.circuit_breaker.record_failure();
            info!(sql = %sql, error = %e, "SQL Planning Failed");
            TitanError::SqlParseError(e.to_string())
        })?;

        let results = df.collect().await.map_err(|e| {
            self.circuit_breaker.record_failure();
            TitanError::ExecutionError(e.to_string())
        })?;

        self.circuit_breaker.record_success();
        Ok(results)
    }

    pub async fn register_parquet(&self, name: &str, path: &str) -> Result<()> {
        if self.ctx.table_exist(name).unwrap_or(false) {
            let _ = self.ctx.deregister_table(name);
        }
        self.ctx
            .register_parquet(name, path, ParquetReadOptions::default())
            .await
            .map_err(|e| TitanError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    pub async fn register_csv(&self, name: &str, path: &str) -> Result<()> {
        if self.ctx.table_exist(name).unwrap_or(false) {
            let _ = self.ctx.deregister_table(name);
        }
        self.ctx
            .register_csv(name, path, CsvReadOptions::default())
            .await
            .map_err(|e| TitanError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    pub async fn register_delta(&self, name: &str, path: &str) -> Result<()> {
        if self.ctx.table_exist(name).unwrap_or(false) {
            let _ = self.ctx.deregister_table(name);
        }

        let abs_path = std::fs::canonicalize(path)
            .map_err(|e| TitanError::DatabaseError(format!("Invalid path {path}: {e}")))?;

        let url = Url::from_file_path(&abs_path).map_err(|()| {
            TitanError::DatabaseError(format!("Failed to create URL from path: {abs_path:?}"))
        })?;

        let table = open_table(url)
            .await
            .map_err(|e| TitanError::DatabaseError(e.to_string()))?;

        let provider = table
            .table_provider()
            .await
            .map_err(|e| TitanError::DatabaseError(e.to_string()))?;

        self.ctx
            .register_table(name, provider)
            .map_err(|e| TitanError::DatabaseError(e.to_string()))?;

        Ok(())
    }
}

impl Default for Muscle {
    fn default() -> Self {
        Self::new()
    }
}
