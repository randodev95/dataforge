use datafusion::prelude::*;
use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;

pub struct Muscle {
    ctx: SessionContext,
}

impl Muscle {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    pub async fn execute(&self, sql: &str) -> Result<()> {
        let df = self.ctx.sql(sql).await
            .map_err(|e| anyhow::anyhow!("DataFusion SQL error: {}", e))?;
        
        let results = df.collect().await
            .map_err(|e| anyhow::anyhow!("DataFusion execution error: {}", e))?;
        
        println!("Executed SQL. Result rows: {}", results.iter().map(|b| b.num_rows()).sum::<usize>());
        
        Ok(())
    }

    pub async fn execute_and_fetch(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(sql).await
            .map_err(|e| anyhow::anyhow!("DataFusion SQL error: {}", e))?;
        
        let results = df.collect().await
            .map_err(|e| anyhow::anyhow!("DataFusion execution error: {}", e))?;
            
        Ok(results)
    }

    pub async fn register_parquet(&self, name: &str, path: &str) -> Result<()> {
        self.ctx.register_parquet(name, path, ParquetReadOptions::default()).await
            .map_err(|e| anyhow::anyhow!("Failed to register parquet: {}", e))?;
        Ok(())
    }

    pub async fn register_csv(&self, name: &str, path: &str) -> Result<()> {
        self.ctx.register_csv(name, path, CsvReadOptions::default()).await
            .map_err(|e| anyhow::anyhow!("Failed to register csv: {}", e))?;
        Ok(())
    }
}

impl Default for Muscle {
    fn default() -> Self {
        Self::new()
    }
}
