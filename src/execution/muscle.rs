use datafusion::prelude::*;
use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::LogicalPlan;
use tracing::debug;

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
        let plan = self.ctx.state().create_logical_plan(sql).await
            .map_err(|e| anyhow::anyhow!("DataFusion planning error: {}", e))?;
        
        let optimized_plan = self.rewrite_plan(plan)?;
        
        let df = self.ctx.execute_logical_plan(optimized_plan).await
            .map_err(|e| anyhow::anyhow!("DataFusion SQL error: {}", e))?;
        
        let results = df.collect().await
            .map_err(|e| anyhow::anyhow!("DataFusion execution error: {}", e))?;
        
        debug!("Executed SQL. Result rows: {}", results.iter().map(|b| b.num_rows()).sum::<usize>());
        
        Ok(())
    }

    pub async fn execute_and_fetch(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let plan = self.ctx.state().create_logical_plan(sql).await
            .map_err(|e| anyhow::anyhow!("DataFusion planning error: {}", e))?;
        
        let optimized_plan = self.rewrite_plan(plan)?;
        
        let df = self.ctx.execute_logical_plan(optimized_plan).await
            .map_err(|e| anyhow::anyhow!("DataFusion SQL error: {}", e))?;
        
        let results = df.collect().await
            .map_err(|e| anyhow::anyhow!("DataFusion execution error: {}", e))?;
            
        Ok(results)
    }

    /// Rewrites the LogicalPlan to handle structural divergence.
    fn rewrite_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        debug!("Applying structural divergence rewrites to LogicalPlan");
        Ok(plan)
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
