use anyhow::Result;
use std::sync::Arc;
use crate::execution::Muscle;
use crate::fingerprint::LogicHash;
use async_trait::async_trait;

pub struct IncrementalMaterializer {
    pub muscle: Arc<Muscle>,
}

#[async_trait]
impl super::Materializer for IncrementalMaterializer {
    async fn materialize(&self, _env: &str, model_name: &str, hash: &LogicHash, sql: &str) -> Result<()> {
        let table_name = format!("{}__{}", model_name, hash);
        
        let sql_cmd = format!("CREATE TABLE IF NOT EXISTS {} AS {}", table_name, sql);
        if let Err(_) = self.muscle.execute(&sql_cmd).await {
            let insert_sql = format!("INSERT INTO {} {}", table_name, sql);
            self.muscle.execute(&insert_sql).await?;
        }
        
        Ok(())
    }
}
