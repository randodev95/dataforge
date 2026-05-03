use anyhow::Result;
use std::sync::Arc;
use crate::execution::Muscle;
use crate::fingerprint::LogicHash;
use crate::core::quote_identifier;
use async_trait::async_trait;

pub struct TableMaterializer {
    pub muscle: Arc<Muscle>,
}

#[async_trait]
impl super::Materializer for TableMaterializer {
    async fn materialize(&self, _env: &str, model_name: &str, hash: &LogicHash, sql: &str) -> Result<()> {
        let table_name = quote_identifier(&format!("{}__{}", model_name, hash));
        let create_table_sql = format!("CREATE OR REPLACE TABLE {} AS {}", table_name, sql);
        self.muscle.execute(&create_table_sql).await?;
        Ok(())
    }
}
