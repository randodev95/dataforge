use crate::error::Result;
use std::sync::Arc;
use crate::execution::Muscle;
use crate::fingerprint::LogicHash;
use async_trait::async_trait;

pub struct TableMaterializer {
    pub muscle: Arc<Muscle>,
}

#[async_trait]
impl super::Materializer for TableMaterializer {
    async fn materialize(&self, _env: &str, model_name: &str, hash: &LogicHash, _exec_id: &uuid::Uuid, sql: &str) -> Result<()> {
        let table_name = format!("{}__{}", model_name, hash);
        let mut builder = crate::utils::SqlBuilder::new(32 + table_name.len() + sql.len());
        builder.push_str("CREATE OR REPLACE TABLE ");
        builder.push_str(&table_name);
        builder.push_str(" AS ");
        builder.push_str(sql);
        
        self.muscle.execute(&builder.finish()).await?;
        Ok(())
    }
}
