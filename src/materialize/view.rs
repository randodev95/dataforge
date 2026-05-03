use anyhow::Result;
use std::sync::Arc;
use crate::materialize::VDE;
use crate::fingerprint::LogicHash;
use async_trait::async_trait;

pub struct ViewMaterializer {
    pub vde: Arc<VDE>,
}

#[async_trait]
impl super::Materializer for ViewMaterializer {
    async fn materialize(&self, _env: &str, model_name: &str, hash: &LogicHash, sql: &str) -> Result<()> {
        let table_name = format!("{}__{}", model_name, hash);
        let create_view_sql = format!("CREATE OR REPLACE VIEW {} AS {}", table_name, sql);
        self.vde.muscle.execute(&create_view_sql).await?;
        Ok(())
    }
}
