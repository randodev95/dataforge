use crate::error::Result;
use crate::fingerprint::LogicHash;
use crate::materialize::vde::VDE;
use async_trait::async_trait;
use std::sync::Arc;

pub struct ViewMaterializer {
    pub vde: Arc<VDE>,
}

#[async_trait]
impl super::Materializer for ViewMaterializer {
    async fn materialize(
        &self,
        _env: &str,
        _model_name: &str,
        target_name: &str,
        hash: &LogicHash,
        _exec_id: &uuid::Uuid,
        sql: &str,
    ) -> Result<()> {
        let table_name = format!("{target_name}__{hash}");
        let create_view_sql = format!("CREATE OR REPLACE VIEW {table_name} AS {sql}");
        self.vde.muscle.execute(&create_view_sql).await?;
        Ok(())
    }
}
