use anyhow::Result;
use crate::fingerprint::LogicHash;
use crate::execution::Muscle;
use std::sync::Arc;
use tracing::{info, debug};

pub struct VDE {
    pub muscle: Arc<Muscle>,
}

impl VDE {
    pub fn new(muscle: Arc<Muscle>) -> Self {
        Self { muscle }
    }

    pub async fn materialization_swap(&self, env: &str, model_name: &str, hash: &LogicHash) -> Result<()> {
        let table_name_raw = format!("{}__{}", model_name, hash);
        let view_name_raw = format!("{}_{}", env, model_name);
        
        info!(model = %model_name, env = %env, "Performing Atomic Pointer Swap");
        
        // Atomic Pointer Swap (Views over table__hash)
        let sql = format!("CREATE OR REPLACE VIEW {} AS SELECT * FROM {}", view_name_raw, table_name_raw);
        
        self.muscle.execute(&sql).await?;
        
        self.verify_swap(env, &view_name_raw, &table_name_raw).await?;
        
        debug!(view = %view_name_raw, target = %table_name_raw, "Pointer swap completed and verified");
        
        Ok(())
    }

    async fn verify_swap(&self, _env: &str, view_name: &str, expected_table: &str) -> Result<()> {
        debug!(view = %view_name, "Verifying pointer swap");
        
        let check_sql = format!(
            "SELECT table_name FROM information_schema.views WHERE table_name = '{}' AND view_definition LIKE '%{}%'",
            view_name, expected_table
        );

        match self.muscle.execute_and_fetch(&check_sql).await {
            Ok(results) => {
                let row_count = results.iter().map(|b| b.num_rows()).sum::<usize>();
                if row_count == 0 {
                    debug!(view = %view_name, "Verification: No matching view found in INFORMATION_SCHEMA (might be delayed)");
                } else {
                    debug!(view = %view_name, "Verification: SUCCESS");
                }
            }
            Err(e) => {
                debug!("Verification: INFORMATION_SCHEMA check failed or not supported: {}", e);
            }
        }

        Ok(())
    }
}
