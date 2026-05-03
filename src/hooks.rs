use anyhow::Result;
use crate::execution::Muscle;
use std::path::Path;
use std::fs;
use tracing::info;

pub struct Hooks {
    pub start_sql: Option<String>,
    pub end_sql: Option<String>,
}

impl Hooks {
    pub fn load(root: &Path) -> Self {
        let start_path = root.join("on-run-start.sql");
        let end_path = root.join("on-run-end.sql");

        Self {
            start_sql: fs::read_to_string(start_path).ok(),
            end_sql: fs::read_to_string(end_path).ok(),
        }
    }

    pub async fn run_start(&self, muscle: &Muscle) -> Result<()> {
        if let Some(sql) = &self.start_sql {
            info!("Running on-run-start hook");
            for stmt in sql.split(';') {
                let stmt = stmt.trim();
                if !stmt.is_empty() {
                    muscle.execute(stmt).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn run_end(&self, muscle: &Muscle) -> Result<()> {
        if let Some(sql) = &self.end_sql {
            info!("Running on-run-end hook");
            for stmt in sql.split(';') {
                let stmt = stmt.trim();
                if !stmt.is_empty() {
                    muscle.execute(stmt).await?;
                }
            }
        }
        Ok(())
    }
}
