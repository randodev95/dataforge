use serde::{Deserialize, Serialize};
use std::path::Path;
use std::fs;
use anyhow::Result;

#[derive(Debug, Serialize, Deserialize)]
pub struct RunResult {
    pub name: String,
    pub status: String,
    pub duration_ms: u128,
    pub rows_affected: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunResults {
    pub generated_at: u64,
    pub results: Vec<RunResult>,
}

impl RunResults {
    pub fn save(&self, project_root: &Path) -> Result<()> {
        let target_dir = project_root.join("target");
        fs::create_dir_all(&target_dir)?;
        let json = serde_json::to_string_pretty(self)?;
        fs::write(target_dir.join("run_results.json"), json)?;
        Ok(())
    }
}
