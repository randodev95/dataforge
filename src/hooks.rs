//! # Lifecycle Hooks
//!
//! This module defines and manages execution hooks (on-run-start, on-run-end)
//! that allow users to run custom SQL commands at specific pipeline stages.

use crate::Muscle;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Hooks {
    #[serde(default)]
    pub on_run_start: Vec<String>,
    #[serde(default)]
    pub on_run_end: Vec<String>,
}

impl Hooks {
    pub fn load(root: &Path) -> Self {
        let hooks_path = root.join("hooks.yaml");
        if !hooks_path.exists() {
            return Self::default();
        }

        let content = fs::read_to_string(hooks_path).unwrap_or_default();
        serde_yml::from_str(&content).unwrap_or_default()
    }

    pub async fn run_start(&self, muscle: &Muscle) -> Result<()> {
        for sql in &self.on_run_start {
            muscle.execute(sql).await?;
        }
        Ok(())
    }

    pub async fn run_end(&self, muscle: &Muscle) -> Result<()> {
        for sql in &self.on_run_end {
            muscle.execute(sql).await?;
        }
        Ok(())
    }
}
