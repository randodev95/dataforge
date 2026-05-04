//! # Pipeline Exposures
//! 
//! This module handles the loading and management of downstream exposures 
//! (e.g. dashboards, ML models) defined in YAML.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::fs;
use anyhow::Result;

#[derive(Debug, Serialize, Deserialize)]
pub struct Exposure {
    pub name: String,
    #[serde(rename = "type")]
    pub exposure_type: String,
    pub owner: String,
    pub depends_on: Vec<String>,
}

pub struct Exposures {
    pub items: Vec<Exposure>,
}

impl Exposures {
    pub fn load(root: &Path) -> Result<Self> {
        let mut items = Vec::new();
        let exposures_dir = root.join("exposures");
        if exposures_dir.exists() {
            for entry in fs::read_dir(exposures_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "yml" || ext == "yaml") {
                    let content = fs::read_to_string(path)?;
                    let exposure: Exposure = serde_yml::from_str(&content)?;
                    items.push(exposure);
                }
            }
        }
        Ok(Self { items })
    }
}
