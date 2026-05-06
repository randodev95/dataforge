//! # Environment Profiles
//!
//! This module handles environment-specific configurations (e.g. dev, prod),
//! including schema prefixes, target execution types, and credentials.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Configuration for a specific environment target.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProfileTarget {
    pub prefix: String,
    #[serde(default = "default_target_type")]
    pub target_type: String, // "local" (default), "adbc"
    pub driver: Option<String>,
    pub uri: Option<String>,
    #[serde(default)]
    pub credentials: HashMap<String, String>,
}

fn default_target_type() -> String {
    "local".to_string()
}

/// A collection of named environment profiles.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Profiles {
    #[serde(flatten)]
    pub targets: HashMap<String, ProfileTarget>,
}

impl Profiles {
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let profiles: Profiles = serde_yml::from_str(&content)?;
        Ok(profiles)
    }

    pub fn get_target(&self, name: &str) -> Option<&ProfileTarget> {
        self.targets.get(name)
    }
}
