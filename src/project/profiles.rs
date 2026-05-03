use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use anyhow::Result;
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProfileTarget {
    pub prefix: String,
    #[serde(default)]
    pub credentials: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Profiles {
    #[serde(flatten)]
    pub targets: HashMap<String, ProfileTarget>,
}

impl Profiles {
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let profiles: Profiles = serde_yaml::from_str(&content)?;
        Ok(profiles)
    }

    pub fn get_target(&self, name: &str) -> Option<&ProfileTarget> {
        self.targets.get(name)
    }
}
