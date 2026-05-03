use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use anyhow::Result;
use std::path::Path;
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Exposure {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub description: Option<String>,
    pub depends_on: Vec<String>,
    pub owner: Option<HashMap<String, String>>,
}

pub struct Exposures {
    pub items: Vec<Exposure>,
}

impl Exposures {
    pub fn load(root: &Path) -> Result<Self> {
        let mut items = Vec::new();
        let exp_dir = root.join("exposures");
        if exp_dir.exists() {
            for entry in fs::read_dir(exp_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "yml" || ext == "yaml") {
                    let content = fs::read_to_string(&path)?;
                    let file_items: Vec<Exposure> = serde_yaml::from_str(&content)?;
                    items.extend(file_items);
                }
            }
        }
        Ok(Self { items })
    }
}
