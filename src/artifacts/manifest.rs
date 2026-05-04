use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::fs;
use anyhow::Result;
use crate::filler::dag::ModelTask;

#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestNode {
    pub name: String,
    pub raw_sql: String,
    pub dependencies: Vec<String>,
    pub materialization: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub metadata: ManifestMetadata,
    pub nodes: HashMap<String, ManifestNode>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestMetadata {
    pub project_name: String,
    pub generated_at: u64,
}

impl Manifest {
    pub fn generate(project_name: String, tasks: &[ModelTask]) -> Self {
        let mut nodes = HashMap::new();
        for task in tasks {
            nodes.insert(task.name.clone(), ManifestNode {
                name: task.name.clone(),
                raw_sql: task.raw_sql.clone(),
                dependencies: task.parent_names.clone(),
                materialization: format!("{:?}", task.materialization),
            });
        }

        Self {
            metadata: ManifestMetadata {
                project_name,
                generated_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            },
            nodes,
        }
    }

    pub fn load(project_root: &Path) -> Result<Self> {
        let path = project_root.join("target/manifest.json");
        let content = fs::read_to_string(path)?;
        let manifest = serde_json::from_str(&content)?;
        Ok(manifest)
    }

    pub fn save(&self, project_root: &Path) -> Result<()> {
        let target_dir = project_root.join("target");
        fs::create_dir_all(&target_dir)?;
        
        let json = serde_json::to_string_pretty(self)?;
        fs::write(target_dir.join("manifest.json"), json)?;
        Ok(())
    }
}
