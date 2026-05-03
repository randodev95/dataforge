use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::fs;
use anyhow::Result;
use regex::Regex;
use once_cell::sync::Lazy;

static REF_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r#"\{\{\s*ref\(['"]([^'"]+)['"]\)\s*\}\}"#).unwrap());
static SOURCE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r#"\{\{\s*source\(['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\)\s*\}\}"#).unwrap());

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub name: String,
    pub storage: String, // e.g. "rocksdb"
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExternalModel {
    pub name: String,
    pub columns: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ModelFile {
    pub name: String,
    pub path: PathBuf,
    pub raw_sql: String,
    pub dependencies: Vec<String>,
}

pub struct Project {
    pub root: PathBuf,
    pub config: ProjectConfig,
    pub models: Vec<ModelFile>,
    pub seeds: Vec<PathBuf>,
}

impl Project {
    pub fn load(root: &Path) -> Result<Self> {
        let config_path = root.join("config.yaml");
        let config_content = fs::read_to_string(config_path)?;
        let config: ProjectConfig = serde_yaml::from_str(&config_content)?;

        let mut models = Vec::new();
        let models_dir = root.join("models");
        if models_dir.exists() {
            for entry in fs::read_dir(models_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "sql") {
                    let raw_sql = fs::read_to_string(&path)?;
                    let name = path.file_stem().unwrap().to_str().unwrap().to_string();
                    
                    // Infer dependencies (with dedup)
                    let mut dependencies = HashSet::new();
                    for cap in REF_RE.captures_iter(&raw_sql) {
                        dependencies.insert(cap[1].to_string());
                    }
                    // Also infer from source()
                    for cap in SOURCE_RE.captures_iter(&raw_sql) {
                        // For sources, we treat the table name as the dependency name in the demo
                        // In a real system, this would be a source-prefixed name
                        dependencies.insert(cap[2].to_string());
                    }
                    
                    models.push(ModelFile {
                        name,
                        path,
                        raw_sql,
                        dependencies: dependencies.into_iter().collect(),
                    });
                }
            }
        }

        let mut seeds = Vec::new();
        let seeds_dir = root.join("seeds");
        if seeds_dir.exists() {
            for entry in fs::read_dir(seeds_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "csv") {
                    seeds.push(path);
                }
            }
        }

        Ok(Self {
            root: root.to_path_buf(),
            config,
            models,
            seeds,
        })
    }

    pub fn filter_models(&self, pattern: &str) -> Vec<ModelFile> {
        let mut selected = HashSet::new();
        let name_to_model: HashMap<String, &ModelFile> = self.models.iter().map(|m| (m.name.clone(), m)).collect();

        if pattern.starts_with('+') {
            // +model (Ancestors)
            let target = &pattern[1..];
            if let Some(model) = name_to_model.get(target) {
                self.collect_ancestors(model, &name_to_model, &mut selected);
            }
        } else if pattern.ends_with('+') {
            // model+ (Descendants)
            let target = &pattern[..pattern.len() - 1];
            if let Some(model) = name_to_model.get(target) {
                self.collect_descendants(model, &name_to_model, &mut selected);
            }
        } else {
            // exact match
            if let Some(model) = name_to_model.get(pattern) {
                selected.insert(model.name.clone());
            }
        }

        self.models.iter()
            .filter(|m| selected.contains(&m.name))
            .cloned()
            .collect()
    }

    fn collect_ancestors(&self, model: &ModelFile, map: &HashMap<String, &ModelFile>, selected: &mut HashSet<String>) {
        if selected.contains(&model.name) { return; }
        selected.insert(model.name.clone());
        for dep in &model.dependencies {
            if let Some(parent) = map.get(dep) {
                self.collect_ancestors(parent, map, selected);
            }
        }
    }

    fn collect_descendants(&self, model: &ModelFile, map: &HashMap<String, &ModelFile>, selected: &mut HashSet<String>) {
        if selected.contains(&model.name) { return; }
        selected.insert(model.name.clone());
        for other in map.values() {
            if other.dependencies.contains(&model.name) {
                self.collect_descendants(other, map, selected);
            }
        }
    }
}
