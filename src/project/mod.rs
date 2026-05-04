//! # Project Management
//! 
//! This module handles the loading, parsing, and management of Titan projects, 
//! including `config.yaml`, `profiles.yml`, and `schema.yml`.

pub mod profiles;
pub mod exposures;
pub mod secrets;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::fs;
use crate::error::{TitanError, Result};
use std::sync::LazyLock;
use regex::Regex;

static REF_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"\{\{\s*ref\(['"]([^'"]+)['"]\)\s*\}\}"#).expect("Titan: internal regex failure (REF_RE)"));
static SOURCE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"\{\{\s*source\(['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\)\s*\}\}"#).expect("Titan: internal regex failure (SOURCE_RE)"));

static CONFIG_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"\{\{\s*config\s*\(\s*materialized\s*=\s*['"]([^'"]+)['"]\s*(?:,\s*unique_key\s*=\s*['"]([^'"]+)['"]\s*)?(?:,\s*on_schema_change\s*=\s*['"]([^'"]+)['"]\s*)?\)\s*\}\}"#).expect("Titan: internal regex failure (CONFIG_RE)"));

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    pub source_type: String, // e.g. "s3", "postgres", "adbc"
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub connection_string: Option<String>,
    pub driver: Option<String>, // For ADBC
}

impl SourceConfig {
    pub fn resolved_connection_string(&self, resolver: &impl secrets::SecretResolver) -> Result<Option<String>> {
        match &self.connection_string {
            Some(s) => Ok(Some(resolver.resolve(s)?)),
            None => Ok(None),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectConfig {
    pub name: String,
    pub storage: String,
    #[serde(default)]
    pub sources: HashMap<String, SourceConfig>,
    #[serde(default)]
    pub retention: Option<RetentionConfig>,
    #[serde(default)]
    pub vars: HashMap<String, serde_yml::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RetentionConfig {
    pub snapshots_days: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum OnSchemaChange {
    #[default]
    AppendOnly,
    Fail,
    Sync,
}

#[derive(Debug, Clone)]
pub struct ModelFile {
    pub name: String,
    pub path: PathBuf,
    pub raw_sql: String,
    pub dependencies: Vec<String>,
    pub materialization: crate::materialize::Materialization,
    pub unique_key: Option<String>,
    pub tests: Vec<ColumnTest>,
    pub on_schema_change: OnSchemaChange,
    pub contract_enforced: bool,
    pub columns: Vec<ModelColumn>,
}

#[derive(Debug, Clone)]
pub struct ModelColumn {
    pub name: String,
    pub data_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ColumnTest {
    pub column_name: String,
    pub test: YamlTest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum YamlTest {
    Simple(String),
    Complex(HashMap<String, serde_yml::Value>),
}

#[derive(Debug, Serialize, Deserialize)]
struct SchemaYaml {
    version: i32,
    models: Vec<SchemaModel>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SchemaConfig {
    #[serde(default)]
    pub enforced: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct SchemaModel {
    name: String,
    #[serde(default)]
    pub config: Option<SchemaConfig>,
    pub columns: Vec<SchemaColumn>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SchemaColumn {
    name: String,
    pub data_type: Option<String>,
    #[serde(default)]
    pub tests: Vec<YamlTest>,
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
        let config_content = fs::read_to_string(config_path)
            .map_err(TitanError::IoError)?;
        let config: ProjectConfig = serde_yml::from_str(&config_content)
            .map_err(|e| TitanError::ProjectLoadError(e.to_string()))?;

        let mut models = Vec::new();
        let mut schemas = HashMap::new();
        let mut contracts = HashMap::new();

        // Parse schema.yml if it exists
        let schema_path = root.join("models/schema.yml");
        if schema_path.exists() {
            let schema_content = fs::read_to_string(schema_path)?;
            let schema_yaml: SchemaYaml = serde_yml::from_str(&schema_content)
                .map_err(|e| TitanError::ProjectLoadError(e.to_string()))?;
            for m in schema_yaml.models {
                let mut col_tests = Vec::new();
                let mut model_cols = Vec::new();
                for c in &m.columns {
                    model_cols.push(ModelColumn {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                    });
                    for t in &c.tests {
                        col_tests.push(ColumnTest {
                            column_name: c.name.clone(),
                            test: t.clone(),
                        });
                    }
                }
                schemas.insert(m.name.clone(), col_tests);
                contracts.insert(m.name, (m.config.map(|c| c.enforced).unwrap_or(false), model_cols));
            }
        }

        let models_dir = root.join("models");
        if models_dir.exists() {
            // Second pass: parse SQL models
            for entry in fs::read_dir(models_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "sql") {
                    let raw_sql = fs::read_to_string(&path)?;
                    let name = path.file_stem()
                        .and_then(|s| s.to_str())
                        .ok_or_else(|| TitanError::ProjectLoadError(format!("Invalid model filename: {:?}", path)))?
                        .to_string();
                    
                    let mut dependencies = HashSet::new();
                    for cap in REF_RE.captures_iter(&raw_sql) {
                        dependencies.insert(cap[1].to_string());
                    }
                    for cap in SOURCE_RE.captures_iter(&raw_sql) {
                        dependencies.insert(cap[2].to_string());
                    }

                    let mut materialization_type = "view".to_string();
                    let mut unique_key = None;
                    let mut on_schema_change = OnSchemaChange::default();
                    
                    if let Some(cap) = CONFIG_RE.captures(&raw_sql) {
                        materialization_type = cap[1].to_string();
                        if let Some(k) = cap.get(2) {
                            unique_key = Some(k.as_str().to_string());
                        }
                        if let Some(s) = cap.get(3) {
                            on_schema_change = match s.as_str() {
                                "fail" => OnSchemaChange::Fail,
                                "sync" => OnSchemaChange::Sync,
                                _ => OnSchemaChange::AppendOnly,
                            };
                        }
                    }

                    let tests = schemas.get(&name).cloned().unwrap_or_default();
                    let (contract_enforced, columns) = contracts.get(&name).cloned().unwrap_or((false, Vec::new()));
                    use std::str::FromStr;
                    let materialization = crate::materialize::Materialization::from_str(&materialization_type).unwrap_or(crate::materialize::Materialization::View);

                    models.push(ModelFile {
                        name,
                        path,
                        raw_sql,
                        dependencies: dependencies.into_iter().collect(),
                        materialization,
                        unique_key,
                        tests,
                        on_schema_change,
                        contract_enforced,
                        columns,
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
                if path.extension().is_some_and(|ext| ext == "csv") {
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

    pub fn filter_models(&self, pattern: &str) -> Vec<&ModelFile> {
        let mut selected = HashSet::new();
        let name_to_model: HashMap<String, &ModelFile> = self.models.iter().map(|m| (m.name.clone(), m)).collect();

        if let Some(target) = pattern.strip_prefix('+') {
            // +model (Ancestors)
            if let Some(model) = name_to_model.get(target) {
                self.collect_ancestors(model, &name_to_model, &mut selected);
            }
        } else if let Some(target) = pattern.strip_suffix('+') {
            // model+ (Descendants)
            if let Some(model) = name_to_model.get(target) {
                self.collect_descendants(model, &mut selected);
            }
        } else {
            // exact match
            if let Some(model) = name_to_model.get(pattern) {
                selected.insert(model.name.clone());
            }
        }

        self.models.iter()
            .filter(|m| selected.contains(&m.name))
            .collect()
    }

    fn collect_ancestors(&self, model: &ModelFile, name_to_model: &HashMap<String, &ModelFile>, selected: &mut HashSet<String>) {
        if selected.insert(model.name.clone()) {
            for dep_name in &model.dependencies {
                if let Some(dep) = name_to_model.get(dep_name) {
                    self.collect_ancestors(dep, name_to_model, selected);
                }
            }
        }
    }

    fn collect_descendants(&self, model: &ModelFile, selected: &mut HashSet<String>) {
        if selected.insert(model.name.clone()) {
            // This is slow, but works for now. In a real system we'd build an adjacency list.
            for other in self.models.iter() {
                if other.dependencies.iter().any(|d| d == &model.name) {
                    self.collect_descendants(other, selected);
                }
            }
        }
    }

    pub fn filter_by_state(&self, prior: &crate::artifacts::Manifest) -> Vec<&ModelFile> {
        self.models.iter()
            .filter(|m| {
                if let Some(node) = prior.nodes.get(&m.name) {
                    node.raw_sql != m.raw_sql
                } else {
                    true // New model
                }
            })
            .collect()
    }
}
