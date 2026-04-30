use std::collections::{HashMap, HashSet};
use starlark::environment::GlobalsBuilder;
use starlark::eval::Evaluator;
use starlark::syntax::{AstModule, Dialect as StarlarkDialect};
use std::sync::{Arc, RwLock};
use sqlparser::dialect::*;
use sqlparser::parser::Parser;
use sqlparser::ast::{Statement, SetExpr, SelectItem, Expr, VisitMut, VisitorMut, ObjectName, ObjectNamePart, Query};
use sha2::{Sha256, Digest};
#[cfg(feature = "native")]
use notify::{Watcher as NotifyWatcher, RecursiveMode, RecommendedWatcher};
use std::path::Path;
use std::ops::ControlFlow;
use std::str::FromStr;
#[cfg(feature = "native")]
use tokio;
#[cfg(feature = "native")]
use async_trait::async_trait;

pub mod error;
pub mod types;
#[cfg(feature = "native")]
pub mod db;
#[cfg(feature = "native")]
pub mod tui;
pub mod project;
pub mod parser;

// DataForge 2.0 Core
pub mod orchestrator;
pub mod api;
pub mod bridge;
pub mod macros;
pub mod plugins;

use crate::error::{DataForgeError, Result};
use crate::types::{Model, ModelName, EnvName, DagHash};

#[derive(Debug, Clone, Copy, Default)]
pub enum TargetDialect {
    BigQuery, Snowflake, Redshift, ClickHouse, DuckDB, Hive, MsSql, MySql, PostgreSql, SQLite, 
    #[default]
    Generic,
}

impl TargetDialect {
    fn to_sql_dialect(self) -> Box<dyn Dialect> {
        match self {
            Self::Snowflake => Box::new(SnowflakeDialect),
            Self::PostgreSql => Box::new(PostgreSqlDialect {}),
            Self::SQLite => Box::new(SQLiteDialect {}),
            Self::MsSql => Box::new(MsSqlDialect {}),
            Self::BigQuery => Box::new(BigQueryDialect {}),
            Self::MySql => Box::new(MySqlDialect {}),
            Self::ClickHouse => Box::new(ClickHouseDialect {}),
            Self::Hive => Box::new(HiveDialect {}),
            Self::Redshift => Box::new(RedshiftSqlDialect {}),
            _ => Box::new(GenericDialect {}),
        }
    }
}

impl FromStr for TargetDialect {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "bigquery" => Ok(Self::BigQuery),
            "snowflake" => Ok(Self::Snowflake),
            "postgres" => Ok(Self::PostgreSql),
            "sqlite" => Ok(Self::SQLite),
            "duckdb" => Ok(Self::DuckDB),
            _ => Ok(Self::Generic),
        }
    }
}

#[derive(Default)]
struct EngineContext {
    environments: RwLock<HashMap<EnvName, HashMap<ModelName, Model>>>,
    watermarks: RwLock<HashMap<ModelName, String>>,
    dialect: TargetDialect,
    cache: RwLock<HashMap<String, crate::types::CacheItem>>,
}

#[derive(Clone)]
pub struct LogicalEngine {
    context: Arc<EngineContext>,
}

impl LogicalEngine {
    pub fn new() -> Self {
        Self::with_dialect(TargetDialect::Generic)
    }

    pub fn with_dialect(dialect: TargetDialect) -> Self {
        Self { 
            context: Arc::new(EngineContext {
                environments: RwLock::new(HashMap::new()),
                watermarks: RwLock::new(HashMap::new()),
                dialect,
                cache: RwLock::new(HashMap::new()),
            })
        }
    }

    pub fn dialect(&self) -> TargetDialect {
        self.context.dialect
    }

    pub fn internal_add_model(&self, env: &EnvName, model: Model) {
        let mut envs = self.context.environments.write().unwrap();
        envs.entry(env.clone()).or_default().insert(model.name.clone(), model);
    }

    pub fn get_environments(&self) -> HashMap<EnvName, Vec<Model>> {
        let envs = self.context.environments.read().unwrap();
        envs.iter()
            .map(|(name, models)| (name.clone(), models.values().cloned().collect()))
            .collect()
    }

    pub fn get_metadata(&self, env: &EnvName) -> Result<Vec<crate::types::ModelMetadata>> {
        let envs = self.context.environments.read().unwrap();
        let models = envs.get(env).ok_or_else(|| DataForgeError::EnvNotFound(env.0.clone()))?;
        
        let mut metadata = vec![];
        for (name, model) in models {
            let hash = self.get_hash(env, name)?;
            metadata.push(crate::types::ModelMetadata {
                name: name.clone(),
                hash,
                deps: model.deps.clone(),
                columns: model.inferred_columns.clone(),
                lineage: model.column_lineage.clone(),
            });
        }
        Ok(metadata)
    }

    pub fn extract_columns(&self, sql: &str, dialect: TargetDialect) -> Result<Vec<String>> {
        let ast = Parser::parse_sql(&*dialect.to_sql_dialect(), sql)
            .map_err(|e| DataForgeError::SqlParseError(e.to_string()))?;
        let mut cols = vec![];
        for stmt in ast {
            if let Statement::Query(q) = stmt {
                if let SetExpr::Select(s) = *q.body {
                    for item in s.projection {
                        match item {
                            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => { cols.push(ident.value); }
                            SelectItem::ExprWithAlias { alias, .. } => { cols.push(alias.value); }
                            SelectItem::Wildcard(_) => { cols.push("*".to_string()); }
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(cols)
    }

    pub fn extract_column_lineage(&self, sql: &str, dialect: TargetDialect, deps: &[ModelName]) -> Result<Vec<crate::types::ColumnLineage>> {
        let ast = Parser::parse_sql(&*dialect.to_sql_dialect(), sql)
            .map_err(|e| DataForgeError::SqlParseError(e.to_string()))?;
        let mut lineage = vec![];
        
        for stmt in ast {
            if let Statement::Query(q) = stmt {
                if let SetExpr::Select(s) = *q.body {
                    for item in s.projection {
                        let col_name = match &item {
                            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.value.clone()),
                            SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                            _ => None,
                        };
                        
                        if let Some(name) = col_name {
                            // Simple heuristic: if a column is selected, it comes from all referenced deps for now
                            // Future: parse Expr to find specific table refs
                            lineage.push(crate::types::ColumnLineage {
                                column: name,
                                source_models: deps.to_vec(),
                            });
                        }
                    }
                }
            }
        }
        Ok(lineage)
    }

    pub fn get_hash(&self, env: &EnvName, name: &ModelName) -> Result<DagHash> {
        let envs = self.context.environments.read().unwrap();
        let models = envs.get(env).ok_or_else(|| DataForgeError::EnvNotFound(env.0.clone()))?;
        let mut stack = HashSet::new();
        self.compute_model_hash_safe(models, name, &mut stack)
    }

    fn compute_model_hash_safe(
        &self, 
        models: &HashMap<ModelName, Model>, 
        name: &ModelName, 
        stack: &mut HashSet<ModelName>
    ) -> Result<DagHash> {
        if stack.contains(name) { return Err(DataForgeError::CycleDetected(name.0.clone())); }
        stack.insert(name.clone());
        
        let model = models.get(name).ok_or_else(|| DataForgeError::ModelNotFound(name.0.clone()))?;
        let mut hasher = Sha256::new();
        hasher.update(&model.name.0);
        hasher.update(&model.query);
        
        let mut dep_hashes: Vec<String> = model.deps.iter()
            .map(|dep| self.compute_model_hash_safe(models, dep, stack).map(|h| h.0))
            .collect::<Result<Vec<_>>>()?;
        
        dep_hashes.sort();
        for h in dep_hashes { hasher.update(h); }
        
        stack.remove(name);
        let result = hasher.finalize();
        Ok(DagHash(result.iter().map(|b| format!("{:02x}", b)).collect()))
    }

    pub fn plan(&self, from_env: &EnvName, to_env: &EnvName) -> Result<Plan> {
        let envs = self.context.environments.read().unwrap();
        let from_models = envs.get(from_env).cloned().unwrap_or_default();
        let to_models = envs.get(to_env).cloned().unwrap_or_default();
        
        let mut sorted_names = vec![];
        let mut visited = HashSet::new();
        let mut visiting = HashSet::new();
        
        for name in from_models.keys() {
            self.topo_sort(name, &from_models, &mut visited, &mut visiting, &mut sorted_names)?;
        }

        let mut actions = vec![];
        for name in sorted_names {
            let model = from_models.get(&name).unwrap();
            
            let from_hash = self.get_hash(from_env, &name)?;
            let to_hash = self.get_hash(to_env, &name).unwrap_or(DagHash("".to_string()));
            
            let mut final_query = model.query.clone();
            if let Some(ref wm_col) = model.watermark {
                let watermarks = self.context.watermarks.read().unwrap();
                if let Some(val) = watermarks.get(&name) {
                    final_query = format!("{} WHERE {} > {}", final_query, wm_col, val);
                }
            }

            if from_hash != to_hash || model.watermark.is_some() {
                actions.push(Action::Update(model.clone(), from_hash, final_query));
            }
        }

        for (name, model) in to_models {
            if !from_models.contains_key(&name) {
                actions.push(Action::Remove(model));
            }
        }

        Ok(Plan { actions, target_env: to_env.clone() })
    }

    fn topo_sort(
        &self,
        name: &ModelName,
        models: &HashMap<ModelName, Model>,
        visited: &mut HashSet<ModelName>,
        visiting: &mut HashSet<ModelName>,
        sorted: &mut Vec<ModelName>,
    ) -> Result<()> {
        if visited.contains(name) { return Ok(()); }
        if visiting.contains(name) { return Err(DataForgeError::CycleDetected(name.0.clone())); }
        
        visiting.insert(name.clone());
        if let Some(model) = models.get(name) {
            for dep in &model.deps {
                self.topo_sort(dep, models, visited, visiting, sorted)?;
            }
        }
        
        visiting.remove(name);
        visited.insert(name.clone());
        sorted.push(name.clone());
        Ok(())
    }

    #[cfg(feature = "native")]
    pub async fn publish<C: WarehouseConnector>(&self, env: &EnvName, connector: &C) -> Result<()> {
        let envs = self.context.environments.read().unwrap();
        if let Some(models) = envs.get(env) {
            for (name, _) in models {
                let hash = self.get_hash(env, name)?;
                let view_sql = format!("CREATE OR REPLACE VIEW {} AS SELECT * FROM model__{}", name.0, hash.0);
                connector.execute(&view_sql).await?;
            }
        }
        Ok(())
    }

    pub fn apply(&self, plan: Plan) -> Result<()> {
        let mut envs = self.context.environments.write().unwrap();
        let target_models = envs.entry(plan.target_env.clone()).or_default();
        for action in plan.actions {
            match action {
                Action::Update(m, _, _) => {
                    target_models.insert(m.name.clone(), m);
                }
                _ => {}
            }
        }
        Ok(())
    }
}

pub type Engine = LogicalEngine;

#[cfg(feature = "native")]
#[async_trait]
pub trait StateStore: Send + Sync {
    async fn save_models(&self, env: &EnvName, models: &HashMap<ModelName, Model>) -> Result<()>;
    async fn load_models(&self, env: &EnvName) -> Result<HashMap<ModelName, Model>>;
}

#[cfg(feature = "native")]
#[async_trait]
pub trait WarehouseConnector: Send + Sync {
    async fn execute(&self, sql: &str) -> Result<()>;
    async fn fetch_columns(&self, table: &str) -> Result<Vec<String>>;
    async fn estimate_cost(&self, sql: &str) -> Result<f64>;
}


#[derive(Debug, Clone)]
pub enum Action {
    Update(Model, DagHash, String),
    Remove(Model),
}

#[derive(Debug)]
pub struct Plan {
    pub actions: Vec<Action>,
    pub target_env: EnvName,
}

