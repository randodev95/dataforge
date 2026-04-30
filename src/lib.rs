use std::collections::{HashMap, HashSet};
use starlark::environment::GlobalsBuilder;
use starlark::eval::Evaluator;
use starlark::syntax::{AstModule, Dialect as StarlarkDialect};
use std::sync::{Arc, RwLock};
use sqlparser::dialect::*;
use sqlparser::parser::Parser;
use sqlparser::ast::{Statement, SetExpr, SelectItem, Expr, VisitMut, VisitorMut, ObjectName, ObjectNamePart};
use sha2::{Sha256, Digest};
use notify::{Watcher as NotifyWatcher, RecursiveMode, RecommendedWatcher};
use std::path::Path;
use async_trait::async_trait;
use std::ops::ControlFlow;
use std::str::FromStr;

pub mod error;
pub mod types;
pub mod starlark_dsl;
pub mod db;
pub mod tui;
pub mod project;
pub mod parser;

use crate::error::{DataForgeError, Result};
use crate::types::{Model, ModelName, EnvName, DagHash};
use crate::starlark_dsl::{dataforge_globals, StarlarkContext};

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
}

#[derive(Clone)]
pub struct Engine {
    context: Arc<EngineContext>,
}

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn save_models(&self, env: &EnvName, models: &HashMap<ModelName, Model>) -> Result<()>;
    async fn load_models(&self, env: &EnvName) -> Result<HashMap<ModelName, Model>>;
}

#[async_trait]
pub trait WarehouseConnector: Send + Sync {
    async fn execute(&self, sql: &str) -> Result<()>;
    async fn fetch_columns(&self, table: &str) -> Result<Vec<String>>;
}

impl Engine {
    pub fn new() -> Self {
        Self::with_dialect(TargetDialect::Generic)
    }

    pub fn with_dialect(dialect: TargetDialect) -> Self {
        Self { 
            context: Arc::new(EngineContext {
                environments: RwLock::new(HashMap::new()),
                watermarks: RwLock::new(HashMap::new()),
                dialect,
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

    pub fn register_model(&mut self, env: &EnvName, source: &str) -> Result<()> {
        let ast = AstModule::parse("model.star", source.to_string(), &StarlarkDialect::Standard)
            .map_err(|e| DataForgeError::StarlarkError(e.to_string()))?;
        let globals = GlobalsBuilder::new().with(dataforge_globals).build();
        let module = starlark::environment::Module::new();
        let starlark_ctx = StarlarkContext {
            refs: std::cell::RefCell::new(vec![]),
            engine: self.clone(),
            env: env.clone(),
            sql_body: None,
        };
        let mut eval = Evaluator::new(&module);
        eval.extra = Some(&starlark_ctx);
        eval.eval_module(ast, &globals).map_err(|e| DataForgeError::StarlarkError(e.to_string()))?;
        Ok(())
    }

    pub fn load_project(&mut self, project: &project::Project, env: &EnvName) -> Result<()> {
        let mut globals_builder = GlobalsBuilder::new().with(dataforge_globals);
        
        let macro_module = starlark::environment::Module::new();
        let base_globals = GlobalsBuilder::new().with(dataforge_globals).build();
        
        let macros = project.discover_macros();
        let combined_macros = macros.iter()
            .map(|p| std::fs::read_to_string(p))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .join("\n");
        
        if !combined_macros.is_empty() {
            let ast = AstModule::parse("macros.stark", combined_macros, &StarlarkDialect::Standard)
                .map_err(|e| DataForgeError::StarlarkError(e.to_string()))?;
            let mut eval = Evaluator::new(&macro_module);
            eval.eval_module(ast, &base_globals).map_err(|e| DataForgeError::StarlarkError(e.to_string()))?;
        }
        
        let frozen_macros = macro_module.freeze().map_err(|e| DataForgeError::StarlarkError(format!("{:?}", e)))?;
        for name in frozen_macros.names() {
            let value = frozen_macros.get(name.as_str()).map_err(|e| DataForgeError::StarlarkError(e.to_string()))?;
            globals_builder.set(name.as_str(), value);
        }
        
        let globals = globals_builder.build();

        let models = project.discover_models();
        for path in models {
            let content = std::fs::read_to_string(&path)?;
            let parsed = parser::parse_sql_file(&content)?;
            
            let ast = AstModule::parse(&path.to_string_lossy(), parsed.header, &StarlarkDialect::Standard)
                .map_err(|e| DataForgeError::StarlarkError(e.to_string()))?;
            let module = starlark::environment::Module::new();
            let starlark_ctx = StarlarkContext {
                refs: std::cell::RefCell::new(vec![]),
                engine: self.clone(),
                env: env.clone(),
                sql_body: Some(parsed.body),
            };
            let mut eval = Evaluator::new(&module);
            eval.extra = Some(&starlark_ctx);
            eval.eval_module(ast, &globals).map_err(|e| DataForgeError::StarlarkError(e.to_string()))?;
        }
        Ok(())
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

pub struct Watcher {
    _engine: Engine,
    _watcher: RecommendedWatcher,
}

impl Watcher {
    pub fn new(engine: Engine, path: &str) -> Result<Self> {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher = RecommendedWatcher::new(tx, notify::Config::default())
            .map_err(|e| DataForgeError::Other(e.into()))?;
        watcher.watch(Path::new(path), RecursiveMode::Recursive)
            .map_err(|e| DataForgeError::Other(e.into()))?;
        let mut engine_clone = engine.clone();
        std::thread::spawn(move || {
            for res in rx {
                if let Ok(event) = res {
                    if let notify::EventKind::Modify(_) = event.kind {
                        for p in event.paths {
                            if let Ok(content) = std::fs::read_to_string(&p) {
                                if let Err(e) = engine_clone.register_model(&EnvName("dev".to_string()), &content) {
                                    eprintln!("DataForge Watcher Error: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        });
        Ok(Self { _engine: engine, _watcher: watcher })
    }
}

pub struct Scheduler;

struct TableRewriter<'a> {
    mapping: &'a HashMap<ModelName, String>,
}

impl<'a> VisitorMut for TableRewriter<'a> {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        for part in &mut relation.0 {
            if let ObjectNamePart::Identifier(ident) = part {
                if let Some(new_name) = self.mapping.get(&ModelName(ident.value.clone())) {
                    ident.value = new_name.clone();
                }
            }
        }
        ControlFlow::Continue(())
    }
}

impl Scheduler {
    pub async fn run_plan<C: WarehouseConnector>(&self, engine: &Engine, plan: Plan, connector: &C) -> Result<()> {
        let mut mapping = HashMap::new();
        let envs = engine.get_environments();
        if let Some(models) = envs.get(&plan.target_env) {
            for m in models {
                let hash = engine.get_hash(&plan.target_env, &m.name)?;
                mapping.insert(m.name.clone(), format!("model__{}", hash.0));
            }
        }
        for action in &plan.actions {
            match action {
                Action::Update(m, hash, _) => {
                    mapping.insert(m.name.clone(), format!("model__{}", hash.0));
                }
                Action::Remove(_) => {}
            }
        }

        for action in plan.actions {
            match action {
                Action::Update(m, hash, query) => {
                    if m.inferred_columns.contains(&"*".to_string()) {
                        let _cols = connector.fetch_columns(&m.name.0).await.unwrap_or_default();
                    }

                    let dialect = engine.context.dialect;
                    let mut ast = Parser::parse_sql(&*dialect.to_sql_dialect(), &query)
                        .map_err(|e| DataForgeError::SqlParseError(e.to_string()))?;
                    let mut rewriter = TableRewriter { mapping: &mapping };
                    for stmt in &mut ast {
                        let _ = stmt.visit(&mut rewriter);
                    }
                    let sql = format!("CREATE OR REPLACE TABLE model__{} AS {}", hash.0, ast[0]);
                    connector.execute(&sql).await?;
                }
                Action::Remove(_) => {}
            }
        }
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_topo_sort_order() {
        let mut engine = Engine::new();
        let dev = EnvName("dev".to_string());
        let prod = EnvName("prod".to_string());
        engine.register_model(&dev, "model(name='parent', query='SELECT 1')").unwrap();
        engine.register_model(&dev, "model(name='child', query='SELECT * FROM ' + ref('parent'))").unwrap();
        
        let plan = engine.plan(&dev, &prod).unwrap();
        if let Action::Update(m0, _, _) = &plan.actions[0] {
            assert_eq!(m0.name.0, "parent");
        }
    }
}
