use crate::fingerprint::{Fingerprinter, LogicHash};
use crate::filler::state::StateStore;
use crate::error::{TitanError, Result};
use crate::execution::Muscle;
use crate::materialize::VDE;
use std::collections::{HashMap, HashSet};
use minijinja::Value;
use std::sync::Arc;
use petgraph::graph::DiGraph;
use petgraph::algo::toposort;
use futures::stream::{FuturesUnordered, StreamExt};
use tracing::{info, debug, warn, error};

#[derive(Clone)]
pub struct ModelTask {
    pub name: String,
    pub env: String,
    pub raw_sql: String,
    pub config: HashMap<String, Value>,
    pub fingerprinter: Arc<Fingerprinter>,
    pub state_store: Arc<StateStore>,
    pub muscle: Arc<Muscle>,
    pub vde: Arc<VDE>,
    pub parent_names: Vec<String>,
    pub plan_only: bool,
}

impl ModelTask {
    pub async fn execute(&self) -> Result<LogicHash> {
        let mut parent_hashes = Vec::new();
        for name in &self.parent_names {
            if let Some(hash) = self.state_store.get_hash_by_name(&self.env, name).map_err(|e| TitanError::StateError(e.to_string()))? {
                parent_hashes.push(hash);
            } else if self.plan_only {
                parent_hashes.push(LogicHash::new("planned_parent".to_string()));
            } else {
                return Err(TitanError::DependencyNotFound(name.clone(), self.env.clone()));
            }
        }

        let (normalized_sql, hash) = self.fingerprinter.fingerprint(
            &self.raw_sql,
            &self.env,
            &self.config,
            &parent_hashes,
        ).map_err(|e| TitanError::TemplateError(e.to_string()))?;

        if let Some(metadata) = self.state_store.get_metadata(&hash).map_err(|e| TitanError::StateError(e.to_string()))? {
            if metadata.status == "success" {
                info!(model = %self.name, hash = %hash, "Smart Skip: Model is clean");
                
                if !self.plan_only {
                    self.vde.materialization_swap(&self.env, &self.name, &hash).await
                        .map_err(|e| TitanError::ExecutionError(e.to_string()))?;
                    
                    self.state_store.put_metadata(&self.env, &self.name, &hash, &metadata)
                        .map_err(|e| TitanError::StateError(e.to_string()))?;
                }
                
                return Ok(hash);
            }
        }

        if self.plan_only {
            info!(model = %self.name, hash = %hash, "Plan: Model will be executed");
            return Ok(hash);
        }

        info!(model = %self.name, hash = %hash, "Executing model in DataFusion");
        
        let table_name = format!("{}__{}", self.name, hash);
        
        let materialization_sql = format!("CREATE OR REPLACE VIEW {} AS {}", table_name, normalized_sql.as_str());
        self.muscle.execute(&materialization_sql).await
            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

        self.vde.materialization_swap(&self.env, &self.name, &hash).await
            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

        let metadata = crate::filler::state::ModelMetadata {
            status: "success".to_string(),
            materialization_path: format!("{}.{}", self.env, self.name),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| TitanError::ExecutionError(e.to_string()))?
                .as_secs(),
        };
        
        self.state_store.put_metadata(&self.env, &self.name, &hash, &metadata)
            .map_err(|e| TitanError::StateError(e.to_string()))?;
        self.state_store.put_value(&hash, normalized_sql.into_inner())
            .map_err(|e| TitanError::StateError(e.to_string()))?;

        Ok(hash)
    }
}

pub struct Filler {
    pub state_store: Arc<StateStore>,
    pub fingerprinter: Arc<Fingerprinter>,
}

impl Filler {
    pub fn new(state_store: StateStore) -> Self {
        Self {
            state_store: Arc::new(state_store),
            fingerprinter: Arc::new(Fingerprinter::new()),
        }
    }

    pub async fn run_dag(&self, tasks: Vec<ModelTask>) -> Result<()> {
        let mut graph = DiGraph::<&ModelTask, ()>::new();
        let mut name_to_node = HashMap::new();

        for task in &tasks {
            let idx = graph.add_node(task);
            name_to_node.insert(&task.name, idx);
        }

        for task in &tasks {
            let child_idx = name_to_node[&task.name];
            for parent_name in &task.parent_names {
                if let Some(&parent_idx) = name_to_node.get(parent_name) {
                    graph.add_edge(parent_idx, child_idx, ());
                }
            }
        }

        let _ = toposort(&graph, None).map_err(|e| {
            let node_idx = e.node_id();
            let name = graph[node_idx].name.clone();
            TitanError::CircularDependency(name)
        })?;

        let mut completed = HashSet::new();
        let mut in_progress = FuturesUnordered::new();
        let mut pending: HashSet<String> = tasks.iter().map(|t| t.name.clone()).collect();
        let name_to_task: HashMap<String, &ModelTask> = tasks.iter().map(|t| (t.name.clone(), t)).collect();

        loop {
            let ready_tasks: Vec<String> = pending.iter()
                .filter(|name| {
                    let task = name_to_task[*name];
                    task.parent_names.iter().all(|p| !name_to_task.contains_key(p) || completed.contains(p))
                })
                .cloned()
                .collect();

            for name in ready_tasks {
                pending.remove(&name);
                let task = name_to_task[&name].clone();
                in_progress.push(tokio::spawn(async move {
                    (task.name.clone(), task.execute().await)
                }));
            }

            if in_progress.is_empty() {
                if !pending.is_empty() {
                    return Err(TitanError::CircularDependency("Unresolved dependencies remained".to_string()));
                }
                break;
            }

            if let Some(res) = in_progress.next().await {
                let (name, result) = res.map_err(|e| TitanError::ExecutionError(e.to_string()))?;
                result?; // Propagate task error
                completed.insert(name);
            }
        }

        Ok(())
    }
}
