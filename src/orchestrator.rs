use crate::error::Result;
use crate::types::{Model, EnvName};
use std::sync::Arc;
use tokio::sync::RwLock;
use notify::{Watcher, RecursiveMode, RecommendedWatcher};
use std::path::Path;

pub enum PipelineStep {
    ExpandMacros,
    ValidateLogic,
    PlanExecution,
    ApplyState,
    Preview,
}

#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    pub watch_mode: bool,
    pub preview_enabled: bool,
}

pub struct Orchestrator {
    config: OrchestratorConfig,
    starlark: crate::macros::starlark::StarlarkEngine,
    sdf: crate::bridge::sdf::SdfBridge,
    rocky: crate::bridge::rocky::RockyBridge,
    wasm: crate::plugins::wasm::WasmRuntime,
}

impl Orchestrator {
    pub fn new(config: OrchestratorConfig, project_root: std::path::PathBuf) -> Self {
        Self { 
            config,
            starlark: crate::macros::starlark::StarlarkEngine::new(),
            sdf: crate::bridge::sdf::SdfBridge::new(project_root.clone()),
            rocky: crate::bridge::rocky::RockyBridge::new(project_root),
            wasm: crate::plugins::wasm::WasmRuntime::new().unwrap(),
        }
    }

    pub fn watch(&self, path: &Path) -> Result<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher = RecommendedWatcher::new(tx, notify::Config::default())
            .map_err(|e| crate::error::DataForgeError::Other(e.into()))?;

        watcher.watch(path, RecursiveMode::Recursive)
            .map_err(|e| crate::error::DataForgeError::Other(e.into()))?;

        println!("Orchestrator: Watching for changes in {:?}", path);

        for res in rx {
            match res {
                Ok(event) => {
                    if let notify::EventKind::Modify(_) = event.kind {
                        for p in event.paths {
                            if p.extension().map_or(false, |ext| ext == "sql" || ext == "star") {
                                // Trigger pipeline in a new thread/task
                                let file_str = p.to_string_lossy().to_string();
                                // For now, blocking call for simplicity
                                let _ = self.process_change(&file_str);
                            }
                        }
                    }
                }
                Err(e) => println!("Watch error: {:?}", e),
            }
        }
        Ok(())
    }

    pub async fn process_change(&self, file_path: &str) -> Result<()> {
        let path = Path::new(file_path);
        if !path.exists() {
            return Err(crate::error::DataForgeError::Other(anyhow::anyhow!("File not found: {}", file_path)));
        }

        let content = std::fs::read_to_string(path)?;
        
        // Phase 1: Macro Expansion
        let expanded_sql = self.starlark.expand_sql(&content)?;
        println!("Orchestrator: SQL expanded for {}", file_path);
        
        // Phase 2: Logic Validation (SDF)
        self.sdf.compile()?;
        
        // Phase 3: Execution Planning (Rocky)
        let _plan = self.rocky.plan_deployment("dev")?;
        
        // Phase 4: WASM Validation (Optional)
        // Future: trigger based on metadata
        
        if self.config.preview_enabled {
            println!("Orchestrator: Previewing data for {}", file_path);
        }

        Ok(())
    }

    async fn run_step(&self, step: PipelineStep) -> Result<()> {
        match step {
            PipelineStep::ExpandMacros => {
                println!("Orchestrator: Expanding macros...");
                Ok(())
            }
            PipelineStep::ValidateLogic => {
                self.sdf.compile()
            }
            PipelineStep::PlanExecution => {
                self.rocky.plan_deployment("dev").map(|_| ())
            }
            PipelineStep::ApplyState => {
                self.rocky.apply_deployment(vec![])
            }
            PipelineStep::Preview => {
                println!("Orchestrator: Refreshing DuckDB preview...");
                Ok(())
            }
        }
    }
}
