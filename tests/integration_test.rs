use DataForge::orchestrator::{Orchestrator, OrchestratorConfig};
use std::fs;
use std::path::PathBuf;

#[tokio::test]
async fn test_orchestrator_basic_flow() {
    let project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let test_model = project_root.join("test_model_v2.sql");
    
    // Create temp model
    fs::write(&test_model, "SELECT 1 as id").unwrap();
    
    let config = OrchestratorConfig {
        watch_mode: false,
        preview_enabled: false,
    };
    
    let orch = Orchestrator::new(config, project_root.clone());
    unsafe { std::env::set_var("DATAFORGE_MOCK_SDF", "1"); }
    
    let res = orch.process_change(test_model.to_str().unwrap()).await;
    
    // Cleanup
    let _ = fs::remove_file(&test_model);
    
    assert!(res.is_ok(), "Orchestrator failed to process basic change: {:?}", res.err());
}
