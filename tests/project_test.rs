use DataForge::project::Project;
use tempfile::tempdir;

#[test]
fn test_project_init() {
    let tmp = tempdir().unwrap();
    let path = tmp.path();
    Project::init(path).expect("Init failed");
    
    assert!(path.join("dataforge.yaml").exists());
    assert!(path.join("models/bronze").is_dir());
    assert!(path.join("models/silver").is_dir());
    assert!(path.join("models/gold").is_dir());
    assert!(path.join("macros").is_dir());
}

#[test]
fn test_project_discovery() {
    let tmp = tempdir().unwrap();
    let path = tmp.path();
    Project::init(path).unwrap();
    
    std::fs::write(path.join("models/bronze/m1.sql"), "---\nmodel(name='m1')\n---\nSELECT 1").unwrap();
    std::fs::write(path.join("models/silver/m2.sql"), "---\nmodel(name='m2')\n---\nSELECT 2").unwrap();
    
    let proj = Project::load(path).unwrap();
    let models = proj.discover_models();
    assert_eq!(models.len(), 2);
}

#[test]
fn test_engine_load_project() {
    let tmp = tempdir().unwrap();
    let path = tmp.path();
    Project::init(path).unwrap();
    
    let content = "---\nmodel(name='m1', watermark='ts', columns=['id', 'val'])\n---\nSELECT id, val, ts FROM table";
    std::fs::write(path.join("models/bronze/m1.sql"), content).unwrap();
    
    let mut engine = DataForge::Engine::new();
    let proj = Project::load(path).unwrap();
    engine.load_project(&proj, &DataForge::types::EnvName("dev".to_string())).expect("Load project failed");
    
    let envs = engine.get_environments();
    let dev_models = envs.get(&DataForge::types::EnvName("dev".to_string())).unwrap();
    assert_eq!(dev_models.len(), 1);
    assert_eq!(dev_models[0].name.0, "m1");
    assert_eq!(dev_models[0].watermark, Some("ts".to_string()));
}

#[test]
fn test_macro_usage() {
    let tmp = tempdir().unwrap();
    let path = tmp.path();
    Project::init(path).unwrap();
    
    std::fs::write(path.join("macros/util.stark"), "def my_macro(n):\n  return n + '_suff'").unwrap();
    
    let content = "---\nmodel(name=my_macro('m1'))\n---\nSELECT 1";
    std::fs::write(path.join("models/bronze/m1.sql"), content).unwrap();
    
    let mut engine = DataForge::Engine::new();
    let proj = Project::load(path).unwrap();
    engine.load_project(&proj, &DataForge::types::EnvName("dev".to_string())).expect("Load project failed");
    
    let envs = engine.get_environments();
    let dev_models = envs.get(&DataForge::types::EnvName("dev".to_string())).unwrap();
    assert_eq!(dev_models[0].name.0, "m1_suff");
}

#[test]
fn test_engine_plan_removals() {
    let mut engine = DataForge::Engine::new();
    let dev = DataForge::types::EnvName("dev".to_string());
    let prod = DataForge::types::EnvName("prod".to_string());
    
    // Register in prod but not dev
    engine.register_model(&prod, "model(name='old_model', query='SELECT 1')").unwrap();
    
    // Plan from dev (empty) to prod
    let plan = engine.plan(&dev, &prod).unwrap();
    
    assert_eq!(plan.actions.len(), 1);
    match &plan.actions[0] {
        DataForge::Action::Remove(m) => assert_eq!(m.name.0, "old_model"),
        _ => panic!("Expected Remove action"),
    }
}

#[test]
fn test_macro_cross_file_dependency() {
    let tmp = tempdir().unwrap();
    let path = tmp.path();
    Project::init(path).unwrap();
    
    // b.stark defines helper
    std::fs::write(path.join("macros/b.stark"), "def helper(n):\n  return n + '_ok'").unwrap();
    // a.stark uses helper
    std::fs::write(path.join("macros/a.stark"), "def main_macro(n):\n  return helper(n)").unwrap();
    
    let content = "---\nmodel(name=main_macro('m1'))\n---\nSELECT 1";
    std::fs::write(path.join("models/m1.sql"), content).unwrap();
    
    let mut engine = DataForge::Engine::new();
    let proj = Project::load(path).unwrap();
    engine.load_project(&proj, &DataForge::types::EnvName("dev".to_string())).expect("Load project failed");
    
    let envs = engine.get_environments();
    let dev_models = envs.get(&DataForge::types::EnvName("dev".to_string())).unwrap();
    assert_eq!(dev_models[0].name.0, "m1_ok");
}
