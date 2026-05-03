use std::path::PathBuf;
use tempfile::TempDir;
use titan_engine::StateStore;

pub struct TestEnv {
    pub db_dir: TempDir,
    pub state_store: StateStore,
}

impl TestEnv {
    pub fn new() -> Self {
        let db_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let state_store = StateStore::open(db_dir.path()).expect("Failed to open state store");
        Self {
            db_dir,
            state_store,
        }
    }
}

pub fn get_resource_path(name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/resources");
    path.push(name);
    path
}
