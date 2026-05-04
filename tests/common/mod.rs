#[allow(dead_code)]
pub struct TestEnv {
    pub state_store: titan_engine::StateStore,
    pub db_dir: tempfile::TempDir,
}

impl Default for TestEnv {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl TestEnv {
    pub fn new() -> Self {
        let db_dir = tempfile::tempdir().unwrap();
        let state_store = titan_engine::StateStore::open(db_dir.path()).unwrap();
        Self { state_store, db_dir }
    }
}
