mod common;
use common::TestEnv;
use titan_engine::{LogicHash, ModelMetadata};

#[test]
fn test_metadata_roundtrip() {
    let env = TestEnv::new();
    let hash = LogicHash::new("test_hash".to_string());
    let metadata = ModelMetadata {
        status: "success".to_string(),
        materialization_path: "path/to/data".to_string(),
        created_at: 100,
    };

    env.state_store.put_metadata("dev", "model", &hash, &metadata).unwrap();
    let retrieved = env.state_store.get_metadata(&hash).unwrap().unwrap();
    assert_eq!(retrieved.status, "success");
    
    let retrieved_hash = env.state_store.get_hash_by_name("dev", "model").unwrap().unwrap();
    assert_eq!(retrieved_hash, hash);
}

#[test]
fn test_missing_keys_return_none() {
    let env = TestEnv::new();
    let hash = LogicHash::new("none".to_string());
    assert!(env.state_store.get_metadata(&hash).unwrap().is_none());
}
