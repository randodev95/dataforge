mod common;
use common::TestEnv;
use titan_engine::LogicHash;
use titan_engine::ModelMetadata;

#[test]
fn test_state_persistence() {
    let env = TestEnv::new();
    let hash = LogicHash::new("test_hash".to_string());
    let name = "test_model";
    let env_name = "prod";
    
    let metadata = ModelMetadata {
        status: "success".to_string(),
        materialization_path: "public.test".to_string(),
        created_at: 123456789,
    };

    // Store
    env.state_store.put_metadata(env_name, name, &hash, &metadata).unwrap();

    // Retrieve by hash
    let retrieved = env.state_store.get_metadata(&hash).unwrap().expect("Metadata should exist");
    assert_eq!(retrieved, metadata);

    // Retrieve hash by name
    let retrieved_hash = env.state_store.get_hash_by_name(env_name, name).unwrap().expect("Hash should exist");
    assert_eq!(retrieved_hash, hash);
}

#[test]
fn test_missing_data() {
    let env = TestEnv::new();
    let hash = LogicHash::new("missing".to_string());
    
    let result = env.state_store.get_metadata(&hash).unwrap();
    assert!(result.is_none());
}
