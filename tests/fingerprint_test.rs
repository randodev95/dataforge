use std::collections::HashMap;
use titan_engine::Fingerprinter;
use titan_engine::fingerprint::hash::TitanHasher;
use titan_engine::fingerprint::normalize::Normalizer;

#[test]
fn test_hashing_determinism() {
    let sql1 = "SELECT a, b FROM users -- comment";
    let sql2 = "SELECT\n  a,\n  b\nFROM users";

    let norm1 = Normalizer::normalize(sql1).unwrap();
    let norm2 = Normalizer::normalize(sql2).unwrap();

    assert_eq!(
        norm1.as_str(),
        norm2.as_str(),
        "Normalized SQL strings should match"
    );

    let mut hasher1 = TitanHasher::default();
    hasher1.update(norm1.as_str());
    let hash1 = hasher1.finalize();

    let mut hasher2 = TitanHasher::default();
    hasher2.update(norm2.as_str());
    let hash2 = hasher2.finalize();

    assert_eq!(
        hash1, hash2,
        "Stylistic changes should not change the semantic hash"
    );
}

#[test]
fn test_logic_change_changes_hash() {
    let fingerprinter = Fingerprinter::new(std::path::Path::new("."));
    let config = HashMap::new();
    let parent_hashes = vec![];
    let env = "prod";

    let sql1 = "SELECT a, b FROM users";
    let sql2 = "SELECT a, b, c FROM users";

    let (_, hash1) = fingerprinter
        .fingerprint(sql1, env, &config, &parent_hashes, "test1", false)
        .unwrap();
    let (_, hash2) = fingerprinter
        .fingerprint(sql2, env, &config, &parent_hashes, "test2", false)
        .unwrap();

    assert_ne!(
        hash1, hash2,
        "Logic changes should change the semantic hash"
    );
}

#[test]
fn test_template_rendering() {
    let fingerprinter = Fingerprinter::new(std::path::Path::new("."));
    let config = HashMap::new();
    let parent_hashes = vec![];
    let env = "prod";

    let sql = "SELECT * FROM {{ ref('stg_users') }}";
    let (rendered, _) = fingerprinter
        .fingerprint(sql, env, &config, &parent_hashes, "test", false)
        .unwrap();

    // With env=prod, ref('stg_users') should render as prod_stg_users
    assert!(
        rendered.as_str().contains("prod_stg_users"),
        "Template should render ref() with environment prefix"
    );
}
