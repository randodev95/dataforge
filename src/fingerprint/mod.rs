//! # Logic Fingerprinting
//!
//! This module handles the generation of logical hashes for SQL models,
//! accounting for dependencies and environment configurations.

pub mod hash;
pub mod normalize;
pub mod template;

pub use hash::{LogicHash, TitanHasher};
pub use normalize::Normalizer;
pub use template::TemplateEngine;

use crate::core::TitanSQL;
use crate::error::Result;
use minijinja::Value;
use regex::Regex;
use std::collections::HashMap;

pub struct Fingerprinter {
    engine: TemplateEngine,
}

impl Fingerprinter {
    pub fn new(project_root: &std::path::Path) -> Self {
        Self {
            engine: TemplateEngine::new(project_root),
        }
    }

    pub fn fingerprint(
        &self,
        raw_sql: &str,
        env_name: &str,
        config: &HashMap<String, Value>,
        parent_hashes: &[LogicHash],
        this_model: &str,
        is_inc: bool,
        vars: &HashMap<String, serde_yml::Value>,
    ) -> Result<(TitanSQL, LogicHash)> {
        // Calculate hash based on ORIGINAL config to be environment-agnostic where possible
        let config_json = serde_json::to_string(config)
            .map_err(|e| crate::error::TitanError::ProjectLoadError(e.to_string()))?;

        let mut context = config.clone();
        context.insert("titan_env".to_string(), Value::from(env_name));
        context.insert("vars".to_string(), Value::from_serialize(vars));

        let rendered_sql = self
            .engine
            .render(raw_sql, &context, this_model, env_name, is_inc)
            .map_err(|e| crate::error::TitanError::TemplateError(e.to_string()))?;

        // Strip config block before normalization
        static CONFIG_STRIP_RE: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
            Regex::new(r"(?s)\{\{\s*config\s*\(.*?\)\s*\}\}")
                .expect("Titan: internal regex failure (CONFIG_STRIP_RE)")
        });
        let stripped_sql = CONFIG_STRIP_RE.replace_all(&rendered_sql, "");

        let normalized_sql = Normalizer::normalize(&stripped_sql)
            .map_err(|e| crate::error::TitanError::SqlParseError(e.to_string()))?;

        let hash =
            TitanHasher::calculate(normalized_sql.as_str(), &config_json, parent_hashes, is_inc);

        Ok((normalized_sql, hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::path::Path;

    proptest! {
        #[test]
        fn test_fingerprint_idempotency(s in "\\PC*") {
            let fp = Fingerprinter::new(Path::new("."));
            let config = HashMap::new();
            let vars = HashMap::new();
            let parent_hashes = vec![];

            let res1 = fp.fingerprint(&s, "dev", &config, &parent_hashes, "test", false, &vars);
            let res2 = fp.fingerprint(&s, "dev", &config, &parent_hashes, "test", false, &vars);

            if let (Ok((_, h1)), Ok((_, h2))) = (res1, res2) {
                prop_assert_eq!(h1, h2);
            }
        }
    }
}
