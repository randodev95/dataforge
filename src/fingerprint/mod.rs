pub mod template;
pub mod normalize;
pub mod hash;

pub use template::TemplateEngine;
pub use normalize::Normalizer;
pub use hash::{TitanHasher, LogicHash};

use crate::core::TitanSQL;
use anyhow::Result;
use std::collections::HashMap;
use minijinja::Value;

pub struct Fingerprinter {
    engine: TemplateEngine,
}

impl Fingerprinter {
    pub fn new() -> Self {
        Self {
            engine: TemplateEngine::new(),
        }
    }

    pub fn fingerprint(
        &self,
        raw_sql: &str,
        env_name: &str,
        config: &HashMap<String, Value>,
        parent_hashes: &[LogicHash],
    ) -> Result<(TitanSQL, LogicHash)> {
        let mut context = config.clone();
        context.insert("titan_env".to_string(), Value::from(env_name));
        let rendered_sql = self.engine.render(raw_sql, &context)?;

        let normalized_sql = Normalizer::normalize(&rendered_sql)?;

        let config_json = serde_json::to_string(config)?;
        let hash = TitanHasher::calculate(normalized_sql.as_str(), &config_json, parent_hashes);

        Ok((normalized_sql, hash))
    }
}
