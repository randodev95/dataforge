//! # Shadow Deployments
//!
//! Implements target rewriting for running pipelines in isolation.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShadowConfig {
    pub suffix: Option<String>,
    pub schema_override: Option<String>,
}

pub struct ShadowRewriter;

impl ShadowRewriter {
    pub fn rewrite_target(target_name: &str, config: &ShadowConfig) -> String {
        if let Some(schema) = &config.schema_override {
            // Assuming target_name is schema.table or just table
            if target_name.contains('.') {
                let parts: Vec<&str> = target_name.split('.').collect();
                format!(
                    "{}.{}",
                    schema,
                    parts
                        .last()
                        .map_or_else(|| target_name.to_string(), std::string::ToString::to_string)
                )
            } else {
                format!("{schema}.{target_name}")
            }
        } else {
            let suffix = config.suffix.as_deref().unwrap_or("_shadow");
            format!("{target_name}{suffix}")
        }
    }
}
