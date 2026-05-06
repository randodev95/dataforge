//! for execution and validation.

pub mod quarantine;
pub mod unit_test;

use crate::error::{Result, TitanError};
use crate::project::YamlTest;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum QuarantineMode {
    Split, // __valid and __quarantine tables
    Drop,  // Discard bad rows
    Tag,   // Add _error columns but keep in one table
}

pub struct TestResult {
    pub model_name: String,
    pub column_name: String,
    pub test_name: String,
    pub status: String, // "pass" | "fail"
    pub violation_count: usize,
    pub query: String,
}

pub struct TestGenerator;

impl TestGenerator {
    pub fn generate_sql(
        engine: &crate::fingerprint::TemplateEngine,
        model_name: &str,
        column_name: &str,
        test: &YamlTest,
    ) -> Result<(String, String)> {
        match test {
            YamlTest::Simple(name) => {
                match name.as_str() {
                    "unique" => {
                        let template = "{% import 'generic_tests.sql' as t %}{{ t.test_unique(model, column) }}";
                        let mut ctx = std::collections::HashMap::new();
                        ctx.insert("model".to_string(), minijinja::Value::from(model_name));
                        ctx.insert("column".to_string(), minijinja::Value::from(column_name));

                        let sql = engine
                            .render(template, &ctx, "test", "test", false)
                            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

                        Ok((name.clone(), format!("SELECT COUNT(*) FROM ({sql}) as t")))
                    }
                    "not_null" => {
                        let template = "{% import 'generic_tests.sql' as t %}{{ t.test_not_null(model, column) }}";
                        let mut ctx = std::collections::HashMap::new();
                        ctx.insert("model".to_string(), minijinja::Value::from(model_name));
                        ctx.insert("column".to_string(), minijinja::Value::from(column_name));

                        let sql = engine
                            .render(template, &ctx, "test", "test", false)
                            .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

                        Ok((name.clone(), format!("SELECT COUNT(*) FROM ({sql}) as t")))
                    }
                    _ => Err(TitanError::ValidationError(format!(
                        "Unsupported simple test: {name}"
                    ))),
                }
            }
            YamlTest::Complex(map) => {
                if let Some(list) = map
                    .get("accepted_values")
                    .and_then(|v| v.get("values"))
                    .and_then(|v| v.as_sequence())
                {
                    let values: Vec<String> = list
                        .iter()
                        .map(|v| v.as_str().unwrap_or_default().to_string())
                        .collect();

                    let template = "{% import 'generic_tests.sql' as t %}{{ t.test_accepted_values(model, column, values) }}";
                    let mut ctx = std::collections::HashMap::new();
                    ctx.insert("model".to_string(), minijinja::Value::from(model_name));
                    ctx.insert("column".to_string(), minijinja::Value::from(column_name));
                    ctx.insert("values".to_string(), minijinja::Value::from(values));

                    let sql = engine
                        .render(template, &ctx, "test", "test", false)
                        .map_err(|e| TitanError::ExecutionError(e.to_string()))?;

                    return Ok((
                        "accepted_values".to_string(),
                        format!("SELECT COUNT(*) FROM ({sql}) as t"),
                    ));
                }
                Err(TitanError::ValidationError(
                    "Unsupported complex test".to_string(),
                ))
            }
        }
    }
}
