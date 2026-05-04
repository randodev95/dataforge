//! # Data Quality Audits
//! 
//! This module translates dbt-style YAML tests into SQL queries 
//! for execution and validation.

use crate::project::YamlTest;
use crate::error::{TitanError, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
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
    pub fn generate_sql(model_name: &str, column_name: &str, test: &YamlTest) -> Result<(String, String)> {
        use crate::utils::quote_identifier;
        let q_col = quote_identifier(column_name);
        let q_model = quote_identifier(model_name);

        match test {
            YamlTest::Simple(name) => match name.as_str() {
                "unique" => Ok((
                    name.to_string(),
                    format!(
                        "SELECT COUNT(*) as count FROM (SELECT {} FROM {} GROUP BY {} HAVING COUNT(*) > 1) as t",
                        q_col, q_model, q_col
                    )
                )),
                "not_null" => Ok((
                    name.to_string(),
                    format!(
                        "SELECT COUNT(*) as count FROM {} WHERE {} IS NULL",
                        q_model, q_col
                    )
                )),
                _ => Err(TitanError::ValidationError(format!("Unsupported simple test: {}", name))),
            },
            YamlTest::Complex(map) => {
                if let Some(list) = map.get("accepted_values")
                    .and_then(|v| v.get("values"))
                    .and_then(|v| v.as_sequence()) {
                        let values_str = list.iter()
                            .map(|v: &serde_yml::Value| format!("'{}'", v.as_str().unwrap_or_default()))
                            .collect::<Vec<_>>()
                            .join(", ");
                        return Ok((
                            "accepted_values".to_string(),
                            format!(
                                "SELECT COUNT(*) as count FROM {} WHERE {} NOT IN ({})",
                                q_model, q_col, values_str
                            )
                        ));
                }
                Err(TitanError::ValidationError("Unsupported complex test".to_string()))
            }
        }
    }
}
