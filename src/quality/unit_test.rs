//! Declarative unit testing for SQL models.
//!
//! Inspired by Rocky RS, this module allows mocking upstream dependencies
//! and asserting on model output in a local, ephemeral context.

use crate::error::{Result, TitanError};
use crate::execution::Muscle;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tracing::{error, info};

/// A unit test definition for a model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnitTest {
    pub name: String,
    pub description: Option<String>,
    pub given: Vec<TestFixture>,
    pub expect: TestExpectation,
}

/// Mock data for an upstream model or source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestFixture {
    #[serde(rename = "ref")]
    pub model_ref: String,
    pub rows: Vec<JsonValue>,
}

/// Expected output for the model under test.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestExpectation {
    pub rows: Vec<JsonValue>,
}

pub struct TestRunner {
    pub muscle: Arc<Muscle>,
    pub fingerprinter: Arc<crate::fingerprint::Fingerprinter>,
}

impl TestRunner {
    pub fn new(project_root: &std::path::Path) -> Self {
        Self {
            muscle: Arc::new(Muscle::new()),
            fingerprinter: Arc::new(crate::fingerprint::Fingerprinter::new(project_root)),
        }
    }

    pub async fn run_test(&self, model_name: &str, raw_sql: &str, test: &UnitTest) -> Result<bool> {
        info!(model = %model_name, test = %test.name, "Running unit test");

        // 1. Register fixtures as in-memory views
        for fixture in &test.given {
            self.register_fixture(fixture).await?;
        }

        // 2. Render SQL (mocking references)
        let config = std::collections::HashMap::new(); // Simplified
        let vars = std::collections::HashMap::new();
        let (titan_sql, _) =
            self.fingerprinter
                .fingerprint(raw_sql, "", &config, &[], model_name, false, &vars)?;

        // 3. Execute model SQL
        let df =
            self.muscle.ctx.sql(titan_sql.as_str()).await.map_err(|e| {
                TitanError::SqlParseError(format!("Failed to plan unit test SQL: {e}"))
            })?;

        let actual_rows = df
            .collect()
            .await
            .map_err(|e| TitanError::ExecutionError(format!("Failed to execute unit test: {e}")))?;

        let mut actual_json_rows = Vec::new();
        for batch in actual_rows {
            let mut writer = datafusion::arrow::json::ArrayWriter::new(Vec::new());
            writer
                .write_batches(&[&batch])
                .map_err(|e| TitanError::ValidationError(e.to_string()))?;
            writer
                .finish()
                .map_err(|e| TitanError::ValidationError(e.to_string()))?;
            let json_data = writer.into_inner();
            let rows: Vec<JsonValue> = serde_json::from_slice(&json_data)
                .map_err(|e| TitanError::ValidationError(e.to_string()))?;
            actual_json_rows.extend(rows);
        }

        let passed = self.compare_rows(&JsonValue::Array(actual_json_rows), &test.expect.rows);

        if passed {
            info!(model = %model_name, test = %test.name, "PASS");
        } else {
            error!(model = %model_name, test = %test.name, "FAIL");
        }

        Ok(passed)
    }

    async fn register_fixture(&self, fixture: &TestFixture) -> Result<()> {
        if fixture.rows.is_empty() {
            return Ok(());
        }

        // Convert JSON rows to Arrow RecordBatches and register in DataFusion
        // For simplicity in this first version, we'll use a hack:
        // generate a VALUES clause or a temporary CSV.
        // Actually, DataFusion has MemTable.

        let json_data = serde_json::to_string(&fixture.rows)
            .map_err(|e| TitanError::ValidationError(e.to_string()))?;

        let cursor = std::io::Cursor::new(json_data);
        let _decoder = datafusion::arrow::json::ReaderBuilder::new(
            Arc::new(datafusion::arrow::datatypes::Schema::empty()), // Will be inferred
        )
        .build(cursor)
        .map_err(|e| TitanError::ValidationError(e.to_string()))?;

        // Note: ReaderBuilder::new(schema) requires a schema.
        // We might need to infer it or just use SQL VALUES for now as it's easier for small fixtures.

        let mut values_list = Vec::new();
        for row in &fixture.rows {
            if let Some(obj) = row.as_object() {
                let row_values = obj
                    .values()
                    .map(|v| match v {
                        JsonValue::String(s) => format!("'{}'", s.replace('\'', "''")),
                        JsonValue::Number(n) => n.to_string(),
                        JsonValue::Bool(b) => b.to_string(),
                        _ => "NULL".to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                values_list.push(format!("({row_values})"));
            }
        }

        if values_list.is_empty() {
            return Ok(());
        }

        let first_row = fixture.rows[0].as_object().ok_or_else(|| {
            TitanError::ValidationError("Unit test fixture row is not an object".to_string())
        })?;
        let cols = first_row.keys().cloned().collect::<Vec<_>>().join(", ");
        let values_sql = format!(
            "SELECT * FROM (VALUES {}) AS t({})",
            values_list.join(", "),
            cols
        );

        let df = self
            .muscle
            .ctx
            .sql(&values_sql)
            .await
            .map_err(|e| TitanError::SqlParseError(e.to_string()))?;

        self.muscle
            .ctx
            .register_table(&fixture.model_ref, df.into_view())
            .map_err(|e| TitanError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    fn compare_rows(&self, actual: &JsonValue, expected: &[JsonValue]) -> bool {
        // Simple comparison: check if all expected rows are in actual
        // In a real implementation, we'd handle ordering and partial matches.
        if let Some(actual_array) = actual.as_array() {
            if actual_array.len() != expected.len() {
                return false;
            }
            // For now, assume exact order match or same content
            // To be robust, we'd sort both or do a multi-set comparison.
            match serde_json::to_value(expected) {
                Ok(expected_val) => expected_val == *actual,
                Err(_) => false,
            }
        } else {
            false
        }
    }
}
