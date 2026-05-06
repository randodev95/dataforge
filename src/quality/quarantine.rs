use crate::error::Result;
use crate::project::{ModelColumn, YamlTest};
use crate::quality::QuarantineMode;
use crate::utils::quote_identifier;

pub struct QuarantinePlan {
    pub mode: QuarantineMode,
    pub source_view: String,
    pub valid_table: String,
    pub quarantine_table: String,
    pub statements: Vec<QuarantineStatement>,
}

pub struct QuarantineStatement {
    pub role: String, // "valid", "quarantine", "tag"
    pub sql: String,
}

pub struct QuarantineCompiler;

impl QuarantineCompiler {
    pub fn compile(
        model_name: &str,
        columns: &[ModelColumn],
        mode: QuarantineMode,
        source_view: String,
    ) -> Result<Option<QuarantinePlan>> {
        let mut predicates = Vec::new();
        let mut error_cols = Vec::new();

        for col in columns {
            for test in &col.tests {
                if let Some(pred) = Self::lower_predicate(&col.name, test)? {
                    let label = format!(
                        "_error_{}_{}",
                        col.name,
                        match test {
                            YamlTest::Simple(n) => n.clone(),
                            YamlTest::Complex(m) => m
                                .keys()
                                .next()
                                .cloned()
                                .unwrap_or_else(|| "test".to_string()),
                        }
                    );

                    predicates.push(pred.clone());
                    error_cols.push(format!(
                        "CASE WHEN NOT ({pred}) THEN '{label}' END AS {label}"
                    ));
                }
            }
        }

        if predicates.is_empty() {
            return Ok(None);
        }

        let valid_where = predicates.join(" AND ");
        let mut statements = Vec::new();
        let valid_table = format!("{model_name}__valid");
        let quarantine_table = format!("{model_name}__quarantine");

        match mode {
            QuarantineMode::Split => {
                let q_sql = format!(
                    "SELECT *, {} FROM {} WHERE NOT ({})",
                    error_cols.join(", "),
                    source_view,
                    valid_where
                );
                statements.push(QuarantineStatement {
                    role: "quarantine".into(),
                    sql: q_sql,
                });

                let v_sql = format!("SELECT * FROM {source_view} WHERE {valid_where}");
                statements.push(QuarantineStatement {
                    role: "valid".into(),
                    sql: v_sql,
                });
            }
            QuarantineMode::Drop => {
                let v_sql = format!("SELECT * FROM {source_view} WHERE {valid_where}");
                statements.push(QuarantineStatement {
                    role: "valid".into(),
                    sql: v_sql,
                });
            }
            QuarantineMode::Tag => {
                let t_sql = format!("SELECT *, {} FROM {}", error_cols.join(", "), source_view);
                statements.push(QuarantineStatement {
                    role: "tag".into(),
                    sql: t_sql,
                });
            }
        }

        Ok(Some(QuarantinePlan {
            mode,
            source_view,
            valid_table,
            quarantine_table,
            statements,
        }))
    }

    fn lower_predicate(column: &str, test: &YamlTest) -> Result<Option<String>> {
        let q_col = quote_identifier(column);
        match test {
            YamlTest::Simple(name) => match name.as_str() {
                "not_null" => Ok(Some(format!("{q_col} IS NOT NULL"))),
                "unique" => Ok(None), // Set-based, can't quarantine row-level easily without window functions
                _ => Ok(None),
            },
            YamlTest::Complex(map) => {
                if let Some(list) = map
                    .get("accepted_values")
                    .and_then(|v| v.get("values"))
                    .and_then(|v| v.as_sequence())
                {
                    let values_str = list
                        .iter()
                        .map(|v| format!("'{}'", v.as_str().unwrap_or_default()))
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Ok(Some(format!(
                        "({q_col} IS NULL OR {q_col} IN ({values_str}))"
                    )));
                }
                Ok(None)
            }
        }
    }
}
