use polyglot_sql::{parse_one, generate, DialectType};
use polyglot_sql::optimizer::{simplify, normalize};
use anyhow::Result;
use crate::core::TitanSQL;
use std::sync::LazyLock;
use regex::Regex;

static COMMENT_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"/\*.*?\*/").unwrap());

pub struct Normalizer;

impl Normalizer {
    pub fn normalize(sql: &str) -> Result<TitanSQL> {
        // Try parsing with various dialects to support transpilation to the internal Postgres standard
        let dialects = [
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::Snowflake,
            DialectType::BigQuery,
        ];

        let mut last_err = None;
        for dialect in dialects {
            if let Ok(ast) = parse_one(sql, dialect) {
                // Once parsed, transpile and optimize into Postgres standard
                let ast = simplify::simplify(ast, Some(DialectType::PostgreSQL));
                let ast = normalize::normalize(ast, false, 1000)
                    .map_err(|e| anyhow::anyhow!("Failed to normalize SQL: {:?}", e))?;

                // Generate normalized SQL back to Postgres string
                let normalized_sql = generate(&ast, DialectType::PostgreSQL)
                    .map_err(|e| anyhow::anyhow!("Failed to generate SQL: {}", e))?;
                
                let stripped = COMMENT_RE.replace_all(&normalized_sql, "");
                let single_spaced = stripped.split_whitespace().collect::<Vec<_>>().join(" ");
                
                return Ok(TitanSQL::new(single_spaced));
            }
            last_err = Some(anyhow::anyhow!("Failed to parse SQL with any supported dialect"));
        }
        
        Err(last_err.unwrap())
    }
}
