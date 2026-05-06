//! # Column-Level Lineage
//!
//! This module implements static analysis of SQL to trace column origins
//! and transformations across models.

use crate::error::Result;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::Arc;

/// How a column value is transformed from source to target.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransformKind {
    /// Direct column reference (no transformation).
    Direct,
    /// Explicit type cast.
    Cast,
    /// Aggregate function (SUM, COUNT, etc.).
    Aggregation(String),
    /// Complex expression (arithmetic, CASE, etc.).
    Expression,
}

/// A column fully qualified by its model (or source) name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QualifiedColumn {
    pub model: Arc<str>,
    pub column: Arc<str>,
}

/// A column lineage edge: source_table.column → target alias.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnLineage {
    pub source: QualifiedColumn,
    pub target_column: String,
    pub transform: TransformKind,
}

/// Full lineage result for a SQL statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageResult {
    pub source_models: Vec<String>,
    pub columns: Vec<ColumnLineage>,
    pub has_star: bool,
}

pub struct LineageExtractor;

impl LineageExtractor {
    pub fn extract(sql: &str) -> Result<LineageResult> {
        let dialect = GenericDialect;
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| crate::error::TitanError::SqlParseError(e.to_string()))?;

        let stmt = statements
            .first()
            .ok_or_else(|| crate::error::TitanError::SqlParseError("Empty SQL".to_string()))?;

        match stmt {
            Statement::Query(query) => Self::extract_query_lineage(query),
            _ => Err(crate::error::TitanError::SqlParseError(
                "Only SELECT statements are supported for lineage".to_string(),
            )),
        }
    }

    fn extract_query_lineage(query: &Query) -> Result<LineageResult> {
        match query.body.as_ref() {
            SetExpr::Select(select) => {
                let mut source_models = Vec::new();
                let mut alias_map = HashMap::new();

                for table_with_joins in &select.from {
                    Self::extract_table_factor(
                        &table_with_joins.relation,
                        &mut source_models,
                        &mut alias_map,
                    );
                    for join in &table_with_joins.joins {
                        Self::extract_table_factor(
                            &join.relation,
                            &mut source_models,
                            &mut alias_map,
                        );
                    }
                }

                let mut columns = Vec::new();
                let mut has_star = false;

                for item in &select.projection {
                    match item {
                        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                            has_star = true;
                        }
                        SelectItem::UnnamedExpr(expr) => {
                            if let Some(lineage) =
                                Self::extract_expr_lineage(expr, &alias_map, &source_models)
                            {
                                columns.push(lineage);
                            }
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            if let Some(mut lineage) =
                                Self::extract_expr_lineage(expr, &alias_map, &source_models)
                            {
                                lineage.target_column = alias.value.clone();
                                columns.push(lineage);
                            }
                        }
                    }
                }

                Ok(LineageResult {
                    source_models,
                    columns,
                    has_star,
                })
            }
            _ => Err(crate::error::TitanError::SqlParseError(
                "Unsupported query type for lineage".to_string(),
            )),
        }
    }

    fn extract_table_factor(
        factor: &TableFactor,
        models: &mut Vec<String>,
        aliases: &mut HashMap<String, String>,
    ) {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                models.push(table_name.clone());
                if let Some(a) = alias {
                    aliases.insert(a.name.value.to_lowercase(), table_name);
                }
            }
            TableFactor::Derived { alias: Some(a), .. } => {
                aliases.insert(a.name.value.to_lowercase(), "(subquery)".to_string());
            }
            _ => {}
        }
    }

    fn extract_expr_lineage(
        expr: &Expr,
        alias_map: &HashMap<String, String>,
        source_models: &[String],
    ) -> Option<ColumnLineage> {
        match expr {
            Expr::Identifier(ident) => {
                let col_name = ident.value.clone();
                let model_name = if source_models.len() == 1 {
                    source_models[0].as_str()
                } else {
                    "unknown"
                };
                Some(ColumnLineage {
                    source: QualifiedColumn {
                        model: Arc::from(model_name),
                        column: Arc::from(col_name.as_str()),
                    },
                    target_column: col_name,
                    transform: TransformKind::Direct,
                })
            }
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                let table_part = parts[parts.len() - 2].value.to_lowercase();
                let col_name = parts[parts.len() - 1].value.clone();
                let resolved_model = alias_map.get(&table_part).cloned().unwrap_or(table_part);
                Some(ColumnLineage {
                    source: QualifiedColumn {
                        model: Arc::from(resolved_model.as_str()),
                        column: Arc::from(col_name.as_str()),
                    },
                    target_column: col_name,
                    transform: TransformKind::Direct,
                })
            }
            Expr::Cast { expr, .. } => {
                let mut lineage = Self::extract_expr_lineage(expr, alias_map, source_models)?;
                lineage.transform = TransformKind::Cast;
                Some(lineage)
            }
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                let args = match &func.args {
                    sqlparser::ast::FunctionArguments::List(list) => &list.args,
                    _ => return None,
                };
                // Find first column reference in arguments
                for arg in args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = arg
                        && let Some(mut lineage) =
                            Self::extract_expr_lineage(inner, alias_map, source_models)
                    {
                        lineage.transform = TransformKind::Aggregation(func_name);
                        return Some(lineage);
                    }
                }
                None
            }
            _ => None,
        }
    }
}

pub struct LineageDiffer;

#[derive(Debug, Serialize, Deserialize)]
pub enum LineageDiff {
    AddedColumn(String),
    RemovedColumn(String),
    ChangedSource {
        column: String,
        old: String,
        new: String,
    },
    ChangedTransform {
        column: String,
        old: TransformKind,
        new: TransformKind,
    },
}

impl LineageDiffer {
    pub fn diff(&self, a: &LineageResult, b: &LineageResult) -> Vec<LineageDiff> {
        let mut diffs = Vec::new();
        let map_a: HashMap<String, &ColumnLineage> = a
            .columns
            .iter()
            .map(|c| (c.target_column.clone(), c))
            .collect();
        let map_b: HashMap<String, &ColumnLineage> = b
            .columns
            .iter()
            .map(|c| (c.target_column.clone(), c))
            .collect();

        for (name, col_b) in &map_b {
            match map_a.get(name) {
                Some(col_a) => {
                    if col_a.source != col_b.source {
                        diffs.push(LineageDiff::ChangedSource {
                            column: name.clone(),
                            old: col_a.source.model.to_string(),
                            new: col_b.source.model.to_string(),
                        });
                    }
                    if col_a.transform != col_b.transform {
                        diffs.push(LineageDiff::ChangedTransform {
                            column: name.clone(),
                            old: col_a.transform.clone(),
                            new: col_b.transform.clone(),
                        });
                    }
                }
                None => {
                    diffs.push(LineageDiff::AddedColumn(name.clone()));
                }
            }
        }

        for name in map_a.keys() {
            if !map_b.contains_key(name) {
                diffs.push(LineageDiff::RemovedColumn(name.clone()));
            }
        }

        diffs
    }
}
