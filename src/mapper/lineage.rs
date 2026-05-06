use anyhow::Result;
use flowscope_core::analyzer::analyze;
use flowscope_core::types::{
    AnalysisOptions, AnalyzeRequest, ColumnSchema, Dialect as FlowDialect, SchemaMetadata,
    SchemaTable,
};
use sqlparser::ast::{ColumnDef, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

pub struct Mapper {
    schema_metadata: SchemaMetadata,
}

impl Default for Mapper {
    fn default() -> Self {
        Self::new()
    }
}

impl Mapper {
    pub fn new() -> Self {
        Self {
            schema_metadata: SchemaMetadata::default(),
        }
    }

    pub fn load_schema(&mut self, ddl: &str) -> Result<()> {
        let dialect = PostgreSqlDialect {};
        let ast = Parser::parse_sql(&dialect, ddl)?;

        for stmt in ast {
            if let Statement::CreateTable(create_table) = stmt {
                let table_name = create_table.name.to_string();
                let flow_columns: Vec<ColumnSchema> = create_table
                    .columns
                    .into_iter()
                    .map(|c: ColumnDef| ColumnSchema {
                        name: c.name.to_string(),
                        data_type: Some(c.data_type.to_string()),
                        is_primary_key: None,
                        foreign_key: None,
                    })
                    .collect();

                self.schema_metadata.tables.push(SchemaTable {
                    catalog: None,
                    schema: None,
                    name: table_name,
                    columns: flow_columns,
                });
            }
        }
        Ok(())
    }

    pub fn analyze_lineage(&self, sql: &str) -> Result<()> {
        let request = AnalyzeRequest {
            sql: sql.to_string(),
            files: None,
            dialect: FlowDialect::Postgres,
            source_name: Some("titan_model".to_string()),
            options: Some(AnalysisOptions {
                enable_column_lineage: Some(true),
                ..Default::default()
            }),
            schema: Some(self.schema_metadata.clone()),
            template_config: None,
        };

        let result = analyze(&request);

        println!(
            "Lineage analysis complete. Found {} statements.",
            result.statements.len()
        );

        Ok(())
    }
}
