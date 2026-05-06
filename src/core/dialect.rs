use datafusion::arrow::datatypes::DataType;
use std::sync::Arc;

/// Abstraction for warehouse-specific SQL generation.
///
/// This trait ensures that the engine can generate compatible DDL and DML
/// for various ADBC-compliant destinations without hardcoding syntax.
pub trait SqlDialect: Send + Sync {
    /// Quote an identifier (e.g., "column_name" or [column_name]).
    fn quote_identifier(&self, ident: &str) -> String;

    /// Format a table name, optionally including environment/schema.
    fn table_name(&self, env: &str, name: &str) -> String;

    /// Generate a CREATE TABLE AS SELECT statement.
    fn create_table_as(&self, target: &str, sql: &str) -> String {
        format!("CREATE TABLE {target} AS {sql}")
    }

    /// Generate a MERGE/UPSERT statement for incremental materialization.
    fn merge_upsert(
        &self,
        target: &str,
        source: &str,
        unique_keys: &[String],
        columns: &[String],
    ) -> String;

    /// Generate an INSERT OVERWRITE PARTITION statement.
    fn partition_overwrite(
        &self,
        target: &str,
        source: &str,
        partition_key: &str,
        columns: &[String],
    ) -> String;

    /// Map Arrow DataType to warehouse-specific SQL type string.
    fn map_type(&self, data_type: &DataType) -> String;

    /// Generate a CAST expression.
    fn cast(&self, expr: &str, to_type: &DataType) -> String {
        format!("CAST({} AS {})", expr, self.map_type(to_type))
    }

    /// Check if changing from source_type to target_type is a safe widening.
    fn is_safe_widening(&self, source_type: &str, target_type: &str) -> bool;

    /// Generate SQL to add a column.
    fn add_column_sql(&self, table: &str, column: &str, data_type: &str) -> String {
        format!("ALTER TABLE {table} ADD COLUMN {column} {data_type}")
    }

    /// Generate SQL to change a column type.
    fn alter_column_type_sql(&self, table: &str, column: &str, new_type: &str) -> String;

    /// Generate SQL to describe a table's schema.
    fn describe_table_sql(&self, table: &str) -> String;
}

/// Dialect for DataFusion and Delta Lake (Postgres-like quoting).
pub struct DefaultDialect;

impl SqlDialect for DefaultDialect {
    fn quote_identifier(&self, ident: &str) -> String {
        format!("\"{ident}\"")
    }

    fn table_name(&self, env: &str, name: &str) -> String {
        format!("{env}_{name}")
    }

    fn merge_upsert(
        &self,
        target: &str,
        source: &str,
        unique_keys: &[String],
        columns: &[String],
    ) -> String {
        // Default implementation uses a robust UNION ALL + NOT EXISTS pattern
        // which works well for DataFusion/Delta overwrite scenarios.
        let pk_join = unique_keys
            .iter()
            .map(|k| {
                format!(
                    "s.{} = t.{}",
                    self.quote_identifier(k),
                    self.quote_identifier(k)
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        let cols = columns
            .iter()
            .map(|c| format!("s.{}", self.quote_identifier(c)))
            .collect::<Vec<_>>()
            .join(", ");

        let t_cols = columns
            .iter()
            .map(|c| format!("t.{}", self.quote_identifier(c)))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "SELECT {cols} FROM {source} s \
             UNION ALL \
             SELECT {t_cols} FROM {target} t \
             WHERE NOT EXISTS (SELECT 1 FROM {source} s WHERE {pk_join})"
        )
    }

    fn partition_overwrite(
        &self,
        target: &str,
        source: &str,
        partition_key: &str,
        columns: &[String],
    ) -> String {
        let cols = columns
            .iter()
            .map(|c| self.quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");

        let _p_quoted = self.quote_identifier(partition_key);

        // For Delta Lake in DataFusion, we often use INSERT OVERWRITE
        format!("INSERT OVERWRITE {target} SELECT {cols} FROM {source}")
        // Note: Delta Lake handles the partition pruning automatically if the
        // partition_key is part of the table definition.
    }

    fn map_type(&self, data_type: &DataType) -> String {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR".to_string(),
            DataType::Int32 | DataType::Int64 => "BIGINT".to_string(),
            DataType::Float32 | DataType::Float64 => "DOUBLE".to_string(),
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
            _ => "VARCHAR".to_string(),
        }
    }

    fn is_safe_widening(&self, source_type: &str, target_type: &str) -> bool {
        let src = source_type.to_uppercase();
        let tgt = target_type.to_uppercase();
        match (src.as_str(), tgt.as_str()) {
            (s, t) if s == t => true,
            ("INT" | "INTEGER", "BIGINT") => true,
            ("FLOAT" | "REAL", "DOUBLE") => true,
            (_, "VARCHAR" | "STRING" | "TEXT") => true,
            _ => false,
        }
    }

    fn alter_column_type_sql(&self, table: &str, column: &str, new_type: &str) -> String {
        format!("ALTER TABLE {table} ALTER COLUMN {column} TYPE {new_type}")
    }

    fn describe_table_sql(&self, table: &str) -> String {
        format!("DESCRIBE TABLE {table}")
    }
}

/// Dialect for Postgres-specific optimizations.
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn quote_identifier(&self, ident: &str) -> String {
        format!("\"{ident}\"")
    }

    fn table_name(&self, env: &str, name: &str) -> String {
        format!("{env}.{name}")
    }

    fn merge_upsert(
        &self,
        target: &str,
        source: &str,
        unique_keys: &[String],
        columns: &[String],
    ) -> String {
        let update_cols = columns
            .iter()
            .filter(|c| !unique_keys.contains(c))
            .map(|c| {
                format!(
                    "{} = EXCLUDED.{}",
                    self.quote_identifier(c),
                    self.quote_identifier(c)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        let pks = unique_keys
            .iter()
            .map(|k| self.quote_identifier(k))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "INSERT INTO {target} SELECT * FROM {source} \
             ON CONFLICT ({pks}) DO UPDATE SET {update_cols}"
        )
    }

    fn partition_overwrite(
        &self,
        target: &str,
        source: &str,
        partition_key: &str,
        _columns: &[String],
    ) -> String {
        // Postgres doesn't have native INSERT OVERWRITE PARTITION
        // as Delta/Hive, so we emulate it with DELETE + INSERT.
        format!(
            "BEGIN; DELETE FROM {target} WHERE {partition_key} IN (SELECT {partition_key} FROM {source}); \
             INSERT INTO {target} SELECT * FROM {source}; COMMIT;",
            target = target,
            partition_key = self.quote_identifier(partition_key),
            source = source
        )
    }

    fn map_type(&self, data_type: &DataType) -> String {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 => "TEXT".to_string(),
            DataType::Int32 => "INT".to_string(),
            DataType::Int64 => "BIGINT".to_string(),
            DataType::Float32 => "REAL".to_string(),
            DataType::Float64 => "DOUBLE PRECISION".to_string(),
            DataType::Boolean => "BOOLEAN".to_string(),
            _ => "TEXT".to_string(),
        }
    }

    fn is_safe_widening(&self, source_type: &str, target_type: &str) -> bool {
        let src = source_type.to_uppercase();
        let tgt = target_type.to_uppercase();
        match (src.as_str(), tgt.as_str()) {
            (s, t) if s == t => true,
            ("INT" | "INTEGER", "BIGINT") => true,
            ("REAL", "DOUBLE PRECISION") => true,
            (_, "TEXT" | "VARCHAR") => true,
            _ => false,
        }
    }

    fn alter_column_type_sql(&self, table: &str, column: &str, new_type: &str) -> String {
        format!("ALTER TABLE {table} ALTER COLUMN {column} TYPE {new_type}")
    }

    fn describe_table_sql(&self, table: &str) -> String {
        format!(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'"
        )
    }
}

pub fn get_dialect(target_type: &str) -> Arc<dyn SqlDialect> {
    match target_type {
        "postgres" => Arc::new(PostgresDialect),
        _ => Arc::new(DefaultDialect),
    }
}
