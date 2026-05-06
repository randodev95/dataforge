//! Robust column mapping and schema widening.

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashMap;

/// Map logical column names to physical names and types.
#[derive(Debug, Clone, Default)]
pub struct ColumnMap {
    pub mappings: HashMap<String, ColumnMapping>,
}

#[derive(Debug, Clone)]
pub struct ColumnMapping {
    pub physical_name: String,
    pub data_type: DataType,
}

impl ColumnMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, logical_name: String, physical_name: String, data_type: DataType) {
        self.mappings.insert(
            logical_name,
            ColumnMapping {
                physical_name,
                data_type,
            },
        );
    }

    /// Reconcile an input schema with the target column map.
    pub fn reconcile(&self, input_schema: &Schema) -> Vec<Field> {
        let mut fields = Vec::new();
        for f in input_schema.fields() {
            if let Some(mapping) = self.mappings.get(f.name()) {
                // Check for type widening (e.g. Int32 -> Int64)
                let target_type = self.widened_type(f.data_type(), &mapping.data_type);
                fields.push(Field::new(
                    &mapping.physical_name,
                    target_type,
                    f.is_nullable(),
                ));
            } else {
                fields.push((**f).clone());
            }
        }
        fields
    }

    fn widened_type(&self, source: &DataType, target: &DataType) -> DataType {
        match (source, target) {
            (DataType::Int32, DataType::Int64) => DataType::Int64,
            (DataType::Float32, DataType::Float64) => DataType::Float64,
            (DataType::Utf8, DataType::LargeUtf8) => DataType::LargeUtf8,
            _ => source.clone(), // Fallback to source type
        }
    }
}
