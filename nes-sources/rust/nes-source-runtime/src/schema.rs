// Schema types for sink authors.
//
// SchemaField represents a single field in a tuple schema. Sinks receive
// a Vec<SchemaField> at construction time and can use it to write headers
// or validate data.

use std::fmt;

/// A single field in a tuple schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaField {
    /// Field name (e.g. "field_1", "multiBuffer$field_1").
    pub name: String,
    /// Data type name (e.g. "UINT64", "INT32", "FLOAT64", "BOOLEAN", "VARSIZED").
    pub data_type: String,
    /// Whether this field is nullable.
    pub nullable: bool,
}

impl SchemaField {
    pub fn new(name: String, data_type: String, nullable: bool) -> Self {
        Self { name, data_type, nullable }
    }
}

impl fmt::Display for SchemaField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nullable_str = if self.nullable { "IS_NULLABLE" } else { "NOT_NULLABLE" };
        write!(f, "{}:{}:{}", self.name, self.data_type, nullable_str)
    }
}

/// Format a schema as a CSV header line matching the C++ SchemaFormatter output.
///
/// Produces: `field1:TYPE1:NULLABLE,field2:TYPE2:NULLABLE,...\n`
pub fn format_schema_header(fields: &[SchemaField]) -> String {
    let mut s = String::new();
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&field.to_string());
    }
    s.push('\n');
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_field_display() {
        let f = SchemaField::new("id".into(), "UINT64".into(), false);
        assert_eq!(f.to_string(), "id:UINT64:NOT_NULLABLE");

        let f = SchemaField::new("name".into(), "VARSIZED".into(), true);
        assert_eq!(f.to_string(), "name:VARSIZED:IS_NULLABLE");
    }

    #[test]
    fn format_schema_header_matches_cpp() {
        let fields = vec![
            SchemaField::new("field_1".into(), "UINT64".into(), false),
            SchemaField::new("field_2".into(), "INT32".into(), true),
        ];
        assert_eq!(
            format_schema_header(&fields),
            "field_1:UINT64:NOT_NULLABLE,field_2:INT32:IS_NULLABLE\n"
        );
    }

    #[test]
    fn format_schema_header_single_field() {
        let fields = vec![SchemaField::new("x".into(), "FLOAT64".into(), false)];
        assert_eq!(format_schema_header(&fields), "x:FLOAT64:NOT_NULLABLE\n");
    }
}
