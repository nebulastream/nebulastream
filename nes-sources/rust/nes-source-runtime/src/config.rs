// Declarative config schema for Tokio sources.
//
// Each source declares its expected parameters (name, type, default) via
// `ConfigParam` metadata. This enables schema-aware validation, unknown-parameter
// rejection, default application, and proper typed config reconstruction on
// the C++ side.

use std::collections::HashMap;

/// Supported parameter types for source configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamType {
    U64,
    I64,
    U32,
    I32,
    F64,
    Bool,
    String,
}

impl ParamType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ParamType::U64 => "u64",
            ParamType::I64 => "i64",
            ParamType::U32 => "u32",
            ParamType::I32 => "i32",
            ParamType::F64 => "f64",
            ParamType::Bool => "bool",
            ParamType::String => "string",
        }
    }
}

/// A single configuration parameter declaration.
pub struct ConfigParam {
    pub name: &'static str,
    pub param_type: ParamType,
    /// None = required parameter; Some(default_str) = optional with default.
    pub default: Option<&'static str>,
}

/// Validate a raw string config against a schema.
///
/// Returns validated HashMap with defaults applied, or error message.
/// The returned HashMap contains the same keys as the schema, with values
/// that are guaranteed to parse as the declared type.
pub fn validate_config(
    schema: &[ConfigParam],
    config: &HashMap<String, String>,
) -> Result<HashMap<String, String>, String> {
    // 1. Reject unknown keys
    for key in config.keys() {
        if !schema.iter().any(|p| p.name == key) {
            return Err(format!("unknown config parameter: {key}"));
        }
    }

    let mut result = HashMap::new();

    for param in schema {
        let value = match config.get(param.name) {
            Some(v) => v.clone(),
            None => match param.default {
                Some(d) => d.to_string(),
                None => return Err(format!("missing required parameter: {}", param.name)),
            },
        };

        // Type-check by attempting to parse
        validate_type(param.name, &value, param.param_type)?;

        result.insert(param.name.to_string(), value);
    }

    Ok(result)
}

fn validate_type(name: &str, value: &str, param_type: ParamType) -> Result<(), String> {
    match param_type {
        ParamType::U64 => {
            value.parse::<u64>().map_err(|e| format!("invalid {name}: {e}"))?;
        }
        ParamType::I64 => {
            value.parse::<i64>().map_err(|e| format!("invalid {name}: {e}"))?;
        }
        ParamType::U32 => {
            value.parse::<u32>().map_err(|e| format!("invalid {name}: {e}"))?;
        }
        ParamType::I32 => {
            value.parse::<i32>().map_err(|e| format!("invalid {name}: {e}"))?;
        }
        ParamType::F64 => {
            value.parse::<f64>().map_err(|e| format!("invalid {name}: {e}"))?;
        }
        ParamType::Bool => {
            match value {
                "true" | "false" | "1" | "0" => {}
                _ => return Err(format!("invalid {name}: expected true/false, got '{value}'")),
            }
        }
        ParamType::String => {
            // Any string is valid
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SCHEMA: &[ConfigParam] = &[
        ConfigParam { name: "count", param_type: ParamType::U64, default: Some("10") },
        ConfigParam { name: "name", param_type: ParamType::String, default: None },
    ];

    #[test]
    fn validate_applies_defaults() {
        let config = HashMap::from([("name".to_string(), "test".to_string())]);
        let result = validate_config(TEST_SCHEMA, &config).unwrap();
        assert_eq!(result["count"], "10");
        assert_eq!(result["name"], "test");
    }

    #[test]
    fn validate_rejects_unknown_keys() {
        let config = HashMap::from([
            ("name".to_string(), "test".to_string()),
            ("bogus".to_string(), "value".to_string()),
        ]);
        let result = validate_config(TEST_SCHEMA, &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown config parameter: bogus"));
    }

    #[test]
    fn validate_rejects_missing_required() {
        let config = HashMap::new();
        let result = validate_config(TEST_SCHEMA, &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("missing required parameter: name"));
    }

    #[test]
    fn validate_rejects_bad_type() {
        let config = HashMap::from([
            ("name".to_string(), "test".to_string()),
            ("count".to_string(), "not_a_number".to_string()),
        ]);
        let result = validate_config(TEST_SCHEMA, &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid count"));
    }
}
