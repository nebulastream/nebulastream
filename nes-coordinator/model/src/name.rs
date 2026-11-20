/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

//! The catalog stores names in their canonical form, and the planner reads them back without parsing them again.
//! A client can create a catalog object without going through the SQL parser,
//! so the statements hold their names in types that cannot be built from an invalid name.

use crate::error::{CodedError, ErrorCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

/// A name in canonical form: not empty, and without a dot or a quote.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CanonicalName(String);

impl TryFrom<String> for CanonicalName {
    type Error = CodedError;

    fn try_from(name: String) -> Result<Self, CodedError> {
        if name.is_empty() {
            return Err(CodedError::new(
                ErrorCode::InvalidIdentifier,
                "a name cannot be empty",
            ));
        }
        if name.contains(['.', '"']) {
            return Err(CodedError::new(
                ErrorCode::InvalidIdentifier,
                format!("name '{name}' contains a dot or a quote"),
            ));
        }
        Ok(Self(name))
    }
}

impl FromStr for CanonicalName {
    type Err = CodedError;

    fn from_str(name: &str) -> Result<Self, CodedError> {
        Self::try_from(name.to_string())
    }
}

impl From<CanonicalName> for String {
    fn from(name: CanonicalName) -> Self {
        name.0
    }
}

impl PartialEq<CanonicalName> for String {
    fn eq(&self, name: &CanonicalName) -> bool {
        *self == name.0
    }
}

impl Deref for CanonicalName {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CanonicalName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A schema as the planner stores it, a list of fields, each with a canonical name.
/// The rest of a field is kept as it is, because only the planner reads it.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Value", into = "Value")]
pub struct Schema(Value);

impl TryFrom<Value> for Schema {
    type Error = CodedError;

    fn try_from(schema: Value) -> Result<Self, CodedError> {
        if let Some(fields) = schema.as_array() {
            for field in fields {
                if let Some(name) = field.get("name").and_then(Value::as_str) {
                    name.parse::<CanonicalName>()?;
                }
            }
        }
        Ok(Self(schema))
    }
}

impl From<Schema> for Value {
    fn from(schema: Schema) -> Self {
        schema.0
    }
}

impl PartialEq<Schema> for Value {
    fn eq(&self, schema: &Schema) -> bool {
        *self == schema.0
    }
}

/// The options of a source, a parser or a sink, by canonical name.
/// An option group, such as the formatter options of a sink, is a nested object with canonical names too.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Value", into = "Value")]
pub struct Config(Value);

impl Config {
    fn check(config: &Value) -> Result<(), CodedError> {
        if let Some(options) = config.as_object() {
            for (name, value) in options {
                name.parse::<CanonicalName>()?;
                Self::check(value)?;
            }
        }
        Ok(())
    }
}

impl TryFrom<Value> for Config {
    type Error = CodedError;

    fn try_from(config: Value) -> Result<Self, CodedError> {
        Self::check(&config)?;
        Ok(Self(config))
    }
}

impl From<Config> for Value {
    fn from(config: Config) -> Self {
        config.0
    }
}

impl PartialEq<Config> for Value {
    fn eq(&self, config: &Config) -> bool {
        *self == config.0
    }
}

#[cfg(test)]
mod tests {
    use super::{CanonicalName, Config, Schema};
    use crate::error::ErrorCode;
    use serde_json::json;

    #[test]
    fn a_name_that_is_not_canonical_is_rejected() {
        for invalid in ["", "A.B", "A\"B"] {
            let error = invalid.parse::<CanonicalName>().unwrap_err();
            assert_eq!(error.code, ErrorCode::InvalidIdentifier);
        }
        assert_eq!(&*"Foo".parse::<CanonicalName>().unwrap(), "Foo");
    }

    #[test]
    fn schemas_and_configs_reject_a_name_that_is_not_canonical() {
        assert!(Schema::try_from(json!([{"name": "A.B", "dataType": [3, false]}])).is_err());
        assert!(Config::try_from(json!({"A.B": "x"})).is_err());
        assert!(Config::try_from(json!({"OUTPUT_FORMATTER": {"": "CSV"}})).is_err());
        assert!(Schema::try_from(json!([{"name": "ID", "dataType": [3, false]}])).is_ok());
        assert!(Config::try_from(json!({"OUTPUT_FORMATTER": {"TYPE": "CSV"}})).is_ok());
    }

    #[test]
    fn a_statement_with_a_name_that_is_not_canonical_does_not_deserialize() {
        let statement = json!({"name": "A.B", "schema": [], "if_not_exists": false});
        assert!(
            serde_json::from_value::<crate::source::logical::CreateLogicalSource>(statement)
                .is_err()
        );
    }
}
