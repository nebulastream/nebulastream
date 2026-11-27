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

//! Names cross the boundary to the C++ planner in their SQL form, and the catalog stores them in their canonical form.
//! The forms are lossy but equal, so a name that the catalog writes back parses to the same identifier:
//!
//! | SQL text | planner writes | catalog stores | catalog writes |
//! |----------|----------------|----------------|----------------|
//! | `foo`    | `FOO`          | `FOO`          | `FOO`          |
//! | `"Foo"`  | `"Foo"`        | `Foo`          | `"Foo"`        |
//! | `"FOO"`  | `"FOO"`        | `FOO`          | `FOO`          |
//!
//! A client can create a catalog object without going through the SQL parser,
//! so the statements hold their names in types that cannot be built from an invalid name.

use crate::error::{CodedError, ErrorCode};
use sea_orm::sea_query::{ArrayType, Nullable, StringLen, ValueType, ValueTypeErr};
use sea_orm::{ColIdx, ColumnType, DbErr, QueryResult, TryFromU64, TryGetError, TryGetable, Value};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as Json;
use std::fmt;
use std::str::FromStr;

/// A SQL identifier, held in the canonical form that the catalog compares and keys on.
/// It mirrors the C++ identifier in `nes-common/src/Identifiers/Identifier.cpp`, and both parse the same SQL text.
/// A change to the parsing rules on one side must be made on the other side too,
/// together with the tests here and in `nes-common/tests/UnitTests/IdentifierTest.cpp`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Identifier(String);

impl Identifier {
    /// The rules of the C++ parser: an unquoted name is upper-cased per ASCII byte, a quoted one keeps its case.
    /// Example: `foo` and `"FOO"` both give `FOO`, and `"Foo"` gives `Foo`.
    pub fn parse(sql: &str) -> Result<Self, CodedError> {
        let invalid = |reason: &str| {
            CodedError::new(
                ErrorCode::InvalidIdentifier,
                format!("identifier '{sql}' {reason}"),
            )
        };
        if sql.is_empty() {
            return Err(CodedError::new(
                ErrorCode::InvalidIdentifier,
                "an identifier cannot be empty",
            ));
        }
        if sql.contains('.') {
            return Err(invalid("contains a dot"));
        }
        match (sql.starts_with('"'), sql.ends_with('"')) {
            (true, _) if sql.len() <= 2 => Err(invalid("is only quotes")),
            (true, true) => Ok(Self(sql[1..sql.len() - 1].to_string())),
            (true, false) => Err(invalid("starts with a quote but does not end with one")),
            (false, true) => Err(invalid("ends with a quote but does not start with one")),
            (false, false) => Ok(Self(sql.to_ascii_uppercase())),
        }
    }

    pub fn canonical(&self) -> &str {
        &self.0
    }

    /// Quoted only where the bare text would parse to a different name: `FOO` stays bare, `Foo` becomes `"Foo"`.
    /// The C++ side writes the user's original text, which parses to the same identifier.
    pub fn to_sql(&self) -> String {
        let canonical = &self.0;
        let bare = *canonical == canonical.to_ascii_uppercase()
            && !canonical.starts_with('"')
            && !canonical.ends_with('"');
        if bare {
            canonical.clone()
        } else {
            format!("\"{canonical}\"")
        }
    }
}

impl FromStr for Identifier {
    type Err = CodedError;

    fn from_str(sql: &str) -> Result<Self, CodedError> {
        Self::parse(sql)
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_sql())
    }
}

impl Serialize for Identifier {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_sql())
    }
}

impl<'de> Deserialize<'de> for Identifier {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let sql = String::deserialize(deserializer)?;
        Self::parse(&sql).map_err(serde::de::Error::custom)
    }
}

#[cfg(any(test, feature = "testing"))]
impl proptest::arbitrary::Arbitrary for Identifier {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<Self>;
    fn arbitrary_with((): ()) -> Self::Strategy {
        use proptest::strategy::Strategy;
        "[^.]{1,12}".prop_map(Self).boxed()
    }
}

impl From<Identifier> for Value {
    fn from(identifier: Identifier) -> Self {
        Value::String(Some(Box::new(identifier.0)))
    }
}

impl TryGetable for Identifier {
    fn try_get_by<I: ColIdx>(res: &QueryResult, index: I) -> Result<Self, TryGetError> {
        String::try_get_by(res, index).map(Self)
    }
}

impl ValueType for Identifier {
    fn try_from(v: Value) -> Result<Self, ValueTypeErr> {
        match v {
            Value::String(Some(canonical)) => Ok(Self(*canonical)),
            _ => Err(ValueTypeErr),
        }
    }

    fn type_name() -> String {
        "Identifier".to_string()
    }

    fn array_type() -> ArrayType {
        ArrayType::String
    }

    fn column_type() -> ColumnType {
        ColumnType::String(StringLen::None)
    }
}

impl Nullable for Identifier {
    fn null() -> Value {
        Value::String(None)
    }
}

impl TryFromU64 for Identifier {
    fn try_from_u64(_: u64) -> Result<Self, DbErr> {
        Err(DbErr::ConvertFromU64("Identifier"))
    }
}

/// Field names are rewritten to their SQL form, so that two spellings of one name are stored as the same JSON:
/// `"A"` and `A` are both stored as `A`, and `"Mixed"` stays `"Mixed"`.
/// The rest of a field is kept as it is, because only the planner reads it.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Json", into = "Json")]
pub struct Schema(Json);

impl TryFrom<Json> for Schema {
    type Error = CodedError;

    fn try_from(mut schema: Json) -> Result<Self, CodedError> {
        if let Some(fields) = schema.as_array_mut() {
            for field in fields {
                if let Some(name) = field.get_mut("name")
                    && let Some(sql) = name.as_str()
                {
                    *name = Json::String(Identifier::parse(sql)?.to_sql());
                }
            }
        }
        Ok(Self(schema))
    }
}

impl From<Schema> for Json {
    fn from(schema: Schema) -> Self {
        schema.0
    }
}

impl PartialEq<Schema> for Json {
    fn eq(&self, schema: &Schema) -> bool {
        *self == schema.0
    }
}

/// Option names are rewritten to their SQL form at every level, as the field names of a schema are.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Json", into = "Json")]
pub struct Config(Json);

impl Config {
    fn normalize(config: Json) -> Result<Json, CodedError> {
        match config {
            Json::Object(options) => options
                .into_iter()
                .map(|(name, value)| {
                    Ok((Identifier::parse(&name)?.to_sql(), Self::normalize(value)?))
                })
                .collect::<Result<_, CodedError>>()
                .map(Json::Object),
            value => Ok(value),
        }
    }
}

impl TryFrom<Json> for Config {
    type Error = CodedError;

    fn try_from(config: Json) -> Result<Self, CodedError> {
        Self::normalize(config).map(Self)
    }
}

impl From<Config> for Json {
    fn from(config: Config) -> Self {
        config.0
    }
}

impl PartialEq<Config> for Json {
    fn eq(&self, config: &Config) -> bool {
        *self == config.0
    }
}

/// The first tests mirror the single-identifier tests in `nes-common/tests/UnitTests/IdentifierTest.cpp`.
/// The qualified-identifier tests there have no counterpart, because the catalog holds only unqualified names.
#[cfg(test)]
mod tests {
    use super::{Config, Identifier, Schema};
    use crate::error::ErrorCode;
    use proptest::prelude::*;
    use serde_json::{Value as Json, json};

    fn parse(sql: &str) -> Identifier {
        Identifier::parse(sql).unwrap()
    }

    #[test]
    fn auto_upper_case() {
        let identifier = parse("test");
        assert_eq!(identifier.canonical(), "TEST");
        assert_eq!(identifier.to_sql(), "TEST");
    }

    #[test]
    fn auto_upper_case_short() {
        let identifier = parse("id");
        assert_eq!(identifier.canonical(), "ID");
        assert_eq!(identifier.to_sql(), "ID");
    }

    #[test]
    fn case_sensitive() {
        let identifier = parse("\"Test\"");
        assert_eq!(identifier.canonical(), "Test");
        assert_eq!(identifier.to_sql(), "\"Test\"");
    }

    #[test]
    fn case_sensitive_caps() {
        let identifier = parse("\"TEST\"");
        assert_eq!(identifier.canonical(), "TEST");
        assert_eq!(identifier.to_sql(), "TEST");
    }

    #[test]
    fn equality() {
        assert_eq!(parse("test"), parse("\"TEST\""));
        assert_eq!(parse("\"TEST\""), parse("test"));
    }

    #[test]
    fn inequality() {
        assert_ne!(parse("test"), parse("\"Test\""));
        assert_ne!(parse("\"Test\""), parse("test"));
    }

    #[test]
    fn not_parse_invalid_identifier() {
        for invalid in ["\"Test", "Test\"", "T.est", ""] {
            let error = Identifier::parse(invalid).unwrap_err();
            assert_eq!(error.code, ErrorCode::InvalidIdentifier, "{invalid}");
        }
    }

    #[test]
    fn shared_cases() {
        for (sql, canonical, written) in [
            ("foo", "FOO", "FOO"),
            ("Foo", "FOO", "FOO"),
            ("FOO", "FOO", "FOO"),
            ("\"FOO\"", "FOO", "FOO"),
            ("\"Foo\"", "Foo", "\"Foo\""),
            ("a\"b", "A\"B", "A\"B"),
            ("\"\"x\"", "\"x", "\"\"x\""),
        ] {
            let identifier = parse(sql);
            assert_eq!(identifier.canonical(), canonical, "{sql}");
            assert_eq!(identifier.to_sql(), written, "{sql}");
            assert_eq!(parse(&identifier.to_sql()), identifier, "{sql}");
        }
        for invalid in ["", "a.b", "\"", "\"\"", "\"a", "a\""] {
            let error = Identifier::parse(invalid).unwrap_err();
            assert_eq!(error.code, ErrorCode::InvalidIdentifier, "{invalid}");
        }
    }

    #[test]
    fn single_quote_parse_is_rejected() {
        assert!(Identifier::parse("\"").is_err());
    }

    #[test]
    fn double_quote_empty_val_parse_is_rejected() {
        assert!(Identifier::parse("\"\"").is_err());
    }

    #[test]
    fn serde_sql_form() {
        let quoted = parse("\"field\"");
        let unquoted = parse("field");
        assert_eq!(serde_json::to_value(&quoted).unwrap(), json!("\"field\""));
        assert_eq!(serde_json::to_value(&unquoted).unwrap(), json!("FIELD"));
        assert_eq!(
            serde_json::from_value::<Identifier>(json!("\"field\"")).unwrap(),
            quoted
        );
        assert_eq!(
            serde_json::from_value::<Identifier>(json!("FIELD")).unwrap(),
            unquoted
        );
    }

    proptest! {
        #[test]
        fn sql_form_round_trip(identifier: Identifier) {
            prop_assert_eq!(Identifier::parse(&identifier.to_sql()).unwrap(), identifier);
        }
    }

    #[test]
    fn one_spelling_per_name() {
        let schema = Schema::try_from(
            json!([{"name": "\"A\"", "dataType": [3, false]}, {"name": "\"Mixed\""}, {"name": "ts"}]),
        );
        assert_eq!(
            Json::from(schema.unwrap()),
            json!([{"name": "A", "dataType": [3, false]}, {"name": "\"Mixed\""}, {"name": "TS"}])
        );
        let config = Config::try_from(
            json!({"\"TYPE\"": "CSV", "OUTPUT_FORMATTER": {"field_delimiter": ","}}),
        );
        assert_eq!(
            Json::from(config.unwrap()),
            json!({"TYPE": "CSV", "OUTPUT_FORMATTER": {"FIELD_DELIMITER": ","}})
        );
    }

    #[test]
    fn invalid_names_rejected() {
        assert!(Schema::try_from(json!([{"name": "A.B", "dataType": [3, false]}])).is_err());
        assert!(Config::try_from(json!({"A.B": "x"})).is_err());
        assert!(Config::try_from(json!({"OUTPUT_FORMATTER": {"": "CSV"}})).is_err());
        assert!(Schema::try_from(json!([{"name": "ID", "dataType": [3, false]}])).is_ok());
        assert!(Schema::try_from(json!([{"name": "\"id\"", "dataType": [3, false]}])).is_ok());
        assert!(Config::try_from(json!({"OUTPUT_FORMATTER": {"TYPE": "CSV"}})).is_ok());
    }

    #[test]
    fn invalid_statement_name() {
        let statement = json!({"name": "A.B", "schema": [], "ifNotExists": false});
        assert!(
            serde_json::from_value::<crate::source::logical::CreateLogicalSource>(statement)
                .is_err()
        );
    }
}
