use std::collections::HashMap;
use std::string::ToString;

#[derive(PartialEq, Eq, Debug)]
pub enum ConfigOptionsType {
    Text(&'static str),
    Number(isize),
}

#[derive(PartialEq, Eq, Debug)]
pub enum ConfigOptionsTypeTag {
    Text,
    Number,
}

impl ConfigOptionsType {
    fn default_string(&self) -> String {
        match self {
            ConfigOptionsType::Text(text) => text.to_string(),
            ConfigOptionsType::Number(number) => number.to_string(),
        }
    }

    const fn tag(&self) -> ConfigOptionsTypeTag {
        match self {
            ConfigOptionsType::Text(_) => ConfigOptionsTypeTag::Text,
            ConfigOptionsType::Number(_) => ConfigOptionsTypeTag::Number,
        }
    }
}

/// Parsed and validated config value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigValue {
    Text(String),
    Number(isize),
}

impl ConfigValue {
    fn to_config_string(&self) -> String {
        match self {
            ConfigValue::Text(text) => text.clone(),
            ConfigValue::Number(n) => n.to_string(),
        }
    }
}

/// Result of validating a single parsed config value.
///
/// The canonical model is [`Validator::Transform`]: a validator receives the
/// parsed value and returns the value to persist, letting it reject, accept,
/// or normalize. [`Validator::Check`] is a shortcut for the common case of
/// only wanting to reject/accept without touching the value.
pub enum Validator {
    Check(&'static (dyn Fn(&ConfigValue) -> Result<(), Error> + Sync + Send)),
    Transform(&'static (dyn Fn(ConfigValue) -> Result<ConfigValue, Error> + Sync + Send)),
}

impl Validator {
    fn apply(&self, value: ConfigValue) -> Result<ConfigValue, Error> {
        match self {
            Validator::Check(f) => f(&value).map(|()| value),
            Validator::Transform(f) => f(value),
        }
    }
}

pub struct ConfigDefinition {
    name: &'static str,
    type_discriminant: ConfigOptionsTypeTag,
    default_value: Option<ConfigOptionsType>,
    validation: Validator,
}

impl ConfigDefinition {
    pub const fn with_type(name: &'static str, tag: ConfigOptionsTypeTag) -> Self {
        ConfigDefinition {
            name,
            type_discriminant: tag,
            default_value: None,
            validation: Validator::Transform(&|v: ConfigValue| Ok(v)),
        }
    }
    pub const fn with_default(name: &'static str, default: ConfigOptionsType) -> Self {
        ConfigDefinition {
            name,
            type_discriminant: default.tag(),
            default_value: Some(default),
            validation: Validator::Transform(&|v: ConfigValue| Ok(v)),
        }
    }
    pub const fn with_default_and_validator(
        name: &'static str,
        default: ConfigOptionsType,
        validation: &'static (impl Fn(ConfigValue) -> Result<ConfigValue, Error> + Sync + Send),
    ) -> Self {
        ConfigDefinition {
            name,
            type_discriminant: default.tag(),
            default_value: Some(default),
            validation: Validator::Transform(validation),
        }
    }
    /// Shortcut for validators that only reject/accept and don't need to
    /// transform the stored value.
    pub const fn with_default_and_check(
        name: &'static str,
        default: ConfigOptionsType,
        check: &'static (impl Fn(&ConfigValue) -> Result<(), Error> + Sync + Send),
    ) -> Self {
        ConfigDefinition {
            name,
            type_discriminant: default.tag(),
            default_value: Some(default),
            validation: Validator::Check(check),
        }
    }
}

pub type ConfigOptions = HashMap<String, String>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Parse a string value into a [`ConfigValue`] according to the expected type.
fn parse_value(raw: &str, expected: &ConfigOptionsTypeTag) -> Result<ConfigValue, String> {
    match expected {
        ConfigOptionsTypeTag::Text => Ok(ConfigValue::Text(raw.to_string())),
        ConfigOptionsTypeTag::Number => raw
            .parse::<isize>()
            .map(ConfigValue::Number)
            .map_err(|_| format!("Expected a number, got `{raw}`")),
    }
}

fn upper_case_config_options(options: &ConfigOptions) -> ConfigOptions {
    let mut upper_case = ConfigOptions::with_capacity(options.len());
    for (k, v) in options {
        upper_case.insert(k.to_uppercase(), v.clone());
    }
    upper_case
}

/// Validate `options` against one or more `ConfigDefinition` schemas.
///
/// Each inner slice is processed in order; missing required options produce errors,
/// options with defaults are filled in, and unknown keys in `options` are ignored.
/// Callers typically pass a global schema followed by a domain-specific one, e.g.
/// `validate(opts, &[GLOBAL, SPECIFIC])`.
pub fn validate(
    options: &ConfigOptions,
    schemas: &[&[ConfigDefinition]],
) -> Result<ConfigOptions, Error> {
    let options = upper_case_config_options(options);
    let mut sanitized_options = ConfigOptions::default();
    let mut errors = vec![];
    for schema in schemas {
        for x in *schema {
            if let Some(raw) = options.get(&x.name.to_uppercase()) {
                let parsed = match parse_value(raw, &x.type_discriminant) {
                    Ok(v) => v,
                    Err(e) => {
                        errors.push(format!(
                            "Validation of Config Option `{}` failed: {e}",
                            x.name
                        ));
                        continue;
                    }
                };

                let validated = match x.validation.apply(parsed) {
                    Ok(v) => v,
                    Err(e) => {
                        errors.push(format!(
                            "Validation of Config Option `{}` failed: {}",
                            x.name, e
                        ));
                        continue;
                    }
                };

                sanitized_options.insert(x.name.to_owned(), validated.to_config_string());
            } else {
                let Some(ref default_value) = x.default_value else {
                    errors.push(format!("Required Config Option `{}` is missing.", x.name));
                    continue;
                };

                sanitized_options.insert(x.name.to_owned(), default_value.default_string());
            }
        }
    }

    if errors.is_empty() {
        Ok(sanitized_options)
    } else {
        Err(errors.join("\n").into())
    }
}
