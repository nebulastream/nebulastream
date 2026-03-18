use linkme::distributed_slice;
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

pub struct ConfigDefinition {
    name: &'static str,
    type_discriminant: ConfigOptionsTypeTag,
    default_value: Option<ConfigOptionsType>,
    validation: &'static (dyn Fn(&ConfigValue) -> Result<(), Error> + Sync + Send),
}

impl ConfigDefinition {
    pub const fn with_type(name: &'static str, tag: ConfigOptionsTypeTag) -> Self {
        ConfigDefinition {
            name,
            type_discriminant: tag,
            default_value: None,
            validation: &|_: &ConfigValue| Ok(()),
        }
    }
    pub const fn with_default(name: &'static str, default: ConfigOptionsType) -> Self {
        ConfigDefinition {
            name,
            type_discriminant: default.tag(),
            default_value: Some(default),
            validation: &|_: &ConfigValue| Ok(()),
        }
    }
}

pub type ConfigOptions = HashMap<String, String>;

#[distributed_slice]
pub static SINK_VALIDATOR: [(&'static str, &'static [ConfigDefinition])];

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

pub fn validate(name: &str, options: &ConfigOptions) -> Result<ConfigOptions, Error> {
    let options = upper_case_config_options(options);
    for (registered_name, registered_config_def) in SINK_VALIDATOR {
        if name.to_uppercase() == *registered_name.to_uppercase() {
            let mut sanitized_options = ConfigOptions::default();
            let mut errors = vec![];
            for x in *registered_config_def {
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

                    if let Err(e) = (x.validation)(&parsed) {
                        errors.push(format!(
                            "Validation of Config Option `{}` failed: {}",
                            x.name, e
                        ));
                        continue;
                    }

                    sanitized_options.insert(x.name.to_owned(), raw.clone());
                } else {
                    let Some(ref default_value) = x.default_value else {
                        errors.push(format!("Required Config Option `{}` is missing.", x.name));
                        continue;
                    };

                    sanitized_options.insert(x.name.to_owned(), default_value.default_string());
                }
            }

            if errors.is_empty() {
                return Ok(sanitized_options);
            }
            return Err(errors.join("\n").into());
        }
    }

    Err(format!("{} not found", name).into())
}
pub fn exists(name: &str) -> bool {
    for (registered_name, _) in SINK_VALIDATOR {
        if *registered_name.to_uppercase() == name.to_uppercase() {
            return true;
        }
    }
    false
}
