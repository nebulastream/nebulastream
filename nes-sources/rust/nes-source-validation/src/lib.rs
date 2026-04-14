use linkme::distributed_slice;

pub use nes_config_options::{
    ConfigDefinition, ConfigOptions, ConfigOptionsType, ConfigOptionsTypeTag, ConfigValue, Error,
    ValidatorEntry,
};

#[distributed_slice]
pub static SOURCE_VALIDATOR: [ValidatorEntry];

pub fn validate(name: &str, options: &ConfigOptions) -> Result<ConfigOptions, Error> {
    nes_config_options::validate(name, options, &SOURCE_VALIDATOR[..])
}

pub fn exists(name: &str) -> bool {
    nes_config_options::exists(name, &SOURCE_VALIDATOR[..])
}
