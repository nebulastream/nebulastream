use linkme::distributed_slice;

pub use nes_config_options::{
    ConfigDefinition, ConfigOptions, ConfigOptionsType, ConfigOptionsTypeTag, ConfigValue, Error,
};

pub type ValidatorEntry = (&'static str, &'static [ConfigDefinition]);

#[distributed_slice]
pub static SINK_VALIDATOR: [ValidatorEntry];

static GLOBAL_SINK_VALIDATIONS: &[ConfigDefinition] = &[];

fn find(name: &str) -> Option<&'static [ConfigDefinition]> {
    SINK_VALIDATOR
        .iter()
        .find(|(n, _)| n.to_uppercase() == name.to_uppercase())
        .map(|(_, defs)| *defs)
}

pub fn validate(name: &str, options: &ConfigOptions) -> Result<ConfigOptions, Error> {
    let Some(specific) = find(name) else {
        return Err(format!("{} not found", name).into());
    };
    nes_config_options::validate(options, &[GLOBAL_SINK_VALIDATIONS, specific])
}

pub fn exists(name: &str) -> bool {
    find(name).is_some()
}
