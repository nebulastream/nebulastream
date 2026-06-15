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

use linkme::distributed_slice;

pub use nes_config_options::{
    ConfigDefinition, ConfigOptions, ConfigOptionsType, ConfigOptionsTypeTag, ConfigValue, Error,
};

pub type ValidatorEntry = (&'static str, &'static [ConfigDefinition]);

#[distributed_slice]
pub static SINK_VALIDATOR: [ValidatorEntry];

static GLOBAL_SINK_VALIDATIONS: &[ConfigDefinition] = &[ConfigDefinition::with_default(
    "stop_flush_timeout_ms",
    ConfigOptionsType::Number(1_000),
)];

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
