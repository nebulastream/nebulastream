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

use nes_sink_validation::ConfigOptions;

#[cxx::bridge]
pub mod ffi {
    extern "Rust" {
        fn exists(name: String) -> bool;
        fn validate(name: String, json_conf: String) -> Result<String>;
    }
}
fn exists(name: String) -> bool {
    nes_sink_validation::exists(&name)
}
fn validate(name: String, json_conf: String) -> Result<String, String> {
    let config = serde_json::from_str::<ConfigOptions>(&json_conf)
        .expect("FFI serialization error. Could not convert config options to rust representation");
    nes_sink_validation::validate(&name, &config)
        .map_err(|e| e.to_string())
        .map(|config_options| {
            serde_json::to_string(&config_options).expect("json conversion should not fail")
        })
}
