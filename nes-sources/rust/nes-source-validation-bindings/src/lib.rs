use nes_source_validation::ConfigOptions;

#[cxx::bridge]
pub mod ffi {
    extern "Rust" {
        fn source_exists(name: String) -> bool;
        fn source_validate(name: String, json_conf: String) -> Result<String>;
    }
}
fn source_exists(name: String) -> bool {
    nes_source_validation::exists(&name)
}
fn source_validate(name: String, json_conf: String) -> Result<String, String> {
    let config = serde_json::from_str::<ConfigOptions>(&json_conf)
        .expect("FFI serialization error. Could not convert config options to rust representation");
    nes_source_validation::validate(&name, &config)
        .map_err(|e| e.to_string())
        .map(|config_options| {
            serde_json::to_string(&config_options).expect("json conversion should not fail")
        })
}
