use std::env;
use std::path::PathBuf;
use cbindgen::Config;

fn main() {
    println!("Generating cpp header from rust.");
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let config_path = PathBuf::from(&crate_dir).join("cbindgen.toml");
    let config = Config::from_file(config_path).unwrap_or_else(|e| {
        panic!("Error reading cbindgen.toml: {e}");
    });

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_language(cbindgen::Language::Cxx)
        .with_config(config)
        .generate()
        .map_or_else(|e| {
            panic!("Unable to generate cpp header: {e}")
        },
        |bindings| {
            bindings.write_to_file("include/RustFileSourceImpl.hpp")
        });

    println!("Successfully generated cpp header from rust.");
}