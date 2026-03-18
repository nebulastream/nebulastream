corrosion_add_cxxbridge(nes-io-runtime-bindings CRATE nes_io_runtime FILES lib.rs)
cxxbridge_remove_rust_link(nes-io-runtime-bindings nes_io_runtime)
cxxbridge_create_header_target(nes-io-runtime-bindings)
