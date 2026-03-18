corrosion_add_cxxbridge(nes-io-runtime-bindings CRATE nes_io_runtime_bindings FILES lib.rs)
cxxbridge_remove_rust_link(nes-io-runtime-bindings nes_io_runtime_bindings)
target_include_directories(nes-io-runtime-bindings
        PUBLIC include
)
target_sources(nes-io-runtime-bindings
    PRIVATE src/IORuntimeBindings.cpp
)
target_link_libraries(nes-io-runtime-bindings PRIVATE nes-common)
