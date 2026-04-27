add_subdirectory(rust/nes-io-runtime-bindings)

add_library(nes-io-runtime src/IORuntime.cpp)
target_include_directories(nes-io-runtime
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_link_libraries(nes-io-runtime PUBLIC nes-io-runtime-bindings nes-common)
target_link_rust_lib(nes-io-runtime nes_io_runtime_bindings)
