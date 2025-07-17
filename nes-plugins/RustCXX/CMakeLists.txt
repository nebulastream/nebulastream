set(RUST_PROJECT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust-lib")
set(RUST_BUILD_MODE "release")
set(RUST_LIB_NAME "librust_lib.a")
set(RUST_LIB_PATH "${RUST_PROJECT_DIR}/target/${RUST_BUILD_MODE}/${RUST_LIB_NAME}")
set(RUST_LIB_PATH_COMMON "${CMAKE_CURRENT_BINARY_DIR}/${RUST_LIB_NAME}")
set(FFI_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/include/cxx.hpp")
set(RUST_LIB_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/include/lib.rs.hpp")
set(RUST_LIB_CPP "${CMAKE_CURRENT_SOURCE_DIR}/src/lib.rs.cpp")
add_custom_command(
        OUTPUT ${RUST_LIB_PATH_COMMON} ${FFI_HEADER} ${RUST_LIB_HEADER} ${RUST_LIB_CPP}
        # Build the rust code
        COMMAND ${CMAKE_COMMAND} -E env cargo build --${RUST_BUILD_MODE} --manifest-path ${RUST_PROJECT_DIR}/Cargo.toml
        # Copy the library, source- and header files to a common location
        COMMAND ${CMAKE_COMMAND} -E copy ${RUST_LIB_PATH} ${RUST_LIB_PATH_COMMON}
        COMMAND ${CMAKE_COMMAND} -E copy "${RUST_PROJECT_DIR}/target/cxxbridge/rust/cxx.h" ${FFI_HEADER}
        COMMAND ${CMAKE_COMMAND} -E copy "${RUST_PROJECT_DIR}/target/cxxbridge/rust-lib/src/lib.rs.h" ${RUST_LIB_HEADER}
        COMMAND ${CMAKE_COMMAND} -E copy "${RUST_PROJECT_DIR}/target/cxxbridge/rust-lib/src/lib.rs.cc" ${RUST_LIB_CPP}

        WORKING_DIRECTORY ${RUST_PROJECT_DIR}
        COMMENT "Building Rust project with Cargo"
        VERBATIM
)

add_custom_target(rust_build_cxx ALL
        DEPENDS ${RUST_LIB_PATH_COMMON} ${FFI_HEADER} ${RUST_LIB_HEADER} ${RUST_LIB_CPP}
)
add_library(rust-lib-cxx STATIC IMPORTED GLOBAL)
set_target_properties(rust-lib-cxx PROPERTIES
        IMPORTED_LOCATION ${RUST_LIB_PATH_COMMON}
)
target_include_directories(rust-lib-cxx INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
)

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

add_plugin_as_library(RustFileCXX Source nes-sources-registry rust_file_cxx_source_plugin_library src/RustFileSourceCXX.cpp)
add_plugin_as_library(RustFileCXX SourceValidation nes-sources-registry rust_file_cxx_validation_plugin_library src/RustFileSourceCXX.cpp)
add_plugin_as_library(RustFileCXX InlineData nes-sources-registry rust_file_cxx_inline_data_plugin_library src/RustFileSourceCXX.cpp)
add_plugin_as_library(RustFileCXX FileData nes-sources-registry rust_file_cxx_file_data_plugin_library src/RustFileSourceCXX.cpp)

add_plugin_as_library(RustChecksumCXX Sink nes-sinks-registry rust_checksum_sink_cxx_plugin src/RustChecksumSinkCXX.cpp)
add_plugin_as_library(RustChecksumCXX SinkValidation nes-sinks-registry rust_checksum_sink_cxx_validation_plugin src/RustChecksumSinkCXX.cpp)

add_plugin_as_library(RustFileSinkCXX Sink nes-sinks-registry rust_file_sink_cxx_plugin src/RustFileSinkCXX.cpp)
add_plugin_as_library(RustFileSinkCXX SinkValidation nes-sinks-registry rust_file_sink_cxx_validation_plugin src/RustFileSinkCXX.cpp)
target_include_directories(rust_file_cxx_validation_plugin_library PRIVATE .)

add_dependencies(rust_file_cxx_source_plugin_library rust_build_cxx)
add_dependencies(rust_file_cxx_validation_plugin_library rust_build_cxx)
add_dependencies(rust_file_cxx_inline_data_plugin_library rust_build_cxx)
add_dependencies(rust_file_cxx_file_data_plugin_library rust_build_cxx)

add_dependencies(rust_checksum_sink_cxx_plugin rust_build_cxx)
add_dependencies(rust_checksum_sink_cxx_validation_plugin rust_build_cxx)

add_dependencies(rust_file_sink_cxx_plugin rust_build_cxx)
add_dependencies(rust_file_sink_cxx_validation_plugin rust_build_cxx)

target_link_libraries(rust_file_cxx_source_plugin_library PUBLIC rust-lib-cxx)
target_link_libraries(rust_checksum_sink_cxx_plugin PRIVATE rust-lib-cxx)
target_link_libraries(rust_file_sink_cxx_plugin PRIVATE rust-lib-cxx)
