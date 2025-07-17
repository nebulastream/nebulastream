set(RUST_PROJECT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust-lib")
set(RUST_BUILD_MODE "release")
set(RUST_LIB_NAME "librust_lib_cbg.a")
set(RUST_LIB_PATH "${RUST_PROJECT_DIR}/target/${RUST_BUILD_MODE}/${RUST_LIB_NAME}")
set(RUST_LIB_PATH_COMMON "${CMAKE_CURRENT_BINARY_DIR}/${RUST_LIB_NAME}")
set(RUST_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/include/RustCBG.hpp")
add_custom_command(
        OUTPUT ${RUST_LIB_PATH_COMMON} ${RUST_HEADER}
        # Build the rust code
        COMMAND ${CMAKE_COMMAND} -E env cargo build --${RUST_BUILD_MODE} --manifest-path ${RUST_PROJECT_DIR}/Cargo.toml
        # Copy the library, source- and header files to a common location
        COMMAND ${CMAKE_COMMAND} -E copy ${RUST_LIB_PATH} ${RUST_LIB_PATH_COMMON}
        COMMAND ${CMAKE_COMMAND} -E copy "${RUST_PROJECT_DIR}/include/RustCBG.hpp" ${RUST_HEADER}

        WORKING_DIRECTORY ${RUST_PROJECT_DIR}
        COMMENT "Building Rust project with Cargo"
        VERBATIM
)

add_custom_target(rust_build_cbg ALL
        DEPENDS ${RUST_LIB_PATH_COMMON} ${RUST_HEADER}
)
add_library(rust-lib-cbg STATIC IMPORTED GLOBAL)
set_target_properties(rust-lib-cbg PROPERTIES
        IMPORTED_LOCATION ${RUST_LIB_PATH_COMMON}
)
target_include_directories(rust-lib-cbg INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
)

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

add_plugin_as_library(RustFileCBG Source nes-sources-registry rust_file_cbg_source_plugin_library src/RustFileSourceCBG.cpp)
add_plugin_as_library(RustFileCBG SourceValidation nes-sources-registry rust_file_cbg_validation_plugin_library src/RustFileSourceCBG.cpp)
add_plugin_as_library(RustFileCBG InlineData nes-sources-registry rust_file_cbg_inline_data_plugin_library src/RustFileSourceCBG.cpp)
add_plugin_as_library(RustFileCBG FileData nes-sources-registry rust_file_cbg_file_data_plugin_library src/RustFileSourceCBG.cpp)

add_plugin_as_library(RustChecksumCBG Sink nes-sinks-registry rust_checksum_sink_cbg_plugin src/RustChecksumSinkCBG.cpp)
add_plugin_as_library(RustChecksumCBG SinkValidation nes-sinks-registry rust_checksum_sink_cbg_validation_plugin src/RustChecksumSinkCBG.cpp)
target_include_directories(rust_file_cbg_validation_plugin_library PRIVATE .)

add_dependencies(rust_file_cbg_source_plugin_library rust_build_cbg)
add_dependencies(rust_file_cbg_validation_plugin_library rust_build_cbg)
add_dependencies(rust_file_cbg_inline_data_plugin_library rust_build_cbg)
add_dependencies(rust_file_cbg_file_data_plugin_library rust_build_cbg)

add_dependencies(rust_checksum_sink_cbg_plugin rust_build_cbg)
add_dependencies(rust_checksum_sink_cbg_validation_plugin rust_build_cbg)

target_link_libraries(rust_file_cbg_source_plugin_library PUBLIC rust-lib-cbg)
target_link_libraries(rust_checksum_sink_cbg_plugin PRIVATE rust-lib-cbg)