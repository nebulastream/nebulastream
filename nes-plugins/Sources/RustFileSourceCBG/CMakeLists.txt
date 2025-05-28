# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
add_plugin_as_library(RustFileCBG Source nes-sources-registry rust_file_cbg_source_plugin_library RustFileSourceCBG.cpp)
add_plugin_as_library(RustFileCBG SourceValidation nes-sources-registry rust_file_cbg_validation_plugin_library RustFileSourceCBG.cpp)
add_plugin_as_library(RustFileCBG InlineData nes-sources-registry rust_file_cbg_inline_data_plugin_library RustFileSourceCBG.cpp)
add_plugin_as_library(RustFileCBG FileData nes-sources-registry rust_file_cbg_file_data_plugin_library RustFileSourceCBG.cpp)
target_include_directories(rust_file_cbg_validation_plugin_library
        PUBLIC include
        PRIVATE .
)

add_dependencies(rust_file_cbg_source_plugin_library rust_build_cbg)
add_dependencies(rust_file_cbg_validation_plugin_library rust_build_cbg)
add_dependencies(rust_file_cbg_inline_data_plugin_library rust_build_cbg)
add_dependencies(rust_file_cbg_file_data_plugin_library rust_build_cbg)
target_link_libraries(rust_file_cbg_source_plugin_library PRIVATE rust-example-cbg)

set(RUST_PROJECT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust-example-cbg")
set(RUST_BUILD_MODE "release")
set(RUST_LIB_NAME "librust_example_cbg.a")
set(RUST_LIB_PATH "${RUST_PROJECT_DIR}/target/${RUST_BUILD_MODE}/${RUST_LIB_NAME}")
set(RUST_LIB_PATH_COMMON "${CMAKE_CURRENT_BINARY_DIR}/${RUST_LIB_NAME}")
set(RUST_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/RustFileSourceImpl.hpp")
add_custom_command(
        OUTPUT ${RUST_LIB_PATH_COMMON} ${RUST_HEADER}
        # Build the rust code
        COMMAND ${CMAKE_COMMAND} -E env cargo build --${RUST_BUILD_MODE} --manifest-path ${RUST_PROJECT_DIR}/Cargo.toml
        # Copy the library, source- and header files to a common location
        COMMAND ${CMAKE_COMMAND} -E copy ${RUST_LIB_PATH} ${RUST_LIB_PATH_COMMON}
        COMMAND ${CMAKE_COMMAND} -E copy "${RUST_PROJECT_DIR}/include/RustFileSourceImpl.hpp" ${RUST_HEADER}

        WORKING_DIRECTORY ${RUST_PROJECT_DIR}
        COMMENT "Building Rust project with Cargo"
        VERBATIM
)

add_custom_target(rust_build_cbg ALL
        DEPENDS ${RUST_LIB_PATH_COMMON} ${RUST_HEADER}
)
add_library(rust-example-cbg STATIC IMPORTED)
set_target_properties(rust-example-cbg PROPERTIES
        IMPORTED_LOCATION ${RUST_LIB_PATH_COMMON}
)
