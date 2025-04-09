# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
add_plugin_as_library(RustCompression Source nes-sources-registry rust_compression_source_plugin_library RustCompressionSource.cpp)
target_include_directories(tcp_source_plugin_library
        PUBLIC include
        PRIVATE .
)
add_plugin_as_library(RustCompression SourceValidation nes-sources-registry rust_compression_validation_plugin_library RustCompressionSource.cpp)
target_include_directories(rust_compression_validation_plugin_library
        PUBLIC include
        PRIVATE .
)

add_dependencies(rust_compression_source_plugin_library rust_build)
target_link_libraries(rust_compression_source_plugin_library PRIVATE rust-example)

set(RUST_PROJECT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust-example")
set(RUST_BUILD_MODE "release")
set(RUST_LIB_NAME "librust_example.a")
set(RUST_LIB_PATH "${RUST_PROJECT_DIR}/target/${RUST_BUILD_MODE}/${RUST_LIB_NAME}")
set(RUST_LIB_PATH_COMMON "${CMAKE_CURRENT_BINARY_DIR}/${RUST_LIB_NAME}")
set(VCPKG_INCLUDE_DIR "${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include")
add_custom_command(
        OUTPUT ${RUST_LIB_PATH_COMMON}
        # Build the rust code
        COMMAND ${CMAKE_COMMAND} -E env cargo build --${RUST_BUILD_MODE} --manifest-path ${RUST_PROJECT_DIR}/Cargo.toml
        # Copy the library, source- and header files to a common location
        COMMAND ${CMAKE_COMMAND} -E copy ${RUST_LIB_PATH} ${RUST_LIB_PATH_COMMON}
        COMMAND ${CMAKE_COMMAND} -E copy "${RUST_PROJECT_DIR}/target/cxxbridge/rust/cxx.h" "${CMAKE_CURRENT_SOURCE_DIR}/cxx.hpp"
        COMMAND ${CMAKE_COMMAND} -E copy "${RUST_PROJECT_DIR}/target/cxxbridge/rust-example/src/lib.rs.h" "${CMAKE_CURRENT_SOURCE_DIR}/lib.rs.hpp"
        COMMAND ${CMAKE_COMMAND} -E copy "${RUST_PROJECT_DIR}/target/cxxbridge/rust-example/src/lib.rs.cc" "${CMAKE_CURRENT_SOURCE_DIR}/lib.rs.cpp"

        WORKING_DIRECTORY ${RUST_PROJECT_DIR}
        COMMENT "Building Rust project with Cargo"
        VERBATIM
)

add_custom_target(rust_build ALL
        DEPENDS ${RUST_LIB_PATH_COMMON}
)
add_library(rust-example STATIC IMPORTED)
set_target_properties(rust-example PROPERTIES
        IMPORTED_LOCATION ${RUST_LIB_PATH_COMMON}
)
