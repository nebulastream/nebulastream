# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
add_plugin_as_library(RustFileCXX Source nes-sources-registry rust_file_cxx_source_plugin_library RustFileSourceCXX.cpp)
add_plugin_as_library(RustFileCXX SourceValidation nes-sources-registry rust_file_cxx_validation_plugin_library RustFileSourceCXX.cpp)
add_plugin_as_library(RustFileCXX InlineData nes-sources-registry rust_file_cxx_inline_data_plugin_library RustFileSourceCXX.cpp)
add_plugin_as_library(RustFileCXX FileData nes-sources-registry rust_file_cxx_file_data_plugin_library RustFileSourceCXX.cpp)
target_include_directories(rust_file_cxx_validation_plugin_library
        PUBLIC include
        PRIVATE .
)

add_dependencies(rust_file_cxx_source_plugin_library rust_build_cxx)
add_dependencies(rust_file_cxx_validation_plugin_library rust_build_cxx)
add_dependencies(rust_file_cxx_inline_data_plugin_library rust_build_cxx)
add_dependencies(rust_file_cxx_file_data_plugin_library rust_build_cxx)
target_link_libraries(rust_file_cxx_source_plugin_library PRIVATE rust-example-cxx)

set(RUST_PROJECT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust-example-cxx")
set(RUST_BUILD_MODE "release")
set(RUST_LIB_NAME "librust_example_cxx.a")
set(RUST_LIB_PATH "${RUST_PROJECT_DIR}/target/${RUST_BUILD_MODE}/${RUST_LIB_NAME}")
set(RUST_LIB_PATH_COMMON "${CMAKE_CURRENT_BINARY_DIR}/${RUST_LIB_NAME}")
set(FFI_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/cxx.hpp")
set(RUST_LIB_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/lib.rs.hpp")
set(RUST_LIB_CPP "${CMAKE_CURRENT_SOURCE_DIR}/lib.rs.cpp")
set(VCPKG_INCLUDE_DIR "${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include")
add_custom_command(
        OUTPUT ${RUST_LIB_PATH_COMMON} ${FFI_HEADER} ${RUST_LIB_HEADER} ${RUST_LIB_CPP}
        # Build the rust code
        COMMAND ${CMAKE_COMMAND} -E env cargo build --${RUST_BUILD_MODE} --manifest-path ${RUST_PROJECT_DIR}/Cargo.toml
        # Copy the library, source- and header files to a common location
        COMMAND ${CMAKE_COMMAND} -E copy ${RUST_LIB_PATH} ${RUST_LIB_PATH_COMMON}
        COMMAND ${CMAKE_COMMAND} -E copy "${RUST_PROJECT_DIR}/target/cxxbridge/rust/cxx.h" ${FFI_HEADER}
        COMMAND ${CMAKE_COMMAND} -E copy "${RUST_PROJECT_DIR}/target/cxxbridge/rust-example-cxx/src/lib.rs.h" ${RUST_LIB_HEADER}
        COMMAND ${CMAKE_COMMAND} -E copy "${RUST_PROJECT_DIR}/target/cxxbridge/rust-example-cxx/src/lib.rs.cc" ${RUST_LIB_CPP}

        WORKING_DIRECTORY ${RUST_PROJECT_DIR}
        COMMENT "Building Rust project with Cargo"
        VERBATIM
)

add_custom_target(rust_build_cxx ALL
        DEPENDS ${RUST_LIB_PATH_COMMON} ${FFI_HEADER} ${RUST_LIB_HEADER} ${RUST_LIB_CPP}
)
add_library(rust-example-cxx STATIC IMPORTED)
set_target_properties(rust-example-cxx PROPERTIES
        IMPORTED_LOCATION ${RUST_LIB_PATH_COMMON}
)
