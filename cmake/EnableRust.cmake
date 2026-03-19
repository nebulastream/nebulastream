# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Unfortunately, compiling rust with sanitizers requires the nightly compiler.
SET(Rust_RESOLVE_RUSTUP_TOOLCHAINS OFF)
SET(Rust_TOOLCHAIN "nightly")

set(CXXFLAGS_LIST "-std=c++23")
set(RUSTFLAGS_LIST "")
set(CARGOFLAGS_LIST "")

# Add flags based on sanitizer options
if (SANITIZER_OPTION STREQUAL "address")
    list(APPEND RUSTFLAGS_LIST "-Zsanitizer=address")
    list(APPEND CARGOFLAGS_LIST "-Zbuild-std")
    list(APPEND CXXFLAGS_LIST "-fsanitize=address")
elseif (SANITIZER_OPTION STREQUAL "undefined")
    list(APPEND CARGOFLAGS_LIST "-Zbuild-std")
    list(APPEND CXXFLAGS_LIST "-fsanitize=undefined")
elseif (SANITIZER_OPTION STREQUAL "thread")
    list(APPEND RUSTFLAGS_LIST "-Zsanitizer=thread")
    list(APPEND CARGOFLAGS_LIST "-Zbuild-std")
    list(APPEND CXXFLAGS_LIST "-fsanitize=thread")
else ()
    SET(Rust_RESOLVE_RUSTUP_TOOLCHAINS ON)
    UNSET(Rust_TOOLCHAIN)
endif ()

# Add libc++ if needed
if (${USING_LIBCXX})
    list(APPEND CXXFLAGS_LIST "-stdlib=libc++")
endif ()

include(FetchContent)
FetchContent_Declare(
        Corrosion
        GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
        GIT_TAG v0.5.2
)

FetchContent_MakeAvailable(Corrosion)

include(RustLibCollection)
# Defer Rust link finalization using double-defer so it runs AFTER all other
# deferred calls (e.g. generate_rust_tokio_source_registry which registers
# the registry crate that the umbrella needs to include).
cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL _defer_finalize_rust_links)

function(_defer_finalize_rust_links)
    cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL _finalize_rust_links)
endfunction()

list(JOIN CARGOFLAGS_LIST " " ADDITIONAL_CARGOFLAGS)

# Import Rust crates for CXX bridge header generation.
# The individual staticlibs are NOT linked directly to executables — target_link_rust() handles
# that via per-executable umbrella crates to avoid duplicate symbol issues.
corrosion_import_crate(
        MANIFEST_PATH nes-network/Cargo.toml
        CRATES nes_rust_bindings
        IMPORTED_CRATES CRATES
        CRATE_TYPES staticlib
        FLAGS ${ADDITIONAL_CARGOFLAGS}
)

# Generate the nes-sources Rust workspace Cargo.toml from the member list.
# This replaces a hand-maintained file — developers should not edit it directly.
set(_NES_SOURCES_RUST_DIR "${PROJECT_SOURCE_DIR}/nes-sources/rust")
generate_rust_workspace("${_NES_SOURCES_RUST_DIR}"
    nes-buffer-bindings nes-source-bindings nes-sink-bindings nes-source-runtime)

corrosion_import_crate(
        MANIFEST_PATH nes-sources/rust/Cargo.toml
        CRATES nes_buffer_bindings nes_source_bindings nes_sink_bindings
        IMPORTED_CRATES SOURCE_CRATES
        CRATE_TYPES staticlib
        FLAGS ${ADDITIONAL_CARGOFLAGS}
)

# Register Rust crate source locations for umbrella generation
register_rust_crate(nes_buffer_bindings "${_NES_SOURCES_RUST_DIR}/nes-buffer-bindings")
register_rust_crate(nes_source_bindings "${_NES_SOURCES_RUST_DIR}/nes-source-bindings")
register_rust_crate(nes_sink_bindings "${_NES_SOURCES_RUST_DIR}/nes-sink-bindings")
register_rust_crate(nes_source_runtime "${_NES_SOURCES_RUST_DIR}/nes-source-runtime")
register_rust_crate(nes_rust_bindings "${PROJECT_SOURCE_DIR}/nes-network/nes-rust-bindings")

# Add Rust unit tests to ctest (one test per crate, run via `cargo test`)
add_rust_cargo_tests("${_NES_SOURCES_RUST_DIR}"
    nes_buffer_bindings nes_source_bindings nes_sink_bindings nes_source_runtime)

# Detect the CXX version used by the cxxbridge CLI so the umbrella can pin the same version.
# All Rust workspaces MUST use the same CXX version (align via Cargo.lock).
execute_process(
        COMMAND ${_CORROSION_CARGO} tree -i cxx --depth=0
        WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}/nes-network/nes-rust-bindings"
        OUTPUT_VARIABLE _cxx_tree_output
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE _cxx_tree_result
)
if("${_cxx_tree_result}" EQUAL "0" AND _cxx_tree_output MATCHES "cxx v([0-9]+\\.[0-9]+\\.[0-9]+)")
    set(NES_CXX_VERSION "${CMAKE_MATCH_1}" CACHE INTERNAL "CXX crate version for umbrella builds")
    message(STATUS "Detected CXX crate version: ${NES_CXX_VERSION}")
else()
    message(FATAL_ERROR "Failed to detect CXX version from nes-network workspace")
endif()

# Cache cargo flags and env vars so target_link_rust() can use them for umbrella builds
list(JOIN CXXFLAGS_LIST " " ADDITIONAL_CXXFLAGS)
list(JOIN RUSTFLAGS_LIST " " ADDITIONAL_RUSTFLAGS)
set(ENV_VARS_LIST "")

if (NOT "${ADDITIONAL_CXXFLAGS}" STREQUAL "")
    list(APPEND ENV_VARS_LIST CXXFLAGS=${ADDITIONAL_CXXFLAGS})
endif ()

if (NOT "${ADDITIONAL_RUSTFLAGS}" STREQUAL "")
    list(APPEND ENV_VARS_LIST RUSTFLAGS=${ADDITIONAL_RUSTFLAGS})
endif ()

set(NES_RUST_CARGO_FLAGS "${ADDITIONAL_CARGOFLAGS}" CACHE INTERNAL "Cargo flags for Rust crate builds")
set(NES_RUST_ENV_VARS "${ENV_VARS_LIST}" CACHE INTERNAL "Environment variables for Rust crate builds")

if (NOT "${ENV_VARS_LIST}" STREQUAL "")
    list(JOIN ENV_VARS_LIST " " ENV_VARS)
    message(STATUS "Additional Corrosion Environment Variables: ${ENV_VARS}")
    set_property(
            TARGET nes_rust_bindings
            PROPERTY CORROSION_ENVIRONMENT_VARIABLES ${ENV_VARS_LIST})
    set_property(
            TARGET nes_buffer_bindings
            PROPERTY CORROSION_ENVIRONMENT_VARIABLES ${ENV_VARS_LIST})
    set_property(
            TARGET nes_source_bindings
            PROPERTY CORROSION_ENVIRONMENT_VARIABLES ${ENV_VARS_LIST})
    set_property(
            TARGET nes_sink_bindings
            PROPERTY CORROSION_ENVIRONMENT_VARIABLES ${ENV_VARS_LIST})
endif ()
