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
set(CORROSION_NO_HOSTBUILD ON CACHE BOOL "Enable proper incremental builds for Rust targets")
FetchContent_Declare(
        Corrosion
        GIT_REPOSITORY https://github.com/ls-1801/corrosion.git
        GIT_TAG fix/avoid-always-dirty-targets
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

# All Rust crates share a single workspace rooted at ${PROJECT_SOURCE_DIR}/Cargo.toml.
# This ensures one Cargo.lock and shared target/ directory. The workspace Cargo.toml
# is checked into the repository. To add a new Rust crate, just add it to the workspace
# members in Cargo.toml — CMake auto-discovers it via `cargo metadata`.

# --- Auto-discover workspace crates via cargo metadata ---
# This eliminates the need to manually list crates in CMake when adding new Rust code.
# Any crate with "staticlib" in its crate-type is imported by Corrosion and registered
# for umbrella generation. All crates (including rlib-only) are registered for umbrella
# path resolution.
execute_process(
        COMMAND ${_CORROSION_CARGO} metadata --no-deps --format-version=1
        WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
        OUTPUT_VARIABLE _cargo_metadata_json
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE _cargo_metadata_result
)
if(NOT "${_cargo_metadata_result}" EQUAL "0")
    message(FATAL_ERROR "Failed to run 'cargo metadata' in ${PROJECT_SOURCE_DIR}")
endif()

# Parse workspace member crate names, paths, and crate-types from cargo metadata JSON.
# We use regex since CMake has no JSON parser, but cargo metadata output is well-structured.
set(_staticlib_crates "")
set(_all_crate_names "")
set(_all_crate_test_names "")

# Extract each package block: "name":"...", "manifest_path":"...", "targets":[...]
# We iterate by finding each "manifest_path" and extracting the surrounding package info.
string(JSON _num_packages LENGTH "${_cargo_metadata_json}" "packages")
math(EXPR _last_pkg "${_num_packages} - 1")

foreach(_pkg_idx RANGE 0 ${_last_pkg})
    string(JSON _pkg_name GET "${_cargo_metadata_json}" "packages" ${_pkg_idx} "name")
    string(JSON _manifest_path GET "${_cargo_metadata_json}" "packages" ${_pkg_idx} "manifest_path")

    # Get the crate directory from manifest path (strip /Cargo.toml)
    get_filename_component(_crate_dir "${_manifest_path}" DIRECTORY)

    # Check if any target has "staticlib" in its crate_types
    string(JSON _num_targets LENGTH "${_cargo_metadata_json}" "packages" ${_pkg_idx} "targets")
    math(EXPR _last_target "${_num_targets} - 1")

    set(_is_staticlib FALSE)
    foreach(_tgt_idx RANGE 0 ${_last_target})
        string(JSON _num_kinds LENGTH "${_cargo_metadata_json}" "packages" ${_pkg_idx} "targets" ${_tgt_idx} "crate_types")
        math(EXPR _last_kind "${_num_kinds} - 1")
        foreach(_kind_idx RANGE 0 ${_last_kind})
            string(JSON _kind GET "${_cargo_metadata_json}" "packages" ${_pkg_idx} "targets" ${_tgt_idx} "crate_types" ${_kind_idx})
            if("${_kind}" STREQUAL "staticlib")
                set(_is_staticlib TRUE)
            endif()
        endforeach()
    endforeach()

    if(_is_staticlib)
        list(APPEND _staticlib_crates "${_pkg_name}")
    endif()

    # Register every crate for umbrella path resolution
    register_rust_crate("${_pkg_name}" "${_crate_dir}")
    list(APPEND _all_crate_names "${_pkg_name}")
endforeach()

message(STATUS "Rust workspace crates (staticlib): ${_staticlib_crates}")

# Import only staticlib crates via Corrosion — these are the ones that produce
# CXX bridge targets. rlib-only crates are internal deps compiled transitively.
corrosion_import_crate(
        MANIFEST_PATH ${PROJECT_SOURCE_DIR}/Cargo.toml
        CRATES ${_staticlib_crates}
        IMPORTED_CRATES ALL_RUST_CRATES
        CRATE_TYPES staticlib
        FLAGS ${ADDITIONAL_CARGOFLAGS}
)

# Add Rust unit tests to ctest (one test per workspace crate, run via `cargo test`)
add_rust_cargo_tests("${PROJECT_SOURCE_DIR}" ${_all_crate_names})

# Detect the CXX version from the unified workspace so umbrella crates can pin it.
# Umbrella crates live outside the workspace, so they need an explicit version pin
# to match the cxxbridge CLI that generated the C++ bridge code.
execute_process(
        COMMAND ${_CORROSION_CARGO} tree -i cxx --depth=0
        WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
        OUTPUT_VARIABLE _cxx_tree_output
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE _cxx_tree_result
)
if("${_cxx_tree_result}" EQUAL "0" AND _cxx_tree_output MATCHES "cxx v([0-9]+\\.[0-9]+\\.[0-9]+)")
    set(NES_CXX_VERSION "${CMAKE_MATCH_1}" CACHE INTERNAL "CXX crate version for umbrella builds")
    message(STATUS "Detected CXX crate version: ${NES_CXX_VERSION}")
else()
    message(FATAL_ERROR "Failed to detect CXX version from root workspace")
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
    foreach(_crate IN LISTS ALL_RUST_CRATES)
        set_property(
                TARGET ${_crate}
                PROPERTY CORROSION_ENVIRONMENT_VARIABLES ${ENV_VARS_LIST})
    endforeach()
endif ()
