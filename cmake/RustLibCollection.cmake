# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# RustLibCollection.cmake — Transitive Rust crate collection and per-executable umbrella generation.
#
# Multiple Rust staticlib crates (e.g. spdlog_bindings, nes_network_bindings) may depend on
# the same crates. Linking those staticlibs directly can produce duplicate symbols, including
# symbols from the CXX runtime. This module:
#
#   1. Tracks which Rust crates each C++ target needs (via INTERFACE properties)
#   2. At executable link time, walks the transitive dependency graph to collect all needed crates
#   3. For multi-crate executables, generates a per-executable umbrella staticlib that bundles
#      exactly those crates — deduplicating shared deps (CXX, tokio, etc.)
#
# The umbrella is a standalone crate generated in ${CMAKE_BINARY_DIR}/rust-umbrellas/<exe>/,
# with path dependencies pointing at the source-tree crate directories. Individual crate
# Cargo.toml files must include "rlib" in crate-type so they can be used as dependencies.

# Register a Rust crate's source directory so the umbrella generator can find it.
function(register_rust_crate rust_crate manifest_dir)
    set_property(GLOBAL PROPERTY NES_RUST_CRATE_${rust_crate}_PATH "${manifest_dir}")
endfunction()

# Add `cargo test` as a ctest test for a Rust crate that can link without CMake-built C++ sources.
#
# Usage:
#   add_rust_cargo_test(nes_network)
#
# Tests are only added when NES_ENABLES_TESTS is ON.
# Uses the same Cargo binary and toolchain that Corrosion detected to avoid
# mismatches with rust-toolchain.toml overrides.
function(add_rust_cargo_test crate)
    if (NOT NES_ENABLES_TESTS)
        return()
    endif ()

    # Use the exact rustup toolchain selected by Corrosion. The Cargo version
    # (for example, 1.91.0) is not necessarily an installed rustup toolchain
    # name and can trigger an unwanted download instead of using `nightly`.
    set(_test_env "RUSTUP_TOOLCHAIN=${Rust_TOOLCHAIN}")
    get_property(_rustc CACHE Rust_COMPILER_CACHED PROPERTY VALUE)
    list(APPEND _test_env "RUSTC=${_rustc}")
    if (NES_RUST_ENV_VARS)
        list(APPEND _test_env ${NES_RUST_ENV_VARS})
    endif ()

    add_test(
            NAME "rust-${crate}"
            COMMAND "${_CORROSION_CARGO}" test -p ${crate} --color=always
            WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    )
    set_tests_properties("rust-${crate}" PROPERTIES
            ENVIRONMENT "${_test_env}"
            LABELS "rust"
    )

    # Serialize Rust tests to avoid parallel cargo/rustup conflicts
    # (shared target directory, potential toolchain downloads).
    get_property(_prev_test GLOBAL PROPERTY NES_PREVIOUS_RUST_CARGO_TEST)
    if (_prev_test)
        set_tests_properties("rust-${crate}" PROPERTIES DEPENDS "${_prev_test}")
    endif ()
    set_property(GLOBAL PROPERTY NES_PREVIOUS_RUST_CARGO_TEST "rust-${crate}")
endfunction()

# Declare that a C++ target requires a Rust crate at link time.
# This propagates transitively through target_link_libraries chains via INTERFACE properties.
#
function(target_link_rust_lib cpp_target rust_crate)
    set_property(TARGET ${cpp_target} APPEND PROPERTY
            INTERFACE_NES_RUST_LIBS "${rust_crate}")
endfunction()

# Create a CXX bridge using the NES defaults:
#   - expose a generated-code-only target through nes-codegen
#   - route Rust symbols through the umbrella instead of linking the crate directly
#
# Usage:
#   add_cxx_bridge(<target> CRATE <crate> FILES <source>...)
function(add_cxx_bridge bridge_target)
    cmake_parse_arguments(PARSE_ARGV 1 ARG "" "CRATE" "FILES")

    if (ARG_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR "add_cxx_bridge(${bridge_target}): unexpected arguments: ${ARG_UNPARSED_ARGUMENTS}")
    endif ()
    if (NOT ARG_CRATE)
        message(FATAL_ERROR "add_cxx_bridge(${bridge_target}): CRATE is required")
    endif ()
    if (NOT ARG_FILES)
        message(FATAL_ERROR "add_cxx_bridge(${bridge_target}): FILES requires at least one Rust source file")
    endif ()

    set(codegen_target "${bridge_target}-codegen")
    corrosion_add_cxxbridge(
            ${bridge_target}
            CRATE ${ARG_CRATE}
            FILES ${ARG_FILES}
            REGEN_TARGET ${codegen_target}
    )
    add_dependencies(nes-codegen ${codegen_target})

    # Register the bridge for umbrella linking and remove Corrosion's direct
    # link to the crate, which would introduce duplicate Rust runtime symbols.
    set_property(GLOBAL PROPERTY NES_CXX_BRIDGE_${ARG_CRATE} "${bridge_target}")
    set(crate_targets "${ARG_CRATE}" "${ARG_CRATE}-static")

    get_target_property(link_libraries ${bridge_target} LINK_LIBRARIES)
    if (link_libraries)
        list(REMOVE_ITEM link_libraries ${crate_targets})
        set_target_properties(${bridge_target} PROPERTIES LINK_LIBRARIES "${link_libraries}")
    endif ()

    get_target_property(interface_link_libraries ${bridge_target} INTERFACE_LINK_LIBRARIES)
    if (interface_link_libraries)
        foreach (crate_target IN LISTS crate_targets)
            list(REMOVE_ITEM interface_link_libraries "${crate_target}" "$<LINK_ONLY:${crate_target}>")
        endforeach ()
        set_target_properties(${bridge_target} PROPERTIES INTERFACE_LINK_LIBRARIES "${interface_link_libraries}")
    endif ()
endfunction()

# Collect all Rust crates transitively required by a target.
# Walks LINK_LIBRARIES and INTERFACE_LINK_LIBRARIES, collecting INTERFACE_NES_RUST_LIBS.
function(collect_transitive_rust_libs target out_var)
    set_property(GLOBAL PROPERTY _NES_RUST_COLLECT_VISITED "")
    set_property(GLOBAL PROPERTY _NES_RUST_COLLECT_RESULT "")
    _collect_rust_libs_recursive(${target})
    get_property(result GLOBAL PROPERTY _NES_RUST_COLLECT_RESULT)
    list(REMOVE_DUPLICATES result)
    set(${out_var} "${result}" PARENT_SCOPE)
endfunction()

function(_collect_rust_libs_recursive target)
    get_property(visited GLOBAL PROPERTY _NES_RUST_COLLECT_VISITED)
    if ("${target}" IN_LIST visited)
        return()
    endif ()
    set_property(GLOBAL APPEND PROPERTY _NES_RUST_COLLECT_VISITED "${target}")

    if (NOT TARGET ${target})
        return()
    endif ()

    get_target_property(rust_libs ${target} INTERFACE_NES_RUST_LIBS)
    if (rust_libs)
        set_property(GLOBAL APPEND PROPERTY _NES_RUST_COLLECT_RESULT ${rust_libs})
    endif ()

    foreach (prop LINK_LIBRARIES INTERFACE_LINK_LIBRARIES)
        get_target_property(deps ${target} ${prop})
        if (deps)
            foreach (dep ${deps})
                if (TARGET ${dep})
                    _collect_rust_libs_recursive(${dep})
                endif ()
            endforeach ()
        endif ()
    endforeach ()
endfunction()

# Recursively collect every EXECUTABLE target defined under a source directory.
# BUILDSYSTEM_TARGETS never contains IMPORTED/ALIAS targets, so they are naturally
# excluded. UTILITY/library targets are filtered out by the TYPE check.
function(_nes_collect_executables dir out_var)
    set(result "")
    get_property(targets DIRECTORY "${dir}" PROPERTY BUILDSYSTEM_TARGETS)
    foreach (t IN LISTS targets)
        get_target_property(type ${t} TYPE)
        if (type STREQUAL "EXECUTABLE")
            list(APPEND result "${t}")
        endif ()
    endforeach ()
    get_property(subdirs DIRECTORY "${dir}" PROPERTY SUBDIRECTORIES)
    foreach (sd IN LISTS subdirs)
        _nes_collect_executables("${sd}" sub)
        list(APPEND result ${sub})
    endforeach ()
    set(${out_var} "${result}" PARENT_SCOPE)
endfunction()

# Link the Rust umbrella into every executable in the project. Run via
# cmake_language(DEFER) at the end of configuration, when all targets exist.
#
# We sweep all executable targets rather than overriding add_executable, because
# the vcpkg toolchain (VCPKG_APPLOCAL_DEPS) already overrides add_executable; a
# second override would recurse through the shared _add_executable backup.
# _target_link_rust_impl is a no-op for executables that transitively need no Rust
# crate, so sweeping every executable is safe.
function(_finalize_rust_links)
    _nes_collect_executables("${CMAKE_SOURCE_DIR}" exe_targets)
    foreach (exe_target IN LISTS exe_targets)
        _target_link_rust_impl("${exe_target}")
    endforeach ()
endfunction()

# Link the Rust crates needed by one executable through an umbrella.
function(_target_link_rust_impl exe_target)
    collect_transitive_rust_libs(${exe_target} rust_libs)
    if (NOT rust_libs)
        return()
    endif ()

    list(REMOVE_DUPLICATES rust_libs)
    list(SORT rust_libs)
    string(MD5 _crate_hash "${rust_libs}")
    string(SUBSTRING "${_crate_hash}" 0 12 _crate_hash_short)

    set(umbrella_name "nes_umbrella_${_crate_hash_short}")
    set(umbrella_dir "${CMAKE_BINARY_DIR}/rust-umbrellas/${_crate_hash_short}")

    # If an umbrella for this exact crate set already exists, just link it.
    if (TARGET ${umbrella_name})
        _link_umbrella_with_bridges(${exe_target} ${umbrella_name} "${rust_libs}")
        return()
    endif ()

    set(dep_lines "")
    set(extern_lines "")
    foreach (crate IN LISTS rust_libs)
        get_property(crate_path GLOBAL PROPERTY NES_RUST_CRATE_${crate}_PATH)
        if (NOT crate_path)
            message(FATAL_ERROR
                    "Rust crate '${crate}' required by ${exe_target} was not registered "
                    "with register_rust_crate(). Add a register_rust_crate() call in EnableRust.cmake.")
        endif ()
        string(APPEND dep_lines "${crate} = { path = \"${crate_path}\" }\n")
        string(APPEND extern_lines "extern crate ${crate};\n")
    endforeach ()

    # Pin the CXX version to match the cxxbridge CLI that generated the C++ bridge code.
    # All workspaces MUST use the same CXX version (enforced by Cargo.lock alignment).
    file(WRITE "${umbrella_dir}/Cargo.toml"
            "[package]
name = \"${umbrella_name}\"
version = \"0.1.0\"
edition = \"2024\"

[lib]
crate-type = [\"staticlib\"]

# Opt out of root workspace — umbrella crates are standalone.
[workspace]

[dependencies]
cxx = \"=${NES_CXX_VERSION}\"
${dep_lines}")

    file(WRITE "${umbrella_dir}/.cargo/config.toml"
            "[build]
rustflags = []
")


    file(MAKE_DIRECTORY "${umbrella_dir}/src")
    file(WRITE "${umbrella_dir}/src/lib.rs" "${extern_lines}")

    set(import_args
            MANIFEST_PATH "${umbrella_dir}/Cargo.toml"
            CRATES ${umbrella_name}
            CRATE_TYPES staticlib
    )
    if (NOT "${NES_RUST_CARGO_FLAGS}" STREQUAL "")
        list(APPEND import_args FLAGS ${NES_RUST_CARGO_FLAGS})
    endif ()
    corrosion_import_crate(${import_args})

    if (NOT "${NES_RUST_ENV_VARS}" STREQUAL "")
        set_property(TARGET ${umbrella_name}
                PROPERTY CORROSION_ENVIRONMENT_VARIABLES ${NES_RUST_ENV_VARS})
    endif ()

    _link_umbrella_with_bridges(${exe_target} ${umbrella_name} "${rust_libs}")
endfunction()

# Link the umbrella in a sandwich: umbrella → bridges → umbrella.
# The sandwich resolves both FFI directions:
#   Pass 1 (umbrella): Rust symbols for earlier C++ callers
#   Pass 2 (bridges):  C++ symbols for Rust extern "C++" calls
#   Pass 3 (umbrella): Rust symbols needed by bridge code from pass 2
function(_link_umbrella_with_bridges exe_target umbrella_name rust_libs)
    get_target_property(_configured ${umbrella_name} _NES_BRIDGE_CONFIGURED)
    if (NOT _configured)
        set(_umbrella_file "$<TARGET_FILE:${umbrella_name}-static>")
        foreach (crate ${rust_libs})
            get_property(_bridge GLOBAL PROPERTY NES_CXX_BRIDGE_${crate})
            if (_bridge)
                set_property(TARGET ${umbrella_name} APPEND PROPERTY
                        INTERFACE_LINK_LIBRARIES ${_bridge})
            endif ()
        endforeach ()
        # Second umbrella copy (file path to avoid dedup) resolves Rust symbols
        # needed by bridge objects extracted in the previous step.
        set_property(TARGET ${umbrella_name} APPEND PROPERTY
                INTERFACE_LINK_LIBRARIES "${_umbrella_file}")
        set_target_properties(${umbrella_name} PROPERTIES _NES_BRIDGE_CONFIGURED TRUE)
    endif ()

    set_property(TARGET ${exe_target} APPEND PROPERTY LINK_LIBRARIES ${umbrella_name})
endfunction()
