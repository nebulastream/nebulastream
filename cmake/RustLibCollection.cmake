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
# Multiple Rust staticlib crates (e.g. nes_source_lib, nes_rust_bindings) each embed
# the CXX runtime, producing duplicate symbols when linked together. This module:
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
    set_property(GLOBAL APPEND PROPERTY NES_RUST_CRATE_NAMES "${rust_crate}")
endfunction()

# Declare that a C++ target requires a Rust crate at link time.
# This propagates transitively through target_link_libraries chains via INTERFACE properties.
function(target_link_rust_lib cpp_target rust_crate)
    set_property(TARGET ${cpp_target} APPEND PROPERTY
        INTERFACE_NES_RUST_LIBS "${rust_crate}")
endfunction()

# Remove Corrosion's automatic link from a CXX bridge target to its Rust crate.
# This prevents the individual Rust staticlib from being pulled into every exe that links
# the bridge target. The actual Rust symbols come from target_link_rust() instead.
function(cxxbridge_remove_rust_link bridge_target rust_crate)
    # Remove from LINK_LIBRARIES (PRIVATE deps added by corrosion_add_cxxbridge)
    get_target_property(libs ${bridge_target} LINK_LIBRARIES)
    if(libs)
        list(REMOVE_ITEM libs "${rust_crate}")
        set_target_properties(${bridge_target} PROPERTIES LINK_LIBRARIES "${libs}")
    endif()
    # Also remove from INTERFACE_LINK_LIBRARIES — Corrosion adds $<LINK_ONLY:crate> there
    get_target_property(iface_libs ${bridge_target} INTERFACE_LINK_LIBRARIES)
    if(iface_libs)
        list(REMOVE_ITEM iface_libs "${rust_crate}")
        list(REMOVE_ITEM iface_libs "$<LINK_ONLY:${rust_crate}>")
        set_target_properties(${bridge_target} PROPERTIES INTERFACE_LINK_LIBRARIES "${iface_libs}")
    endif()
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
    if("${target}" IN_LIST visited)
        return()
    endif()
    set_property(GLOBAL APPEND PROPERTY _NES_RUST_COLLECT_VISITED "${target}")

    if(NOT TARGET ${target})
        return()
    endif()

    get_target_property(rust_libs ${target} INTERFACE_NES_RUST_LIBS)
    if(rust_libs)
        set_property(GLOBAL APPEND PROPERTY _NES_RUST_COLLECT_RESULT ${rust_libs})
    endif()

    foreach(prop LINK_LIBRARIES INTERFACE_LINK_LIBRARIES)
        get_target_property(deps ${target} ${prop})
        if(deps)
            foreach(dep ${deps})
                if(TARGET ${dep})
                    _collect_rust_libs_recursive(${dep})
                endif()
            endforeach()
        endif()
    endforeach()
endfunction()

# Register an executable for deferred Rust link processing.
function(target_link_rust exe_target)
    set_property(GLOBAL APPEND PROPERTY _NES_RUST_EXE_TARGETS "${exe_target}")
endfunction()

# Process all accumulated target_link_rust requests (called via cmake_language(DEFER)).
function(_finalize_rust_links)
    get_property(targets GLOBAL PROPERTY _NES_RUST_EXE_TARGETS)
    if(NOT targets)
        return()
    endif()
    foreach(exe_target IN LISTS targets)
        _target_link_rust_impl("${exe_target}")
    endforeach()
endfunction()

# Deferred implementation of target_link_rust.
function(_target_link_rust_impl exe_target)
    if(NOT TARGET ${exe_target})
        return()
    endif()

    get_target_property(already_linked ${exe_target} _NES_RUST_LINKED)
    if(already_linked)
        return()
    endif()

    collect_transitive_rust_libs(${exe_target} rust_libs)
    list(LENGTH rust_libs num_libs)

    if(num_libs EQUAL 0)
        return()
    endif()

    set_target_properties(${exe_target} PROPERTIES _NES_RUST_LINKED TRUE)

    if(num_libs EQUAL 1)
        # Single crate: link the Corrosion-imported staticlib directly
        list(GET rust_libs 0 single_crate)
        set_property(TARGET ${exe_target} APPEND PROPERTY LINK_LIBRARIES ${single_crate})
        return()
    endif()

    # Multiple crates: generate a per-executable umbrella staticlib
    string(REPLACE "-" "_" safe_name "${exe_target}")
    set(umbrella_name "nes_umbrella_${safe_name}")
    set(umbrella_dir "${CMAKE_BINARY_DIR}/rust-umbrellas/${exe_target}")

    if(TARGET ${umbrella_name})
        set_property(TARGET ${exe_target} APPEND PROPERTY LINK_LIBRARIES ${umbrella_name})
        return()
    endif()

    set(dep_lines "")
    set(extern_lines "")
    foreach(crate ${rust_libs})
        get_property(crate_path GLOBAL PROPERTY NES_RUST_CRATE_${crate}_PATH)
        if(NOT crate_path)
            message(FATAL_ERROR
                "Rust crate '${crate}' required by ${exe_target} was not registered "
                "with register_rust_crate(). Add a register_rust_crate() call in EnableRust.cmake.")
        endif()
        string(APPEND dep_lines "${crate} = { path = \"${crate_path}\" }\n")
        string(APPEND extern_lines "extern crate ${crate};\n")
    endforeach()

    # Pin the CXX version to match the cxxbridge CLI that generated the C++ bridge code.
    # All workspaces MUST use the same CXX version (enforced by Cargo.lock alignment).
    file(WRITE "${umbrella_dir}/Cargo.toml"
"[package]
name = \"${umbrella_name}\"
version = \"0.1.0\"
edition = \"2021\"

[lib]
crate-type = [\"staticlib\"]

[dependencies]
cxx = \"=${NES_CXX_VERSION}\"
${dep_lines}")

    file(MAKE_DIRECTORY "${umbrella_dir}/src")
    file(WRITE "${umbrella_dir}/src/lib.rs" "${extern_lines}")

    set(import_args
        MANIFEST_PATH "${umbrella_dir}/Cargo.toml"
        CRATES ${umbrella_name}
        CRATE_TYPES staticlib
    )
    if(NOT "${NES_RUST_CARGO_FLAGS}" STREQUAL "")
        list(APPEND import_args FLAGS ${NES_RUST_CARGO_FLAGS})
    endif()
    corrosion_import_crate(${import_args})

    if(NOT "${NES_RUST_ENV_VARS}" STREQUAL "")
        set_property(TARGET ${umbrella_name}
            PROPERTY CORROSION_ENVIRONMENT_VARIABLES ${NES_RUST_ENV_VARS})
    endif()

    set_property(TARGET ${exe_target} APPEND PROPERTY LINK_LIBRARIES ${umbrella_name})
endfunction()
