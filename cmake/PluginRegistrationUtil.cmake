# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Includes all CMake utility functions to generate plugin registrars, which register plugins at plugin registries.

# creates a library that exposes only the registries and not the registrars
# assumes that registries are located in '${CMAKE_CURRENT_SOURCE_DIR}/registry/include' (see nes-sources for an example)
function(create_plugin_registry_library plugin_registry_library plugin_registry_component)
    add_library(${plugin_registry_library} registry)
    target_link_libraries(${plugin_registry_library} PUBLIC ${plugin_registry_component})
    target_include_directories(${plugin_registry_library}
            PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/registry/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/registry/templates> # link against generated registrar headers
            $<INSTALL_INTERFACE:include/nebulastream/>
    )
endfunction()

# enables/disables an optional plugin; if enabled, the path to the plugin becomes part of the build
function(activate_optional_plugin plugin_path plugin_option)
    if (${plugin_option})
        message(STATUS "Activating optional plugin: ${plugin_path} (and all of its dependencies).")
        add_subdirectory(${plugin_path})
    else ()
        message(STATUS "Skipping optional plugin: ${plugin_path}.")
    endif ()
endfunction()

# create a new library for the plugin and link the component that the plugin registry belongs to against it
# adds the name of plugin to the list of plugin names for the plugin registry
# adds the name of the library of the plugin to the list of libraries for the plugin registry
function(add_plugin_as_library plugin_name plugin_registry plugin_registry_component plugin_library)
    set(sources ${ARGN})
    add_library(${plugin_library} STATIC ${sources})
    target_link_libraries(${plugin_library} PRIVATE ${plugin_registry_component})

    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_names" "${plugin_name}")
    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_libraries" "${plugin_library}")
endfunction()

# adds the source files of the plugin to the source files of the component that the plugin registry belongs to
# adds the name of plugin to the list of plugin names for the plugin registry
macro(add_plugin plugin_name plugin_registry plugin_registry_component)
    set(sources ${ARGN})
    if (TARGET ${plugin_registry_component})
        foreach (source ${sources})
            set_property(TARGET ${plugin_registry_component} APPEND PROPERTY SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/${source})
        endforeach ()
    else ()
        add_source_files(${plugin_registry_component}
                ${sources}
        )
    endif ()
    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_names" "${plugin_name}")
endmacro()

# iterates over all plugins, collect all plugins with given name, inject plugins into registrar
function(generate_plugin_registrar current_dir current_binary_dir plugin_registry plugin_registry_component)
    set(registrar_header_template_path ${current_dir}/registry/templates/${plugin_registry}GeneratedRegistrar.inc.in)
    set(registrar_header_generated_path ${current_binary_dir}/registry/templates/${plugin_registry}GeneratedRegistrar.inc)

    # get the names of plugins and all plugin libraries for the plugin registry
    get_property(plugin_registry_plugin_names_final GLOBAL PROPERTY ${plugin_registry}_plugin_names)
    get_property(plugin_registry_plugin_libraries_final GLOBAL PROPERTY ${plugin_registry}_plugin_libraries)

    # first, read the Configuration(RETURN_TYPE, ARGUMENTS) from the '.in' file
    file(READ ${registrar_header_template_path} registrar_header_file_data)

    # second, remove the configuration and write the modified version of the registrar header template to a temporary file
    # we generate the final '.inc' file from that temporary file
    set(temp_registrar_header_template_file "${current_binary_dir}/temp_registrar_header_template.inc.in")
    file(WRITE ${temp_registrar_header_template_file} "${registrar_header_file_data}")

    # generate the list of declarations of the register functions that the plugins implement to register themselves
    # generate the list of concrete register calls that are called in the 'registerAll' function call of the Registrar to populate the registry
    set(REGISTER_FUNCTION_DECLARATIONS "")
    set(REGISTER_ALL_FUNCTION_CALLS "")
    foreach (reg_func IN LISTS plugin_registry_plugin_names_final)
        list(APPEND REGISTER_FUNCTION_DECLARATIONS "${plugin_registry}RegistryReturnType Register${reg_func}${plugin_registry}(${plugin_registry}RegistryArguments)")
        list(APPEND REGISTER_ALL_FUNCTION_CALLS "registry.addEntry(\"${reg_func}\", Register${reg_func}${plugin_registry})")
    endforeach ()

    # link all plugin libraries against the component that the plugin registry belongs to, this makes the implementation
    # details accessible to the library of the component that the plugin registry belongs to
    foreach (plugin_library IN LISTS plugin_registry_plugin_libraries_final)
        target_link_libraries(${plugin_registry_component} PUBLIC ${plugin_library})
    endforeach ()

    # add ';'s to the end of the generated function [declarations|calls], and add further formatting (new line and tab)
    string(REPLACE ";" ";\n" REGISTER_FUNCTION_DECLARATIONS "${REGISTER_FUNCTION_DECLARATIONS};")
    string(REPLACE ";" ";\n\t" REGISTER_ALL_FUNCTION_CALLS "${REGISTER_ALL_FUNCTION_CALLS};")

    # remove the '.in' from the end of the file and write the result to the parent directory of the the template

    configure_file(
            ${temp_registrar_header_template_file}
            ${registrar_header_generated_path}
            @ONLY
    )
    file(REMOVE ${temp_registrar_header_template_file})
endfunction()

function(generate_plugin_registrars plugin_registry_component)
    foreach (plugin_registry ${ARGN})
        cmake_language(EVAL CODE "
                cmake_language(DEFER DIRECTORY [[${PROJECT_SOURCE_DIR}]] CALL generate_plugin_registrar [[${CMAKE_CURRENT_SOURCE_DIR}]] [[${CMAKE_CURRENT_BINARY_DIR}]] [[${plugin_registry}]] [[${plugin_registry_component}]])
        ")
    endforeach ()
endfunction()

# Provide the names of all registries that the component creates as ARGS
# Registries are typically located in the 'registry' directory of the component, e.g., 'nes-sources/registry'
function(create_registries_for_component)
    get_filename_component(COMPONENT_NAME "${CMAKE_CURRENT_LIST_DIR}" NAME)
    set(registries_library ${COMPONENT_NAME}-registry)
    create_plugin_registry_library(${registries_library} ${COMPONENT_NAME})
    target_link_libraries(${COMPONENT_NAME} PRIVATE ${registries_library})
    generate_plugin_registrars(${COMPONENT_NAME} ${ARGN})
endfunction()

# Register a Rust-backed Tokio source. Zero C++ code per source.
#
# Usage:
#   add_rust_tokio_source(TokioGenerator nes_source_runtime::generator::create_generator_source)
#   add_rust_tokio_source(TokioTcp nes_tokio_tcp_source::create_tcp_source /path/to/crate)
#
# The optional 3rd argument is the absolute path to an external plugin crate.
# Sources in nes_source_runtime don't need it (it's a dependency of the registry by default).
function(add_rust_tokio_source plugin_name rust_factory_path)
    add_plugin(${plugin_name} TokioSource nes-sources)
    add_plugin(${plugin_name} SourceValidation nes-sources)

    set_property(GLOBAL APPEND PROPERTY NES_RUST_TOKIO_SOURCE_NAMES "${plugin_name}")
    set_property(GLOBAL APPEND PROPERTY NES_RUST_TOKIO_SOURCE_FACTORIES "${rust_factory_path}")

    # Optional: external crate path (for plugin crates outside nes-sources)
    if(ARGC GREATER 2)
        # Extract crate name from factory path (first :: segment)
        string(REPLACE "::" ";" _parts "${rust_factory_path}")
        list(GET _parts 0 _crate_name)
        set_property(GLOBAL APPEND PROPERTY NES_RUST_TOKIO_SOURCE_CRATE_NAMES "${_crate_name}")
        set_property(GLOBAL APPEND PROPERTY NES_RUST_TOKIO_SOURCE_CRATE_PATHS "${ARGV2}")
    endif()
endfunction()

# Generate a standalone registry crate + C++ RustTokioSources.inc from accumulated
# add_rust_tokio_source() calls. Called via cmake_language(DEFER) after all
# CMakeLists.txt files are processed.
#
# The registry crate is a separate Rust crate that:
#   - Depends on nes_source_runtime + all plugin crates
#   - Contains the AnySource enum with dispatch
#   - Exports extern "C" functions (#[no_mangle]) for spawn + validate
#   - Is linked at link time via the umbrella crate (nes_source_lib has no
#     compile-time dependency on plugin crates)
function(generate_rust_tokio_source_registry)
    get_property(source_names GLOBAL PROPERTY NES_RUST_TOKIO_SOURCE_NAMES)
    get_property(factory_paths GLOBAL PROPERTY NES_RUST_TOKIO_SOURCE_FACTORIES)
    get_property(crate_names GLOBAL PROPERTY NES_RUST_TOKIO_SOURCE_CRATE_NAMES)
    get_property(crate_paths GLOBAL PROPERTY NES_RUST_TOKIO_SOURCE_CRATE_PATHS)

    if(NOT source_names)
        return()
    endif()

    set(registry_dir "${CMAKE_BINARY_DIR}/rust/nes-source-registry")
    file(MAKE_DIRECTORY "${registry_dir}/src")

    # --- Build Cargo.toml for registry crate ---

    set(cargo_deps "nes_source_runtime = { path = \"${PROJECT_SOURCE_DIR}/nes-sources/rust/nes-source-runtime\" }\nnes_io_bindings = { path = \"${PROJECT_SOURCE_DIR}/nes-sources/rust/nes-source-bindings\" }\n")
    if(crate_names)
        list(LENGTH crate_names num_crates)
        math(EXPR last_crate "${num_crates} - 1")
        foreach(cidx RANGE 0 ${last_crate})
            list(GET crate_names ${cidx} cname)
            list(GET crate_paths ${cidx} cpath)
            string(APPEND cargo_deps "${cname} = { path = \"${cpath}\" }\n")
        endforeach()
    endif()

    file(WRITE "${registry_dir}/Cargo.toml"
"[package]
name = \"nes_source_registry\"
version = \"0.1.0\"
edition = \"2024\"

[lib]
crate-type = [\"rlib\"]

[dependencies]
${cargo_deps}")

    # --- Build enum variants and match arms ---

    set(rust_enum_variants "")
    set(rust_match_run "")
    set(rust_match_create "")
    set(rust_match_schema "")

    list(LENGTH source_names num_sources)
    math(EXPR last_idx "${num_sources} - 1")

    foreach(idx RANGE 0 ${last_idx})
        list(GET source_names ${idx} name)
        list(GET factory_paths ${idx} factory_path)

        string(REPLACE "::" ";" path_parts "${factory_path}")
        list(LENGTH path_parts num_parts)

        math(EXPR fn_idx "${num_parts} - 1")
        list(GET path_parts ${fn_idx} factory_fn)

        math(EXPR mod_end "${num_parts} - 2")
        set(module_parts "")
        foreach(part_idx RANGE 0 ${mod_end})
            list(GET path_parts ${part_idx} part)
            if(module_parts)
                set(module_parts "${module_parts}::${part}")
            else()
                set(module_parts "${part}")
            endif()
        endforeach()

        # Derive PascalCase type name from factory function name
        string(REGEX REPLACE "^create_" "" type_suffix "${factory_fn}")
        string(REPLACE "_" ";" type_words "${type_suffix}")
        set(pascal_name "")
        foreach(word ${type_words})
            string(SUBSTRING ${word} 0 1 first_char)
            string(TOUPPER ${first_char} first_char)
            string(SUBSTRING ${word} 1 -1 rest)
            set(pascal_name "${pascal_name}${first_char}${rest}")
        endforeach()

        set(full_type "${module_parts}::${pascal_name}")
        set(full_factory "${module_parts}::${factory_fn}")
        set(full_schema "${module_parts}::CONFIG_SCHEMA")

        string(APPEND rust_enum_variants "    ${name}(${full_type}),\n")
        string(APPEND rust_match_run "            Self::${name}(s) => s.run(ctx).await,\n")
        string(APPEND rust_match_create "        \"${name}\" => Ok(AnySource::${name}(\n            ${full_factory}(config)?\n        )),\n")
        string(APPEND rust_match_schema "        \"${name}\" => Ok(${full_schema}),\n")
    endforeach()

    # --- Write src/lib.rs via configure_file to avoid CMake escaping issues ---

    set(RUST_ENUM_VARIANTS "${rust_enum_variants}")
    set(RUST_MATCH_RUN "${rust_match_run}")
    set(RUST_MATCH_CREATE "${rust_match_create}")
    set(RUST_MATCH_SCHEMA "${rust_match_schema}")

    configure_file(
        "${PROJECT_SOURCE_DIR}/cmake/tokio_source_registry_template.rs.in"
        "${registry_dir}/src/lib.rs"
        @ONLY
    )

    # Register for umbrella generation. The registry crate is NOT imported via Corrosion
    # (it's an rlib, not a staticlib). It's compiled as a dependency of the per-executable
    # umbrella crate, which is where all Rust crates are unified and linked.
    register_rust_crate(nes_source_registry "${registry_dir}")
    target_link_rust_lib(nes-sources nes_source_registry)

    # --- Generate C++ RustTokioSources.inc ---

    set(inc_content "// AUTO-GENERATED by CMake. Do not edit.\n")
    foreach(name IN LISTS source_names)
        string(APPEND inc_content "RUST_TOKIO_SOURCE_IMPL(${name})\n")
    endforeach()

    file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/nes-sources")
    file(WRITE "${CMAKE_BINARY_DIR}/nes-sources/RustTokioSources.inc" "${inc_content}")
endfunction()
