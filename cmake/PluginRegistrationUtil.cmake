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

    # Store the current source directory for deriving header path (for unreflection support)
    # Store relative to the component directory (nes-logical-operators/src/Functions/BooleanFunctions)
    get_filename_component(component_dir "${CMAKE_CURRENT_LIST_DIR}/../.." ABSOLUTE)
    file(RELATIVE_PATH rel_dir "${component_dir}" "${CMAKE_CURRENT_SOURCE_DIR}")
    list(GET sources 0 first_source)
    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_sources" "${rel_dir}/${first_source}")
endmacro()

# iterates over all plugins, collect all plugins with given name, inject plugins into registrar
function(generate_plugin_registrar current_dir current_binary_dir plugin_registry plugin_registry_component)
    set(registrar_header_template_path ${current_dir}/registry/templates/${plugin_registry}GeneratedRegistrar.inc.in)
    set(registrar_header_generated_path ${current_binary_dir}/registry/templates/${plugin_registry}GeneratedRegistrar.inc)

    # get the names of plugins and all plugin libraries for the plugin registry
    get_property(plugin_registry_plugin_names_final GLOBAL PROPERTY ${plugin_registry}_plugin_names)
    get_property(plugin_registry_plugin_libraries_final GLOBAL PROPERTY ${plugin_registry}_plugin_libraries)
    get_property(plugin_registry_plugin_sources_final GLOBAL PROPERTY ${plugin_registry}_plugin_sources)

    # Check if unreflection is enabled for this registry
    get_property(unreflection_enabled GLOBAL PROPERTY "${plugin_registry}_UNREFLECTION_ENABLED")

    # Check if factory functions are disabled for this registry
    get_property(factory_disabled GLOBAL PROPERTY "${plugin_registry}_FACTORY_DISABLED")

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

    # Unreflection-specific lists
    set(UNREFLECTOR_FUNCTION_DECLARATIONS "")
    set(UNREFLECTOR_REGISTER_CALLS "")

    list(LENGTH plugin_registry_plugin_names_final plugin_count)
    math(EXPR plugin_count_minus_one "${plugin_count} - 1")

    foreach (plugin_index RANGE ${plugin_count_minus_one})
        list(GET plugin_registry_plugin_names_final ${plugin_index} reg_func)

        # Generate factory functions unless explicitly disabled
        if(NOT factory_disabled)
            list(APPEND REGISTER_FUNCTION_DECLARATIONS "${plugin_registry}RegistryReturnType Register${reg_func}${plugin_registry}(${plugin_registry}RegistryArguments)")
            list(APPEND REGISTER_ALL_FUNCTION_CALLS "registry.addEntry(\"${reg_func}\", Register${reg_func}${plugin_registry})")
        endif()

        # Generate unreflection support if enabled
        if(unreflection_enabled)
            # Get the source file path for this plugin
            list(GET plugin_registry_plugin_sources_final ${plugin_index} plugin_source)

            # Derive header path from source path
            # Transform: src/Functions/ArithmeticalFunctions/AbsoluteLogicalFunction.cpp
            #       ->   Functions/ArithmeticalFunctions/AbsoluteLogicalFunction.hpp
            string(REGEX REPLACE "^src/" "" header_path "${plugin_source}")
            string(REGEX REPLACE "\\.cpp$" ".hpp" header_path "${header_path}")

            # Extract the actual type name from the source filename
            # AbsoluteLogicalFunction.cpp -> AbsoluteLogicalFunction
            get_filename_component(type_name "${plugin_source}" NAME_WE)

            # Declare the unreflector function (follows same pattern as factory functions)
            list(APPEND UNREFLECTOR_FUNCTION_DECLARATIONS "${plugin_registry}RegistryReturnType Unreflect${reg_func}${plugin_registry}(const Reflected&, const ReflectionContext&)")

            # Call to register the unreflector function in the registry
            list(APPEND UNREFLECTOR_REGISTER_CALLS "unreflectionRegistry.addUnreflectorEntry(\"${reg_func}\", Unreflect${reg_func}${plugin_registry})")

            # Generate per-plugin unreflector .cpp file that implements the unreflector function
            set(unreflector_cpp_path "${current_binary_dir}/registry/generated/${reg_func}${plugin_registry}_unreflector.cpp")
            file(WRITE ${unreflector_cpp_path}
"// Auto-generated unreflector implementation for ${type_name}
#include <${plugin_registry}Registry.hpp>
#include <${header_path}>

namespace NES::${plugin_registry}GeneratedRegistrar {

${plugin_registry}RegistryReturnType Unreflect${reg_func}${plugin_registry}(
    const Reflected& data,
    const ReflectionContext& context
) {
    return context.unreflect<${type_name}>(data);
}

} // namespace NES::${plugin_registry}GeneratedRegistrar
")

            # Add the generated file to the component
            target_sources(${plugin_registry_component} PRIVATE ${unreflector_cpp_path})
        endif()
    endforeach ()

    # link all plugin libraries against the component that the plugin registry belongs to, this makes the implementation
    # details accessible to the library of the component that the plugin registry belongs to
    foreach (plugin_library IN LISTS plugin_registry_plugin_libraries_final)
        target_link_libraries(${plugin_registry_component} PUBLIC ${plugin_library})
    endforeach ()

    # add ';'s to the end of the generated function [declarations|calls], and add further formatting (new line and tab)
    if(factory_disabled)
        set(REGISTER_FUNCTION_DECLARATIONS "")
        set(REGISTER_ALL_FUNCTION_CALLS "")
    else()
        string(REPLACE ";" ";\n" REGISTER_FUNCTION_DECLARATIONS "${REGISTER_FUNCTION_DECLARATIONS};")
        string(REPLACE ";" ";\n\t" REGISTER_ALL_FUNCTION_CALLS "${REGISTER_ALL_FUNCTION_CALLS};")
    endif()

    # Format unreflection lists
    if(unreflection_enabled)
        string(REPLACE ";" ";\n" UNREFLECTOR_FUNCTION_DECLARATIONS "${UNREFLECTOR_FUNCTION_DECLARATIONS};")
        string(REPLACE ";" ";\n\t" UNREFLECTOR_REGISTER_CALLS "${UNREFLECTOR_REGISTER_CALLS};")
    endif()

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

# Mark specific registries to use unreflection (dynamic dispatch deserialization)
# Call this BEFORE create_registries_for_component to enable unreflection for specific registries
function(enable_unreflection_for_registries)
    foreach(plugin_registry ${ARGN})
        set_property(GLOBAL PROPERTY "${plugin_registry}_UNREFLECTION_ENABLED" TRUE)
    endforeach()
endfunction()

# Disable factory function generation for registries that only need unreflection
# Call this BEFORE create_registries_for_component to disable factory functions
function(disable_factory_functions_for_registries)
    foreach(plugin_registry ${ARGN})
        set_property(GLOBAL PROPERTY "${plugin_registry}_FACTORY_DISABLED" TRUE)
    endforeach()
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
