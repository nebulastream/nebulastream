# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generic CMake helpers for runtime registries (see nes-common/include/Util/RuntimeRegistry.hpp).
#
# Each registry has a dedicated static glue sub-library (e.g. nes-sources-Source-registration).
# Per-plugin self-registering glue translation units are added to it, and the parent target's
# INTERFACE link line wraps the sub-library with $<LINK_LIBRARY:WHOLE_ARCHIVE,...> so the linker
# keeps every glue TU alive even though nothing in the rest of the binary references the static
# initializers.
#
# create_runtime_registry must run AFTER add_library(<target>) so that the parent target exists.
# add_registry_entry calls that run before the registry is declared (possible across top-level
# components, which configure in alphabetical order) are deferred to the end of configuration.
#
# create_runtime_registry(<name> <target>
#     ENTRY_TEMPLATE <expr> | GLUE_TEMPLATE <tu>
#     [REGISTRY_CLASS <class>]        # defaults to ${name}Registry
#     [REGISTRY_HEADER <header>]      # defaults to ${REGISTRY_CLASS}.hpp
#     [HEADER_TEMPLATE <header>]      # per-plugin header to #include; supports ${PLUGIN_NAME}
#     [EXTRA_INCLUDES <header>...]    # additional #includes emitted into every glue TU
#     [INCLUDE_DIRS <dir>...]         # additional include dirs for the glue sub-library
#     [GLUE_SUFFIX <suffix>]          # glue sub-library suffix, defaults to "registration"
# )
#   name           : registry name (e.g. Source); drives generated symbol/property names.
#   target         : parent CMake target that owns the registry. Must already exist. The
#                    registry's hand-written header dir (<component>/registry/include) is exposed
#                    on this target, and the WHOLE_ARCHIVE INTERFACE link to the glue sub-library
#                    is attached here so any consumer pulls in all plugin registrations.
#   ENTRY_TEMPLATE : C++ expression passed as the second argument to
#                    ${REGISTRY_CLASS}::instance().addEntry("<key>", <expr>). The surrounding
#                    glue TU (includes, anonymous namespace, static registration, duplicate
#                    check) is generated. Supports ${PLUGIN_NAME} and ${KEY} placeholders.
#                    This is the common case; prefer it over GLUE_TEMPLATE.
#   GLUE_TEMPLATE  : full content of the generated glue TU, for registries that need a
#                    different registration shape. Supports ${PLUGIN_NAME}, ${KEY},
#                    ${REGISTRY}, ${REGISTRY_CLASS}, ${REGISTRY_HEADER} and ${PLUGIN_HEADER}
#                    (the derived per-plugin include path) placeholders.
function(create_runtime_registry name target)
    # PARSE_ARGV (not ${ARGN}) so that ENTRY_TEMPLATE/GLUE_TEMPLATE values containing
    # semicolons (C++ statements) survive argument parsing without being split as lists.
    cmake_parse_arguments(PARSE_ARGV 2 ARG "" "ENTRY_TEMPLATE;GLUE_TEMPLATE;REGISTRY_CLASS;REGISTRY_HEADER;HEADER_TEMPLATE;GLUE_SUFFIX" "EXTRA_INCLUDES;INCLUDE_DIRS")

    if(NOT TARGET ${target})
        message(FATAL_ERROR "create_runtime_registry(${name} ${target}): target '${target}' does not exist. Call add_library(${target}) before create_runtime_registry().")
    endif()
    if(NOT ARG_ENTRY_TEMPLATE AND NOT ARG_GLUE_TEMPLATE)
        message(FATAL_ERROR "create_runtime_registry(${name}): either ENTRY_TEMPLATE or GLUE_TEMPLATE must be provided.")
    endif()
    if(ARG_ENTRY_TEMPLATE AND ARG_GLUE_TEMPLATE)
        message(FATAL_ERROR "create_runtime_registry(${name}): ENTRY_TEMPLATE and GLUE_TEMPLATE are mutually exclusive.")
    endif()

    if(NOT ARG_REGISTRY_CLASS)
        set(ARG_REGISTRY_CLASS "${name}Registry")
    endif()
    if(NOT ARG_REGISTRY_HEADER)
        set(ARG_REGISTRY_HEADER "${ARG_REGISTRY_CLASS}.hpp")
    endif()
    if(NOT ARG_GLUE_SUFFIX)
        set(ARG_GLUE_SUFFIX "registration")
    endif()

    set(glue_lib "${target}-${name}-${ARG_GLUE_SUFFIX}")
    add_library(${glue_lib} STATIC)
    set_target_properties(${glue_lib} PROPERTIES LINKER_LANGUAGE CXX)
    target_link_libraries(${glue_lib} PRIVATE ${target})
    target_include_directories(${glue_lib} PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/registry/include
            ${ARG_INCLUDE_DIRS}
    )

    # Expose the whole <component>/registry/include directory on the parent's PUBLIC include
    # path so consumers can #include the registry header. For components that also call
    # create_registries_for_component, the factory registry library already exposes the same
    # directory and this is redundant (idempotent).
    target_include_directories(${target} PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/registry/include>
    )

    # Wrap the glue sub-library with WHOLE_ARCHIVE on the parent's INTERFACE link line
    # so every consumer (executable / final shared lib) keeps all glue TUs alive at link
    # time. Without this, static initializers in unreferenced TUs would be stripped and
    # the registry would be silently empty.
    target_link_libraries(${target} INTERFACE
            $<LINK_LIBRARY:WHOLE_ARCHIVE,${glue_lib}>
    )

    set_property(GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_GLUE_LIB" "${glue_lib}")
    set_property(GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_CLASS" "${ARG_REGISTRY_CLASS}")
    set_property(GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_HEADER" "${ARG_REGISTRY_HEADER}")
    set_property(GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_ENTRY_TEMPLATE" "${ARG_ENTRY_TEMPLATE}")
    set_property(GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_GLUE_TEMPLATE" "${ARG_GLUE_TEMPLATE}")
    set_property(GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_HEADER_TEMPLATE" "${ARG_HEADER_TEMPLATE}")
    set_property(GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_EXTRA_INCLUDES" "${ARG_EXTRA_INCLUDES}")
endfunction()

# add_registry_entry(<registry> <plugin_name> [KEY <key>] [HEADER <include-path>])
#   plugin_name : substituted into ${PLUGIN_NAME} in the registry's entry/header templates.
#                 By convention also the registry key.
#   KEY         : registry lookup key. Defaults to plugin_name. Use when the registered name
#                 differs from the C++ type basename, e.g. plugin_name "Absolute" / KEY "Abs".
#   HEADER      : per-entry include path override. Use when the plugin's header does not follow
#                 the derived convention, e.g. HEADER "Sources/NetworkSource.hpp".
# The caller's source directory is added to the glue sub-library's include path so headers living
# next to the plugin sources (nes-plugins layout) resolve without extra wiring.
#
# Top-level components are configured in alphabetical order, so a plugin (e.g. under nes-plugins)
# may call add_registry_entry before the owning component declared the registry. In that case the
# entry is deferred to the end of top-level configuration, when all registries exist.
function(add_registry_entry registry plugin_name)
    cmake_parse_arguments(PARSE_ARGV 2 ARG "" "KEY;HEADER" "")

    if(ARG_KEY)
        set(registry_key "${ARG_KEY}")
    else()
        set(registry_key "${plugin_name}")
    endif()

    get_property(glue_lib GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_GLUE_LIB")
    if(glue_lib)
        _add_registry_entry_impl("${registry}" "${plugin_name}" "${registry_key}" "${ARG_HEADER}"
                "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}" "${CMAKE_CURRENT_LIST_DIR}")
    else()
        cmake_language(EVAL CODE "
                cmake_language(DEFER DIRECTORY [[${PROJECT_SOURCE_DIR}]] CALL _add_registry_entry_impl [[${registry}]] [[${plugin_name}]] [[${registry_key}]] [[${ARG_HEADER}]] [[${CMAKE_CURRENT_SOURCE_DIR}]] [[${CMAKE_CURRENT_BINARY_DIR}]] [[${CMAKE_CURRENT_LIST_DIR}]])
        ")
    endif()
endfunction()

# link_plugin_library(<component> <plugin_library>)
# Links a plugin implementation library into the component that owns the registries so that the
# generated glue TUs (which reference the plugin's symbols) resolve at final link time. Defers
# until the component target exists, since top-level components configure in alphabetical order.
function(link_plugin_library component plugin_library)
    if(TARGET ${component})
        target_link_libraries(${component} INTERFACE ${plugin_library})
    else()
        cmake_language(EVAL CODE "
                cmake_language(DEFER DIRECTORY [[${PROJECT_SOURCE_DIR}]] CALL target_link_libraries [[${component}]] INTERFACE [[${plugin_library}]])
        ")
    endif()
endfunction()

function(_add_registry_entry_impl registry plugin_name registry_key header_override src_dir bin_dir list_dir)
    get_property(glue_lib GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_GLUE_LIB")
    if(NOT glue_lib)
        message(FATAL_ERROR "add_registry_entry(${registry} ${plugin_name}): registry '${registry}' has not been declared with create_runtime_registry()")
    endif()
    get_property(registry_class GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_CLASS")
    get_property(registry_header GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_HEADER")
    get_property(entry_template GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_ENTRY_TEMPLATE")
    get_property(glue_template GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_GLUE_TEMPLATE")
    get_property(header_template GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_HEADER_TEMPLATE")
    get_property(extra_includes GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_EXTRA_INCLUDES")

    target_include_directories(${glue_lib} PRIVATE ${src_dir})

    # Derive the per-plugin include path relative to the component's src/ root (mirrors the
    # include tree convention: <component>/include/<rel-dir>/<header_basename>).
    set(include_path "")
    if(header_override)
        set(include_path "${header_override}")
    elseif(header_template)
        string(REPLACE "\${PLUGIN_NAME}" "${plugin_name}" header_basename "${header_template}")
        set(search_dir "${list_dir}")
        set(component_src_dir "")
        while(NOT component_src_dir)
            get_filename_component(parent_dir "${search_dir}" DIRECTORY)
            get_filename_component(dir_name "${search_dir}" NAME)
            if(dir_name STREQUAL "src")
                set(component_src_dir "${search_dir}")
            elseif(search_dir STREQUAL parent_dir)
                set(component_src_dir "${list_dir}")
                break()
            else()
                set(search_dir "${parent_dir}")
            endif()
        endwhile()
        file(RELATIVE_PATH rel_dir "${component_src_dir}" "${src_dir}")
        if(rel_dir STREQUAL "" OR rel_dir STREQUAL ".")
            set(include_path "${header_basename}")
        else()
            set(include_path "${rel_dir}/${header_basename}")
        endif()
    endif()

    # Disambiguate the generated TU and its self-registration symbol by the registry key
    # rather than the plugin name: the key is unique within a registry, which lets a single
    # C++ type register under several keys (e.g. ExtractFromTimestamp as Day_Of, Month_Of,
    # Year_Of). For the common case where KEY is omitted, registry_key == plugin_name.
    string(MAKE_C_IDENTIFIER "${registry_key}" registry_key_identifier)

    if(glue_template)
        set(glue_content "${glue_template}")
    else()
        set(includes "#include <\${REGISTRY_HEADER}>")
        foreach(extra_include IN LISTS extra_includes)
            string(APPEND includes "\n#include <${extra_include}>")
        endforeach()
        if(include_path)
            string(APPEND includes "\n#include <\${PLUGIN_HEADER}>")
        endif()
        set(glue_content
"/// Auto-generated registration glue for \${REGISTRY}::\${KEY}.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
${includes}

namespace NES
{
namespace
{
const auto registered_${registry_key_identifier}_\${REGISTRY} = [] {
    const bool registered = \${REGISTRY_CLASS}::instance().addEntry(
        \"\${KEY}\",
        ${entry_template});
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{\"Duplicate registration for \\\"\${KEY}\\\" in \${REGISTRY}\"}};
    }
    return 0;
}();
}
}
")
    endif()

    string(REPLACE "\${PLUGIN_NAME}" "${plugin_name}" glue_content "${glue_content}")
    string(REPLACE "\${KEY}" "${registry_key}" glue_content "${glue_content}")
    string(REPLACE "\${REGISTRY_CLASS}" "${registry_class}" glue_content "${glue_content}")
    string(REPLACE "\${REGISTRY_HEADER}" "${registry_header}" glue_content "${glue_content}")
    string(REPLACE "\${REGISTRY}" "${registry}" glue_content "${glue_content}")
    string(REPLACE "\${PLUGIN_HEADER}" "${include_path}" glue_content "${glue_content}")

    set(glue_path "${bin_dir}/registry/generated/${registry_key_identifier}${registry}_registration.cpp")
    file(WRITE ${glue_path} "${glue_content}")
    target_sources(${glue_lib} PRIVATE "${glue_path}")
endfunction()
