# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# CMake helpers for the unreflection registry.
#
# Each registry has a dedicated static glue sub-library (e.g. nes-logical-operators-
# LogicalFunction-unreflection). Per-plugin self-registering glue translation units
# are added to it, and the parent target's INTERFACE link line wraps the sub-library
# with $<LINK_LIBRARY:WHOLE_ARCHIVE,...> so the linker keeps every glue TU alive even
# though nothing in the rest of the binary references the static initializers.
#
# create_unreflection_registry must run AFTER add_library(<target>) so that the
# parent target exists and AFTER which add_subdirectory'd CMakeLists can call
# add_unreflection_plugin. There is no deferred phase.

# create_unreflection_registry(<name> <target> <type_template> <header_template>)
#   name            : registry name (e.g. LogicalFunction); drives generated symbol names.
#   target          : parent CMake target that owns the registry. Must already exist.
#                     The registry's hand-written header dir is exposed on this target,
#                     and the WHOLE_ARCHIVE INTERFACE link to the glue sub-library is
#                     attached here so any consumer pulls in all plugin registrations.
#   type_template   : format string with "${PLUGIN_NAME}" placeholder; expands to the
#                     C++ type passed to context.unreflect<T>(data).
#                     Examples: "TypedLogicalOperator<${PLUGIN_NAME}LogicalOperator>",
#                               "${PLUGIN_NAME}LogicalFunction", "DataType".
#   header_template : format string with "${PLUGIN_NAME}" placeholder; expands to the
#                     header file basename to #include for each plugin.
#                     Example: "${PLUGIN_NAME}LogicalFunction.hpp"
function(create_unreflection_registry name target type_template header_template)
    if(NOT TARGET ${target})
        message(FATAL_ERROR "create_unreflection_registry(${name} ${target}): target '${target}' does not exist. Call add_library(${target}) before create_unreflection_registry().")
    endif()

    set(glue_lib "${target}-${name}-unreflection")
    add_library(${glue_lib} STATIC)
    set_target_properties(${glue_lib} PROPERTIES LINKER_LANGUAGE CXX)
    target_link_libraries(${glue_lib} PRIVATE ${target})
    target_include_directories(${glue_lib} PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/registry/include
    )

    # Expose the whole <component>/registry/include directory on the parent's PUBLIC include
    # path so consumers can #include <${name}UnreflectionRegistry.hpp>. For components that
    # also call create_registries_for_component, the factory registry library already exposes
    # the same directory and this is redundant (idempotent). For unreflection-only components
    # (e.g. Trait) this is the only thing making the registry header reachable.
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

    set_property(GLOBAL PROPERTY "${name}_UNREFLECTION_GLUE_LIB" "${glue_lib}")
    set_property(GLOBAL PROPERTY "${name}_UNREFLECTION_TYPE_TEMPLATE" "${type_template}")
    set_property(GLOBAL PROPERTY "${name}_UNREFLECTION_HEADER_TEMPLATE" "${header_template}")
endfunction()

# add_unreflection_plugin(<registry> <plugin_name> [KEY <serialized_name>])
#   plugin_name : substituted into ${PLUGIN_NAME} in the registry's type/header templates.
#                 By convention also the registry key.
#   KEY         : registry lookup key (the string the wire format carries). Defaults to
#                 plugin_name. Use when the registered name differs from the C++ type
#                 basename, e.g. plugin_name "Absolute" / KEY "Abs".
function(add_unreflection_plugin registry plugin_name)
    cmake_parse_arguments(ARG "" "KEY" "" ${ARGN})

    get_property(glue_lib GLOBAL PROPERTY "${registry}_UNREFLECTION_GLUE_LIB")
    if(NOT glue_lib)
        message(FATAL_ERROR "add_unreflection_plugin(${registry} ${plugin_name}): registry '${registry}' has not been declared with create_unreflection_registry()")
    endif()
    get_property(type_template GLOBAL PROPERTY "${registry}_UNREFLECTION_TYPE_TEMPLATE")
    get_property(header_template GLOBAL PROPERTY "${registry}_UNREFLECTION_HEADER_TEMPLATE")

    if(ARG_KEY)
        set(registry_key "${ARG_KEY}")
    else()
        set(registry_key "${plugin_name}")
    endif()

    string(REPLACE "\${PLUGIN_NAME}" "${plugin_name}" unreflect_type "${type_template}")
    string(REPLACE "\${PLUGIN_NAME}" "${plugin_name}" header_basename "${header_template}")

    # Derive the include path relative to the component's src/ root (mirrors the include
    # tree convention: <component>/include/<rel-dir>/<header_basename>).
    set(search_dir "${CMAKE_CURRENT_LIST_DIR}")
    set(component_src_dir "")
    while(NOT component_src_dir)
        get_filename_component(parent_dir "${search_dir}" DIRECTORY)
        get_filename_component(dir_name "${search_dir}" NAME)
        if(dir_name STREQUAL "src")
            set(component_src_dir "${search_dir}")
        elseif(search_dir STREQUAL parent_dir)
            set(component_src_dir "${CMAKE_CURRENT_LIST_DIR}")
            target_include_directories(${glue_lib} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
            break()
        else()
            set(search_dir "${parent_dir}")
        endif()
    endwhile()
    file(RELATIVE_PATH rel_dir "${component_src_dir}" "${CMAKE_CURRENT_SOURCE_DIR}")
    if(rel_dir STREQUAL "" OR rel_dir STREQUAL ".")
        set(include_path "${header_basename}")
    else()
        set(include_path "${rel_dir}/${header_basename}")
    endif()

    # Disambiguate the generated TU and its self-registration symbol by the registry key
    # rather than the plugin name: the key is unique within a registry, which lets a single
    # C++ type register under several serialized names (e.g. ExtractFromTimestamp as Day_Of,
    # Month_Of, Year_Of). For the common case where KEY is omitted, registry_key == plugin_name
    # and the generated names are unchanged.
    set(glue_path "${CMAKE_CURRENT_BINARY_DIR}/registry/generated/${registry_key}${registry}_unreflector.cpp")
    file(WRITE ${glue_path}
"/// Auto-generated unreflector glue for ${registry}::${registry_key}.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <${registry}UnreflectionRegistry.hpp>
#include <${include_path}>

namespace NES
{
namespace
{
const auto registered_${registry_key}_${registry} = [] {
    const bool registered = ${registry}UnreflectionRegistry::instance().addUnreflectorEntry(
        \"${registry_key}\",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<${unreflect_type}>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{\"Duplicate unreflection registration for \\\"${registry_key}\\\" in ${registry}\"}};
    }
    return 0;
}();
}
}
")
    target_sources(${glue_lib} PRIVATE "${glue_path}")
endfunction()
