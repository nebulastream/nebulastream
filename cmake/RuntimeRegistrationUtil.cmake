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
# Plugins only DESCRIBE what they provide — registries (RegistryProvision) and entries
# (EntryProvision), see nes-common/include/Plugins/PluginDescriptor.hpp. The host's
# PluginLoader performs the actual registration and owns all policy (duplicate keys,
# missing registries, optional entries, type checks). Built-in and runtime-loaded plugins share
# this protocol; only the transport differs:
#
#  - Built-in: each registry/entry gets a generated glue TU whose static initializer merely
#    COLLECTS a provision-producing function pointer in BuiltinPlugins (trivial,
#    order-independent bookkeeping). loadBuiltinPlugins() assembles the descriptor and hands it
#    to the PluginLoader. Glue TUs live in a per-registry static glue sub-library
#    (e.g. nes-sources-Source-registration) that the parent target's INTERFACE link line wraps
#    with $<LINK_LIBRARY:WHOLE_ARCHIVE,...>, so the linker keeps the otherwise-unreferenced
#    collector TUs alive.
#  - Dynamic: the same provision producers are aggregated into the plugin's generated
#    nes_plugin_describe entry point (see finalize_dynamic_plugin), which the host's
#    PluginCatalog invokes after dlopen.
#
# create_runtime_registry must run AFTER add_library(<target>) so that the parent target exists.
# add_registry_entry calls that run before the registry is declared (possible across top-level
# components, which configure in alphabetical order) are deferred to the end of configuration.
#
# create_runtime_registry(<name> <target>
#     ENTRY_TEMPLATE <expr>
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
#                    on this target. If the target is a MODULE library (a dynamic plugin), the
#                    registry is provided through the plugin's nes_plugin_describe entry point
#                    instead of the built-in collection.
#   ENTRY_TEMPLATE : C++ expression for the registry entry; it is coerced to the registry's
#                    EntryType inside the generated EntryProvision. Supports ${PLUGIN_NAME} and
#                    ${KEY} placeholders; reference the plugin's type (declared by the header
#                    from HEADER_TEMPLATE) rather than TU-local symbols.
function(create_runtime_registry name target)
    # PARSE_ARGV (not ${ARGN}) so that ENTRY_TEMPLATE values containing semicolons
    # (C++ statements) survive argument parsing without being split as lists.
    cmake_parse_arguments(PARSE_ARGV 2 ARG "" "ENTRY_TEMPLATE;REGISTRY_CLASS;REGISTRY_HEADER;HEADER_TEMPLATE;GLUE_SUFFIX" "EXTRA_INCLUDES;INCLUDE_DIRS")

    if(NOT TARGET ${target})
        message(FATAL_ERROR "create_runtime_registry(${name} ${target}): target '${target}' does not exist. Call add_library(${target}) before create_runtime_registry().")
    endif()
    if(NOT ARG_ENTRY_TEMPLATE)
        message(FATAL_ERROR "create_runtime_registry(${name}): ENTRY_TEMPLATE must be provided.")
    endif()
    get_property(existing_glue_lib GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_GLUE_LIB")
    if(existing_glue_lib)
        message(FATAL_ERROR "create_runtime_registry(${name}): a registry named '${name}' already exists (glue library '${existing_glue_lib}'). Registry names must be unique across the build.")
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
    target_include_directories(${glue_lib} PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/registry/include
            ${ARG_INCLUDE_DIRS}
    )

    # Expose the whole <component>/registry/include directory on the parent's PUBLIC include
    # path so consumers can #include the registry header (idempotent across a component's
    # registries).
    target_include_directories(${target} PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/registry/include>
    )

    # Wrap the glue sub-library with WHOLE_ARCHIVE so the linker keeps all glue TUs alive:
    # without this, the collector static initializers in unreferenced TUs would be stripped and
    # loadBuiltinPlugins() would silently see an empty collection. For libraries the link goes on
    # the INTERFACE line (the final consumer — executable or libnes — pulls the glue in); an
    # executable owning a registry is its own final consumer, so it links the glue directly
    # (INTERFACE links of executables only reach targets linking against them, which requires
    # ENABLE_EXPORTS anyway).
    get_target_property(owner_type ${target} TYPE)
    if(owner_type STREQUAL "EXECUTABLE")
        # The glue takes the executable's usage requirements compile-only: a real link edge
        # would form a glue <-> executable cycle, which CMake only allows among static libraries.
        target_link_libraries(${glue_lib} PRIVATE $<COMPILE_ONLY:${target}>)
        target_link_libraries(${target} PRIVATE
                $<LINK_LIBRARY:WHOLE_ARCHIVE,${glue_lib}>
        )
    else()
        target_link_libraries(${glue_lib} PRIVATE ${target})
        target_link_libraries(${target} INTERFACE
                $<LINK_LIBRARY:WHOLE_ARCHIVE,${glue_lib}>
        )
    endif()

    # Provide the registry to the plugin system. For dynamic plugin modules the provision goes
    # through the plugin's nes_plugin_describe entry point (generated by finalize_dynamic_plugin);
    # for everything else a collector TU feeds the built-in descriptor of loadBuiltinPlugins().
    get_target_property(target_type ${target} TYPE)
    if(target_type STREQUAL "MODULE_LIBRARY")
        set_property(GLOBAL APPEND PROPERTY "${target}_DECLARED_REGISTRY_CLASSES" "${ARG_REGISTRY_CLASS}")
        set_property(GLOBAL APPEND PROPERTY "${target}_DECLARED_REGISTRY_HEADERS" "${ARG_REGISTRY_HEADER}")
    else()
        set(provision_path "${CMAKE_CURRENT_BINARY_DIR}/registry/generated/${name}_registry_provision.cpp")
        file(WRITE ${provision_path}
"/// Auto-generated provision of ${ARG_REGISTRY_CLASS} for the plugin system.
/// Static initialization only collects the describe function; loadBuiltinPlugins() assembles
/// the descriptor and the PluginLoader performs the registration.
#include <Plugins/BuiltinPlugins.hpp>
#include <${ARG_REGISTRY_HEADER}>

namespace NES
{
namespace
{
void describeRegistry(PluginDescriptor& descriptor)
{
    descriptor.registries.push_back(provideRegistry<${ARG_REGISTRY_CLASS}>(\"${ARG_REGISTRY_CLASS}\"));
}
const auto collected${name}Registry = [] {
    BuiltinPlugins::instance().addDescriber(&describeRegistry);
    return 0;
}();
}
}
")
        target_sources(${glue_lib} PRIVATE ${provision_path})
    endif()

    set_property(GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_GLUE_LIB" "${glue_lib}")
    set_property(GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_CLASS" "${ARG_REGISTRY_CLASS}")
    set_property(GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_HEADER" "${ARG_REGISTRY_HEADER}")
    set_property(GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_ENTRY_TEMPLATE" "${ARG_ENTRY_TEMPLATE}")
    set_property(GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_HEADER_TEMPLATE" "${ARG_HEADER_TEMPLATE}")
    set_property(GLOBAL PROPERTY "${name}_RUNTIME_REGISTRY_EXTRA_INCLUDES" "${ARG_EXTRA_INCLUDES}")
endfunction()

# Defers a function call to the end of top-level configuration, when all components, targets and
# registries exist (top-level directories configure in alphabetical order, so e.g. nes-plugins
# runs before nes-sources has declared its registries). The nested EVAL expands the arguments
# NOW while deferring the CALL itself; each argument is wrapped in [[...]] bracket quoting so
# values containing spaces or semicolons survive re-parsing.
function(_defer_to_end_of_configure fn)
    list(JOIN ARGN "]] [[" deferred_args)
    cmake_language(EVAL CODE "
            cmake_language(DEFER DIRECTORY [[${PROJECT_SOURCE_DIR}]] CALL ${fn} [[${deferred_args}]])
    ")
endfunction()

# add_registry_entry(<registry> <plugin_name> [KEY <key>] [HEADER <include-path>] [OPTIONAL])
#   plugin_name : substituted into ${PLUGIN_NAME} in the registry's entry/header templates.
#                 By convention also the registry key.
#   KEY         : registry lookup key. Defaults to plugin_name. Use when the registered name
#                 differs from the C++ type basename, e.g. plugin_name "Absolute" / KEY "Abs".
#   HEADER      : per-entry include path override. Use when the plugin's header does not follow
#                 the derived convention, e.g. HEADER "Sources/NetworkSource.hpp".
#   OPTIONAL    : the entry is skipped when the loading host does not know the registry;
#                 without it, a missing registry fails the load.
# The caller's source directory is added to the glue sub-library's include path so headers living
# next to the plugin sources (nes-plugins layout) resolve without extra wiring.
#
# Top-level components are configured in alphabetical order, so a plugin (e.g. under nes-plugins)
# may call add_registry_entry before the owning component declared the registry. In that case the
# entry is deferred to the end of top-level configuration, when all registries exist.
function(add_registry_entry registry plugin_name)
    cmake_parse_arguments(PARSE_ARGV 2 ARG "OPTIONAL" "KEY;HEADER" "")

    if(ARG_KEY)
        set(registry_key "${ARG_KEY}")
    else()
        set(registry_key "${plugin_name}")
    endif()
    if(ARG_OPTIONAL)
        set(optional_literal "true")
    else()
        set(optional_literal "false")
    endif()

    get_property(glue_lib GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_GLUE_LIB")
    if(glue_lib)
        _add_registry_entry_impl("${registry}" "${plugin_name}" "${registry_key}" "${ARG_HEADER}" "${optional_literal}"
                "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}" "${CMAKE_CURRENT_LIST_DIR}")
    else()
        _defer_to_end_of_configure(_add_registry_entry_impl "${registry}" "${plugin_name}" "${registry_key}" "${ARG_HEADER}"
                "${optional_literal}" "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}" "${CMAKE_CURRENT_LIST_DIR}")
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
        _defer_to_end_of_configure(target_link_libraries "${component}" INTERFACE "${plugin_library}")
    endif()
endfunction()

# Derives the per-plugin include path relative to the component's src/ root (mirrors the
# include tree convention: <component>/include/<rel-dir>/<header_basename>).
function(_runtime_registry_derive_plugin_header out_var header_override header_template plugin_name src_dir list_dir)
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
    set(${out_var} "${include_path}" PARENT_SCOPE)
endfunction()

# Renders the EntryProvision expression shared by the built-in and dynamic glue shapes.
# Placeholders (${REGISTRY_CLASS}, ${KEY}, ${PLUGIN_NAME}) are substituted by the caller;
# ${indent} is prepended to the continuation lines.
function(_runtime_registry_render_entry_provision out_var entry_template optional_literal indent)
    set(${out_var}
"EntryProvision{
${indent}    .registryClassName = \"\${REGISTRY_CLASS}\",
${indent}    .key = \"\${KEY}\",
${indent}    .entry = std::any{\${REGISTRY_CLASS}::EntryType{${entry_template}}},
${indent}    .optional = ${optional_literal}}" PARENT_SCOPE)
endfunction()

function(_add_registry_entry_impl registry plugin_name registry_key header_override optional_literal src_dir bin_dir list_dir)
    get_property(glue_lib GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_GLUE_LIB")
    if(NOT glue_lib)
        message(FATAL_ERROR "add_registry_entry(${registry} ${plugin_name}): registry '${registry}' has not been declared with create_runtime_registry()")
    endif()
    get_property(registry_class GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_CLASS")
    get_property(registry_header GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_HEADER")
    get_property(entry_template GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_ENTRY_TEMPLATE")
    get_property(header_template GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_HEADER_TEMPLATE")
    get_property(extra_includes GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_EXTRA_INCLUDES")

    target_include_directories(${glue_lib} PRIVATE ${src_dir})

    # Include block: the registry header, registry-declared extras, and the plugin's own header.
    _runtime_registry_derive_plugin_header(include_path "${header_override}" "${header_template}"
            "${plugin_name}" "${src_dir}" "${list_dir}")
    set(includes "#include <${registry_header}>")
    foreach(extra_include IN LISTS extra_includes)
        string(APPEND includes "\n#include <${extra_include}>")
    endforeach()
    if(include_path)
        string(APPEND includes "\n#include <${include_path}>")
    endif()

    _runtime_registry_render_entry_provision(entry_provision "${entry_template}" "${optional_literal}" "    ")

    # Disambiguate the generated TU and its collector symbol by the registry key rather than
    # the plugin name: the key is unique within a registry, which lets a single C++ type
    # register under several keys (e.g. ExtractFromTimestamp as Day_Of, Month_Of, Year_Of).
    # For the common case where KEY is omitted, registry_key == plugin_name.
    string(MAKE_C_IDENTIFIER "${registry_key}" registry_key_identifier)

    set(glue_content
"/// Auto-generated entry provision for \${REGISTRY}::\${KEY}.
/// Static initialization only collects the describe function; loadBuiltinPlugins() assembles
/// the descriptor and the PluginLoader performs the registration.
#include <any>
#include <Plugins/BuiltinPlugins.hpp>
${includes}

namespace NES
{
namespace
{
void describeEntry(PluginDescriptor& descriptor)
{
    descriptor.entries.push_back(${entry_provision});
}
const auto collected_${registry_key_identifier}_\${REGISTRY} = [] {
    BuiltinPlugins::instance().addDescriber(&describeEntry);
    return 0;
}();
}
}
")

    string(REPLACE "\${PLUGIN_NAME}" "${plugin_name}" glue_content "${glue_content}")
    string(REPLACE "\${KEY}" "${registry_key}" glue_content "${glue_content}")
    string(REPLACE "\${REGISTRY_CLASS}" "${registry_class}" glue_content "${glue_content}")
    string(REPLACE "\${REGISTRY}" "${registry}" glue_content "${glue_content}")

    set(glue_path "${bin_dir}/registry/generated/${registry_key_identifier}${registry}_registration.cpp")
    file(WRITE ${glue_path} "${glue_content}")
    target_sources(${glue_lib} PRIVATE "${glue_path}")
endfunction()

# add_dynamic_registry_entry(<module> <registry> <plugin_name> [KEY <key>] [HEADER <include-path>] [OPTIONAL])
# Dynamic-plugin counterpart of add_registry_entry; accepts the same KEY/HEADER/OPTIONAL options
# with the same meaning. Only records the entry: finalize_dynamic_plugin renders all of a
# module's entries into its single generated entry-point TU, so the describers can stay in an
# anonymous namespace next to the nes_plugin_describe that calls them.
function(add_dynamic_registry_entry module registry plugin_name)
    cmake_parse_arguments(PARSE_ARGV 3 ARG "OPTIONAL" "KEY;HEADER" "")

    if(ARG_KEY)
        set(registry_key "${ARG_KEY}")
    else()
        set(registry_key "${plugin_name}")
    endif()
    if(ARG_OPTIONAL)
        set(optional_literal "true")
    else()
        set(optional_literal "false")
    endif()

    target_include_directories(${module} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

    # Encoded with '|' since the values may not contain it; resolved in _finalize_dynamic_plugin_impl,
    # which runs deferred — after every registry has been declared.
    set_property(GLOBAL APPEND PROPERTY "${module}_DYNAMIC_PLUGIN_ENTRIES"
            "${registry}|${plugin_name}|${registry_key}|${optional_literal}|${ARG_HEADER}|${CMAKE_CURRENT_SOURCE_DIR}|${CMAKE_CURRENT_LIST_DIR}")
endfunction()

# finalize_dynamic_plugin(<module>)
# Generates the exported entry points of a dynamic plugin MODULE library:
#     extern "C" int nes_plugin_abi_version();
#     extern "C" void nes_plugin_describe(NES::PluginDescriptor&);
# nes_plugin_describe fills the descriptor with the registries the module declared with
# create_runtime_registry and the entries generated by add_dynamic_registry_entry; the host's
# PluginLoader performs the registration. Deferred to end of configuration so all
# registries/entries are known.
function(finalize_dynamic_plugin module)
    _defer_to_end_of_configure(_finalize_dynamic_plugin_impl "${module}" "${CMAKE_CURRENT_BINARY_DIR}")
endfunction()

function(_finalize_dynamic_plugin_impl module bin_dir)
    get_property(entry_records GLOBAL PROPERTY "${module}_DYNAMIC_PLUGIN_ENTRIES")
    get_property(declared_classes GLOBAL PROPERTY "${module}_DECLARED_REGISTRY_CLASSES")
    get_property(declared_headers GLOBAL PROPERTY "${module}_DECLARED_REGISTRY_HEADERS")

    set(includes "")
    set(registry_provisions "")
    foreach(declared_class declared_header IN ZIP_LISTS declared_classes declared_headers)
        list(APPEND includes "${declared_header}")
        string(APPEND registry_provisions "    descriptor.registries.push_back(provideRegistry<${declared_class}>(\"${declared_class}\"));\n")
    endforeach()

    set(entry_provisions "")
    foreach(entry_record IN LISTS entry_records)
        string(REPLACE "|" ";" entry_record "${entry_record}")
        list(GET entry_record 0 registry)
        list(GET entry_record 1 plugin_name)
        list(GET entry_record 2 registry_key)
        list(GET entry_record 3 optional_literal)
        list(GET entry_record 4 header_override)
        list(GET entry_record 5 src_dir)
        list(GET entry_record 6 list_dir)

        get_property(registry_class GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_CLASS")
        if(NOT registry_class)
            message(FATAL_ERROR "add_dynamic_registry_entry(${module} ${registry} ${plugin_name}): registry '${registry}' has not been declared with create_runtime_registry()")
        endif()
        get_property(registry_header GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_HEADER")
        get_property(entry_template GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_ENTRY_TEMPLATE")
        get_property(header_template GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_HEADER_TEMPLATE")
        get_property(extra_includes GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_EXTRA_INCLUDES")

        _runtime_registry_derive_plugin_header(include_path "${header_override}" "${header_template}"
                "${plugin_name}" "${src_dir}" "${list_dir}")
        list(APPEND includes "${registry_header}" ${extra_includes})
        if(include_path)
            list(APPEND includes "${include_path}")
        endif()

        _runtime_registry_render_entry_provision(entry_provision "${entry_template}" "${optional_literal}" "    ")
        string(REPLACE "\${PLUGIN_NAME}" "${plugin_name}" entry_provision "${entry_provision}")
        string(REPLACE "\${KEY}" "${registry_key}" entry_provision "${entry_provision}")
        string(REPLACE "\${REGISTRY_CLASS}" "${registry_class}" entry_provision "${entry_provision}")
        string(APPEND entry_provisions "    descriptor.entries.push_back(${entry_provision});\n")
    endforeach()

    list(REMOVE_DUPLICATES includes)
    set(include_block "")
    foreach(include IN LISTS includes)
        string(APPEND include_block "#include <${include}>\n")
    endforeach()

    set(entry_point_path "${bin_dir}/registry/generated/${module}_plugin_entry.cpp")
    file(WRITE ${entry_point_path}
"/// Auto-generated NES dynamic plugin entry points for ${module}.
/// Resolved via dlsym by NES::PluginCatalog (see nes-common/include/Plugins/PluginCatalog.hpp).
#include <any>
#include <Plugins/PluginAbi.hpp>
${include_block}
extern \"C\" int nes_plugin_abi_version()
{
    return NES::PLUGIN_ABI_VERSION;
}

extern \"C\" void nes_plugin_describe([[maybe_unused]] NES::PluginDescriptor& descriptor)
{
    using namespace NES;
${registry_provisions}${entry_provisions}}
")
    target_sources(${module} PRIVATE "${entry_point_path}")
endfunction()
