# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function(is_inlining_supported result_var verbose)
    if (NOT TARGET nautilus::InliningPass)
        if (${verbose})
            message(WARNING "Nautilus function inlining pass was not built with nautilus. Inlining can't be applied. You can safely ignore this warning.")
        endif ()
        set(${result_var} FALSE PARENT_SCOPE)
        return()
    endif ()

    if (NOT (CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
        if (${verbose})
            message(WARNING "Nautilus function inlining requires clang and will not be applied. You can safely ignore this warning.")
        endif ()
        set(${result_var} FALSE PARENT_SCOPE)
        return()
    endif ()

    # Check clang version
    if (NOT (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "19.0.0"
            AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "20"))
        set(${result_var} FALSE PARENT_SCOPE)
        if (${verbose})
            message(WARNING "Nautilus function inlining requires clang 19 during compilation. Probably also works with other clang versions. Adjust the version-check in CMake and find out. You can safely ignore this warning.")
        endif ()
        return()
    endif ()

    # Disable inlining for ARM
    string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" SYSTEM_PROCESSOR)
    if (SYSTEM_PROCESSOR MATCHES "arm" OR SYSTEM_PROCESSOR MATCHES "aarch64")
        if (${verbose})
            message(WARNING "Nautilus function inlining is not supported on ARM and will not be applied. You can safely ignore this warning.")
        endif ()
        set(${result_var} FALSE PARENT_SCOPE)
        return()
    endif ()

    set(${result_var} TRUE PARENT_SCOPE)
endfunction()

find_package(nautilus REQUIRED CONFIG)
find_package(fmt REQUIRED CONFIG)
find_package(spdlog REQUIRED CONFIG)
find_package(MLIR REQUIRED CONFIG)
is_inlining_supported(NAUTILUS_INLINE_SUPPORTED TRUE)

# ---------------------------------------------------------------------------
# Tagged inline configuration
#
# NES_INLINE_ALL:       When ON, all NAUTILUS_TAGGED_INLINE(tag) expand to NAUTILUS_INLINE.
# NES_INLINE_TAG_FILE:  Path to a YAML file with 'enable' and 'disable' lists of tags.
#
# The YAML format:
#   enable:
#     - parse_null
#     - parse_not_null
#   disable:
#     - csv_parse_field
#
# After editing the YAML or changing these options, reconfigure CMake and rebuild.
# ---------------------------------------------------------------------------
option(NES_INLINE_ALL "Enable Nautilus inlining for ALL tagged functions" OFF)
set(NES_INLINE_TAG_FILE "${PROJECT_SOURCE_DIR}/testing/configurations/InlineTagConfig.yaml" CACHE FILEPATH "Path to YAML file with inline tag configuration")

set(NES_INLINE_TAG_CONFIG_DIR "${CMAKE_BINARY_DIR}/generated/inline")

# Parse a simple YAML file with 'enable' and 'disable' lists.
# Sets ${out_enabled} and ${out_disabled} in parent scope.
function(nes_parse_inline_tag_yaml yaml_path out_enabled out_disabled)
    set(enabled_tags "")
    set(disabled_tags "")

    if (NOT EXISTS "${yaml_path}")
        message(STATUS "InlineTagConfig: YAML file '${yaml_path}' not found, no tags configured.")
        set(${out_enabled} "" PARENT_SCOPE)
        set(${out_disabled} "" PARENT_SCOPE)
        return()
    endif ()

    file(READ "${yaml_path}" yaml_content)
    # Normalize line endings
    string(REPLACE "\r\n" "\n" yaml_content "${yaml_content}")

    # State machine: track which section we're in (enable / disable / none)
    set(current_section "none")
    string(REPLACE "\n" ";" yaml_lines "${yaml_content}")
    foreach (line IN LISTS yaml_lines)
        # Strip leading/trailing whitespace
        string(STRIP "${line}" line)
        # Skip empty lines and comments
        if ("${line}" STREQUAL "" OR "${line}" MATCHES "^#")
            continue()
        endif ()
        # Check for section headers
        if ("${line}" MATCHES "^enable:")
            set(current_section "enable")
            continue()
        elseif ("${line}" MATCHES "^disable:")
            set(current_section "disable")
            continue()
        endif ()
        # Parse list items (  - tag_name)
        if ("${line}" MATCHES "^- +(.+)$")
            set(tag "${CMAKE_MATCH_1}")
            string(STRIP "${tag}" tag)
            if ("${current_section}" STREQUAL "enable")
                list(APPEND enabled_tags "${tag}")
            elseif ("${current_section}" STREQUAL "disable")
                list(APPEND disabled_tags "${tag}")
            endif ()
        endif ()
    endforeach ()

    set(${out_enabled} "${enabled_tags}" PARENT_SCOPE)
    set(${out_disabled} "${disabled_tags}" PARENT_SCOPE)
endfunction()

# Generate InlineTagConfig.generated.hpp from the YAML configuration.
# Called once at configure time; the generated header is included by InlineTagMacro.hpp.
function(nes_generate_inline_tag_config)
    set(header_path "${NES_INLINE_TAG_CONFIG_DIR}/InlineTagConfig.generated.hpp")

    # Parse the YAML config
    nes_parse_inline_tag_yaml("${NES_INLINE_TAG_FILE}" enabled_tags disabled_tags)

    set(content "// Auto-generated by NautilusInlining.cmake from ${NES_INLINE_TAG_FILE} — do not edit\n")
    string(APPEND content "#pragma once\n\n")

    if (NES_INLINE_ALL)
        string(APPEND content "#define NES_INLINE_GLOBAL 1\n")
    else ()
        string(APPEND content "#define NES_INLINE_GLOBAL 0\n")
    endif ()

    string(APPEND content "\n// Per-tag inline settings (1 = inline, 0 = no inline)\n")
    foreach (tag IN LISTS enabled_tags)
        string(APPEND content "#define NES_INLINE_TAG_${tag} 1\n")
    endforeach ()
    foreach (tag IN LISTS disabled_tags)
        string(APPEND content "#define NES_INLINE_TAG_${tag} 0\n")
    endforeach ()

    file(WRITE "${header_path}" "${content}")
    message(STATUS "InlineTagConfig: global=${NES_INLINE_ALL}, enabled=[${enabled_tags}], disabled=[${disabled_tags}]")
endfunction()

nes_generate_inline_tag_config()

function(nautilus_inline target)
    # nautilus needs to be linked either way to build targets that use the inline header
    target_link_libraries(${target} PUBLIC nautilus::nautilus)

    # Make the generated InlineTagConfig.generated.hpp available
    target_include_directories(${target} PRIVATE "${NES_INLINE_TAG_CONFIG_DIR}")

    is_inlining_supported(NAUTILUS_INLINE_SUPPORTED FALSE)
    if (NAUTILUS_INLINE_SUPPORTED)
        target_compile_options(${target} PRIVATE "-fpass-plugin=$<TARGET_FILE:nautilus::InliningPass>")

        message(STATUS "Applying nautilus inlining pass to target ${target}")
    endif ()
endfunction()
