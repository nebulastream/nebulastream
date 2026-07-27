# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generates BuildInfo.hpp in nes-common containing build metadata embedded at CMake generation time:
# git SHA (+ dirty flag), build timestamp, build type, sanitizer, compiler, compiler flags, C++ standard library,
# log level and the vcpkg baseline.
# The generated header is produced via file(GENERATE) from the template next to this module.
#
# Call generate_build_info(<target> <output_dir>) after <target> has been fully configured. The header
# is written to a configuration-specific subdirectory of <output_dir>.

function(generate_build_info TARGET OUTPUT_DIR)
    # --- git SHA + dirty flag ------------------------------------------------
    set(NES_BUILD_GIT_SHA "unknown")
    set(NES_BUILD_GIT_DIRTY "")
    find_package(Git QUIET)
    if (Git_FOUND)
        execute_process(
                COMMAND ${GIT_EXECUTABLE} rev-parse HEAD
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
                OUTPUT_VARIABLE NES_BUILD_GIT_SHA
                OUTPUT_STRIP_TRAILING_WHITESPACE
                ERROR_QUIET)
        if (NES_BUILD_GIT_SHA STREQUAL "")
            set(NES_BUILD_GIT_SHA "unknown")
        endif ()

        execute_process(
                COMMAND ${GIT_EXECUTABLE} status --porcelain
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
                OUTPUT_VARIABLE NES_BUILD_GIT_STATUS
                OUTPUT_STRIP_TRAILING_WHITESPACE
                ERROR_QUIET)
        if (NOT NES_BUILD_GIT_STATUS STREQUAL "")
            set(NES_BUILD_GIT_DIRTY "+dirty")
        endif ()
    endif ()

    # --- build type ----------------------------------------------------------
    set(NES_BUILD_TYPE "$<CONFIG>")

    # --- sanitizer -----------------------------------------------------------
    # SANITIZER_OPTION is set in cmake/Sanitizers.cmake (one of none/thread/address/undefined).
    set(NES_BUILD_SANITIZER "${SANITIZER_OPTION}")

    # --- compiler ------------------------------------------------------------
    set(NES_BUILD_COMPILER "${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")

    # CMake keeps global and build-type flags outside the target's COMPILE_OPTIONS property.
    set(NES_BUILD_COMPILER_FLAGS "${CMAKE_CXX_FLAGS}")
    if (CMAKE_CONFIGURATION_TYPES)
        foreach (CONFIG IN LISTS CMAKE_CONFIGURATION_TYPES)
            string(TOUPPER "${CONFIG}" CONFIG_UPPER)
            string(APPEND NES_BUILD_COMPILER_FLAGS " $<$<CONFIG:${CONFIG}>:${CMAKE_CXX_FLAGS_${CONFIG_UPPER}}>")
        endforeach ()
    elseif (CMAKE_BUILD_TYPE)
        string(TOUPPER "${CMAKE_BUILD_TYPE}" NES_BUILD_TYPE_UPPER)
        string(APPEND NES_BUILD_COMPILER_FLAGS " ${CMAKE_CXX_FLAGS_${NES_BUILD_TYPE_UPPER}}")
    endif ()
    # Evaluate the target's options at generation time so configuration-dependent expressions are
    # resolved in the target context. COMPILE_OPTIONS already contains inherited directory options.
    string(APPEND NES_BUILD_COMPILER_FLAGS
            " $<JOIN:$<TARGET_GENEX_EVAL:${TARGET},$<TARGET_PROPERTY:${TARGET},COMPILE_OPTIONS>>, >")
    string(STRIP "${NES_BUILD_COMPILER_FLAGS}" NES_BUILD_COMPILER_FLAGS)

    # --- C++ standard library ------------------------------------------------
    # CheckToolchain.cmake has already selected and validated the standard library.
    if (USING_LIBCXX)
        set(NES_BUILD_STDLIB "libc++")
    elseif (USING_LIBSTDCXX)
        set(NES_BUILD_STDLIB "libstdc++")
    else ()
        set(NES_BUILD_STDLIB "unknown")
    endif ()

    # --- log level -----------------------------------------------------------
    set(NES_BUILD_LOG_LEVEL "${NES_LOG_LEVEL}")

    # --- vcpkg baseline ------------------------------------------------------
    set(NES_BUILD_VCPKG_BASELINE "unknown")
    set(VCPKG_MANIFEST "${PROJECT_SOURCE_DIR}/vcpkg/vcpkg.json")
    if (EXISTS "${VCPKG_MANIFEST}")
        file(READ "${VCPKG_MANIFEST}" VCPKG_MANIFEST_CONTENT)
        string(JSON NES_BUILD_VCPKG_BASELINE ERROR_VARIABLE VCPKG_BASELINE_ERROR
                GET "${VCPKG_MANIFEST_CONTENT}" "builtin-baseline")
        if (VCPKG_BASELINE_ERROR OR NES_BUILD_VCPKG_BASELINE STREQUAL "")
            set(NES_BUILD_VCPKG_BASELINE "unknown")
        endif ()
    endif ()

    # --- version placeholder -------------------------------------------------
    set(NES_BUILD_VERSION "TBA")

    # --- build timestamp -----------------------------------------------------
    string(TIMESTAMP NES_BUILD_TIMESTAMP "%Y-%m-%d %H:%M:%S %z")

    # --- generate the header -------------------------------------------------
    file(MAKE_DIRECTORY "${OUTPUT_DIR}")
    file(READ "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/templates/BuildInfo.hpp.in" NES_BUILD_INFO_TEMPLATE)
    string(CONFIGURE "${NES_BUILD_INFO_TEMPLATE}" NES_BUILD_INFO_CONTENT @ONLY)
    file(GENERATE
            OUTPUT "${OUTPUT_DIR}/$<CONFIG>/BuildInfo.hpp"
            CONTENT "${NES_BUILD_INFO_CONTENT}"
            TARGET ${TARGET})
    # Re-run CMake (and thus refresh the embedded SHA) when Git metadata changes.
    if (Git_FOUND)
        foreach (GIT_METADATA_FILE HEAD index)
            execute_process(
                    COMMAND ${GIT_EXECUTABLE} rev-parse --git-path "${GIT_METADATA_FILE}"
                    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
                    OUTPUT_VARIABLE NES_BUILD_GIT_METADATA_PATH
                    OUTPUT_STRIP_TRAILING_WHITESPACE
                    ERROR_QUIET)
            if (EXISTS "${NES_BUILD_GIT_METADATA_PATH}")
                set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
                        "${NES_BUILD_GIT_METADATA_PATH}")
            endif ()
        endforeach ()
    endif ()
endfunction()
