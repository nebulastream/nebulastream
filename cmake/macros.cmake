# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${CMAKE_ROOT}/Modules/ExternalProject.cmake)

# Takes a target and a list of source files and calls 'add_source' on the target (e.g., nes-memory) and the source files
macro(add_source_files)
    set(SOURCE_FILES "${ARGN}")
    list(POP_FRONT SOURCE_FILES TARGET_NAME)
    add_source(${TARGET_NAME} "${SOURCE_FILES}")
endmacro()

# Adds a list of source files to a global property that is tied to a global PROP_NAME (target, e.g., nes-memory_SOURCE_PROP)
macro(add_source PROP_NAME SOURCE_FILES)
    set(SOURCE_FILES_ABSOLUTE)
    foreach (it ${SOURCE_FILES})
        get_filename_component(ABSOLUTE_PATH ${it} ABSOLUTE)
        set(SOURCE_FILES_ABSOLUTE ${SOURCE_FILES_ABSOLUTE} ${ABSOLUTE_PATH})
    endforeach ()

    get_property(OLD_PROP_VAL GLOBAL PROPERTY "${PROP_NAME}_SOURCE_PROP")
    set_property(GLOBAL PROPERTY "${PROP_NAME}_SOURCE_PROP" ${SOURCE_FILES_ABSOLUTE} ${OLD_PROP_VAL})
endmacro()

# (builds on top of add_source_files)
# looks up the source files using a global source property (e.g., nes-memory_SOURCE_PROP) and adds the source files to SOURCE_FILES
macro(get_source PROP_NAME SOURCE_FILES)
    get_property(SOURCE_FILES_LOCAL GLOBAL PROPERTY "${PROP_NAME}_SOURCE_PROP")
    set(${SOURCE_FILES} ${SOURCE_FILES_LOCAL})
endmacro()

# Looks for the configured clang format version and enabled the format target if available.
function(project_enable_format)
    find_program(CLANG_FORMAT_EXECUTABLE NAMES clang-format-${LLVM_TOOLCHAIN_VERSION} clang-format)
    if (NOT CLANG_FORMAT_EXECUTABLE)
        message(WARNING "Clang-Format not found, but can be installed with 'sudo apt install clang-format'. Disabling format target.")
        return()
    endif ()

    execute_process(
            COMMAND ${CLANG_FORMAT_EXECUTABLE} --version
            OUTPUT_VARIABLE CLANG_FORMAT_VERSION
    )

    string(REGEX MATCH "^.* version ([0-9]+)\\.([0-9]+)\\.([0-9]+)" CLANG_FORMAT_MAJOR_MINOR_PATCH "${CLANG_FORMAT_VERSION}")

    if (NOT CMAKE_MATCH_1 STREQUAL ${LLVM_TOOLCHAIN_VERSION})
        message(WARNING "Incompatible clang-format version requires ${LLVM_TOOLCHAIN_VERSION}, got \"${CMAKE_MATCH_1}\". Disabling format target")
        return()
    endif ()

    message(STATUS "Enabling format targets using ${CLANG_FORMAT_EXECUTABLE}")
    add_custom_target(format COMMAND scripts/format.sh -i WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} USES_TERMINAL)
    add_custom_target(check-format COMMAND scripts/format.sh WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} USES_TERMINAL)
endfunction(project_enable_format)

macro(set_nes_log_level_value NES_LOGGING_VALUE)
    message(STATUS "Provided log level is: ${NES_LOG_LEVEL}")
    if (${NES_LOG_LEVEL} STREQUAL "TRACE")
        message(STATUS "-- Log level is set to TRACE!")
        add_compile_definitions(NES_LOGLEVEL_TRACE)
    elseif (${NES_LOG_LEVEL} STREQUAL "DEBUG")
        message(STATUS "-- Log level is set to DEBUG!")
        add_compile_definitions(NES_LOGLEVEL_DEBUG)
    elseif (${NES_LOG_LEVEL} STREQUAL "INFO")
        message(STATUS "-- Log level is set to INFO!")
        add_compile_definitions(NES_LOGLEVEL_INFO)
    elseif (${NES_LOG_LEVEL} STREQUAL "WARN")
        message(STATUS "-- Log level is set to WARN!")
        add_compile_definitions(NES_LOGLEVEL_WARN)
    elseif (${NES_LOG_LEVEL} STREQUAL "ERROR")
        message(STATUS "-- Log level is set to ERROR!")
        add_compile_definitions(NES_LOGLEVEL_ERROR)
    elseif (${NES_LOG_LEVEL} STREQUAL "LEVEL_NONE")
        message(STATUS "-- Log level is set to LEVEL_NONE!")
        add_compile_definitions(NES_LOGLEVEL_NONE)
    else ()
        message(WARNING "-- Could not set NES_LOG_LEVEL as ${NES_LOG_LEVEL} did not equal any logging level!!!  Defaulting to DEBUG!")
        add_compile_definitions(NES_LOGLEVEL_DEBUG)
    endif ()
endmacro(set_nes_log_level_value NES_LOGGING_VALUE)

macro(add_tests_if_enabled TEST_FOLDER_NAME)
    if (NES_ENABLES_TESTS)
        add_subdirectory(${TEST_FOLDER_NAME})
    endif ()
endmacro()

# Adds a bats end-to-end test that uses the shared lib at
# scripts/testing/distributed_bats_lib.bash. Gated on ENABLE_BATS_TESTS; if
# DOCKER_COMPOSE is set, additionally gated on ENABLE_DOCKER_TESTS. No-op
# when gates are OFF, so callers do not need to wrap calls in if() guards.
#
# Required:
#   NAME — ctest name
#   BATS_FILE — bats file path relative to CMAKE_CURRENT_SOURCE_DIR
#
# Optional:
#   DOCKER_COMPOSE — flag: run as a docker-compose-backed test (adds the
#                    DockerCompose label, RuntimeBaseImage fixture, and the
#                    NES_RUNTIME_BASE_IMAGE env var). Without it the test
#                    runs the binaries directly (offline).
#   JOBS      — bats parallelism (omit for sequential)
#   EXTRA_ENV — additional KEY=VALUE env entries (escape hatch)
#
# Always-on bats flags: --verbose-run --timing.
#
# Auto-injected env (the .bats picks whichever it needs):
#   NES_DIR, NES_TEST_TMP_DIR, NES_BATS_LIB
#   NES_WORKER         = $<TARGET_FILE:nes-single-node-worker>
#   NES_CLI            = $<TARGET_FILE:nes-cli>
#   NES_REPL           = $<TARGET_FILE:nes-repl>
#   NES_REPL_EMBEDDED  = $<TARGET_FILE:nes-repl-embedded>
#   NES_SYSTEST        = $<TARGET_FILE:systest>
#   NES_RUNTIME_BASE_IMAGE (only when DOCKER_COMPOSE is set)
#
# Fixture dependencies: E2eBinaries always; RuntimeBaseImage when
# DOCKER_COMPOSE is set. Per-test app images are built by the .bats
# setup_file with anonymous tags and rely on docker layer caching.
function(add_e2e_test)
    cmake_parse_arguments(ARG
        "DOCKER_COMPOSE"
        "NAME;BATS_FILE;JOBS"
        "EXTRA_ENV"
        ${ARGN}
    )
    if (NOT ARG_NAME)
        message(FATAL_ERROR "add_e2e_test requires NAME")
    endif ()
    if (NOT ARG_BATS_FILE)
        message(FATAL_ERROR "add_e2e_test requires BATS_FILE")
    endif ()
    if (NOT ENABLE_BATS_TESTS)
        return()
    endif ()
    if (ARG_DOCKER_COMPOSE AND NOT ENABLE_DOCKER_TESTS)
        return()
    endif ()

    set(_bats_flags --verbose-run --timing)
    if (ARG_JOBS)
        list(APPEND _bats_flags --jobs ${ARG_JOBS})
    endif ()

    set(_env_pairs
        NES_DIR=${CMAKE_SOURCE_DIR}
        NES_TEST_TMP_DIR=${CMAKE_BINARY_DIR}/test-tmp
        NES_BATS_LIB=${CMAKE_SOURCE_DIR}/scripts/testing/distributed_bats_lib.bash
        NES_WORKER=$<TARGET_FILE:nes-single-node-worker>
        NES_CLI=$<TARGET_FILE:nes-cli>
        NES_REPL=$<TARGET_FILE:nes-repl>
        NES_REPL_EMBEDDED=$<TARGET_FILE:nes-repl-embedded>
        NES_SYSTEST=$<TARGET_FILE:systest>
    )
    if (ARG_DOCKER_COMPOSE)
        list(APPEND _env_pairs NES_RUNTIME_BASE_IMAGE=${NES_RUNTIME_BASE_IMAGE})
    endif ()
    list(APPEND _env_pairs ${ARG_EXTRA_ENV})

    set(_env_cmd)
    foreach (_pair IN LISTS _env_pairs)
        list(APPEND _env_cmd env ${_pair})
    endforeach ()

    add_test(
        NAME ${ARG_NAME}
        COMMAND ${_env_cmd}
            ${BATS} ${_bats_flags}
            ${CMAKE_CURRENT_SOURCE_DIR}/${ARG_BATS_FILE}
    )

    set(_fixtures E2eBinaries)
    set(_labels "")
    if (ARG_DOCKER_COMPOSE)
        list(APPEND _fixtures RuntimeBaseImage)
        set(_labels DockerCompose)
    endif ()
    set_tests_properties(${ARG_NAME} PROPERTIES
        FIXTURES_REQUIRED "${_fixtures}"
        LABELS "${_labels}"
    )
endfunction()

# Registers a systest that needs an external system (an MQTT broker, a
# Postgres instance, ...). Plugin author supplies:
#
#   NAME         ctest name
#   PROFILE_DIR  dir holding compose.snippet.yaml (mandatory) plus aux files
#                (configs, seed data) referenced from it
#   GROUP        systest group label used to select the .test files. The
#                .test files declare `# groups: [<GROUP>, external]` so they
#                are excluded from normal systest lanes and selected here.
#
# Under the hood: shells out to the shared scripts/testing/external_systest.bats
# (no per-plugin bats file needed). At run time the base compose template is
# merged with the plugin's snippet via `docker compose -f base -f snippet`.
# The snippet can extend services.systest.depends_on to gate on the profile's
# healthchecks.
#
# Gated on ENABLE_BATS_TESTS + ENABLE_DOCKER_TESTS; no-op when either is OFF.
#
# TODO(follow-up): replace GROUP + the `external` group convention with a
# first-class `# requires: <profile>` directive in the .test parser. Today
# three things are coupled by naming convention — the .test's `groups:`
# list, the `external` exclusion in every regular lane, and this GROUP arg —
# with no validation that they match. Once systest learns about `# requires:`,
# this macro can pass the profile name directly and the `external` group
# exclusions in normal lanes go away.
function(add_systest_with_profile)
    cmake_parse_arguments(ARG "" "NAME;PROFILE_DIR;GROUP" "" ${ARGN})
    foreach (arg NAME PROFILE_DIR GROUP)
        if (NOT ARG_${arg})
            message(FATAL_ERROR "add_systest_with_profile requires ${arg}")
        endif ()
    endforeach ()
    if (NOT ENABLE_BATS_TESTS OR NOT ENABLE_DOCKER_TESTS)
        return()
    endif ()
    if (NOT IS_DIRECTORY "${ARG_PROFILE_DIR}")
        message(FATAL_ERROR
            "add_systest_with_profile: PROFILE_DIR not a directory: ${ARG_PROFILE_DIR}")
    endif ()
    if (NOT EXISTS "${ARG_PROFILE_DIR}/compose.snippet.yaml")
        message(FATAL_ERROR
            "add_systest_with_profile: missing ${ARG_PROFILE_DIR}/compose.snippet.yaml")
    endif ()

    add_test(
        NAME ${ARG_NAME}
        COMMAND
            env NES_DIR=${CMAKE_SOURCE_DIR}
            env NES_TEST_TMP_DIR=${CMAKE_BINARY_DIR}/test-tmp
            env NES_BATS_LIB=${CMAKE_SOURCE_DIR}/scripts/testing/distributed_bats_lib.bash
            env NES_SYSTEST=$<TARGET_FILE:systest>
            env NES_RUNTIME_BASE_IMAGE=${NES_RUNTIME_BASE_IMAGE}
            env PROFILE_DIR=${ARG_PROFILE_DIR}
            env PROFILE_GROUP=${ARG_GROUP}
            ${BATS} --verbose-run --timing
                ${CMAKE_SOURCE_DIR}/scripts/testing/external_systest.bats
    )
    set_tests_properties(${ARG_NAME} PROPERTIES
        FIXTURES_REQUIRED "E2eBinaries;RuntimeBaseImage"
        LABELS "DockerCompose"
    )
endfunction()
