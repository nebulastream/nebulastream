
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
macro(add_source PROP_NAME SOURCE_FILES)
    set(SOURCE_FILES_ABSOLUTE)
    foreach (it ${SOURCE_FILES})
        get_filename_component(ABSOLUTE_PATH ${it} ABSOLUTE)
        set(SOURCE_FILES_ABSOLUTE ${SOURCE_FILES_ABSOLUTE} ${ABSOLUTE_PATH})
    endforeach ()

    get_property(OLD_PROP_VAL GLOBAL PROPERTY "${PROP_NAME}_SOURCE_PROP")
    set_property(GLOBAL PROPERTY "${PROP_NAME}_SOURCE_PROP" ${SOURCE_FILES_ABSOLUTE} ${OLD_PROP_VAL})
endmacro()

macro(get_source PROP_NAME SOURCE_FILES)
    get_property(SOURCE_FILES_LOCAL GLOBAL PROPERTY "${PROP_NAME}_SOURCE_PROP")
    set(${SOURCE_FILES} ${SOURCE_FILES_LOCAL})
endmacro()

macro(add_source_files)
    set(SOURCE_FILES "${ARGN}")
    list(POP_FRONT SOURCE_FILES TARGET_NAME)
    add_source(${TARGET_NAME} "${SOURCE_FILES}")
endmacro()

macro(add_code_coverage)
    message(STATUS "Adding necessary flags for code coverage")
    add_compile_options(-fprofile-instr-generate -fcoverage-mapping)
    add_link_options(-fprofile-instr-generate -fcoverage-mapping)
endmacro(add_code_coverage)

macro(get_header_nes HEADER_FILES)
    file(GLOB_RECURSE ${HEADER_FILES} "include/*.h" "include/*.hpp")
endmacro()

macro(register_public_header TARGET HEADER_FILE_DIR)
    add_custom_command(TARGET ${TARGET} POST_BUILD
            COMMENT "COPY ${CMAKE_CURRENT_SOURCE_DIR}"
            COMMAND ${CMAKE_COMMAND} -E copy_directory
            ${HEADER_FILE_DIR} ${CMAKE_BINARY_DIR}/include/nebulastream)
endmacro()

macro(register_public_header_dir TARGET HEADER_FILE_DIR TARGET_DIR)
    add_custom_command(TARGET ${TARGET} POST_BUILD
            COMMENT "COPY ${CMAKE_CURRENT_SOURCE_DIR}"
            COMMAND ${CMAKE_COMMAND} -E copy_directory
            ${HEADER_FILE_DIR} ${CMAKE_BINARY_DIR}/include/nebulastream/${TARGET_DIR})
endmacro()

macro(register_public_header_file TARGET HEADER_FILE_DIR TARGET_DIR)
    add_custom_command(TARGET ${TARGET} POST_BUILD
            COMMENT "COPY ${CMAKE_CURRENT_SOURCE_DIR}"
            COMMAND ${CMAKE_COMMAND} -E copy
            ${HEADER_FILE_DIR} ${CMAKE_BINARY_DIR}/include/nebulastream/${TARGET_DIR})
endmacro()

macro(get_header_nes_client HEADER_FILES)
    file(GLOB_RECURSE ${HEADER_FILES} "include/*.h" "include/*.hpp")
endmacro()

macro(get_nes_folders output_var)
    file(GLOB NES_FOLDERS
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-*")
    # Create a semicolon-separated list of folder names
    set(NES_FOLDER_NAMES "")
    foreach(FOLDER ${NES_FOLDERS})
        get_filename_component(FOLDER_NAME ${FOLDER} NAME)
        list(APPEND NES_FOLDER_NAMES ${CMAKE_CURRENT_SOURCE_DIR}/${FOLDER_NAME})
    endforeach()
    # Join the folder names with a comma
    string(REPLACE ";" "," NES_FOLDER_NAMES_COMMA_SEPARATED "${NES_FOLDER_NAMES}")

    # Set the output variable
    set(${output_var} "${NES_FOLDER_NAMES_COMMA_SEPARATED}")
endmacro(get_nes_folders)

# Looks for the configured clang format version and enabled the format target if available.
function(project_enable_clang_format)
    find_program(CLANG_FORMAT_EXECUTABLE NAMES clang-format-${CLANG_FORMAT_MAJOR_VERSION} clang-format)
    if (NOT CLANG_FORMAT_EXECUTABLE)
        message(WARNING "Clang-Format not found, but can be installed with 'sudo apt install clang-format'. Disabling format target.")
        return()
    endif ()

    execute_process(
            COMMAND ${CLANG_FORMAT_EXECUTABLE} --version
            OUTPUT_VARIABLE CLANG_FORMAT_VERSION
    )

    string(REGEX MATCH "^.* version ([0-9]+)\\.([0-9]+)\\.([0-9]+)" CLANG_FORMAT_MAJOR_MINOR_PATCH "${CLANG_FORMAT_VERSION}")

    if (NOT CMAKE_MATCH_1 STREQUAL ${CLANG_FORMAT_MAJOR_VERSION})
        message(WARNING "Incompatible clang-format version requires ${CLANG_FORMAT_MAJOR_VERSION}, got \"${CMAKE_MATCH_1}\". Disabling format target")
        return()
    endif ()

    message(STATUS "Enabling format targets using ${CLANG_FORMAT_EXECUTABLE}")
    get_nes_folders(NES_FOLDER_NAMES_COMMA_SEPARATED)
    add_custom_target(format COMMAND python3 ${CMAKE_SOURCE_DIR}/scripts/build/run_clang_format.py ${CLANG_FORMAT_EXECUTABLE} --exclude_globs ${CMAKE_SOURCE_DIR}/clang_suppressions.txt --source_dirs ${NES_FOLDER_NAMES_COMMA_SEPARATED} --fix USES_TERMINAL)
    add_custom_target(check-format COMMAND python3 ${CMAKE_SOURCE_DIR}/scripts/build/run_clang_format.py ${CLANG_FORMAT_EXECUTABLE} --exclude_globs ${CMAKE_SOURCE_DIR}/clang_suppressions.txt --source_dirs ${NES_FOLDER_NAMES_COMMA_SEPARATED} USES_TERMINAL)
endfunction(project_enable_clang_format)

macro(project_enable_check_comment_format)
    get_nes_folders(NES_FOLDER_NAMES_COMMA_SEPARATED)
    if (NOT_ALLOWED_COMMENT_STYLE_REGEX)
        message(STATUS "comment formatting check enabled through 'check-comment-format' target.")
        add_custom_target(check-comment-format COMMAND python3 ${CMAKE_SOURCE_DIR}/scripts/build/run_check_correct_comment_style.py --source_dirs ${NES_FOLDER_NAMES_COMMA_SEPARATED} --not_allowed_comment_style \"${NOT_ALLOWED_COMMENT_STYLE_REGEX}\" USES_TERMINAL)
    else ()
        message(FATAL_ERROR "check-comment-format is not enabled as ${NOT_ALLOWED_COMMENT_STYLE_REGEX} is not set.")
    endif ()
endmacro(project_enable_check_comment_format)

macro(project_enable_check_preamble)
    get_nes_folders(NES_FOLDER_NAMES_COMMA_SEPARATED)
    if (NOT_ALLOWED_COMMENT_STYLE_REGEX)
        message(STATUS "Check Preamble (License and pragma once check) is available via the 'check-preamble' target")
        add_custom_target(check-preamble COMMAND python3 ${CMAKE_SOURCE_DIR}/scripts/build/check_preamble.py ${CMAKE_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/.no-license-check)
    else ()
        message(FATAL_ERROR "check-preamble is not enabled as ${NOT_ALLOWED_COMMENT_STYLE_REGEX} is not set.")
    endif ()
endmacro(project_enable_check_preamble)

macro(project_enable_emulated_tests)
    find_program(QEMU_EMULATOR qemu-aarch64)
    string(CONCAT SYSROOT_DIR
            "/opt/sysroots/aarch64-linux-gnu")
    string(CONCAT TESTS_DIR
            "${CMAKE_SOURCE_DIR}/build/tests")
    if (NOT ${QEMU_EMULATOR} STREQUAL "QEMU_EMULATOR-NOTFOUND")
        message("-- QEMU-emulator found, enabled testing via 'make test_$ARCH_debug' target.")
        add_custom_target(test_aarch64_debug COMMAND python3 ${CMAKE_SOURCE_DIR}/scripts/build/run_tests_cross_build.py ${QEMU_EMULATOR} ${SYSROOT_DIR} ${TESTS_DIR} USES_TERMINAL)
    else ()
        message(FATAL_ERROR "qemu-user is not installed.")
    endif ()
endmacro(project_enable_emulated_tests)

macro(get_nes_log_level_value NES_LOGGING_VALUE)
    message(STATUS "Provided log level is: ${NES_LOG_LEVEL}")
    if (${NES_LOG_LEVEL} STREQUAL "TRACE")
        message("-- Log level is set to TRACE!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_TRACE=1")
    elseif (${NES_LOG_LEVEL} STREQUAL "DEBUG")
        message("-- Log level is set to DEBUG!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_DEBUG=1")
    elseif (${NES_LOG_LEVEL} STREQUAL "INFO")
        message("-- Log level is set to INFO!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_INFO=1")
    elseif (${NES_LOG_LEVEL} STREQUAL "WARN")
        message("-- Log level is set to WARN!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_WARNING=1")
    elseif (${NES_LOG_LEVEL} STREQUAL "ERROR")
        message("-- Log level is set to ERROR!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_ERROR=1")
    elseif (${NES_LOG_LEVEL} STREQUAL "FATAL_ERROR")
        message("-- Log level is set to FATAL_ERROR!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_FATAL_ERROR=1")
    elseif (${NES_LOG_LEVEL} STREQUAL "LEVEL_NONE")
        message("-- Log level is set to LEVEL_NONE!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_NONE=1")
    else ()
        message(WARNING "-- Could not set NES_LOG_LEVEL as ${NES_LOG_LEVEL} did not equal any logging level!!!  Defaulting to DEBUG!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_DEBUG=1")
    endif ()
endmacro(get_nes_log_level_value NES_LOGGING_VALUE)