
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

# Looks for the configured clang format version and enabled the format target if available.
function(project_enable_format)
    find_program(CLANG_FORMAT_EXECUTABLE NAMES clang-format-${CLANG_FORMAT_MAJOR_VERSION} clang-format)
    find_program(CLANG_FORMAT_EXECUTABLE NAMES clang-format-${LLVM_MAJOR_VERSION} clang-format)
    if (NOT CLANG_FORMAT_EXECUTABLE)
        message(WARNING "Clang-Format not found, but can be installed with 'sudo apt install clang-format'. Disabling format target.")
        return()
    endif ()

    execute_process(
            COMMAND ${CLANG_FORMAT_EXECUTABLE} --version
            OUTPUT_VARIABLE CLANG_FORMAT_VERSION
    )

    string(REGEX MATCH "^.* version ([0-9]+)\\.([0-9]+)\\.([0-9]+)" CLANG_FORMAT_MAJOR_MINOR_PATCH "${CLANG_FORMAT_VERSION}")

    if (NOT CMAKE_MATCH_1 STREQUAL ${LLVM_MAJOR_VERSION})
        message(WARNING "Incompatible clang-format version requires ${LLVM_MAJOR_VERSION}, got \"${CMAKE_MATCH_1}\". Disabling format target")
        return()
    endif ()

    message(STATUS "Enabling format targets using ${CLANG_FORMAT_EXECUTABLE}")
    add_custom_target(format       COMMAND scripts/format.sh -i WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} USES_TERMINAL)
    add_custom_target(check-format COMMAND scripts/format.sh    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} USES_TERMINAL)
endfunction(project_enable_format)

macro(get_nes_log_level_value NES_LOGGING_VALUE)
    message(STATUS "Provided log level is: ${NES_LOG_LEVEL}")
    if (${NES_LOG_LEVEL} STREQUAL "TRACE")
        message(STATUS "-- Log level is set to TRACE!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_TRACE=1")
    elseif (${NES_LOG_LEVEL} STREQUAL "DEBUG")
        message(STATUS "-- Log level is set to DEBUG!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_DEBUG=1")
    elseif (${NES_LOG_LEVEL} STREQUAL "INFO")
        message(STATUS "-- Log level is set to INFO!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_INFO=1")
    elseif (${NES_LOG_LEVEL} STREQUAL "WARN")
        message(STATUS "-- Log level is set to WARN!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_WARNING=1")
    elseif (${NES_LOG_LEVEL} STREQUAL "ERROR")
        message(STATUS "-- Log level is set to ERROR!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_ERROR=1")
    elseif (${NES_LOG_LEVEL} STREQUAL "FATAL_ERROR")
        message(STATUS "-- Log level is set to FATAL_ERROR!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_FATAL_ERROR=1")
    elseif (${NES_LOG_LEVEL} STREQUAL "LEVEL_NONE")
        message(STATUS "-- Log level is set to LEVEL_NONE!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_NONE=1")
    else ()
        message(WARNING "-- Could not set NES_LOG_LEVEL as ${NES_LOG_LEVEL} did not equal any logging level!!!  Defaulting to DEBUG!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_DEBUG=1")
    endif ()
endmacro(get_nes_log_level_value NES_LOGGING_VALUE)

function(cached_fetch_and_extract url dest)
    # WARNING: the `dest` directory must not exist before the initial configure run,
    #          e.g. this function breaks with e.g. dest set to CMAKE_BINARY_DIR
    #
    # This is so that copying only happens when `dest` does not exist, to speed up reconfiguration,
    # since copying the NES deps and clang has takes roughly 2.5s while reconfiguration takes ~15s.
    message(STATUS "Fetching ${dest} from cache ${CMAKE_DEPS_CACHE_DIR} or from url ${url}")
    string(REGEX REPLACE "/" "_"  filename ${url})  # url to filename
    string(REPLACE ":" "_"  filename ${filename})  # filename to filename
    string(REPLACE ".com" "_"  filename ${filename})  # filename to filename
    string(REGEX REPLACE "^(.*)(.tar)?\\.[A-Za-z0-9]+$" "\\1" extracted ${filename})  # remove last suffix and maybe .tar, i.e. foo.7z -> foo, bar.tar.gz -> bar

    # prevent concurrent downloads to fix concurrent configures
    file(LOCK "${CMAKE_DEPS_CACHE_DIR}/${filename}.lock" GUARD FUNCTION)

    message(STATUS "Cache filename zipped: ${filename}")
    message(STATUS "Cache filename extracted: ${extracted}")
    set(FRESH_DOWNLOAD FALSE)
    if (NOT EXISTS ${CMAKE_DEPS_CACHE_DIR}/${filename})
        message(STATUS "File ${filename} not cached, downloading!")
        set(FRESH_DOWNLOAD TRUE)
        set(CURRENT_ITERATION "0")
        set(MAX_RETRIES "3")
        while (CURRENT_ITERATION LESS MAX_RETRIES)
            # Throws an error if download is inactive for 10s
            file(DOWNLOAD ${url} ${CMAKE_DEPS_CACHE_DIR}/${filename}_tmp SHOW_PROGRESS TIMEOUT 0 INACTIVITY_TIMEOUT 10 STATUS DOWNLOAD_STATUS)
            # Retrieve download status info
            list(GET DOWNLOAD_STATUS 0 STATUS_CODE)
            list(GET DOWNLOAD_STATUS 1 ERROR_MESSAGE)
            math(EXPR CURRENT_ITERATION "${CURRENT_ITERATION} + 1") # CURRENT_ITERATION++
            if (${STATUS_CODE} EQUAL 0)
                message(STATUS "Download completed successfully")
                break()
            else ()
                message(STATUS "Error occurred during download: ${ERROR_MESSAGE}")
                message(STATUS "Retry attempt ${CURRENT_ITERATION}/${MAX_RETRIES}")
                # Remove created (incomplete) file which failed to get downloaded
                file(REMOVE ${CMAKE_DEPS_CACHE_DIR}/${filename}_tmp)
            endif ()
        endwhile ()
        if (CURRENT_ITERATION EQUAL MAX_RETRIES)
            message(FATAL_ERROR "Aborting: retry attempts exceeded while failing to download ${url}")
        endif ()
        file(RENAME ${CMAKE_DEPS_CACHE_DIR}/${filename}_tmp ${CMAKE_DEPS_CACHE_DIR}/${filename})
    endif ()
    set(FRESH_EXTRACT FALSE)
    if (FRESH_DOWNLOAD OR (NOT EXISTS ${CMAKE_DEPS_CACHE_DIR}/${extracted}))
        set(FRESH_EXTRACT TRUE)
        message(STATUS "File/Dir ${extracted} not cached, extracting!")
        file(REMOVE_RECURSE ${CMAKE_DEPS_CACHE_DIR}/${extracted})
        file(REMOVE_RECURSE ${CMAKE_DEPS_CACHE_DIR}/${extracted}_tmp)
        file(ARCHIVE_EXTRACT INPUT ${CMAKE_DEPS_CACHE_DIR}/${filename} DESTINATION ${CMAKE_DEPS_CACHE_DIR}/${extracted}_tmp)
        file(RENAME ${CMAKE_DEPS_CACHE_DIR}/${extracted}_tmp ${CMAKE_DEPS_CACHE_DIR}/${extracted})
        file(REMOVE_RECURSE ${CMAKE_DEPS_CACHE_DIR}/${extracted}_tmp)
    endif ()
    if (FRESH_EXTRACT OR (NOT EXISTS ${dest}))
        message(STATUS "Copying from cache to ${dest}!")
        file(REMOVE_RECURSE ${dest})
        file(REMOVE_RECURSE ${dest}_tmp)
        file(COPY ${CMAKE_DEPS_CACHE_DIR}/${extracted}/ DESTINATION ${dest}_tmp)
        get_filename_component(dest_name ${dest} NAME)
        if (EXISTS ${dest}_tmp/${dest_name})
            # remove duplicate dest folder name
            file(RENAME ${dest}_tmp/${dest_name} ${dest})
        else()
            file(RENAME ${dest}_tmp/ ${dest})
        endif()
        file(REMOVE_RECURSE ${dest}_tmp)
    endif ()
endfunction()

function(get_linux_lsb_release_information)
    find_program(LSB_RELEASE_EXEC lsb_release)
    if (NOT LSB_RELEASE_EXEC)
        message(WARNING "Could not detect lsb_release executable, can not gather required information")
        set(LSB_RELEASE_ID_SHORT "NULL" PARENT_SCOPE)
        set(LSB_RELEASE_VERSION_SHORT "NULL" PARENT_SCOPE)
        set(LSB_RELEASE_CODENAME_SHORT "NULL" PARENT_SCOPE)
    else ()
        execute_process(COMMAND "${LSB_RELEASE_EXEC}" --short --id OUTPUT_VARIABLE LSB_RELEASE_ID_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)
        execute_process(COMMAND "${LSB_RELEASE_EXEC}" --short --release OUTPUT_VARIABLE LSB_RELEASE_VERSION_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)
        execute_process(COMMAND "${LSB_RELEASE_EXEC}" --short --codename OUTPUT_VARIABLE LSB_RELEASE_CODENAME_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)

        set(LSB_RELEASE_ID_SHORT "${LSB_RELEASE_ID_SHORT}" PARENT_SCOPE)
        set(LSB_RELEASE_VERSION_SHORT "${LSB_RELEASE_VERSION_SHORT}" PARENT_SCOPE)
        set(LSB_RELEASE_CODENAME_SHORT "${LSB_RELEASE_CODENAME_SHORT}" PARENT_SCOPE)
    endif ()
endfunction()

macro(add_tests_if_enabled TEST_FOLDER_NAME)
    if (NES_ENABLES_TESTS)
        add_subdirectory(${TEST_FOLDER_NAME})
    endif ()
endmacro()
