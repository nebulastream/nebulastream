
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

find_program(CLANG_FORMAT_EXE clang-format)

macro(project_enable_clang_format)
    string(CONCAT FORMAT_DIRS
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-benchmark/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-benchmark/include,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-core/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-core/tests,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-core/include,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-common/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-common/tests,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-common/include,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-compiler/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-compiler/tests,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-compiler/include,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-unikernel/include,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-unikernel/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-unikernel/include,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-unikernel/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-unikernel-sink/include" ,
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-unikernel-sink/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-unikernel-source/include,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-unikernel-source/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-unikernel-export/include,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-unikernel-export/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-unikernel-common/include,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-unikernel-common/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-client/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-client/tests,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-client/include,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-data-types/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-data-types/tests,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-data-types/include,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-runtime/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-runtime/tests,"
            "${CMAKE_CURRENT_SOURCE_DIR}/nes-runtime/include ")
    if (NOT ${CLANG_FORMAT_EXECUTABLE} STREQUAL " CLANG_FORMAT_EXE-NOTFOUND")
        message(STATUS "clang-format found, whole source formatting enabled through 'format' target. ")
        add_custom_target(format COMMAND python3 ${CMAKE_SOURCE_DIR}/scripts/build/run_clang_format.py ${CLANG_FORMAT_EXECUTABLE} --exclude_globs ${CMAKE_SOURCE_DIR}/clang_suppressions.txt --source_dirs ${FORMAT_DIRS} --fix USES_TERMINAL)
        add_custom_target(format-check COMMAND python3 ${CMAKE_SOURCE_DIR}/scripts/build/run_clang_format.py ${CLANG_FORMAT_EXECUTABLE} --exclude_globs ${CMAKE_SOURCE_DIR}/clang_suppressions.txt --source_dirs ${FORMAT_DIRS} USES_TERMINAL)
    else ()
        message(FATAL_ERROR " clang-format is not installed.")
    endif ()
endmacro(project_enable_clang_format)

macro(project_enable_emulated_tests)
    find_program(QEMU_EMULATOR qemu-aarch64)
    string(CONCAT SYSROOT_DIR
            "/opt/sysroots/aarch64-linux-gnu")
    string(CONCAT TESTS_DIR
            "${CMAKE_SOURCE_DIR}/build/tests ")
    if (NOT ${QEMU_EMULATOR} STREQUAL " QEMU_EMULATOR-NOTFOUND")
        message("-- QEMU-emulator found, enabled testing via 'make test_$ARCH_debug' target. ")
        add_custom_target(test_aarch64_debug COMMAND python3 ${CMAKE_SOURCE_DIR}/scripts/build/run_tests_cross_build.py ${QEMU_EMULATOR} ${SYSROOT_DIR} ${TESTS_DIR} USES_TERMINAL)
    else ()
        message(FATAL_ERROR " qemu-user is not installed.")
    endif ()
endmacro(project_enable_emulated_tests)

macro(project_enable_release)
    if (NOT IS_GIT_DIRECTORY)
        message(AUTHOR_WARNING " -- Disabled release target as git not configured. ")
    elseif (${${PROJECT_NAME}_BRANCH_NAME} STREQUAL " master")
        add_custom_target(release COMMAND echo "Releasing Patch Version of NebulaStream")
        add_custom_command(TARGET release
                COMMAND echo "GIT-CI: Updating NES version "
                COMMAND ${CMAKE_COMMAND} -DGIT_EXECUTABLE=${GIT_EXECUTABLE} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/semver/UpdateVersion.cmake
                COMMAND echo " Push new tag to the repository "
                COMMAND ${CMAKE_COMMAND} -DGIT_EXECUTABLE=${GIT_EXECUTABLE} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/release/PushTag.cmake
                COMMAND echo " Performing post tag release steps "
                COMMAND ${CMAKE_COMMAND} -DPOST_RELEASE=1 -DGIT_EXECUTABLE=${GIT_EXECUTABLE} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/semver/UpdateVersion.cmake
                COMMAND echo " Tag release completed"
        )

        add_custom_target(minor_release COMMAND echo "Releasing Minor Version of NebulaStream")
        add_custom_command(TARGET minor_release
                COMMAND echo "GIT-CI: Updating NES version "
                COMMAND ${CMAKE_COMMAND} -DMINOR_RELEASE=1 -DGIT_EXECUTABLE=${GIT_EXECUTABLE} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/semver/UpdateVersion.cmake
                COMMAND echo " Push new tag to the repository "
                COMMAND ${CMAKE_COMMAND} -DGIT_EXECUTABLE=${GIT_EXECUTABLE} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/release/PushTag.cmake
                COMMAND echo " Performing post tag release steps "
                COMMAND ${CMAKE_COMMAND} -DPOST_RELEASE=1 -DGIT_EXECUTABLE=${GIT_EXECUTABLE} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/semver/UpdateVersion.cmake
                COMMAND echo " Tag release completed"
        )

        add_custom_target(major_release COMMAND echo "Releasing Major Version of NebulaStream")
        add_custom_command(TARGET major_release
                COMMAND echo "GIT-CI: Updating NES version "
                COMMAND ${CMAKE_COMMAND} -DMAJOR_RELEASE=1 -DGIT_EXECUTABLE=${GIT_EXECUTABLE} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/semver/UpdateVersion.cmake
                COMMAND echo " Push new tag to the repository "
                COMMAND ${CMAKE_COMMAND} -DGIT_EXECUTABLE=${GIT_EXECUTABLE} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/release/PushTag.cmake
                COMMAND echo " Performing post tag release steps "
                COMMAND ${CMAKE_COMMAND} -DPOST_RELEASE=1 -DGIT_EXECUTABLE=${GIT_EXECUTABLE} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/semver/UpdateVersion.cmake
                COMMAND echo " Tag release completed"
        )
    else ()
        message(INFO " -- Disabled release target as currently not on master branch.")
    endif ()
endmacro(project_enable_release)

macro(project_enable_version)
    if (NOT IS_GIT_DIRECTORY)
        message(AUTHOR_WARNING " -- Disabled version target as git not configured.")
    else ()
        add_custom_target(version COMMAND echo "version: ${${PROJECT_NAME}_VERSION} ")
        add_custom_command(TARGET version COMMAND cp -f ${CMAKE_CURRENT_SOURCE_DIR}/cmake/semver/version.hpp.in ${CMAKE_CURRENT_SOURCE_DIR}/nes-core/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION_MAJOR@/${${PROJECT_NAME}_VERSION_MAJOR}/g' ${CMAKE_CURRENT_SOURCE_DIR}/nes-core/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION_MINOR@/${${PROJECT_NAME}_VERSION_MINOR}/g' ${CMAKE_CURRENT_SOURCE_DIR}/nes-core/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION_PATCH@/${${PROJECT_NAME}_VERSION_PATCH}/g' ${CMAKE_CURRENT_SOURCE_DIR}/nes-core/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION_POST_FIX@/${${PROJECT_NAME}_NES_VERSION_POST_FIX}/g' ${CMAKE_CURRENT_SOURCE_DIR}/nes-core/include/Version/version.hpp)
    endif ()
endmacro(project_enable_version)

macro(get_nes_log_level_value NES_LOGGING_VALUE)
    message(STATUS " Provided log level is: ${NES_LOG_LEVEL} ")
    if (${NES_LOG_LEVEL} STREQUAL " TRACE")
        message("-- Log level is set to TRACE!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_TRACE=1 ")
    elseif (${NES_LOG_LEVEL} STREQUAL " DEBUG")
        message("-- Log level is set to DEBUG!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_DEBUG=1 ")
    elseif (${NES_LOG_LEVEL} STREQUAL " INFO")
        message("-- Log level is set to INFO!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_INFO=1 ")
    elseif (${NES_LOG_LEVEL} STREQUAL " WARN")
        message("-- Log level is set to WARN!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_WARNING=1 ")
    elseif (${NES_LOG_LEVEL} STREQUAL " ERROR")
        message("-- Log level is set to ERROR!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_ERROR=1 ")
    elseif (${NES_LOG_LEVEL} STREQUAL " FATAL_ERROR")
        message("-- Log level is set to FATAL_ERROR!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_FATAL_ERROR=1 ")
    elseif (${NES_LOG_LEVEL} STREQUAL " LEVEL_NONE")
        message("-- Log level is set to LEVEL_NONE!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_NONE=1")
    else ()
        message(WARNING "-- Could not set NES_LOG_LEVEL as ${NES_LOG_LEVEL} did not equal any logging level!!! Defaulting to DEBUG!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGLEVEL_DEBUG=1")
    endif ()
endmacro(get_nes_log_level_value NES_LOGGING_VALUE)

function(download_file url filename)
    message("Download: ${url} ")
    file(REMOVE ${filename})
    if (NOT EXISTS ${filename})
        set(CURRENT_ITERATION " 0")
        set(MAX_RETRIES "3 ")
        while (CURRENT_ITERATION LESS MAX_RETRIES)
            # Throws an error if download is inactive for 10s
            file(DOWNLOAD ${url} ${filename} SHOW_PROGRESS TIMEOUT 0 INACTIVITY_TIMEOUT 10 STATUS DOWNLOAD_STATUS)
            # Retrieve download status info
            list(GET DOWNLOAD_STATUS 0 STATUS_CODE)
            list(GET DOWNLOAD_STATUS 1 ERROR_MESSAGE)
            math(EXPR CURRENT_ITERATION " ${CURRENT_ITERATION} + 1 ") # CURRENT_ITERATION++
            if (${STATUS_CODE} EQUAL 0)
                message(STATUS " Download completed successfully")
                break()
            else ()
                message(STATUS "Error occurred during download: ${ERROR_MESSAGE} ")
                message(STATUS " Retry attempt ${CURRENT_ITERATION}/${MAX_RETRIES} ")
                # Remove created (incomplete) file which failed to get downloaded
                file(REMOVE ${filename})
            endif ()
        endwhile ()
        if (CURRENT_ITERATION EQUAL MAX_RETRIES)
            message(FATAL_ERROR " Aborting: retry attempts exceeded while failing to download ${url} ")
        endif ()
    endif ()
endfunction(download_file)

function(get_linux_lsb_release_information)
    find_program(LSB_RELEASE_EXEC lsb_release)
    if (NOT LSB_RELEASE_EXEC)
        message(WARNING " Could not detect lsb_release executable, can not gather required information")
        set(LSB_RELEASE_ID_SHORT "NULL" PARENT_SCOPE)
        set(LSB_RELEASE_VERSION_SHORT "22.04" PARENT_SCOPE)
        set(LSB_RELEASE_CODENAME_SHORT "NULL" PARENT_SCOPE)
    else ()
        execute_process(COMMAND "${LSB_RELEASE_EXEC} " --short --id OUTPUT_VARIABLE LSB_RELEASE_ID_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)
        execute_process(COMMAND " ${LSB_RELEASE_EXEC} " --short --release OUTPUT_VARIABLE LSB_RELEASE_VERSION_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)
        execute_process(COMMAND " ${LSB_RELEASE_EXEC} " --short --codename OUTPUT_VARIABLE LSB_RELEASE_CODENAME_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)

        set(LSB_RELEASE_ID_SHORT " ${LSB_RELEASE_ID_SHORT} " PARENT_SCOPE)
        set(LSB_RELEASE_VERSION_SHORT " ${LSB_RELEASE_VERSION_SHORT} " PARENT_SCOPE)
        set(LSB_RELEASE_CODENAME_SHORT " ${LSB_RELEASE_CODENAME_SHORT} " PARENT_SCOPE)
    endif ()
endfunction()

function(set_linux_musl_release_information)
    file(STRINGS " /etc/os-release" data_list REGEX "^ (ID|VERSION_ID) =")
    # Look for lines like "ID="..." and VERSION_ID="..."
    foreach (_var ${data_list})
        if ("${_var}" MATCHES "^(ID)=(.*)$")
            set(ID "${CMAKE_MATCH_2}" PARENT_SCOPE)
        elseif ("${_var}" MATCHES "^(VERSION_ID)=(.*)$")
            set(VERSION_ID "${CMAKE_MATCH_2}" PARENT_SCOPE)
        endif ()
    endforeach ()
endfunction()

function(is_host_musl)
    file(STRINGS "/etc/os-release" data_list REGEX "^(ID|VERSION_ID)=")
    # Look for lines like "ID="..." and VERSION_ID="..."
    foreach (_var ${data_list})
        if ("${_var}" MATCHES "^(ID)=(.*)$")
            if ("${CMAKE_MATCH_2}" MATCHES "alpine")
                set(HOST_IS_MUSL TRUE PARENT_SCOPE)
            else ()
                set(HOST_IS_MUSL FALSE PARENT_SCOPE)
            endif ()
        endif ()
    endforeach ()
endfunction()