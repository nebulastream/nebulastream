
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

macro(add_source_nes PROP_NAME)
    add_source(nes "${ARGN}")
endmacro()

macro(add_source_files)
    set(SOURCE_FILES "${ARGN}")
    list(POP_FRONT SOURCE_FILES TARGET_NAME)
    MESSAGE("TARGET: ${TARGET_NAME} OUTPUT: ${SOURCE_FILES}")
    add_source(${TARGET_NAME} "${SOURCE_FILES}")
endmacro()

macro(get_source_nes SOURCE_FILES)
    get_source(nes SOURCE_FILES_LOCAL)
    set(${SOURCE_FILES} ${SOURCE_FILES_LOCAL})
endmacro()

macro(get_source_nes_client SOURCE_FILES)
    get_source(nes_client SOURCE_FILES_LOCAL)
    set(${SOURCE_FILES} ${SOURCE_FILES_LOCAL})
endmacro()

macro(get_header_nes HEADER_FILES)
    file(GLOB_RECURSE ${HEADER_FILES} "include/*.h" "include/*.hpp")
endmacro()

macro(get_header_nes_client HEADER_FILES)
    file(GLOB_RECURSE ${HEADER_FILES} "include/*.h" "include/*.hpp")
endmacro()

find_program(CLANG_FORMAT_EXE clang-format)

macro(project_enable_clang_format)
    string(CONCAT FORMAT_DIRS
            "${CMAKE_CURRENT_SOURCE_DIR}/benchmark,"
            "${CMAKE_CURRENT_SOURCE_DIR}/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests,"
            "${CMAKE_CURRENT_SOURCE_DIR}/include")
    if (NOT ${CLANG_FORMAT_EXE} STREQUAL "CLANG_FORMAT_EXE-NOTFOUND")
        message("-- clang-format found, whole source formatting enabled through 'format' target.")
        add_custom_target(format COMMAND python3 ${CMAKE_SOURCE_DIR}/scripts/build/run_clang_format.py clang-format ${CMAKE_SOURCE_DIR}/clang_suppressions.txt --source_dirs ${FORMAT_DIRS} --fix USES_TERMINAL)
        add_custom_target(format-check COMMAND python3 ${CMAKE_SOURCE_DIR}/scripts/build/run_clang_format.py clang-format ${CMAKE_SOURCE_DIR}/clang_suppressions.txt --source_dirs ${FORMAT_DIRS} USES_TERMINAL)
    else ()
        message(FATAL_ERROR "clang-format is not installed.")
    endif ()
endmacro(project_enable_clang_format)

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

macro(project_enable_release)
    if (NOT IS_GIT_DIRECTORY)
        message(AUTHOR_WARNING " -- Disabled release target as git not configured.")
    elseif (${${PROJECT_NAME}_BRANCH_NAME} STREQUAL "master")
        message(INFO "-- Enabled release target.")
        add_custom_target(release
                COMMAND echo "Releasing NES ${${PROJECT_NAME}_VERSION}"
                )
        add_custom_command(TARGET release
                COMMAND ${GIT_EXECUTABLE} commit -am "GIT-CI: Updating NES version to ${${PROJECT_NAME}_VERSION}"
                COMMAND ${GIT_EXECUTABLE} push
                COMMENT "Updated NES version ${${PROJECT_NAME}_VERSION}")
        add_custom_command(TARGET release
                COMMAND ${GIT_EXECUTABLE} tag v${${PROJECT_NAME}_VERSION} -m "GIT-CI: Releasing New Tag v${${PROJECT_NAME}_VERSION}"
                COMMAND ${GIT_EXECUTABLE} push origin v${${PROJECT_NAME}_VERSION}
                COMMENT "Released and pushed new tag to the repository")
    else ()
        message(INFO " -- Disabled release target as currently not on master branch.")
    endif ()
endmacro(project_enable_release)

macro(project_enable_version)
    if (NOT IS_GIT_DIRECTORY)
        message(AUTHOR_WARNING " -- Disabled version target as git not configured.")
    else ()
        add_custom_target(version COMMAND echo "version: ${${PROJECT_NAME}_VERSION}")
        add_custom_command(TARGET version COMMAND cp -f ${CMAKE_CURRENT_SOURCE_DIR}/cmake/version.hpp.in ${CMAKE_CURRENT_SOURCE_DIR}/nes-core/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION_MAJOR@/${${PROJECT_NAME}_VERSION_MAJOR}/g' ${CMAKE_CURRENT_SOURCE_DIR}/nes-core/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION_MINOR@/${${PROJECT_NAME}_VERSION_MINOR}/g' ${CMAKE_CURRENT_SOURCE_DIR}/nes-core/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION_PATCH@/${${PROJECT_NAME}_VERSION_PATCH}/g' ${CMAKE_CURRENT_SOURCE_DIR}/nes-core/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION@/${${PROJECT_NAME}_VERSION}/g' ${CMAKE_CURRENT_SOURCE_DIR}/nes-core/include/Version/version.hpp)
    endif ()
endmacro(project_enable_version)

macro(get_nes_log_level_value NES_LOGGING_VALUE)
    if (${NES_LOGGING_LEVEL} STREQUAL "TRACE")
        message("-- Log level is set to TRACE!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -Werror=unused-variable -Werror=unused-parameter -DNES_LOGGING_TRACE_LEVEL=1")
    elseif (${NES_LOGGING_LEVEL} STREQUAL "DEBUG")
        message("-- Log level is set to DEBUG!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -Werror=unused-variable -Werror=unused-parameter -DNES_LOGGING_DEBUG_LEVEL=1")
    elseif (${NES_LOGGING_LEVEL} STREQUAL "INFO")
        message("-- Log level is set to INFO!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGGING_INFO_LEVEL=1")
    elseif (${NES_LOGGING_LEVEL} STREQUAL "WARN")
        message("-- Log level is set to WARN!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGGING_WARNING_LEVEL=1")
    elseif (${NES_LOGGING_LEVEL} STREQUAL "ERROR")
        message("-- Log level is set to ERROR!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGGING_ERROR_LEVEL=1")
    elseif (${NES_LOGGING_LEVEL} STREQUAL "FATAL_ERROR")
        message("-- Log level is set to FATAL_ERROR!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGGING_FATAL_ERROR_LEVEL=1")
    elseif (${NES_LOGGING_LEVEL} STREQUAL "LEVEL_NONE")
        message("-- Log level is set to LEVEL_NONE!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_LOGGING_NO_LEVEL=1")
    else ()
        message(WARNING "-- Could not set NES_LOGGING_VALUE as ${NES_LOGGING_LEVEL} did not equal any logging level!!!  Defaulting to debug!")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -Werror=unused-variable -Werror=unused-parameter -DNES_LOGGING_TRACE_LEVEL=1")
    endif ()
endmacro(get_nes_log_level_value NES_LOGGING_VALUE)

function(download_file url filename)
    message("Download: ${url}")
    if (NOT EXISTS ${filename})
        file(DOWNLOAD ${url} ${filename} SHOW_PROGRESS TIMEOUT 0)
    endif ()
endfunction(download_file)

function(get_linux_lsb_release_information)
    find_program(LSB_RELEASE_EXEC lsb_release)
    if (NOT LSB_RELEASE_EXEC)
        message(WARNING "Could not detect lsb_release executable, can not gather required information")
        set(LSB_RELEASE_ID_SHORT "NULL" PARENT_SCOPE)
        set(LSB_RELEASE_VERSION_SHORT "NULL"  PARENT_SCOPE)
        set(LSB_RELEASE_CODENAME_SHORT "NULL"  PARENT_SCOPE)
    else()
        execute_process(COMMAND "${LSB_RELEASE_EXEC}" --short --id OUTPUT_VARIABLE LSB_RELEASE_ID_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)
        execute_process(COMMAND "${LSB_RELEASE_EXEC}" --short --release OUTPUT_VARIABLE LSB_RELEASE_VERSION_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)
        execute_process(COMMAND "${LSB_RELEASE_EXEC}" --short --codename OUTPUT_VARIABLE LSB_RELEASE_CODENAME_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)

        set(LSB_RELEASE_ID_SHORT "${LSB_RELEASE_ID_SHORT}" PARENT_SCOPE)
        set(LSB_RELEASE_VERSION_SHORT "${LSB_RELEASE_VERSION_SHORT}" PARENT_SCOPE)
        set(LSB_RELEASE_CODENAME_SHORT "${LSB_RELEASE_CODENAME_SHORT}" PARENT_SCOPE)
    endif ()
endfunction()

