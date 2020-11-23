# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

macro(add_source_nes)
    add_source(nes "${ARGN}")
endmacro()

macro(get_source_nes SOURCE_FILES)
    get_source(nes SOURCE_FILES_LOCAL)
    set(${SOURCE_FILES} ${SOURCE_FILES_LOCAL})
endmacro()

macro(get_header_nes HEADER_FILES)
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
    else()
        add_custom_target(version COMMAND echo "version: ${${PROJECT_NAME}_VERSION}")
        add_custom_command(TARGET version COMMAND cp -f ${CMAKE_CURRENT_SOURCE_DIR}/cmake/version.hpp.in ${CMAKE_CURRENT_SOURCE_DIR}/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION_MAJOR@/${${PROJECT_NAME}_VERSION_MAJOR}/g' ${CMAKE_CURRENT_SOURCE_DIR}/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION_MINOR@/${${PROJECT_NAME}_VERSION_MINOR}/g' ${CMAKE_CURRENT_SOURCE_DIR}/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION_PATCH@/${${PROJECT_NAME}_VERSION_PATCH}/g' ${CMAKE_CURRENT_SOURCE_DIR}/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION@/${${PROJECT_NAME}_VERSION}/g' ${CMAKE_CURRENT_SOURCE_DIR}/include/Version/version.hpp)
    endif ()
endmacro(project_enable_version)

macro(get_nes_log_level_value NES_LOGGING_VALUE)
    if (${NES_LOGGING_LEVEL} STREQUAL "TRACE")
        message("-- Log level is set to TRACE!")
        set(NES_LOGGING_VALUE 6)
    elseif(${NES_LOGGING_LEVEL} STREQUAL "DEBUG")
        message("-- Log level is set to DEBUG!")
        set(NES_LOGGING_VALUE 5)
    elseif(${NES_LOGGING_LEVEL} STREQUAL "INFO")
        message("-- Log level is set to INFO!")
        set(NES_LOGGING_VALUE 4)
    elseif (${NES_LOGGING_LEVEL} STREQUAL "WARN")
        message("-- Log level is set to WARN!")
        set(NES_LOGGING_VALUE 3)
    elseif(${NES_LOGGING_LEVEL} STREQUAL "ERROR")
        message("-- Log level is set to ERROR!")
        set(NES_LOGGING_VALUE 2)
    elseif(${NES_LOGGING_LEVEL} STREQUAL "FATAL_ERROR")
        message("-- Log level is set to FATAL_ERROR!")
        set(NES_LOGGING_VALUE 1)
    else()
        message(WARNING "-- Could not set NES_LOGGING_VALUE as ${NES_LOGGING_LEVEL} did not equal any logging level!!!  Defaulting to debug!")
        set(NES_LOGGING_VALUE 5)
    endif ()
endmacro(get_nes_log_level_value NES_LOGGING_VALUE)