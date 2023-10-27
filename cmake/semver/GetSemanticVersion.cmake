
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find_package(Git)

if (Git_FOUND)
    # Check if inside git repository
    execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse --is-inside-work-tree
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE IS_GIT_DIRECTORY
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET)

    # Get current branch name
    execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse --abbrev-ref HEAD
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE ${PROJECT_NAME}_BRANCH_NAME
            OUTPUT_STRIP_TRAILING_WHITESPACE)
else()
    set(${PROJECT_NAME}_BRANCH_NAME "dev")
endif ()

# Read the version file into variable
file(READ ${CMAKE_CURRENT_SOURCE_DIR}/nes-common/include/Version/version.hpp VERSION_INFO)

# Find the Major version
string(REGEX MATCH "\\NES_VERSION_MAJOR[^\n]*" ${PROJECT_NAME}_VERSION_MAJOR "${VERSION_INFO}")
string(REPLACE "NES_VERSION_MAJOR" " " ${PROJECT_NAME}_VERSION_MAJOR "${${PROJECT_NAME}_VERSION_MAJOR}")
string(STRIP "${${PROJECT_NAME}_VERSION_MAJOR}" ${PROJECT_NAME}_VERSION_MAJOR)
message(STATUS "${PROJECT_NAME}_VERSION_MAJOR ${${PROJECT_NAME}_VERSION_MAJOR}")

#Find the Minor version
string(REGEX MATCH "\\NES_VERSION_MINOR[^\n]*" ${PROJECT_NAME}_VERSION_MINOR "${VERSION_INFO}")
string(REPLACE "NES_VERSION_MINOR" " " ${PROJECT_NAME}_VERSION_MINOR "${${PROJECT_NAME}_VERSION_MINOR}")
string(STRIP "${${PROJECT_NAME}_VERSION_MINOR}" ${PROJECT_NAME}_VERSION_MINOR)
message(STATUS "MINOR_VERSION ${${PROJECT_NAME}_VERSION_MINOR}")

#Find the patch version
string(REGEX MATCH "\\NES_VERSION_PATCH[^\n]*" ${PROJECT_NAME}_VERSION_PATCH "${VERSION_INFO}")
string(REPLACE "NES_VERSION_PATCH" " " ${PROJECT_NAME}_VERSION_PATCH "${${PROJECT_NAME}_VERSION_PATCH}")
string(STRIP "${${PROJECT_NAME}_VERSION_PATCH}" ${PROJECT_NAME}_VERSION_PATCH)
message(STATUS "PATCH_VERSION ${${PROJECT_NAME}_VERSION_PATCH}")

if (NOT ${${PROJECT_NAME}_BRANCH_NAME} STREQUAL "master")
    #Find version post fix
    string(REGEX MATCH "\\NES_VERSION_POST_FIX[^\n]*" ${PROJECT_NAME}_NES_VERSION_POST_FIX "${VERSION_INFO}")
    string(REPLACE "NES_VERSION_POST_FIX" " " ${PROJECT_NAME}_NES_VERSION_POST_FIX "${${PROJECT_NAME}_NES_VERSION_POST_FIX}")
    string(STRIP "${${PROJECT_NAME}_NES_VERSION_POST_FIX}" ${PROJECT_NAME}_NES_VERSION_POST_FIX)
    if (NOT "${${PROJECT_NAME}_NES_VERSION_POST_FIX}" STREQUAL "")
        set(${PROJECT_NAME}_NES_VERSION_POST_FIX -${${PROJECT_NAME}_NES_VERSION_POST_FIX})
    endif ()
    message("VERSION_POSTFIX ${${PROJECT_NAME}_NES_VERSION_POST_FIX}")
endif ()

# Set version information
set(${PROJECT_NAME}_VERSION ${${PROJECT_NAME}_VERSION_MAJOR}.${${PROJECT_NAME}_VERSION_MINOR}.${${PROJECT_NAME}_VERSION_PATCH}${${PROJECT_NAME}_NES_VERSION_POST_FIX})

message(STATUS "NebulaStream Version: " ${${PROJECT_NAME}_VERSION})
