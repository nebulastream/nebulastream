# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generates BuildInfo.hpp/.cpp in nes-common containing build metadata embedded at configure time:
# git SHA (+ dirty flag), build type, sanitizer, compiler, C++ standard library and the vcpkg baseline.
# The generated files are produced via configure_file from the templates next to this module.
#
# Call generate_build_info(<output_dir>) where <output_dir> is the directory in which the generated
# header/source are written. The function appends the generated .cpp to the nes-common source property
# and exposes the directory of the generated header so consumers can add it to their include path.

include(CheckCXXSourceCompiles)

function(generate_build_info OUTPUT_DIR)
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
    if (CMAKE_BUILD_TYPE)
        set(NES_BUILD_TYPE "${CMAKE_BUILD_TYPE}")
    else ()
        set(NES_BUILD_TYPE "unknown")
    endif ()

    # --- sanitizer -----------------------------------------------------------
    # SANITIZER_OPTION is set in cmake/Sanitizers.cmake (one of none/thread/address/undefined).
    if (DEFINED SANITIZER_OPTION AND NOT SANITIZER_OPTION STREQUAL "")
        set(NES_BUILD_SANITIZER "${SANITIZER_OPTION}")
    else ()
        set(NES_BUILD_SANITIZER "none")
    endif ()

    # --- compiler ------------------------------------------------------------
    set(NES_BUILD_COMPILER "${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")

    # --- C++ standard library ------------------------------------------------
    # Probe at configure time which standard library is in use.
    check_cxx_source_compiles("
        #include <version>
        #ifndef _LIBCPP_VERSION
        #error not libc++
        #endif
        int main() { return 0; }
    " NES_STDLIB_IS_LIBCXX)

    if (NES_STDLIB_IS_LIBCXX)
        set(NES_BUILD_STDLIB "libc++")
    else ()
        check_cxx_source_compiles("
            #include <version>
            #ifndef __GLIBCXX__
            #error not libstdc++
            #endif
            int main() { return 0; }
        " NES_STDLIB_IS_LIBSTDCXX)
        if (NES_STDLIB_IS_LIBSTDCXX)
            set(NES_BUILD_STDLIB "libstdc++")
        else ()
            set(NES_BUILD_STDLIB "unknown")
        endif ()
    endif ()

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
    set(NES_BUILD_VERSION "0.0.0-dev")

    # --- generate the files --------------------------------------------------
    file(MAKE_DIRECTORY "${OUTPUT_DIR}")
    configure_file(
            "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/templates/BuildInfo.hpp.in"
            "${OUTPUT_DIR}/BuildInfo.hpp"
            @ONLY)
    configure_file(
            "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/templates/BuildInfo.cpp.in"
            "${OUTPUT_DIR}/BuildInfo.cpp"
            @ONLY)

    # Re-run CMake (and thus refresh the embedded SHA) when the checked-out commit changes.
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
            "${PROJECT_SOURCE_DIR}/.git/HEAD")
    if (EXISTS "${PROJECT_SOURCE_DIR}/.git/index")
        set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
                "${PROJECT_SOURCE_DIR}/.git/index")
    endif ()
endfunction()
