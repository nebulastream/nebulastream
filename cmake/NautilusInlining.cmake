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

    # Disable inlining when sanitizers are enabled in debug mode
    if (CMAKE_BUILD_TYPE MATCHES "Debug")
        string(TOUPPER "${SANITIZER_OPTION}" VCPKG_SANITIZER)
        string(TOUPPER "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG}" ALL_CXX_FLAGS)
        if (ALL_CXX_FLAGS MATCHES "-FSANITIZE=UNDEFINED" OR ALL_CXX_FLAGS MATCHES "-FSANITIZE=ADDRESS"
                OR ALL_CXX_FLAGS MATCHES "-FSANITIZE=.*UNDEFINED" OR ALL_CXX_FLAGS MATCHES "-FSANITIZE=.*ADDRESS"
                OR VCPKG_SANITIZER MATCHES "UNDEFINED" OR VCPKG_SANITIZER MATCHES "ADDRESS")
            if (${verbose})
                message(WARNING "Nautilus function inlining is not supported with sanitizers in debug mode and will not be applied. You can safely ignore this warning.")
            endif ()
            set(${result_var} FALSE PARENT_SCOPE)
            return()
        endif ()
    endif ()

    set(${result_var} TRUE PARENT_SCOPE)
endfunction()

find_package(nautilus REQUIRED CONFIG)
find_package(fmt REQUIRED CONFIG)
find_package(spdlog REQUIRED CONFIG)
find_package(MLIR REQUIRED CONFIG)
is_inlining_supported(NAUTILUS_INLINE_SUPPORTED TRUE)

function(nautilus_inline target)
    # nautilus needs to be linked either way to build targets that use the inline header
    target_link_libraries(${target} PUBLIC nautilus::nautilus)

    is_inlining_supported(NAUTILUS_INLINE_SUPPORTED FALSE)
    if (NAUTILUS_INLINE_SUPPORTED)
        target_compile_options(${target} PRIVATE "-fpass-plugin=$<TARGET_FILE:nautilus::InliningPass>")

        message(STATUS "Applying nautilus inlining pass to target ${target}")
    endif ()
endfunction()
