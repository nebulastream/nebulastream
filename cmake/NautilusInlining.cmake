# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# NES_ENABLE_NAUTILUS_INLINING is a tristate:
#   unset: inline when possible, otherwise silently fall back to plain proxy calls
#   ON:    inline, and fail configuration if the pass cannot be applied
#   OFF:   never apply the pass
# Returns via result_var the reason inlining cannot be applied, or an empty string.
function(nautilus_inlining_unsupported_reason result_var)
    set(reason "")
    if (NOT SANITIZER_OPTION STREQUAL "none")
        set(reason "Nautilus function inlining is not supported with sanitizers.")
    elseif (NOT TARGET nautilus::InliningPass)
        set(reason "Nautilus function inlining pass was not built with nautilus.")
    elseif (NOT (CMAKE_CXX_COMPILER_ID MATCHES "Clang"
            AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "19.0.0"
            AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "23"))
        set(reason "Nautilus function inlining requires clang 19-22 during compilation.")
    endif ()
    set(${result_var} "${reason}" PARENT_SCOPE)
endfunction()

find_package(nautilus REQUIRED CONFIG)
find_package(fmt REQUIRED CONFIG)
find_package(spdlog REQUIRED CONFIG)
find_package(MLIR REQUIRED CONFIG)

if (DEFINED NES_ENABLE_NAUTILUS_INLINING AND NOT NES_ENABLE_NAUTILUS_INLINING)
    set(NES_NAUTILUS_INLINING_ACTIVE FALSE)
    message(STATUS "Nautilus function inlining disabled (NES_ENABLE_NAUTILUS_INLINING=${NES_ENABLE_NAUTILUS_INLINING})")
else ()
    nautilus_inlining_unsupported_reason(NAUTILUS_INLINING_UNSUPPORTED_REASON)
    if (NAUTILUS_INLINING_UNSUPPORTED_REASON STREQUAL "")
        set(NES_NAUTILUS_INLINING_ACTIVE TRUE)
    elseif (DEFINED NES_ENABLE_NAUTILUS_INLINING)
        message(FATAL_ERROR "${NAUTILUS_INLINING_UNSUPPORTED_REASON} NES_ENABLE_NAUTILUS_INLINING is ${NES_ENABLE_NAUTILUS_INLINING}; unset it to fall back silently or set it to OFF.")
    else ()
        set(NES_NAUTILUS_INLINING_ACTIVE FALSE)
        message(STATUS "Nautilus function inlining not applied: ${NAUTILUS_INLINING_UNSUPPORTED_REASON}")
    endif ()
endif ()

function(nautilus_inline target)
    # Targets using the inline header need the inlining runtime even when the pass is disabled.
    target_link_libraries(${target} PUBLIC nautilus::nautilus-inlining)

    if (NES_NAUTILUS_INLINING_ACTIVE)
        target_compile_options(${target} PRIVATE "-fpass-plugin=$<TARGET_FILE:nautilus::InliningPass>")
        message(STATUS "Applying nautilus inlining pass to target ${target}")
    endif ()
endfunction()
