# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_guard(GLOBAL)

function(get_nes_dependency_hash output_variable)
    get_property(dependency_hash_is_set GLOBAL PROPERTY NES_DEPENDENCY_HASH SET)
    if (dependency_hash_is_set)
        get_property(dependency_hash GLOBAL PROPERTY NES_DEPENDENCY_HASH)
    else ()
        execute_process(
                COMMAND ${CMAKE_SOURCE_DIR}/scripts/hash_dependencies.sh
                WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
                OUTPUT_VARIABLE dependency_hash
                ERROR_VARIABLE dependency_hash_error
                RESULT_VARIABLE dependency_hash_result
                OUTPUT_STRIP_TRAILING_WHITESPACE
                ERROR_STRIP_TRAILING_WHITESPACE
        )
        if (NOT dependency_hash_result EQUAL 0)
            set(dependency_hash_failure_output "${dependency_hash}")
            if (NOT dependency_hash_error STREQUAL "")
                string(APPEND dependency_hash_failure_output "\n${dependency_hash_error}")
            endif ()
            message(FATAL_ERROR "Failed to calculate the dependency hash: ${dependency_hash_failure_output}")
        endif ()
        set_property(GLOBAL PROPERTY NES_DEPENDENCY_HASH "${dependency_hash}")
    endif ()

    set(${output_variable} "${dependency_hash}" PARENT_SCOPE)
endfunction()
