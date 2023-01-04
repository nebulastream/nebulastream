
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(ExternalProject)

function(add_nes_runtime_test)
    add_executable(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-runtime-tests)
    if (CODE_COVERAGE)
        target_code_coverage(${TARGET_NAME} PUBLIC AUTO ALL EXTERNAL OBJECTS nes-runtime nes-common EXCLUDE tests/*)
        message(STATUS "Added cc test ${TARGET_NAME}")
    endif ()
    add_test(NAME ${TARGET_NAME} COMMAND ${TARGET_NAME})
    message(STATUS "Added ut test ${TARGET_NAME}")
endfunction()


add_library(nes-runtime-tests INTERFACE)
target_include_directories(nes-runtime-tests INTERFACE "./include/")
target_link_libraries(nes-runtime-tests INTERFACE nes-runtime nes-tests-util)

add_subdirectory(UnitTests)

set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

