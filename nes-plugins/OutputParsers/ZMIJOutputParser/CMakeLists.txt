# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(FetchContent)
include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

FetchContent_Declare(
        zmij
        GIT_REPOSITORY https://github.com/vitaut/zmij.git
        GIT_TAG        v1.0
)
set(ZMIJ_BUILD_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(zmij)

foreach(_zmij_target zmij zmij-test zmij-no-int128-test zmij-no-builtins-test
        zmij-c-test zmij-no-simd-test double-check float-check
        benchmark pow10-test gtest)
    if(TARGET ${_zmij_target})
        target_compile_options(${_zmij_target} PRIVATE -w)
    endif()
endforeach()

add_plugin_as_library(ZMIJF32 OutputParser nes-output-formatters-registry zmij_output_parser_plugin ZMIJF32OutputParser.cpp)
add_plugin(ZMIJF64 OutputParser zmij_output_parser_plugin ZMIJF64OutputParser.cpp)

target_link_libraries(zmij_output_parser_plugin PRIVATE zmij)
target_include_directories(zmij_output_parser_plugin PUBLIC include PRIVATE .)
