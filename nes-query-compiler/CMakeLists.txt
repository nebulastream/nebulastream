# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-query-compiler NES_QUERY_COMPILER_SOURCE_FILES)

# Add Library
add_library(nes-query-compiler ${NES_QUERY_COMPILER_SOURCE_FILES})
target_link_libraries(nes-query-compiler PUBLIC nes-nautilus nes-physical-operators nes-query-optimizer nes-runtime nes-input-formatters)
target_include_directories(nes-query-compiler PUBLIC
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-query-compiler REUSE_FROM nes-common)
    # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
    target_compile_options(nes-query-compiler PUBLIC "-fPIC")
endif ()
