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
get_source(nes-sources NES_SOURCES_SOURCE_FILES)

# Add Library
add_library(nes-sources ${NES_SOURCES_SOURCE_FILES})

# Todo: #72 we can potentially remove nes-data-types, if we move parsing/formatting out of nes-sources
target_link_libraries(nes-sources PUBLIC nes-common nes-configurations nes-memory nes-source-parsers)

target_include_directories(nes-sources PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-sources REUSE_FROM nes-common)
endif ()

# There are currently no tests in nes-sources. There are system level tests and integration tests (nes-single-node-worker)
