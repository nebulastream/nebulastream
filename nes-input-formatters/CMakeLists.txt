# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# include registrar generation functions
include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-input-formatters NES_SOURCE_PARSERS_SOURCE_FILES)

# Add Library
add_library(nes-input-formatters ${NES_SOURCE_PARSERS_SOURCE_FILES})

# Exposing TupleBuffer (nes-memory) and PipelineExecutionContext (nes-executable) from public APIs
target_link_libraries(nes-input-formatters PUBLIC nes-memory nes-executable PRIVATE nes-common nes-configurations nes-runtime nes-executable nes-physical-operators)

target_include_directories(nes-input-formatters PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-input-formatters REUSE_FROM nes-common)
endif ()

create_registries_for_component(InputFormatIndexer)

add_tests_if_enabled(tests)
