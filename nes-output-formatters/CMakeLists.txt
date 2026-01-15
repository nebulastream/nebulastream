# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)
add_subdirectory(src)
get_source(nes-output-formatters NES_OUTPUT_FORMATTERS_SOURCE_FILES)

# Add Library
add_library(nes-output-formatters ${NES_OUTPUT_FORMATTERS_SOURCE_FILES})

target_link_libraries(nes-output-formatters PUBLIC nes-common nes-data-types nes-memory nes-nautilus)

target_include_directories(nes-output-formatters PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-output-formatters REUSE_FROM nes-common)
endif ()

create_registries_for_component(OutputFormatter)
