# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(src)
get_source(nes-sources NES_SOURCES_SOURCE_FILES)
get_source(nes-sources-runtime NES_SOURCES_RUNTIME_SOURCE_FILES)

add_library(nes-sources ${NES_SOURCES_SOURCE_FILES})
target_link_libraries(nes-sources PUBLIC nes-common nes-configurations nes-data-types)
target_include_directories(nes-sources PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

add_library(nes-sources-runtime ${NES_SOURCES_RUNTIME_SOURCE_FILES})
target_link_libraries(nes-sources-runtime PUBLIC nes-common nes-configurations nes-memory nes-sources)

target_include_directories(nes-sources-runtime PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/runtime>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

# Create library for the registries, and privately link it against nes-sources so that nes-sources can access its registries.
create_plugin_registry_library(nes-sources-registry nes-sources)
target_link_libraries(nes-sources PRIVATE nes-sources-registry)
# Generate a registrar for every registry in nes-sources
generate_plugin_registrars(nes-sources SourceValidation)

create_plugin_registry_library(nes-sources-runtime-registry nes-sources-runtime)
target_link_libraries(nes-sources-runtime PRIVATE nes-sources-runtime-registry)
# Generate a registrar for every registry in nes-sources
generate_plugin_registrars(nes-sources-runtime Source)

add_tests_if_enabled(tests)
