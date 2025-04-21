# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Sinks and Headers
add_subdirectory(src)
get_source(nes-sinks NES_SINKS_SOURCE_FILES)
get_source(nes-sinks-runtime NES_SINKS_RUNTIME_SOURCE_FILES)

# Add Library
add_library(nes-sinks ${NES_SINKS_SOURCE_FILES})
add_library(nes-sinks-runtime ${NES_SINKS_RUNTIME_SOURCE_FILES})

target_link_libraries(nes-sinks PUBLIC nes-common nes-configurations nes-data-types)

# Todo: #72 we can potentially remove nes-data-types, if we move parsing/formatting out of nes-sinks
target_link_libraries(nes-sinks-runtime PUBLIC nes-common nes-configurations nes-data-types nes-memory nes-executable nes-sinks)

target_include_directories(nes-sinks PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

target_include_directories(nes-sinks-runtime PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/runtime>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# Create library for the registries, and privately link it against nes-sinks so that nes-sinks can access its registries.
create_plugin_registry_library(nes-sinks-runtime-registry nes-sinks-runtime)
target_link_libraries(nes-sinks-runtime PRIVATE nes-sinks-runtime-registry)
# Generate a registrar for every registry in nes-sinks
generate_plugin_registrars(nes-sinks-runtime Sink)

# Create library for the registries, and privately link it against nes-sinks so that nes-sinks can access its registries.
create_plugin_registry_library(nes-sinks-registry nes-sinks)
target_link_libraries(nes-sinks PRIVATE nes-sinks-registry)
# Generate a registrar for every registry in nes-sinks
generate_plugin_registrars(nes-sinks SinkValidation)
