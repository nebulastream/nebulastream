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

get_source(nes-data-types NES_COMPILER_SOURCE_FILES)

# Add Library
add_library(nes-data-types ${NES_COMPILER_SOURCE_FILES})
target_link_libraries(nes-data-types PUBLIC nes-configurations)
target_include_directories(nes-data-types PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-data-types REUSE_FROM nes-common)
endif ()

add_tests_if_enabled(tests)

# # Create library for the registries, and privately link it against nes-data-types so that nes-data-types can access its registries
create_plugin_registry_library(nes-data-types-registry nes-data-types)
target_link_libraries(nes-data-types PRIVATE nes-data-types-registry)
# Generate a registrar for every registry in nes-data-types
generate_plugin_registrars(nes-data-types DataType)
