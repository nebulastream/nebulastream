# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)
add_library(nes-codecs STATIC)
set_target_properties(nes-codecs PROPERTIES LINKER_LANGUAGE CXX)

# PUBLIC nes-configurations: the registry headers and the descriptor expose the config system.
target_link_libraries(nes-codecs PUBLIC nes-common nes-data-types nes-memory nes-nautilus nes-configurations)

target_include_directories(nes-codecs PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

# Runtime registries
create_runtime_registry(InternalCodec nes-codecs
        ENTRY_TEMPLATE "&\${PLUGIN_NAME}InternalCodec::provideCodec"
        HEADER_TEMPLATE "\${PLUGIN_NAME}InternalCodec.hpp")

add_subdirectory(src)
get_source(nes-codecs NES_CODECS_SOURCE_FILES)
target_sources(nes-codecs PRIVATE ${NES_CODECS_SOURCE_FILES})
