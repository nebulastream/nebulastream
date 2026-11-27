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
target_link_libraries(nes-codecs PUBLIC nes-common nes-memory nes-configurations nes-schema)

target_include_directories(nes-codecs PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# A codec plugin provides ${name}Decoder::getConfigSchema() and ${name}DecoderConfig::fromConfig(), and registers in all three.
create_runtime_registry(Decoder nes-codecs
        ENTRY_TEMPLATE "makeDecoderFactory<\${PLUGIN_NAME}Decoder, \${PLUGIN_NAME}DecoderConfig>()"
        HEADER_TEMPLATE "\${PLUGIN_NAME}Decoder.hpp")

create_runtime_registry(DecoderConfigSchema nes-codecs
        ENTRY_TEMPLATE "[] { return \${PLUGIN_NAME}Decoder::getConfigSchema(); }"
        HEADER_TEMPLATE "\${PLUGIN_NAME}Decoder.hpp")

create_runtime_registry(DecoderConfig nes-codecs
        ENTRY_TEMPLATE "makeDecoderConfigEntry<\${PLUGIN_NAME}DecoderConfig>()"
        HEADER_TEMPLATE "\${PLUGIN_NAME}Decoder.hpp")

add_subdirectory(src)
get_source(nes-codecs NES_CODECS_SOURCE_FILES)
target_sources(nes-codecs PRIVATE ${NES_CODECS_SOURCE_FILES})

if (NES_ENABLES_TESTS)
    add_subdirectory(codec-test)
endif ()
