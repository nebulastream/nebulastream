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

# The Decoder: what happens to a source's bytes between reading and emitting them, as an ordered list of
# stages (decryption, decompression, deserialization). Runs on the source thread (nes-sources/src/SourceThread.cpp).
add_library(nes-decoder STATIC)
set_target_properties(nes-decoder PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(nes-decoder PUBLIC nes-common nes-memory nes-configurations)

target_include_directories(nes-decoder PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

create_runtime_registry(DecoderStage nes-decoder
        ENTRY_TEMPLATE "makeDecoderStageFactory<\${PLUGIN_NAME}Decoder, \${PLUGIN_NAME}DecoderConfig>()"
        HEADER_TEMPLATE "\${PLUGIN_NAME}Decoder.hpp")
create_runtime_registry(DecoderStageConfigSchema nes-decoder
        ENTRY_TEMPLATE "[] { return \${PLUGIN_NAME}Decoder::getConfigSchema(); }"
        HEADER_TEMPLATE "\${PLUGIN_NAME}Decoder.hpp")
create_runtime_registry(DecoderStageConfig nes-decoder
        ENTRY_TEMPLATE "makeDecoderStageConfigEntry<\${PLUGIN_NAME}DecoderConfig>()"
        HEADER_TEMPLATE "\${PLUGIN_NAME}Decoder.hpp")

add_subdirectory(src)
get_source(nes-decoder NES_DECODER_SOURCE_FILES)
target_sources(nes-decoder PRIVATE ${NES_DECODER_SOURCE_FILES})

add_tests_if_enabled(tests)
