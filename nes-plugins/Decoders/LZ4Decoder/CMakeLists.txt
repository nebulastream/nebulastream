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

# Decoder stage for LZ4 frame format (lz4 CLI, LZ4F API). Adapted from the decoder PoC on
# nebulastream branch PoC-decoders-for-source (Leo Raasch).
find_package(lz4 CONFIG REQUIRED)

add_library(lz4_decoder_plugin_library STATIC LZ4Decoder.cpp)
target_link_libraries(lz4_decoder_plugin_library PRIVATE nes-decoder lz4::lz4)
target_include_directories(lz4_decoder_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

link_plugin_library(nes-decoder lz4_decoder_plugin_library)
add_registry_entry(DecoderStage LZ4)
add_registry_entry(DecoderStageConfigSchema LZ4)
add_registry_entry(DecoderStageConfig LZ4)

add_tests_if_enabled(tests)
