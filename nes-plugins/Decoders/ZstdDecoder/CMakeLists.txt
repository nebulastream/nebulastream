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

find_package(zstd CONFIG REQUIRED)

add_library(zstd_decoder_plugin_library STATIC ZstdDecoder.cpp)
target_link_libraries(zstd_decoder_plugin_library PRIVATE nes-decoder zstd::libzstd_static)
target_include_directories(zstd_decoder_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

link_plugin_library(nes-decoder zstd_decoder_plugin_library)
add_registry_entry(DecoderStage Zstd)
add_registry_entry(DecoderStageConfigSchema Zstd)
add_registry_entry(DecoderStageConfig Zstd)

add_tests_if_enabled(tests)
