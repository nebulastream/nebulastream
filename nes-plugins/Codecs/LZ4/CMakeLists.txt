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

find_package(lz4 CONFIG REQUIRED)

add_library(lz4_codec_plugin STATIC LZ4Decoder.cpp)
target_link_libraries(lz4_codec_plugin PUBLIC nes-codecs PRIVATE LZ4::lz4)
target_include_directories(lz4_codec_plugin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

link_plugin_library(nes-codecs lz4_codec_plugin)
add_registry_entry(Decoder LZ4)
add_registry_entry(DecoderConfigSchema LZ4)
add_registry_entry(DecoderConfig LZ4)

add_codec_test(PLUGIN LZ4 LIBRARIES LZ4::lz4)
