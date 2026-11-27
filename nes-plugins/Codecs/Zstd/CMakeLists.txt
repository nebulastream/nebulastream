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
# vcpkg provides zstd as a static library, the Nix environment only as a shared one.
if (TARGET zstd::libzstd_static)
    set(ZSTD_LIBRARY zstd::libzstd_static)
elseif (TARGET zstd::libzstd_shared)
    set(ZSTD_LIBRARY zstd::libzstd_shared)
else ()
    message(FATAL_ERROR "The zstd package provides neither zstd::libzstd_static nor zstd::libzstd_shared")
endif ()

add_library(zstd_codec_plugin STATIC ZstdDecoder.cpp)
target_link_libraries(zstd_codec_plugin PUBLIC nes-codecs PRIVATE ${ZSTD_LIBRARY})
target_include_directories(zstd_codec_plugin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

link_plugin_library(nes-codecs zstd_codec_plugin)
add_registry_entry(Decoder Zstd)
add_registry_entry(DecoderConfigSchema Zstd)
add_registry_entry(DecoderConfig Zstd)

add_codec_test(PLUGIN Zstd LIBRARIES ${ZSTD_LIBRARY})
