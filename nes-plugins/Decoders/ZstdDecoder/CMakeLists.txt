# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
include(FetchContent)

# Zstd
FetchContent_Declare(
        zstd
        GIT_REPOSITORY https://github.com/facebook/zstd.git
        GIT_TAG v1.5.5
)
FetchContent_GetProperties(zstd)
if(NOT zstd_POPULATED)
    FetchContent_Populate(zstd)
    # The source is now downloaded to ${zstd_SOURCE_DIR}
endif()
# Custom target for building Zstd
set(ZSTD_LIBRARY ${zstd_SOURCE_DIR}/lib/libzstd.so)
add_custom_command(
        OUTPUT ${ZSTD_LIBRARY}
        COMMAND make -j10
        WORKING_DIRECTORY ${zstd_SOURCE_DIR}/lib
        COMMENT "Building Zstd"
)
add_custom_target(BuildZstd DEPENDS ${ZSTD_LIBRARY})
# Specify where the built libraries and headers are locatedS
set(ZSTD_INCLUDE_DIR ${zstd_SOURCE_DIR}/lib)

add_plugin_as_library(Zstd Decoder nes-codecs-registry zstd_decoder_plugin_library ZstdDecoder.cpp)

add_dependencies(zstd_decoder_plugin_library BuildZstd)
target_include_directories(zstd_decoder_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/ PRIVATE ${ZSTD_INCLUDE_DIR})
target_link_libraries(zstd_decoder_plugin_library PRIVATE ${ZSTD_LIBRARY})
