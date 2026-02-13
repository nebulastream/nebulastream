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

# Snappy
set(SNAPPY_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SNAPPY_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(CRC32C_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CRC32C_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(CRC32C_USE_GLOG OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
        snappy
        GIT_REPOSITORY https://github.com/google/snappy.git
        GIT_TAG        1.1.10
)
FetchContent_MakeAvailable(snappy)
# To stop clang from detecting errors in snappy's source code
if(TARGET snappy)
    target_compile_options(snappy PRIVATE -Wno-error=sign-compare)
endif()

# Library for checksum calculations
FetchContent_Declare(
        crc32c
        GIT_REPOSITORY https://github.com/google/crc32c.git
        GIT_TAG 1.1.2
)
FetchContent_MakeAvailable(crc32c)

add_plugin_as_library(Snappy Decoder nes-codecs-registry snappy_decoder_plugin_library SnappyDecoder.cpp)
target_include_directories(snappy_decoder_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
target_link_libraries(snappy_decoder_plugin_library PRIVATE snappy crc32c)
