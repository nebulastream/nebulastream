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
add_subdirectory(src)
get_source(nes-sources NES_SOURCES_SOURCE_FILES)

# Get codec libraries
# LZ4
FetchContent_Declare(lz4
        URL            https://github.com/lz4/lz4/archive/refs/tags/v1.9.4.tar.gz
        URL_HASH
        SHA256=0b0e3aa07c8c063ddf40b082bdf7e37a1562bda40a0ff5272957f3e987e0e54b
        SOURCE_SUBDIR  build/cmake
)
FetchContent_MakeAvailable(lz4)

add_library(nes-sources ${NES_SOURCES_SOURCE_FILES})
target_link_libraries(nes-sources PUBLIC nes-common nes-configurations nes-memory nes-systest-sources PRIVATE lz4)

target_include_directories(nes-sources PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-sources REUSE_FROM nes-common)
endif ()

create_registries_for_component(Decoder Source SourceValidation FileData InlineData GeneratorData)

add_tests_if_enabled(tests)
