# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)
include(FetchContent)
add_subdirectory(src)
get_source(nes-codecs NES_CODECS_SOURCE_FILES)

# Get codec libraries
# LZ4
FetchContent_Declare(lz4
        URL            https://github.com/lz4/lz4/archive/refs/tags/v1.9.4.tar.gz
        URL_HASH
        SHA256=0b0e3aa07c8c063ddf40b082bdf7e37a1562bda40a0ff5272957f3e987e0e54b
        SOURCE_SUBDIR  build/cmake
)
FetchContent_MakeAvailable(lz4)

set(LZ4_LIBRARY ${lz4_SOURCE_DIR}/lib/liblz4.so)
add_custom_command(
        OUTPUT ${LZ4_LIBRARY}
        COMMAND make -j10
        WORKING_DIRECTORY ${lz4_SOURCE_DIR}/lib
        COMMENT "Building LZ4"
)
add_custom_target(BuildLz4 DEPENDS ${LZ4_LIBRARY})
# Specify where the built libraries and headers are locatedS
set(LZ4_INCLUDE_DIR ${lz4_SOURCE_DIR}/lib)

add_library(nes-codecs ${NES_CODECS_SOURCE_FILES})

add_dependencies(nes-codecs BuildLz4)
target_link_libraries(nes-codecs PUBLIC nes-common nes-memory PRIVATE  ${LZ4_LIBRARY})

target_include_directories(nes-codecs PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private> ${LZ4_INCLUDE_DIR})

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-codecs REUSE_FROM nes-common)
endif ()

create_registries_for_component(Decoder)
