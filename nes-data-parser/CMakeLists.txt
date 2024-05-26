# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find_package(scn CONFIG REQUIRED)

add_library(simdjson OBJECT ${CMAKE_CURRENT_SOURCE_DIR}/include/generation/third_party/simdjson.cpp)
target_include_directories(simdjson PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/generation/>
)

add_subdirectory(src)
add_subdirectory(tests)
add_subdirectory(benchmarks)

get_source(nes-data-parser NES_DATA_PARSER_SOURCE_FILES)
add_library(nes-data-parser STATIC ${NES_DATA_PARSER_SOURCE_FILES})
target_link_libraries(nes-data-parser PRIVATE nes-compiler nes-common nes-data-types fmt::fmt)

target_include_directories(nes-data-parser PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

register_public_header_dir(nes-data-parser ${CMAKE_CURRENT_SOURCE_DIR}/include/generation generation)

#Add the nes data parser and headers to cpack
install(TARGETS nes-data-parser
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)