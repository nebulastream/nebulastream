# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#set(CMAKE_VERBOSE_MAKEFILE on)

#include_directories(tests/include) //

# Set Sources and Headers
add_subdirectory(src)
# Todo: NES_EXECUTION_ENGINE_SOURCE_FILES <-- renamed to NES_SOURCES_SOURCE_FILES, but must fill with source files
# Todo: does the below actually do anything -> also check for other modules
get_source(nes-sources NES_SOURCES_SOURCE_FILES)

# Add Library
add_library(nes-sources ${NES_SOURCES_SOURCE_FILES})
# Todo: what are the dependencies of nes-sources? How to avoid cyclic dependencies with nes-runtime?
#target_link_libraries(nes-sources PUBLIC nes-common nes-operators nes-data-types)
# Todo: #72 we can potentially remove nes-data-types, if we move parsing/formatting out of nes-sources
# Todo: nes-execution for query compiler options -> could remove
target_link_libraries(nes-sources PUBLIC nes-common nes-configurations nes-data-types nes-operators nes-memory)

target_include_directories(nes-sources PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-sources REUSE_FROM nes-common)
endif ()

#target_compile_options(nes-sources PUBLIC "-Wno-bitwise-instead-of-logical")
#target_compile_options(nes-sources PUBLIC "-Wno-bitwise-instead-of-logical" "-fPIC")


# Add Private Dependencies
#if (NES_ENABLES_TESTS AND NOT (CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin"))
#    # this allows to mock read calls
#    target_compile_definitions(nes-sources PRIVATE WRAP_READ_CALL)
#    target_link_libraries(nes-sources PRIVATE  "-Wl,--wrap,read")
#endif ()

if (NES_ENABLES_TESTS)
    add_subdirectory(tests)
endif ()

register_public_header_file(nes-sources ${CMAKE_CURRENT_SOURCE_DIR}/include/SourceHandle.hpp SourceHandle.hpp)
#Todo: need registry interface

#Add the nes common and headers to cpack
install(TARGETS nes-sources
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)
