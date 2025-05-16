# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# include registrar generation functions
include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

include_directories(tests/include)

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-runtime NES_EXECUTION_ENGINE_SOURCE_FILES)

# Add Library
add_library(nes-runtime ${NES_EXECUTION_ENGINE_SOURCE_FILES})
target_link_libraries(nes-runtime PUBLIC nes-common nes-logical-operators nes-physical-operators nes-data-types nes-memory nes-nautilus nes-sources nes-query-engine)
target_include_directories(nes-runtime PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# folly
include(CMakeFindDependencyMacro)
find_package(folly CONFIG REQUIRED)
target_link_libraries(nes-runtime PRIVATE folly::folly)
target_include_directories(nes-runtime PRIVATE ${FOLLY_INCLUDE_DIR})


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-runtime REUSE_FROM nes-common)
endif ()

target_compile_options(nes-runtime PUBLIC "-Wno-bitwise-instead-of-logical")

add_tests_if_enabled(tests)
