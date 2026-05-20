# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_directories(tests/include)

# Add interface library
add_library(nes-query-optimizer-interface INTERFACE)
target_include_directories(nes-query-optimizer-interface INTERFACE interface)
target_link_libraries(nes-query-optimizer-interface INTERFACE nes-configurations nes-network-interface)

include(${PROJECT_SOURCE_DIR}/cmake/UnreflectionRegistrationUtil.cmake)

# Declare the library up front so create_unreflection_registry can attach the
# WHOLE_ARCHIVE INTERFACE link, and so add_subdirectory(src) can add to its
# target_sources.
add_library(nes-query-optimizer STATIC)
set_target_properties(nes-query-optimizer PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(nes-query-optimizer
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

# Trait is unreflection-only — no factory registry on this side.
create_unreflection_registry(Trait nes-query-optimizer
        "\${PLUGIN_NAME}Trait"
        "\${PLUGIN_NAME}Trait.hpp")

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-query-optimizer NES_OPTIMIZER_SOURCE_FILES)
target_sources(nes-query-optimizer PRIVATE ${NES_OPTIMIZER_SOURCE_FILES})

find_package(highs CONFIG REQUIRED)
target_link_libraries(nes-query-optimizer PUBLIC nes-configurations nes-logical-operators nes-single-node-worker-interface nes-distributed PRIVATE nes-input-formatter-provider highs::highs)


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-query-optimizer REUSE_FROM nes-common)
    # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
    target_compile_options(nes-query-optimizer PUBLIC "-fPIC")
endif ()

add_tests_if_enabled(tests)
