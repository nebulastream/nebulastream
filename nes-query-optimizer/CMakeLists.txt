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

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-query-optimizer NES_OPTIMIZER_SOURCE_FILES)

# Add Library
add_library(nes-query-optimizer ${NES_OPTIMIZER_SOURCE_FILES})
target_link_libraries(nes-query-optimizer PUBLIC nes-configurations nes-nautilus nes-logical-operators nes-physical-operators nes-physical-operators-registry)
target_include_directories(nes-query-optimizer
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-query-optimizer REUSE_FROM nes-common)
    # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
    target_compile_options(nes-query-optimizer PUBLIC "-fPIC")
endif ()

create_registries_for_component(RewriteRule Trait)
