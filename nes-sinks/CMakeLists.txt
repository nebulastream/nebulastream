# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# zmij: shortest-form float serializer (== ZMIJF64) for the legacy/uncompiled CSV output path (CSVFormat).
# FetchContent dedups by declare-name, so declaring "zmij" here (also declared by the ZMIJ output plugin) is
# safe/idempotent. Silence its warnings like the plugin does.
include(FetchContent)
FetchContent_Declare(
        zmij
        GIT_REPOSITORY https://github.com/vitaut/zmij.git
        GIT_TAG        v1.0
)
set(ZMIJ_BUILD_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(zmij)
if (TARGET zmij)
    target_compile_options(zmij PRIVATE -w)
endif ()

# Set Sinks and Headers
add_subdirectory(src)
get_source(nes-sinks NES_SINKS_SOURCE_FILES)

# Add Library
add_library(nes-sinks ${NES_SINKS_SOURCE_FILES})

# Todo: #72 we can potentially remove nes-data-types, if we move parsing/formatting out of nes-sinks
target_link_libraries(nes-sinks PUBLIC nes-common nes-configurations nes-data-types nes-memory nes-executable nes-network-bindings PRIVATE zmij)

target_include_directories(nes-sinks PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-sinks REUSE_FROM nes-common)
endif ()

create_registries_for_component(Sink SinkValidation)
