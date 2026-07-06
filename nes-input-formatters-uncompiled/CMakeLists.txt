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

# fast_float: hardcoded fast numeric input parse for the uncompiled path (UncompiledFastParse.hpp) -- the
# same core the compiled FastINT parser uses (integer from_chars + Eisel-Lemire float). FetchContent dedups
# by declare-name, so declaring "fastfloat" here (also declared by the FastInteger plugin) is safe/idempotent.
include(FetchContent)
FetchContent_Declare(
        fastfloat
        GIT_REPOSITORY https://github.com/fastfloat/fast_float
        GIT_TAG        v8.2.4
)
FetchContent_MakeAvailable(fastfloat)

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-input-formatters-uncompiled NES_INPUT_FORMATTERS_UNCOMPILED_SOURCE_FILES)

# Add Library
add_library(nes-input-formatters-uncompiled ${NES_INPUT_FORMATTERS_UNCOMPILED_SOURCE_FILES})

# Exposing TupleBuffer (nes-memory) and PipelineExecutionContext (nes-executable) from public APIs
# nes-input-formatters (PRIVATE) provides the shared SIMD CSV scan: SimdCsv::indexBandInto /
# indexBandSequentialInto + the runtime-dispatched block kernel (selectComputeBlocks). The uncompiled
# CSV indexers call these directly (fast flat band == SIMDCSV/SequentialCSV), so we also need its
# private/ headers (SIMDCSVScan.hpp, SIMDCSVKernel.hpp) on the include path.
# NOTE (debt): reaching into another module's private/ is a benchmark-branch shortcut; productionizing
# should factor the SIMD kernel + scan into a small shared library both modules link.
target_link_libraries(nes-input-formatters-uncompiled PUBLIC nes-memory nes-executable PRIVATE nes-common nes-configurations nes-runtime nes-executable nes-input-formatters-registry nes-input-formatters FastFloat::fast_float)

target_include_directories(nes-input-formatters-uncompiled PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-input-formatters/private>)


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-input-formatters-uncompiled REUSE_FROM nes-common)
endif ()

create_registries_for_component(UncompiledInputFormatIndexer UncompiledInputParser)
