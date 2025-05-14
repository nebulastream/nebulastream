# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# include(../cmake/LLVMIRUtil.cmake)

# include registrar generation functions
include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

include_directories(tests/include)

# Set Sources and Headers
add_subdirectory(src)

get_source(nes-execution NES_EXECUTION_SOURCE_FILES)

# Add Library
add_library(nes-execution ${NES_EXECUTION_SOURCE_FILES})
# link libraries (nes-input-formatters: create an InputFormatterTask in LowerToExecutableQueryPlanPhase)
target_link_libraries(nes-execution PUBLIC nes-nautilus PRIVATE nes-input-formatters)
target_include_directories(nes-execution PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-execution REUSE_FROM nes-common)
    # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
    target_compile_options(nes-execution PUBLIC "-fPIC")
endif ()

add_tests_if_enabled(tests)

create_registries_for_component(ExecutableFunction ExecutableOperator)
