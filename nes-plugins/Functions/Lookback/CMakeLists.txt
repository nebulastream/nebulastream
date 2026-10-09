# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)
include(${PROJECT_SOURCE_DIR}/cmake/UnreflectionRegistrationUtil.cmake)

# The template, connection-config, provider and per-thread-state machinery, kept out of the two
# plugin libraries so it can be unit-tested on its own and shared by both halves of the function.
add_library(lookback_runtime STATIC
        LookbackTemplate.cpp
        LookbackConnections.cpp
        LookbackProvider.cpp
        LookbackContext.cpp)
target_include_directories(lookback_runtime PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
target_link_libraries(lookback_runtime PUBLIC odbc_connection)

# fmt_logical_function_plugin_library: the parameter arguments are wrapped in to_string during
# inference, exactly as FMT wraps its placeholders, so the physical side only ever sees VARSIZED.
add_library(lookback_logical_function_plugin_library STATIC LookbackLogicalFunction.cpp)
target_link_libraries(lookback_logical_function_plugin_library PRIVATE nes-logical-operators lookback_runtime fmt_logical_function_plugin_library)
target_include_directories(lookback_logical_function_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
link_plugin_library(nes-logical-operators lookback_logical_function_plugin_library)

add_library(lookback_physical_function_plugin_library STATIC LookbackPhysicalFunction.cpp)
target_link_libraries(lookback_physical_function_plugin_library PRIVATE nes-physical-operators lookback_runtime)
target_include_directories(lookback_physical_function_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
link_plugin_library(nes-physical-operators lookback_physical_function_plugin_library)

# KEY: the wire/SQL name is LOOKBACK; the C++ types are Lookback*Function. The unreflection
# registry is case sensitive, so the KEY must match LookbackLogicalFunction::NAME byte for byte.
add_registry_entry(LogicalFunction Lookback KEY LOOKBACK)
add_registry_entry(PhysicalFunction Lookback KEY LOOKBACK)
add_unreflection_entry(LogicalFunction Lookback KEY LOOKBACK)

if (NES_ENABLES_TESTS)
    add_nes_unit_test(LookbackFunctionTest LookbackFunctionTest.cpp)
    target_link_libraries(LookbackFunctionTest lookback_runtime nes-test-util)
    target_include_directories(LookbackFunctionTest PRIVATE .)
endif ()
