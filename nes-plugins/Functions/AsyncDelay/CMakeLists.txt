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

find_package(reflectcpp CONFIG REQUIRED)

add_library(async_delay_plugin_library STATIC AsyncDelayAsyncOperator.cpp)
target_link_libraries(async_delay_plugin_library PRIVATE nes-sources reflectcpp::reflectcpp)
target_include_directories(async_delay_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
link_plugin_library(nes-sources async_delay_plugin_library)
add_registry_entry(AsyncOperator AsyncDelay KEY ASYNC_DELAY)
add_unreflection_entry(AsyncOperator AsyncDelay KEY ASYNC_DELAY)

add_library(async_delay_logical_plugin_library STATIC AsyncDelayLogicalOperator.cpp)
target_link_libraries(async_delay_logical_plugin_library PRIVATE async_delay_plugin_library nes-logical-operators)
target_include_directories(async_delay_logical_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
link_plugin_library(nes-logical-operators async_delay_logical_plugin_library)
add_registry_entry(OperatorFunction AsyncDelay KEY ASYNC_DELAY)
add_registry_entry(AsyncOperatorLowering AsyncDelay KEY AsyncDelay)
add_unreflection_entry(LogicalOperator AsyncDelay KEY AsyncDelay)

if (NES_ENABLES_TESTS)
    add_nes_unit_test(async-delay-function-test AsyncDelayFunctionTest.cpp)
    target_link_libraries(async-delay-function-test async_delay_plugin_library async_delay_logical_plugin_library nes-sources nes-logical-operators reflectcpp::reflectcpp)

    add_nes_unit_test(async-delay-logical-operator-test AsyncDelayLogicalOperatorTest.cpp)
    target_link_libraries(async-delay-logical-operator-test async_delay_plugin_library async_delay_logical_plugin_library nes-logical-operators reflectcpp::reflectcpp)
endif ()
