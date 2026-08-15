# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)
include(${PROJECT_SOURCE_DIR}/cmake/UnreflectionRegistrationUtil.cmake)

# Logical function: registers with the LogicalFunction factory registry owned by nes-logical-operators.
add_plugin_as_library(Conditional LogicalFunction ConditionalLogicalFunctionPlugin ConditionalLogicalFunction.cpp)
target_include_directories(ConditionalLogicalFunctionPlugin PUBLIC include PRIVATE .)
find_package(reflectcpp CONFIG REQUIRED)
target_link_libraries(ConditionalLogicalFunctionPlugin PRIVATE reflectcpp::reflectcpp)

# Serialized-plan reconstruction for the logical function. The generated glue includes the concrete
# header by basename, so the plugin's header directory has to be on the glue sub-library's include path.
add_unreflection_plugin(LogicalFunction Conditional)
get_property(logical_function_unreflection_glue GLOBAL PROPERTY "LogicalFunction_UNREFLECTION_GLUE_LIB")
target_include_directories(${logical_function_unreflection_glue} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include/Functions)

# Physical function: registers with the PhysicalFunction factory registry owned by nes-physical-operators.
add_plugin_as_library(Conditional PhysicalFunction ConditionalPhysicalFunctionPlugin ConditionalPhysicalFunction.cpp)
target_include_directories(ConditionalPhysicalFunctionPlugin PUBLIC include PRIVATE .)

if (NES_ENABLES_TESTS)
    add_nes_unit_test(conditional-logical-function-test ConditionalLogicalFunctionTest.cpp)
    target_link_libraries(conditional-logical-function-test ConditionalLogicalFunctionPlugin nes-logical-operators nes-test-util reflectcpp::reflectcpp)
    target_include_directories(conditional-logical-function-test PRIVATE include)
endif ()
