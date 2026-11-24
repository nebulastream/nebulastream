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
# Beast is header-only; b2-built Boost installs its headers through boost_headers.
find_package(boost_headers CONFIG REQUIRED)

add_library(http_call_plugin_library STATIC HttpCallAsyncOperator.cpp)
target_link_libraries(http_call_plugin_library PRIVATE nes-sources Boost::headers reflectcpp::reflectcpp)
target_include_directories(http_call_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
link_plugin_library(nes-sources http_call_plugin_library)
add_registry_entry(AsyncOperator HttpCall KEY HTTP_CALL)
add_unreflection_entry(AsyncOperator HttpCall KEY HTTP_CALL)

add_library(http_call_logical_plugin_library STATIC HttpCallLogicalOperator.cpp)
target_link_libraries(http_call_logical_plugin_library PRIVATE http_call_plugin_library nes-logical-operators Boost::headers)
target_include_directories(http_call_logical_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
link_plugin_library(nes-logical-operators http_call_logical_plugin_library)
add_registry_entry(OperatorFunction HttpCall KEY HTTP_CALL)
add_registry_entry(AsyncOperatorLowering HttpCall KEY HttpCall)
add_unreflection_entry(LogicalOperator HttpCall KEY HttpCall)

if (NES_ENABLES_TESTS)
    add_nes_common_test(http-call-resolver-test HttpCallResolverTest.cpp)
    target_link_libraries(http-call-resolver-test nes-sources Boost::headers)
    target_include_directories(http-call-resolver-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

    add_nes_unit_test(http-call-function-test HttpCallFunctionTest.cpp)
    target_link_libraries(http-call-function-test http_call_plugin_library http_call_logical_plugin_library nes-sources nes-logical-operators reflectcpp::reflectcpp Boost::headers)

    add_nes_unit_test(http-call-logical-operator-test HttpCallLogicalOperatorTest.cpp)
    target_link_libraries(http-call-logical-operator-test http_call_plugin_library http_call_logical_plugin_library nes-logical-operators reflectcpp::reflectcpp Boost::headers)
endif ()
