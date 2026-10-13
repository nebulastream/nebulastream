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

# Logical function only: HTTP_CALL is asynchronous and has no physical (Nautilus) implementation.
add_library(http_call_logical_function_plugin_library STATIC HttpCallLogicalFunction.cpp)
target_link_libraries(http_call_logical_function_plugin_library PRIVATE nes-logical-operators reflectcpp::reflectcpp)
target_include_directories(http_call_logical_function_plugin_library PUBLIC include PRIVATE .)
link_plugin_library(nes-logical-operators http_call_logical_function_plugin_library)
add_registry_entry(LogicalFunction HttpCall KEY HTTP_CALL)
add_unreflection_entry(LogicalFunction HttpCall)

# Async implementation: run by the Async source, registered in the registry owned by nes-sources.
find_package(boost_beast CONFIG REQUIRED)
add_library(http_call_async_function_plugin_library STATIC HttpCallAsyncFunction.cpp)
target_link_libraries(http_call_async_function_plugin_library PRIVATE nes-sources Boost::beast)
target_include_directories(http_call_async_function_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
link_plugin_library(nes-sources http_call_async_function_plugin_library)
add_registry_entry(AsyncFunction HttpCall)

# Each generated glue translation unit includes the plugin header by basename, and the headers of
# this plugin sit one directory deeper than the plugin root that the glue picks up by itself.
foreach (registry IN ITEMS LogicalFunction LogicalFunctionUnreflection)
    get_property(glue_lib GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_GLUE_LIB")
    target_include_directories(${glue_lib} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include/Functions)
endforeach ()
