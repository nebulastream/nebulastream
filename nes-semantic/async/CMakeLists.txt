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

# Async implementation of SEM_CALL: run by the Async source, registered in the registry owned by nes-sources.
find_package(boost_beast CONFIG REQUIRED)
add_library(semantic_call_async_function_plugin_library STATIC SemanticCallAsyncFunction.cpp SemanticHttpClient.cpp)
target_link_libraries(semantic_call_async_function_plugin_library PRIVATE nes-sources nes-semantic Boost::beast)
target_include_directories(semantic_call_async_function_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
link_plugin_library(nes-sources semantic_call_async_function_plugin_library)
add_registry_entry(AsyncFunction SemanticCall)
