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

# Links nes-async for AsyncExecutionTrait and the HttpCall function plugin for the literals it
# reads out of the query.
add_library(HttpCallAsyncRulePlugin STATIC HttpCallAsyncRule.cpp)
target_link_libraries(HttpCallAsyncRulePlugin PRIVATE nes-query-optimizer nes-async http_call_logical_function_plugin_library)
target_include_directories(HttpCallAsyncRulePlugin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

link_plugin_library(nes-query-optimizer HttpCallAsyncRulePlugin)
add_registry_entry(PlanRule HttpCallAsyncRule KEY HttpCallAsync)
