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

add_plugin_as_library(RedundantUnionRemoval PlanRule RedundantUnionRemovalRulePlugin RedundantUnionRemovalRule.cpp)

target_include_directories(RedundantUnionRemovalRulePlugin
        PUBLIC include
        PRIVATE .
)

add_nes_unit_test(RedundantUnionRemovalRuleTest RedundantUnionRemovalRuleTest.cpp)
target_link_libraries(RedundantUnionRemovalRuleTest nes-query-optimizer nes-logical-operators nes-test-util nes-query-optimizer-test-utils-lib)
target_include_directories(RedundantUnionRemovalRuleTest PRIVATE .)
