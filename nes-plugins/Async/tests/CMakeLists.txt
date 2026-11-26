# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_test(async-source-value-test AsyncSourceValueTest.cpp)
target_link_libraries(async-source-value-test async_source_plugin_library nes-sources nes-nautilus reflectcpp::reflectcpp)

add_nes_test(async-source-buffer-reuse-test AsyncSourceBufferReuseTest.cpp)
target_link_libraries(async-source-buffer-reuse-test async_source_plugin_library nes-sources nes-nautilus)

add_nes_test(async-graph-execution-test AsyncGraphExecutionTest.cpp)
target_include_directories(async-graph-execution-test PRIVATE ${PROJECT_SOURCE_DIR}/nes-sources/tests)
target_link_libraries(async-graph-execution-test async_graph_execution)

add_nes_test(async-occurrence-test AsyncOccurrenceTest.cpp)
target_link_libraries(async-occurrence-test nes-logical-operators nes-sources nes-query-optimizer nes-query-optimizer-test-utils-lib async_graph_execution)
