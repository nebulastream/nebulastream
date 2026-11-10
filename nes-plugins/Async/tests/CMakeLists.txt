# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_test(handoff-channel-test HandoffChannelTest.cpp)
target_link_libraries(handoff-channel-test async_handoff_channel nes-memory nes-common)

add_nes_test(async-source-value-test AsyncSourceValueTest.cpp)
target_link_libraries(async-source-value-test async_source_plugin_library nes-sources nes-nautilus reflectcpp::reflectcpp)

add_nes_test(async-graph-execution-test AsyncGraphExecutionTest.cpp)
target_link_libraries(async-graph-execution-test async_graph_execution)
