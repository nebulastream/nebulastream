# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_unit_test(distributed-planning-test DistributedPlanningTest.cpp)
target_link_libraries(distributed-planning-test nes-coordinator-bridge)
target_include_directories(distributed-planning-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_nes_unit_test(descriptor-config-test DescriptorConfigTest.cpp)
target_link_libraries(descriptor-config-test nes-coordinator-bridge tcp_sink_plugin_library)
target_include_directories(descriptor-config-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_nes_unit_test(create-worker-config-test CreateWorkerConfigTest.cpp)
target_link_libraries(create-worker-config-test nes-coordinator-bridge)

add_nes_unit_test(submit-wait-test SubmitWaitTest.cpp)
target_link_libraries(submit-wait-test nes-coordinator-bridge)

add_nes_unit_test(dropped-worker-test DroppedWorkerTest.cpp)
target_link_libraries(dropped-worker-test nes-coordinator-bridge)

add_nes_unit_test(drop-statement-planning-test DropStatementPlanningTest.cpp)
target_link_libraries(drop-statement-planning-test nes-coordinator-bridge)

