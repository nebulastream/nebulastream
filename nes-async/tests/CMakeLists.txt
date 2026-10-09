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
target_link_libraries(handoff-channel-test nes-async nes-memory)

add_nes_test(async-record-layout-test AsyncRecordLayoutTest.cpp)
target_link_libraries(async-record-layout-test nes-async nes-memory)

add_nes_test(async-executor-test AsyncExecutorTest.cpp)
target_link_libraries(async-executor-test nes-async nes-memory)

# Links the sink and source libraries so that their plugin registrations are pulled in.
add_nes_test(async-plugin-registration-test AsyncPluginRegistrationTest.cpp)
target_link_libraries(async-plugin-registration-test nes-async nes-sinks nes-sources)
