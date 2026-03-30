# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_test(pipe-service-test PipeServiceTest.cpp)
target_link_libraries(pipe-service-test pipe-service nes-memory nes-executable)

add_nes_test(pipe-integration-test PipeIntegrationTest.cpp)
target_link_libraries(pipe-integration-test pipe-service nes-memory nes-executable nes-sinks nes-sources pipe_sink_plugin pipe_source_plugin_library)
target_include_directories(pipe-integration-test PRIVATE ${CMAKE_SOURCE_DIR}/nes-plugins/Pipe)
