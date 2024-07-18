# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_unit_test(single-node-integration-test SingleNodeIntegrationTest.cpp ../../src/GrpcService.cpp ../../src/SingleNodeWorker.cpp)
target_include_directories(single-node-integration-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include/)
target_link_libraries(
        single-node-integration-test
        nes-grpc
        nes-runtime
        nes-execution
        nes-execution-test-util
)
target_compile_definitions(single-node-integration-test PRIVATE TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../testdata")