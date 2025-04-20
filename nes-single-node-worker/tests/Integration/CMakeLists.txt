# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find_package(boost_asio REQUIRED CONFIG)
add_nes_unit_test(single-node-integration-tests-csv SingleNodeIntegrationTestsCSV.cpp ../../src/GrpcService.cpp ../../src/SingleNodeWorker.cpp)
target_include_directories(single-node-integration-tests-csv PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include/ ${CMAKE_CURRENT_SOURCE_DIR}/../Util/include/)
target_link_libraries(
        single-node-integration-tests-csv
        nes-grpc
        nes-runtime
        nes-single-node-worker-test-util
)

add_nes_unit_test(single-node-integration-tests-tcp SingleNodeIntegrationTestsTCP.cpp ../../src/GrpcService.cpp ../../src/SingleNodeWorker.cpp)
target_include_directories(single-node-integration-tests-tcp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include/ ${CMAKE_CURRENT_SOURCE_DIR}/../Util/include/)
target_link_libraries(
        single-node-integration-tests-tcp
        nes-grpc
        nes-runtime
        Boost::asio
        nes-single-node-worker-test-util
)

add_nes_unit_test(single-node-integration-tests-mixed-sources SingleNodeIntegrationTestsMixedSources.cpp ../../src/GrpcService.cpp ../../src/SingleNodeWorker.cpp)
target_include_directories(single-node-integration-tests-mixed-sources PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include/ ${CMAKE_CURRENT_SOURCE_DIR}/../Util/include/)
target_link_libraries(
        single-node-integration-tests-mixed-sources
        nes-grpc
        nes-runtime
        Boost::asio
        nes-single-node-worker-test-util
)

add_nes_unit_test(single-node-integration-tests-query-status SingleNodeIntegrationTestsQueryStatus.cpp ../../src/GrpcService.cpp ../../src/SingleNodeWorker.cpp)
target_include_directories(single-node-integration-tests-query-status PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include/ ${CMAKE_CURRENT_SOURCE_DIR}/../Util/include/)
target_link_libraries(
        single-node-integration-tests-query-status
        nes-grpc
        nes-runtime
        Boost::asio
        nes-single-node-worker-test-util
)
