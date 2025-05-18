# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Called whenever TARGET CMake builds TARGET
# Executes 'nes-nebuli dump -i INPUT_PATH -o OUTPUT_PATH', generating the proto version of the yaml input query
function(serialize_query_with_nebuli TARGET QUERY_NAME)
    # Get the full path to the nes-nebuli binary
    set(NEBULI_BINARY "${CMAKE_BINARY_DIR}/nes-nebuli/nes-nebuli")
    set(YAML_QUERIES_DIR "${CMAKE_SOURCE_DIR}/${INTEGRATION_TEST_DATA_PATH}/queries")
    set(SERIALIZED_QUERIES_DIR "${CMAKE_BINARY_DIR}/${INTEGRATION_TEST_DATA_PATH}/queries")
    file(MAKE_DIRECTORY ${SERIALIZED_QUERIES_DIR})

    # Add dependency to ensure that the nebuli binary is built before the target that uses it
    add_dependencies(${TARGET} nes-nebuli)

    # Run nebuli on the specified yaml query (QUERY_NAME) and output the serialized protobuf version in the build directory
    add_custom_command(
            TARGET ${TARGET} PRE_BUILD
            COMMAND "${NEBULI_BINARY}" dump -i "${YAML_QUERIES_DIR}/${QUERY_NAME}.yaml" -o "${SERIALIZED_QUERIES_DIR}/${QUERY_NAME}.bin"
            COMMENT "Running nes-nebuli: ${INPUT_PATH} -> ${OUTPUT_PATH}"
            VERBATIM
    )
endfunction()

find_package(boost_asio REQUIRED CONFIG)

# TCP integration tests
add_nes_unit_test(single-node-integration-tests-tcp SingleNodeIntegrationTestsTCP.cpp ../../src/GrpcService.cpp ../../src/SingleNodeWorker.cpp)
target_include_directories(single-node-integration-tests-tcp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include/ ${CMAKE_CURRENT_SOURCE_DIR}/../Util/include/)
target_link_libraries(
        single-node-integration-tests-tcp
        nes-grpc
        nes-runtime
        nes-execution
        Boost::asio
        nes-single-node-worker-test-util
)
serialize_query_with_nebuli(single-node-integration-tests-tcp "qOneTCPSource")
serialize_query_with_nebuli(single-node-integration-tests-tcp "qOneTCPSourceWithFilter")
serialize_query_with_nebuli(single-node-integration-tests-tcp "qTwoTCPSourcesWithFilter")

# Mixed CSV and TCP integration tests
add_nes_unit_test(single-node-integration-tests-mixed-sources SingleNodeIntegrationTestsMixedSources.cpp ../../src/GrpcService.cpp ../../src/SingleNodeWorker.cpp)
target_include_directories(single-node-integration-tests-mixed-sources PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include/ ${CMAKE_CURRENT_SOURCE_DIR}/../Util/include/)
target_link_libraries(
        single-node-integration-tests-mixed-sources
        nes-grpc
        nes-runtime
        nes-execution
        Boost::asio
        nes-single-node-worker-test-util
)
serialize_query_with_nebuli(single-node-integration-tests-mixed-sources "qOneCSVSourceAndOneTCPSourceWithFilter")

add_nes_unit_test(single-node-integration-tests-query-status SingleNodeIntegrationTestsQueryStatus.cpp ../../src/GrpcService.cpp ../../src/SingleNodeWorker.cpp)
target_include_directories(single-node-integration-tests-query-status PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include/ ${CMAKE_CURRENT_SOURCE_DIR}/../Util/include/)
target_link_libraries(
        single-node-integration-tests-query-status
        nes-grpc
        nes-runtime
        nes-execution
        Boost::asio
        nes-single-node-worker-test-util
)
