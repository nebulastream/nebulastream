# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Creates a SingleNode Integration Test
# Queries located in the queriesYaml folder can be specified when registering a new
# integration test. Before building the test CMake makes sure that the query spec is
# parsed and transformed into a fully specified query plan, which is stored as a binary
# protobuf file. CMake injects a compilation definition into the test prefixed with QUERY_
# which contains the path to the binary query plan.
function(add_integration_test)
    # Parse Arguments. Both QUERIES and SOURCES may accept multiple files
    set(options "")
    set(oneValueArgs "")
    set(multiValueArgs QUERIES SOURCES)
    cmake_parse_arguments(ADD_INTEGRATION_TEST "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    add_nes_test(${ARGV0} ${ADD_INTEGRATION_TEST_SOURCES})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-single-node-worker-test-util nes-execution-test-util)
    foreach (QUERY ${ADD_INTEGRATION_TEST_QUERIES})
        # Register NebuLI invocation with each query
        add_custom_target(
                ${QUERY}_PARSING
                COMMAND nebuli dump --input ${CMAKE_CURRENT_SOURCE_DIR}/../testdata/queriesYaml/${QUERY}.yaml --output ${CMAKE_CURRENT_BINARY_DIR}/${QUERY}.bin
                DEPENDS nebuli
        )
        # Inject Compilation Definition pointing to the output of NebuLI
        target_compile_definitions(${TARGET_NAME} PRIVATE QUERY_${QUERY}="${CMAKE_CURRENT_BINARY_DIR}/${QUERY}.bin")
        # Building the test target depends on the invocation of NebuLI
        add_dependencies(${TARGET_NAME} ${QUERY}_PARSING)
    endforeach ()
endfunction()

add_integration_test(single-node-integration-tests-csv
        SOURCES SingleNodeIntegrationTestsCSV.cpp
        QUERIES qOneCSVSource qOneCSVSourceWithFilter qTwoCSVSourcesWithFilter)

find_package(boost_asio REQUIRED CONFIG)

add_integration_test(single-node-integration-tests-tcp
        SOURCES SingleNodeIntegrationTestsTCP.cpp
        QUERIES qOneTCPSource qOneTCPSourceWithFilter qTwoTCPSourcesWithFilter)
target_link_libraries(single-node-integration-tests-tcp Boost::asio)

add_integration_test(single-node-integration-tests-mixed-sources
        SOURCES SingleNodeIntegrationTestsMixedSources.cpp
        QUERIES qOneCSVSourceAndOneTCPSourceWithFilter
)
target_link_libraries(single-node-integration-tests-mixed-sources Boost::asio)
