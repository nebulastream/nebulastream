# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Google Testing Framework ----------------------------------------------------
include(ExternalProject)
ExternalProject_Add(
        gtest
        URL https://github.com/google/googletest/archive/release-1.8.1.zip
        PREFIX ${CMAKE_CURRENT_BINARY_DIR}/gtest
        # Disable install step
        INSTALL_COMMAND ""
        URL_HASH SHA256=927827c183d01734cc5cfef85e0ff3f5a92ffe6188e0d18e909c5efebf28a0c7
)

# Get GTest source and binary directories from CMake project
ExternalProject_Get_Property(gtest source_dir binary_dir)

# Create a libgtest target to be used as a dependency by test programs
add_library(libgtest IMPORTED STATIC GLOBAL)
add_dependencies(libgtest gtest gtest_main)

# Set libgtest properties
set_target_properties(libgtest PROPERTIES
        "IMPORTED_LOCATION" "${binary_dir}/googlemock/gtest/libgtest.a"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        )

add_library(libgtest_main IMPORTED STATIC GLOBAL)
add_dependencies(libgtest_main gtest_main)

set_target_properties(libgtest_main PROPERTIES
        "IMPORTED_LOCATION" "${binary_dir}/googlemock/gtest/libgtest_main.a"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        )

# Create a libgmock target to be used as a dependency by test programs
add_library(libgmock IMPORTED STATIC GLOBAL)
add_dependencies(libgmock gtest)

# Set libgmock properties
set_target_properties(libgmock PROPERTIES
        "IMPORTED_LOCATION" "${binary_dir}/googlemock/libgmock.a"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        )

# Set Include Dirs
include_directories("${source_dir}/googletest/include"
        "${source_dir}/googlemock/include")

set(GTEST_LIBRARIES ${LIBRARIES} ${GRPC_LIBRARIES} libgtest libgtest_main libgmock cpprestsdk::cpprest)

# Copy Test Data --------------------------------------------------------------
file(COPY test_data DESTINATION .)
add_definitions(-DTEST_DATA_DIRECTORY="${CMAKE_BINARY_DIR}/tests/test_data")
message(STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}")


#-------------------------------##############----------------------------------
#------------------------------- # Unit Test Suite --------------------------------

### Node Engine Tests ###
add_executable(node-engine-test "UnitTests/NodeEngineTest.cpp")
target_link_libraries(node-engine-test nes ${GTEST_LIBRARIES})
add_test(NAME node-engine-test COMMAND node-engine-test)

### Code Generation Tests ###
add_executable(code-generation-tests "UnitTests/CodeGenerationTest.cpp")
target_link_libraries(code-generation-tests nes ${GTEST_LIBRARIES})
add_test(NAME code-generation-tests COMMAND code-generation-tests)

### NES Topology Tests ###
add_executable(topology-tests "UnitTests/Topology/TopologyTest.cpp")
target_link_libraries(topology-tests nes ${GTEST_LIBRARIES})
add_test(NAME topology-tests COMMAND topology-tests)

### Buffer Manager Test ###
add_executable(buffer-manager-tests "UnitTests/BufferManagerTest.cpp" UnitTests/CoordinatorEngineTest.cpp)
target_link_libraries(buffer-manager-tests nes ${GTEST_LIBRARIES})
add_test(NAME buffer-manager-tests COMMAND buffer-manager-tests)

### AbstractWindowHandler Manager Test ###
add_executable(window-manager-tests "UnitTests/WindowManagerTest.cpp")
target_link_libraries(window-manager-tests nes ${GTEST_LIBRARIES})
add_test(NAME window-manager-tests COMMAND window-manager-tests)

### State Manager Test ###
add_executable(state-manager-tests "UnitTests/StateTest.cpp")
target_link_libraries(state-manager-tests nes ${GTEST_LIBRARIES})
add_test(NAME state-manager-tests COMMAND state-manager-tests)

### ZMQ Data Source Sink Tests ###
add_executable(zmq-tests "UnitTests/ZMQTest.cpp")
target_link_libraries(zmq-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME zmq-tests COMMAND zmq-tests)

### Source Tests ###
add_executable(source-tests "UnitTests/SourceTest.cpp")
target_link_libraries(source-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME source-tests COMMAND source-tests)

### Sink Tests ###
add_executable(sink-tests "UnitTests/SinkTest.cpp" UnitTests/SinkTest.cpp)
target_link_libraries(sink-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME sink-tests COMMAND sink-tests)

### Kafka Tests ###
if (NES_USE_KAFKA)
    add_executable(kafka-tests "UnitTests/KafkaTest.cpp")
    message("libraies: ${LIBRARIES}")
    target_link_libraries(kafka-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
    add_test(NAME kafka-tests COMMAND kafka-tests)
endif (NES_USE_KAFKA)

### OPC Tests ###
if(NES_USE_OPC)
    add_executable(opc-source-tests "UnitTests/OPCSourceTest.cpp" UnitTests/OPCSourceTest.cpp)
    target_link_libraries(opc-source-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
    add_test(NAME opc-source-tests COMMAND opc-source-tests)
    add_executable(opc-sink-tests "UnitTests/OPCSinkTest.cpp" UnitTests/OPCSinkTest.cpp)
    target_link_libraries(opc-sink-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
    add_test(NAME opc-sink-tests COMMAND opc-sink-tests)
endif(NES_USE_OPC)

### Memory Layout Tests ###
add_executable(memory-layout-tests "UnitTests/MemoryLayoutTest.cpp")
target_link_libraries(memory-layout-tests nes ${GTEST_LIBRARIES})
add_test(NAME memory-layout-tests COMMAND memory-layout-tests)

### Stream Catalog Tests ###
add_executable(stream-catalog-test "UnitTests/Catalogs/StreamCatalogTest.cpp")
target_link_libraries(stream-catalog-test nes ${GTEST_LIBRARIES})
add_test(NAME stream-catalog-test COMMAND stream-catalog-test)

### QueryCatalog Tests ###
add_executable(query-catalog-tests "UnitTests/QueryCatalogTest.cpp")
target_link_libraries(query-catalog-tests nes ${GTEST_LIBRARIES})
add_test(NAME query-catalog-tests COMMAND query-catalog-tests)

### QueryCompilerTest ###
add_executable(query-execution-test "UnitTests/QueryExecutionTest.cpp")
target_link_libraries(query-execution-test nes ${GTEST_LIBRARIES})
add_test(NAME query-execution-test COMMAND query-execution-test)

### TupleBufferTest ###
add_executable(tuple-buffer-test "UnitTests/TupleBufferTest.cpp")
target_link_libraries(tuple-buffer-test nes ${GTEST_LIBRARIES})
add_test(NAME tuple-buffer-test COMMAND tuple-buffer-test)

# LogicalOperatorPlanNodeTest
add_executable(logical-operator-node-tests "UnitTests/LogicalOperatorNodeTest.cpp")
target_link_libraries(logical-operator-node-tests nes ${GTEST_LIBRARIES})
add_test(NAME logical-operator-node-tests COMMAND logical-operator-node-tests)

# ExpressionNodeTest
add_executable(predicate-node-tests "UnitTests/ExpressionNodeTest.cpp")
target_link_libraries(predicate-node-tests nes ${GTEST_LIBRARIES})
add_test(NAME predicate-node-tests COMMAND predicate-node-tests)

# QueryTest
add_executable(query-tests "UnitTests/QueryTest.cpp")
target_link_libraries(query-tests nes ${GTEST_LIBRARIES})
add_test(NAME query-tests COMMAND query-tests)

# QueryTest
add_executable(translate-to-generatable-tests "UnitTests/QueryCompiler/TranslateToGeneratablePlanTest.cpp")
target_link_libraries(translate-to-generatable-tests nes ${GTEST_LIBRARIES})
add_test(NAME translate-to-generatable-tests COMMAND translate-to-generatable-tests)

# ConvertLogicalToPhysicalSinkTest
add_executable(convert-logical-to-physical-sink-tests "UnitTests/ConvertLogicalToPhysicalSinkTest.cpp")
target_link_libraries(convert-logical-to-physical-sink-tests nes ${GTEST_LIBRARIES})
add_test(NAME convert-logical-to-physical-sink-tests COMMAND convert-logical-to-physical-sink-tests)

# ConvertLogicalToPhysicalSourceTest
add_executable(convert-logical-to-physical-source-tests "UnitTests/ConvertLogicalToPhysicalSourceTest.cpp")
target_link_libraries(convert-logical-to-physical-source-tests nes ${GTEST_LIBRARIES})
add_test(NAME convert-logical-to-physical-source-tests COMMAND convert-logical-to-physical-source-tests)

# Type inference test
add_executable(type-inference-tests "UnitTests/Phases/TypeInferencePhaseTest.cpp")
target_link_libraries(type-inference-tests nes ${GTEST_LIBRARIES})
add_test(NAME type-inference-tests COMMAND type-inference-tests)

# FilterPushDownTest
add_executable(filter-push-down-rule-test "UnitTests/Optimizer/QueryRewrite/FilterPushDownRuleTest.cpp")
target_link_libraries(filter-push-down-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME filter-push-down-rule-test COMMAND filter-push-down-rule-test)

# DistributeWindowRuleTest
add_executable(window-distr-rule-test "UnitTests/Optimizer/QueryRewrite/DistributeWindowRuleTest.cpp")
target_link_libraries(window-distr-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME window-distr-rule-test COMMAND window-distr-rule-test)

# SerializationUtilTest
add_executable(serialize-operator-test "UnitTests/Nodes/SerializationUtilTest.cpp")
target_link_libraries(serialize-operator-test nes ${GTEST_LIBRARIES})
add_test(NAME serialize-operator-test COMMAND serialize-operator-test)

### CoordinatorEngine Tests ###
add_executable(coordinator-engine-tests "UnitTests/CoordinatorEngineTest.cpp")
target_link_libraries(coordinator-engine-tests nes ${GTEST_LIBRARIES})
add_test(NAME coordinator-engine-tests COMMAND coordinator-engine-tests)

### Query Plan Tests ###
add_executable(query-plan-tests "UnitTests/Plans/Query/QueryPlanTest.cpp")
target_link_libraries(query-plan-tests nes ${GTEST_LIBRARIES})
add_test(NAME query-plan-tests COMMAND query-plan-tests)

# SimplePatternTest
add_executable(simple-pattern-test "Integration/SimplePatternTest.cpp")
target_link_libraries(simple-pattern-test nes ${GTEST_LIBRARIES})
add_test(NAME simple-pattern-test COMMAND simple-pattern-test)

### Sensor Bus Tests ###
#add_executable(sensor-bus-tests "UnitTests/SensorBusTest.cpp")
#target_link_libraries(sensor-bus-tests nes ${GTEST_LIBRARIES})
#add_test(NAME sensor-bus-tests COMMAND sensor-bus-tests)

### Adaptive Source Tests ###
add_executable(adaptive-source-tests "UnitTests/AdaptiveSourceTest.cpp")
target_link_libraries(adaptive-source-tests nes ${GTEST_LIBRARIES})
add_test(NAME adaptive-source-tests COMMAND adaptive-source-tests)

### Global Query Node Tests ###
add_executable(global-query-node-tests "UnitTests/Plans/Global/Query/GlobalQueryNodeTest.cpp")
target_link_libraries(global-query-node-tests nes ${GTEST_LIBRARIES})
add_test(NAME global-query-node-tests COMMAND global-query-node-tests)

### Global Query Plan Tests ###
add_executable(global-query-plan-tests "UnitTests/Plans/Global/Query/GlobalQueryPlanTest.cpp")
target_link_libraries(global-query-plan-tests nes ${GTEST_LIBRARIES})
add_test(NAME global-query-plan-tests COMMAND global-query-plan-tests)

### Query Merger Phase Tests ###
add_executable(global-query-plan-update-phase-tests "UnitTests/Phases/GlobalQueryPlanUpdatePhaseTest.cpp")
target_link_libraries(global-query-plan-update-phase-tests nes ${GTEST_LIBRARIES})
add_test(NAME global-query-plan-update-phase-tests COMMAND global-query-plan-update-phase-tests)

### Logical Source Expansion Rule Tests ###
add_executable(logical-source-expansion-rule-test "UnitTests/Optimizer/QueryRewrite/LogicalSourceExpansionRuleTest.cpp")
target_link_libraries(logical-source-expansion-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME logical-source-expansion-rule-test COMMAND logical-source-expansion-rule-test)

### L0 Query Merger Rule Tests ###
add_executable(l0-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/L0QueryMergerRuleTest.cpp")
target_link_libraries(l0-query-merger-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME l0-query-merger-rule-test COMMAND l0-query-merger-rule-test)

### Query Placement Tests ###
add_executable(query-placement-test "UnitTests/Optimizer/QueryPlacement/QueryPlacementTest.cpp")
target_link_libraries(query-placement-test nes ${GTEST_LIBRARIES})
add_test(NAME query-placement-test COMMAND query-placement-test)

### Thread Naming Test ###
add_executable(thread-naming-tests "UnitTests/ThreadNamingTest.cpp")
target_link_libraries(thread-naming-tests nes ${GTEST_LIBRARIES})
add_test(NAME thread-naming-tests COMMAND thread-naming-tests)

### Thread Naming Test ###
add_executable(global-execution-plan-tests "UnitTests/Plans/Global/Execution/GlobalExecutionPlanTest.cpp")
target_link_libraries(global-execution-plan-tests nes ${GTEST_LIBRARIES})
add_test(NAME global-execution-plan-tests COMMAND global-execution-plan-tests)

### Z3 Validation Test ###
add_executable(z3-validation-tests "UnitTests/Z3ValidationTest.cpp")
target_link_libraries(z3-validation-tests nes ${GTEST_LIBRARIES})
add_test(NAME z3-validation-tests COMMAND z3-validation-tests)

### Operator To FOL Util Test ###
add_executable(operator-to-FOL-util-tests "UnitTests/Optimizer/Utils/OperatorToQuerySignatureUtilTests.cpp")
target_link_libraries(operator-to-FOL-util-tests nes ${GTEST_LIBRARIES})
add_test(NAME operator-to-FOL-util-tests COMMAND operator-to-FOL-util-tests)

### Signature Inference Phase Test ###
add_executable(signature-inference-phase-test "UnitTests/Optimizer/Phases/SignatureInferencePhaseTest.cpp")
target_link_libraries(signature-inference-phase-test nes ${GTEST_LIBRARIES})
add_test(NAME signature-inference-phase-test COMMAND signature-inference-phase-test)

### Circular Buffer Tests ###
add_executable(circular-buffer-tests "UnitTests/CircularBufferTest.cpp")
target_link_libraries(circular-buffer-tests nes ${GTEST_LIBRARIES})
add_test(NAME circular-buffer-tests COMMAND circular-buffer-tests)

### Z3 Expression Inference Phase Test ###
add_executable(equal-query-merger-rule-tests "UnitTests/Optimizer/QueryMerger/EqualQueryMergerRuleTest.cpp")
target_link_libraries(equal-query-merger-rule-tests nes ${GTEST_LIBRARIES})
add_test(NAME equal-query-merger-rule-tests COMMAND equal-query-merger-rule-tests)

#------------------------------- # Integration Test Suite --------------------------------
### MultiWorkerTest ###
add_executable(multi-worker-test "Integration/MultiWorkerTest.cpp")
target_link_libraries(multi-worker-test nes ${GTEST_LIBRARIES})
add_test(NAME multi-worker-test COMMAND multi-worker-test)

### QueryDeploymentTest ###
add_executable(query-deployment-test "Integration/QueryDeploymentTest.cpp")
target_link_libraries(query-deployment-test nes ${GTEST_LIBRARIES})
add_test(NAME query-deployment-test COMMAND query-deployment-test)

### windowDeploymentTest ###
add_executable(window-deployment-test "Integration/WindowDeploymentTest.cpp")
target_link_libraries(window-deployment-test nes ${GTEST_LIBRARIES})
add_test(NAME window-deployment-test COMMAND window-deployment-test)


### mergeDeploymentTest ###
add_executable(merge-deployment-test "Integration/MergeDeploymentTest.cpp")
target_link_libraries(merge-deployment-test nes ${GTEST_LIBRARIES})
add_test(NAME merge-deployment-test COMMAND merge-deployment-test)

### ContiniousSourceTest ###
add_executable(continious-source-test "Integration/ContinuousSourceTest.cpp")
target_link_libraries(continious-source-test nes ${GTEST_LIBRARIES})
add_test(NAME continious-source-test COMMAND continious-source-test)

### Catalog Remote Tests ###
add_executable(stream-catalog-remote-test "Integration/StreamCatalogRemoteTest.cpp")
target_link_libraries(stream-catalog-remote-test nes ${GTEST_LIBRARIES})
add_test(NAME stream-catalog-remote-test COMMAND stream-catalog-remote-test)

### WorkerCoordinatorStarter Tests ###
add_executable(worker-coordinator-starter-tests "Integration/WorkerCoordinatorStarterTest.cpp")
target_link_libraries(worker-coordinator-starter-tests nes ${GTEST_LIBRARIES})
add_test(NAME worker-coordinator-starter-tests COMMAND worker-coordinator-starter-tests)

# RESTEndpointTest
add_executable(rest-endpoint-test "Integration/RESTEndpointTest.cpp")
target_link_libraries(rest-endpoint-test nes ${GTEST_LIBRARIES})
add_test(NAME rest-endpoint-test COMMAND rest-endpoint-test)

### WorkerCoordinatorStarter Tests ###
add_executable(update-topology-remote-test "Integration/UpdateTopologyRemoteTest.cpp")
target_link_libraries(update-topology-remote-test nes ${GTEST_LIBRARIES})
add_test(NAME update-topology-remote-test COMMAND update-topology-remote-test)

# NetworkStackTest
add_executable(network-stack-test "UnitTests/NetworkStackTest.cpp")
target_link_libraries(network-stack-test nes ${GTEST_LIBRARIES})
add_test(NAME network-stack-test COMMAND network-stack-test)

# MonitoringStackTest
add_executable(monitoring-stack-test "UnitTests/MonitoringStackTest.cpp")
target_link_libraries(monitoring-stack-test nes ${GTEST_LIBRARIES})
add_test(NAME monitoring-stack-test COMMAND monitoring-stack-test)

#------------------------------- # E2E Test Suite --------------------------------
### E2E Tests ###
add_executable(e2e-coordinator-single-worker-test "E2e/E2ECoordinatorSingleWorkerTest.cpp")
target_link_libraries(e2e-coordinator-single-worker-test nes ${GTEST_LIBRARIES})
add_test(NAME e2e-coordinator-single-worker-test COMMAND e2e-coordinator-single-worker-test)
set_tests_properties(e2e-coordinator-single-worker-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

add_executable(e2e-coordinator-multi-worker-test "E2e/E2ECoordinatorMultiWorkerTest.cpp")
target_link_libraries(e2e-coordinator-multi-worker-test nes ${GTEST_LIBRARIES})
add_test(NAME e2e-coordinator-multi-worker-test COMMAND e2e-coordinator-multi-worker-test)
set_tests_properties(e2e-coordinator-multi-worker-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

add_executable(e2e-coordinator-multi-query-test "E2e/E2ECoordinatorMultiQueryTest.cpp")
target_link_libraries(e2e-coordinator-multi-query-test nes ${GTEST_LIBRARIES})
add_test(NAME e2e-coordinator-multi-query-test COMMAND e2e-coordinator-multi-query-test)
set_tests_properties(e2e-coordinator-multi-query-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

# Custom test commands --------------------------------------------------------
add_custom_target(test_debug COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure DEPENDS ${UNITTESTS})