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
add_definitions( -DTEST_DATA_DIRECTORY="${CMAKE_BINARY_DIR}/tests/test_data" )
message( STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}" )


#-------------------------------##############----------------------------------
#------------------------------- # Unit Test Suite --------------------------------

### Node Engine Tests ###
add_executable(node-engine-test "UnitTests/NodeEngineTest.cpp")
target_link_libraries(node-engine-test  nes ${GTEST_LIBRARIES})
add_test(NAME node-engine-test COMMAND node-engine-test)

### Code Generation Tests ###
add_executable(code-generation-tests "UnitTests/CodeGenerationTest.cpp")
target_link_libraries(code-generation-tests nes ${GTEST_LIBRARIES})
add_test(NAME code-generation-tests COMMAND code-generation-tests)

### NES Topology Tests ###
add_executable(nes-topology-manager-tests "UnitTests/TopologyManagerTest.cpp")
target_link_libraries(nes-topology-manager-tests nes ${GTEST_LIBRARIES})
add_test(NAME nes-topology-manager-tests COMMAND nes-topology-manager-tests)

### Buffer Manager Test ###
add_executable(buffer-manager-tests "UnitTests/BufferManagerTest.cpp" UnitTests/CoordinatorEngineTest.cpp)
target_link_libraries(buffer-manager-tests nes ${GTEST_LIBRARIES})
add_test(NAME buffer-manager-tests COMMAND buffer-manager-tests)

### WindowHandler Manager Test ###
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
if(NES_USE_KAFKA)
add_executable(kafka-tests "UnitTests/KafkaTest.cpp")
message("libraies: ${LIBRARIES}")
target_link_libraries(kafka-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME kafka-tests COMMAND kafka-tests)
endif(NES_USE_KAFKA)

###  Optimizer Service Tests ###
add_executable(optimizer-service-tests "UnitTests/OptimizerServiceTest.cpp")
target_link_libraries(optimizer-service-tests nes ${GTEST_LIBRARIES})
add_test(NAME optimizer-service-tests COMMAND optimizer-service-tests)

### Memory Layout Tests ###
add_executable(memory-layout-tests "UnitTests/MemoryLayoutTest.cpp")
target_link_libraries(memory-layout-tests nes ${GTEST_LIBRARIES})
add_test(NAME memory-layout-tests COMMAND memory-layout-tests)

### Stream Catalog Tests ###
add_executable(stream-catalog-test "UnitTests/StreamCatalogTest.cpp")
target_link_libraries(stream-catalog-test nes ${GTEST_LIBRARIES})
add_test(NAME stream-catalog-test COMMAND stream-catalog-test)

### QueryCatalog Tests ###
add_executable(query-catalog-tests "UnitTests/QueryCatalogTest.cpp")
target_link_libraries(query-catalog-tests nes ${GTEST_LIBRARIES})
add_test(NAME query-catalog-tests COMMAND query-catalog-tests)

### Path Finder Tests ###
add_executable(path-finder-test "UnitTests/Optimizer/Utils/PathFinderTest.cpp")
target_link_libraries(path-finder-test nes ${GTEST_LIBRARIES})
add_test(NAME path-finder-test COMMAND path-finder-test)

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

# ConvertLogicalToPhysicalSinkTest
add_executable(convert-logical-to-physical-sink-tests "UnitTests/ConvertLogicalToPhysicalSinkTest.cpp")
target_link_libraries(convert-logical-to-physical-sink-tests nes ${GTEST_LIBRARIES})
add_test(NAME convert-logical-to-physical-sink-tests COMMAND convert-logical-to-physical-sink-tests)

# ConvertLogicalToPhysicalSourceTest
add_executable(convert-logical-to-physical-source-tests "UnitTests/ConvertLogicalToPhysicalSourceTest.cpp")
target_link_libraries(convert-logical-to-physical-source-tests nes ${GTEST_LIBRARIES})
add_test(NAME convert-logical-to-physical-source-tests COMMAND convert-logical-to-physical-source-tests)

# Type inference test
add_executable(type-inference-tests "UnitTests/TypeInferencePhaseTest.cpp")
target_link_libraries(type-inference-tests nes ${GTEST_LIBRARIES})
add_test(NAME type-inference-tests COMMAND type-inference-tests)

# FilterPushDownTest
add_executable(filter-push-down-test "UnitTests/Optimizer/QueryRewrite/FilterPushDownTest.cpp")
target_link_libraries(filter-push-down-test nes ${GTEST_LIBRARIES})
add_test(NAME filter-push-down-test COMMAND filter-push-down-test)

# TranslateFromLegacyPlanPhaseTest
add_executable(translate-from-legacy-plan-phase-test "UnitTests/Nodes/Phases/TranslateFromLegacyPlanPhaseTest.cpp")
target_link_libraries(translate-from-legacy-plan-phase-test nes ${GTEST_LIBRARIES})
add_test(NAME translate-from-legacy-plan-phase-test COMMAND translate-from-legacy-plan-phase-test)

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

#------------------------------- # Integration Test Suite --------------------------------
### MultiWorkerTest ###
add_executable(multi-worker-test "Integration/MultiWorkerTest.cpp")
target_link_libraries(multi-worker-test nes ${GTEST_LIBRARIES})
add_test(NAME multi-worker-test COMMAND multi-worker-test)

### QueryDeploymentTest ###
add_executable(query-deployment-test "Integration/QueryDeploymentTest.cpp")
target_link_libraries(query-deployment-test nes ${GTEST_LIBRARIES})
add_test(NAME query-deployment-test COMMAND query-deployment-test)

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

### WorkerCoordinatorStarter Tests ###
add_executable(update-topology-remote-test "Integration/UpdateTopologyRemoteTest.cpp")
target_link_libraries(update-topology-remote-test nes ${GTEST_LIBRARIES})
add_test(NAME update-topology-remote-test COMMAND update-topology-remote-test)

# NetworkStackTest
add_executable(network-stack-test "UnitTests/NetworkStackTest.cpp")
target_link_libraries(network-stack-test nes ${GTEST_LIBRARIES})
add_test(NAME network-stack-test COMMAND network-stack-test)

#------------------------------- # E2E Test Suite --------------------------------
### E2E Tests ###
add_executable(e2e-single-worker-test "E2e/E2ECoordinatorSingleWorkerTest.cpp")
target_link_libraries(e2e-single-worker-test nes ${GTEST_LIBRARIES})
add_test(NAME e2e-single-worker-test COMMAND e2e-single-worker-test)
set_tests_properties(e2e-single-worker-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

add_executable(e2e-multi-worker-test "E2e/E2ECoordinatorMultiWorkerTest.cpp")
target_link_libraries(e2e-multi-worker-test nes ${GTEST_LIBRARIES})
add_test(NAME e2e-multi-worker-test COMMAND e2e-multi-worker-test)
set_tests_properties(e2e-multi-worker-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

add_executable(e2e-multi-query-test "E2e/E2ECoordinatorMultiQueryTest.cpp")
target_link_libraries(e2e-multi-query-test nes ${GTEST_LIBRARIES})
add_test(NAME e2e-multi-query-test COMMAND e2e-multi-query-test)
set_tests_properties(e2e-multi-query-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})


set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

# Custom test commands --------------------------------------------------------
add_custom_target(test_debug COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure DEPENDS ${UNITTESTS})