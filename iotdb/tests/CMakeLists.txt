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

set(GTEST_LIBRARIES ${LIBRARIES} libgtest libgtest_main libgmock cpprestsdk::cpprest)

# Copy Test Data --------------------------------------------------------------
file(COPY test_data DESTINATION .)
add_definitions( -DTEST_DATA_DIRECTORY="${CMAKE_BINARY_DIR}/tests/test_data" )
message( STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}" )


#-------------------------------##############----------------------------------
#------------------------------- # E2E Test Suite --------------------------------
### E2E Tests ###
add_executable(e2e-rest-test "E2e/E2eRestTest.cpp")
target_link_libraries(e2e-rest-test iotdb ${GTEST_LIBRARIES})
add_test(NAME e2e-rest-test COMMAND e2e-rest-test)
set_tests_properties(e2e-rest-test PROPERTIES
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

#------------------------------- # Integration Test Suite --------------------------------
### MultiWorkerTest ###
add_executable(multi-worker-test "Integration/MultiWorkerTest.cpp")
target_link_libraries(multi-worker-test iotdb ${GTEST_LIBRARIES})
add_test(NAME multi-worker-test-XYZ COMMAND multi-worker-test)

### ContiniousSourceTest ###
#add_executable(continious-source-test "Integration/ContiniousSourceTest.cpp")
#target_link_libraries(continious-source-test iotdb ${GTEST_LIBRARIES})
#add_test(NAME continious-source-test COMMAND continious-source-test)

### Catalog Remote Tests ###
add_executable(stream-catalog-remote-test "Integration/StreamCatalogRemoteTest.cpp")
target_link_libraries(stream-catalog-remote-test iotdb ${GTEST_LIBRARIES})
add_test(NAME stream-catalog-remote-test COMMAND stream-catalog-remote-test)

### WorkerCoordinatorStarter Tests ###
add_executable(worker-coordinator-starter-tests "Integration/WorkerCoordinatorStarterTest.cpp")
target_link_libraries(worker-coordinator-starter-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME worker-coordinator-starter-tests COMMAND worker-coordinator-starter-tests)

#------------------------------- # Unit Test Suite --------------------------------

### Node Engine Tests ###
add_executable(node-engine-test "UnitTests/NodeEngineTest.cpp")
target_link_libraries(node-engine-test  iotdb ${GTEST_LIBRARIES})
add_test(NAME node-engine-test COMMAND node-engine-test)

### Code Generation Tests ###
add_executable(code-generation-tests "UnitTests/CodeGenerationTest.cpp")
target_link_libraries(code-generation-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME code-generation-tests COMMAND code-generation-tests)

### Query Interface Tests ###
add_executable(query-interface-tests "UnitTests/QueryInterfaceTest.cpp")
target_link_libraries(query-interface-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME query-interface-tests COMMAND query-interface-tests)

### NES Topology Tests ###
add_executable(nes-topology-manager-tests "UnitTests/NESTopologyManagerTest.cpp")
target_link_libraries(nes-topology-manager-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME nes-topology-manager-tests COMMAND nes-topology-manager-tests)

### Buffer Manager Test ###
add_executable(buffer-manager-tests "UnitTests/BufferManagerTest.cpp")
target_link_libraries(buffer-manager-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME buffer-manager-tests COMMAND buffer-manager-tests)

### WindowHandler Manager Test ###
add_executable(window-manager-tests "UnitTests/WindowManagerTest.cpp")
target_link_libraries(window-manager-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME window-manager-tests COMMAND window-manager-tests)

### State Manager Test ###
add_executable(state-manager-tests "UnitTests/StateTest.cpp")
target_link_libraries(state-manager-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME state-manager-tests COMMAND state-manager-tests)

### ZMQ Data Source Sink Tests ###
add_executable(zmq-tests "UnitTests/ZMQTest.cpp")
target_link_libraries(zmq-tests iotdb ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME zmq-tests COMMAND zmq-tests)

### Source Tests ###
add_executable(source-tests "UnitTests/SourceTest.cpp")
target_link_libraries(source-tests iotdb ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME source-tests COMMAND source-tests)

### Kafka Tests ###
add_executable(kafka-tests "UnitTests/KafkaTest.cpp")
message("libraies: ${LIBRARIES}")
target_link_libraries(kafka-tests iotdb ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME kafka-tests COMMAND kafka-tests)

### Predicate Tree Tests ###
add_executable(predicate-tree-tests "UnitTests/PredicateTest.cpp")
target_link_libraries(predicate-tree-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME predicate-tree-tests COMMAND predicate-tree-tests)

###  Predicate Tree Tests ###
add_executable(optimizer-service-tests "UnitTests/Services/OptimizerServiceTest.cpp")
target_link_libraries(optimizer-service-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME optimizer-service-tests COMMAND optimizer-service-tests)

### Serialization Tools Tree Tests ###
add_executable(serialization-tools-tests "UnitTests/SerializationToolsTest.cpp")
target_link_libraries(serialization-tools-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME serialization-tools-tests COMMAND serialization-tools-tests)

### Memory Layout Tests ###
add_executable(memory-layout-tests "UnitTests/MemoryLayoutTest.cpp")
target_link_libraries(memory-layout-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME memory-layout-tests COMMAND memory-layout-tests)

### CoordinatorService Tests ###
add_executable(coordinator-service-tests "UnitTests/Services/CoordinatorServiceTest.cpp")
target_link_libraries(coordinator-service-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME coordinator-service-tests COMMAND coordinator-service-tests)

### Actors Tests ###
add_executable(actors-cli-tests "UnitTests/ActorsCliTest.cpp")
target_link_libraries(actors-cli-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME actors-cli-tests COMMAND actors-cli-tests)

### Stream Catalog Tests ###
add_executable(stream-catalog-test "UnitTests/StreamCatalogTest.cpp")
target_link_libraries(stream-catalog-test iotdb ${GTEST_LIBRARIES})
add_test(NAME stream-catalog-test COMMAND stream-catalog-test)

### StreamCatalogService Tests ###
add_executable(stream-catalog-service-tests "UnitTests/Services/StreamCatalogServiceTest.cpp")
target_link_libraries(stream-catalog-service-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME stream-catalog-service-tests COMMAND stream-catalog-service-tests)

### QueryCatalog Tests ###
add_executable(query-catalog-tests "UnitTests/QueryCatalogTest.cpp")
target_link_libraries(query-catalog-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME query-catalog-tests COMMAND query-catalog-tests)

### QueryCatalogService Tests ###
add_executable(query-catalog-service-tests "UnitTests/Services/QueryCatalogServiceTest.cpp")
target_link_libraries(query-catalog-service-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME query-catalog-service-tests COMMAND query-catalog-service-tests)

### Internal Networking Tests ###
add_executable(internal-data-transmission-test "UnitTests/InternalDataTransmissionTest.cpp")
target_link_libraries(internal-data-transmission-test iotdb ${GTEST_LIBRARIES})
add_test(NAME internal-data-transmission-test COMMAND internal-data-transmission-test)

### Path Finder Tests ###
add_executable(path-finder-test "UnitTests/Optimizer/Utils/PathFinderTest.cpp")
target_link_libraries(path-finder-test iotdb ${GTEST_LIBRARIES})
add_test(NAME path-finder-test COMMAND path-finder-test)

### QueryCompilerTest ###
add_executable(query-execution-test "UnitTests/QueryExecutionTest.cpp")
target_link_libraries(query-execution-test iotdb ${GTEST_LIBRARIES})
add_test(NAME query-execution-test COMMAND query-execution-test)

### TupleBufferTest ###
add_executable(tuple-buffer-test "UnitTests/TupleBufferTest.cpp")
target_link_libraries(tuple-buffer-test iotdb ${GTEST_LIBRARIES})
add_test(NAME tuple-buffer-test COMMAND tuple-buffer-test)

# LogicalOperatorPlanNodeTest
add_executable(logical-operator-node-tests "UnitTests/LogicalOperatorNodeTest.cpp")
target_link_libraries(logical-operator-node-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME logical-operator-node-tests COMMAND logical-operator-node-tests)

# ExpressionNodeTest
add_executable(predicate-node-tests "UnitTests/ExpressionNodeTest.cpp")
target_link_libraries(predicate-node-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME predicate-node-tests COMMAND predicate-node-tests)

# Custom test commands --------------------------------------------------------
add_custom_target(test_debug COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure DEPENDS ${UNITTESTS})

add_custom_target(test_ext COMMAND ${CMAKE_CTEST_COMMAND} --gtest_filter=*XYZ*)
#add_custom_target(test_e2e COMMAND ${CMAKE_CTEST_COMMAND} --gtest_filter=E2e*)


