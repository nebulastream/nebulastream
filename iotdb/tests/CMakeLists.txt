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

# Test Suites -----------------------------------------------------------------

# Engine Tests
add_executable(engine-test "UnitTests/EngineTest.cpp")
target_link_libraries(engine-test  iotdb ${GTEST_LIBRARIES})
add_test(NAME engine-test  COMMAND engine-test )

# Code Generation Tests
add_executable(code-generation-tests "UnitTests/CodeGenTest.cpp")
target_link_libraries(code-generation-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME code-generation-tests COMMAND code-generation-tests)

# Query Interface Tests
add_executable(query-interface-tests "UnitTests/QueryInterfaceTest.cpp")
target_link_libraries(query-interface-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME query-interface-tests COMMAND query-interface-tests)

# Fog Topology Tests
add_executable(fog-topology-tests "UnitTests/FogTopologyManagerTest.cpp")
target_link_libraries(fog-topology-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME fog-topology-tests COMMAND fog-topology-tests)

# Client Server Communication Tests
#add_executable(client-server-communication-tests "UnitTests/ClientServerCommunicationTest.cpp")
#target_link_libraries(client-server-communication-tests ${GTEST_LIBRARIES})
#add_test(NAME client-server-communication-tests COMMAND client-server-communication-tests)

# Buffer Manager Test
add_executable(buffer-manager-tests "UnitTests/BufferManagerTest.cpp")
target_link_libraries(buffer-manager-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME buffer-manager-tests COMMAND buffer-manager-tests)


# WindowHandler Manager Test
add_executable(window-manager-tests "UnitTests/WindowManagerTest.cpp")
target_link_libraries(window-manager-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME window-manager-tests COMMAND window-manager-tests)

# State Manager Test
add_executable(state-manager-tests "UnitTests/StateTest.cpp")
target_link_libraries(state-manager-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME state-manager-tests COMMAND state-manager-tests)

# Test data sources and data sinks of runtime
add_executable(runtime-data-source-sink-tests "UnitTests/RuntimeDataSourceSinkTest.cpp")
target_link_libraries(runtime-data-source-sink-tests iotdb ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME runtime-data-source-sink-tests COMMAND runtime-data-source-sink-tests)

# Yahoo Streaming Benchmark Tests
#add_executable(yahoo-streaming-benchmark-tests "UnitTests/YahooStreamingBenchmarkTest.cpp")
#target_link_libraries(yahoo-streaming-benchmark-tests iotdb ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
#add_test(NAME yahoo-streaming-benchmark-tests COMMAND yahoo-streaming-benchmark-tests)

add_executable(file-source-tests "UnitTests/FileSourceTest.cpp")
target_link_libraries(file-source-tests iotdb ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME file-source-tests COMMAND file-source-tests)

# YSB Performance Test
add_executable(ysb_performance_test "PerformanceTests/YSB_SingleNode_PerformanceTest.cpp")
target_link_libraries(ysb_performance_test iotdb ${GTEST_LIBRARIES})
add_test(NAME ysb_performance_test COMMAND ysb_performance_test)

# Predicate Tree Tests
add_executable(predicate-tree-tests "UnitTests/PredicateTest.cpp")
target_link_libraries(predicate-tree-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME predicate-tree-tests COMMAND predicate-tree-tests)

# Predicate Tree Tests
add_executable(optimizer-service-tests "UnitTests/Services/OptimizerServiceTest.cpp")
target_link_libraries(optimizer-service-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME optimizer-service-tests COMMAND optimizer-service-tests)

# Serialization Tools Tree Tests
add_executable(serialization-tools-tests "UnitTests/SerializationToolsTest.cpp")
target_link_libraries(serialization-tools-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME serialization-tools-tests COMMAND serialization-tools-tests)

# Serialization NesCoordinator Tests
#add_executable(coordinator-service-tests "UnitTests/CoordinatorServiceTest.cpp")
#target_link_libraries(coordinator-service-tests iotdb ${GTEST_LIBRARIES})
#add_test(NAME coordinator-service-tests COMMAND coordinator-service-tests)

# ActorCoordinator and ActorWorker Tests
add_executable(actors-cli-tests "UnitTests/ActorsCliTest.cpp")
target_link_libraries(actors-cli-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME actors-cli-tests COMMAND actors-cli-tests)

# Custom test commands --------------------------------------------------------
add_custom_target(test_debug COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
        DEPENDS ${UNITTESTS})
