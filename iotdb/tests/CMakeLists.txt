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

set(GTEST_LIBRARIES ${LIBRARIES} libgtest libgtest_main libgmock)

# Copy Test Data --------------------------------------------------------------
file(COPY test_data DESTINATION .)
add_definitions( -DTEST_DATA_DIRECTORY="${CMAKE_BINARY_DIR}/tests/test_data" )
message( STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}" )

# Test Suites -----------------------------------------------------------------

# Engine Start and Stop Tests
add_executable(engine-start-stop-test "UnitTests/EngineStartAndStopTest.cpp")
target_link_libraries(engine-start-stop-test iotdb ${GTEST_LIBRARIES})
add_test(NAME engine-start-stop-test COMMAND engine-start-stop-test)

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
add_executable(client-server-communication-tests "UnitTests/ClientServerCommunicationTest.cpp")
target_link_libraries(client-server-communication-tests ${GTEST_LIBRARIES})
add_test(NAME client-server-communication-tests COMMAND client-server-communication-tests)

# Buffer Manager Test
add_executable(buffer-manager-tests "UnitTests/BufferManagerTest.cpp")
target_link_libraries(buffer-manager-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME buffer-manager-tests COMMAND buffer-manager-tests)

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

# Custom test commands --------------------------------------------------------
add_custom_target(test_debug COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
        DEPENDS ${UNITTESTS})
