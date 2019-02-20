# Google Testing Framework
enable_testing()
set(GTEST_LIBRARIES ${LIBRARIES} libgtest libgtest_main libgmock)

# Copy Test Data
file(COPY test_data DESTINATION .)

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
add_executable(buffer-manager-test "UnitTests/BufferManagerTest.cpp")
target_link_libraries(buffer-manager-test iotdb ${GTEST_LIBRARIES})
add_test(NAME buffer-manager-test COMMAND buffer-manager-test)

# Test data sources of runtime
add_executable(runtime-data-source-tests "UnitTests/RuntimeDataSourceTest.cpp")
target_link_libraries(runtime-data-source-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME runtime-data-source-tests COMMAND runtime-data-source-tests)


# Command to execute all tests at once ----------------------------------------

add_custom_target(test COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS
        engine-start-stop-test
        code-generation-tests
        query-interface-tests
        fog-topology-tests
        client-server-communication-tests
        buffer-manager-test
        runtime-data-source-tests)
