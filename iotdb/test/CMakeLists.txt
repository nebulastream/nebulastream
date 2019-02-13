# Google Testing Framework
enable_testing()
set(GTEST_LIBRARIES libgtest libgtest_main libgmock)

# Engine Start and Stop Test
add_executable(engine-start-stop-test "UnitTests/EngineStartAndStopTest.cpp")
target_link_libraries(engine-start-stop-test iotdb ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME engine-start-stop-test COMMAND engine-start-stop-test)

# Copy Test Data
file(COPY test_data DESTINATION .)
add_definitions( -DTEST_DATA_PATH="${CMAKE_BINARY_DIR}/test/test_data/" )

# Code Generation Tests
add_executable(code-generation-tests "UnitTests/CodeGenTest.cpp")
target_link_libraries(code-generation-tests iotdb ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME code-generation-tests COMMAND code-generation-tests)

# Fog Topology Tests
add_executable(fog-topology-tests "UnitTests/FogTopologyManagerTest.cpp")
target_link_libraries(fog-topology-tests iotdb ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME fog-topology-tests COMMAND fog-topology-tests)

# Client Server Communication Tests
add_executable(client-server-communication-tests "UnitTests/ClientServerCommunicationTest.cpp")
target_link_libraries(client-server-communication-tests ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME client-server-communication-tests COMMAND client-server-communication-tests)

# Test data sources of runtime
add_executable(runtime-data-source-tests "UnitTests/RuntimeDataSourceTest.cpp")
target_link_libraries(runtime-data-source-tests iotdb ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME runtime-data-source-tests COMMAND runtime-data-source-tests)
