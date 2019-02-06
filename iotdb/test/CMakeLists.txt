# Google Testing Framework
enable_testing(true)
set(GTEST_LIBRARIES libgtest libgtest_main libgmock)

# Copy Test Data
file(COPY test_data DESTINATION .)
add_definitions( -DTEST_DATA_PATH="${CMAKE_BINARY_DIR}/test/test_data/" )

# Code Generation Tests
add_executable(code-generation-tests "UnitTests/CodeGen.cpp")
target_link_libraries(code-generation-tests iotdb ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME code-generation-tests COMMAND code-generation-tests)

# Fog Topology Tests
add_executable(fog-topology-tests "UnitTests/FogTopologyManagerTest.cpp")
target_link_libraries(fog-topology-tests iotdb ${GTEST_LIBRARIES})
add_test(NAME fog-topology-tests COMMAND fog-topology-tests)

# Client Server Communication Tests
add_executable(client-server-communication-tests "UnitTests/ClientServerCommunicationTest.cpp")
target_link_libraries(client-server-communication-tests ${Boost_LIBRARIES} ${ZMQ_LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME client-server-communication-tests COMMAND client-server-communication-tests)
