#google testing framework
enable_testing(true)
set(GTEST_LIBRARIES libgtest libgtest_main libgmock)

# Code Generation
add_executable(code-generation-tests "UnitTests/CodeGen.cpp")
target_link_libraries(code-generation-tests iotdb ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME code-generation-tests COMMAND code-generation-tests)

# Topology
add_executable(fog-topology-tests "UnitTests/Topology.cpp")
target_link_libraries(fog-topology-tests iotdb  ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME fog-topology-tests COMMAND fog-topology-tests)

# Client Server Communication
add_executable(client-server-communication-tests "UnitTests/ClientServerCommunication.cpp")
target_link_libraries(client-server-communication-tests ${GTEST_LIBRARIES} ${Boost_LIBRARIES} ${ZMQ_LIBRARIES})
add_test(NAME client-server-communication-tests COMMAND client-server-communication-tests)

