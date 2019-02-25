#google testing framework
enable_testing(true)
set(GTEST_LIBRARIES iotdb libgtest libgtest_main libgmock)

# Engine Start and Stop Test
add_executable(engine-start-stop-test "UnitTests/EngineStartAndStopTest.cpp")
target_link_libraries(engine-start-stop-test ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME engine-start-stop-test COMMAND engine-start-stop-test)

# Code Generation
add_executable(code-generation-tests "UnitTests/CodeGenTest.cpp")
target_link_libraries(code-generation-tests ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME code-generation-tests COMMAND code-generation-tests)

#Query Interface
add_executable(query-interface-tests "UnitTests/QueryInterfaceTest.cpp")
target_link_libraries(query-interface-tests ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME query-interface-tests COMMAND query-interface-tests)

# Topology
add_executable(fog-topology-tests "UnitTests/TopologyTest.cpp")
target_link_libraries(fog-topology-tests ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})

add_test(NAME fog-topology-tests COMMAND fog-topology-tests)

# Client Server Communication
file(COPY test_data DESTINATION .)
add_definitions( -DTEST_DATA_PATH="${CMAKE_BINARY_DIR}/test/test_data/" )

add_executable(client-server-communication-tests "UnitTests/ClientServerCommunication.cpp")
target_link_libraries(client-server-communication-tests ${GTEST_LIBRARIES} ${Boost_LIBRARIES} ${ZMQ_LIBRARIES})
add_test(NAME client-server-communication-tests COMMAND client-server-communication-tests)
