# Google Testing Framework
enable_testing()
set(GTEST_LIBRARIES libgtest libgtest_main libgmock)

# Engine Start and Stop Test
add_executable(engine-start-stop-test "UnitTests/EngineStartAndStopTest.cpp")
target_link_libraries(engine-start-stop-test iotdb ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME engine-start-stop-test COMMAND engine-start-stop-test)

# Copy Test Data
file(COPY test_data DESTINATION .)
add_definitions( -DTEST_DATA_PATH="${CMAKE_BINARY_DIR}/tests/test_data/" )

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

# Query Interface Tests
add_executable(query-interface-tests "UnitTests/QueryInterfaceTest.cpp")
target_link_libraries(query-interface-tests iotdb ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME query-interface-tests COMMAND query-interface-tests)

# Buffer Manager Test
add_executable(buffer-manager-test "UnitTests/BufferManagerTest.cpp")
target_link_libraries(buffer-manager-test iotdb ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME buffer-manager-test COMMAND buffer-manager-test)

# Test data sources and data sinks of runtime
add_executable(runtime-data-source-sink-tests "UnitTests/RuntimeDataSourceSinkTest.cpp")
target_link_libraries(runtime-data-source-sink-tests iotdb ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME runtime-data-source-sink-tests COMMAND runtime-data-source-sink-tests)


# YSB Test source to out
add_executable(ysb_test_source_out "UnitTests/YSBSourceToOutTest.cpp")
target_link_libraries(ysb_test_source_out iotdb ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME ysb_test_source_out COMMAND ysb_test_source_out)

# YSB Test source to sink
add_executable(ysb_test_source_sink "UnitTests/YSB_SourceToSink.cpp")
target_link_libraries(ysb_test_source_sink iotdb ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME ysb_test_source_sink COMMAND ysb_test_source_sink)


