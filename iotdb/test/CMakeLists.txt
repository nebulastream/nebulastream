#google testing framework
enable_testing(true)
set(GTEST_LIBRARIES iotdb libgtest libgtest_main libgmock)

# Code Generation
add_executable(code-generation-tests "UnitTests/CodeGen.cpp")
target_link_libraries(code-generation-tests ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME code-generation-tests COMMAND code-generation-tests)

# Topology
add_executable(fog-topology-tests "UnitTests/Topology.cpp")
target_link_libraries(fog-topology-tests ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})
add_test(NAME fog-topology-tests COMMAND fog-topology-tests)

