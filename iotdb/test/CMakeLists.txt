#google testing framework
enable_testing(true)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(GTEST_LIBRARIES iotdb libgtest libgtest_main libgmock)

add_executable(code-generation-tests "UnitTests/CodeGen.cpp")
target_link_libraries(code-generation-tests ${GTEST_LIBRARIES} ${CMAKE_DL_LIBS})

#add_test(code-generation-tests code-generation-tests)
add_test(NAME code-generation-tests COMMAND code-generation-tests)

