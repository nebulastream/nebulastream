

add_executable(runtime-map-operator-test "MapOperatorTest.cpp")
target_link_libraries(runtime-map-operator-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-map-operator-test COMMAND runtime-map-operator-test)


add_executable(runtime-selection-operator-test "SelectionOperatorTest.cpp")
target_link_libraries(runtime-selection-operator-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-selection-operator-test COMMAND runtime-selection-operator-test)