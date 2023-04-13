
add_nes_unit_test(filter-query-execution-test "FilterQueryExecutionTest.cpp")

add_nes_unit_test(projection-query-execution-test "ProjectionQueryExecutionTest.cpp")

add_nes_unit_test(merge-query-execution-test "MergeQueryExecutionTest.cpp")

add_nes_unit_test(map-query-execution-test "MapQueryExecutionTest.cpp")

add_nes_unit_test(map-java-udf-query-execution-test "MapJavaUdfExecutionTest.cpp")

add_nes_unit_test(flatmap-java-udf-query-execution-test "FlatMapJavaUdfExecutionTest.cpp")

add_nes_unit_test(stream-join-execution-test "StreamJoinExecutionTest.cpp")

add_subdirectory(Windowing)