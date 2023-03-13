
### FilterQueryExecutionTest ###
add_nes_unit_test(filter-query-execution-test "FilterQueryExecutionTest.cpp")

### ProjectionQueryExecutionTest ###
add_nes_unit_test(projection-query-execution-test "ProjectionQueryExecutionTest.cpp")

### MergeQueryExecutionTest ###
add_nes_unit_test(merge-query-execution-test "MergeQueryExecutionTest.cpp")

### MapQueryExecutionTest ###
add_nes_unit_test(map-query-execution-test "MapQueryExecutionTest.cpp")

### MapQueryExecutionTest ###
add_nes_unit_test(map-java-udf-query-execution-test "MapJavaUdfExecutionTest.cpp")

### StreamJoinExecutionTest ###
add_nes_unit_test(stream-join-execution-test "StreamJoinExecutionTest.cpp")

add_subdirectory(Windowing)