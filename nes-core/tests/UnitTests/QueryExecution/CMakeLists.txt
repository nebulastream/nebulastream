
### FilterQueryExecutionTest ###
add_nes_unit_test(filter-query-execution-test "FilterQueryExecutionTest.cpp")

### MapQueryExecutionTest ###
add_nes_unit_test(map-query-execution-test "MapQueryExecutionTest.cpp")

### ThresholdWindowQueryExecutionTest ###
add_nes_unit_test(threshold-window-query-execution-test "ThresholdWindowQueryExecutionTest.cpp")

### GlobalTumblingWindowQueryExecutionTest ###
add_nes_unit_test(global-tumbling-window-query-execution-test "GlobalTumblingWindowQueryExecutionTest.cpp")

# TODO #3339 this test has to be enabled once the integration of stream join has been finalized
### StreamJoinExecutionTest ###
add_nes_unit_test(stream-join-execution-test "StreamJoinExecutionTest.cpp")