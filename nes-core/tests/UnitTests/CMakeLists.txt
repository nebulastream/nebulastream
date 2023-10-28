
#-------------------------------##############----------------------------------
#------------------------------- # Unit Test Suite --------------------------------

add_subdirectory(Catalogs)
add_subdirectory(Topology)

add_nes_unit_test(hashmap-test "Util/HashMapTest.cpp")


### Node Engine Tests ###
add_nes_integration_test(node-engine-test "Runtime/NodeEngineTest.cpp")


add_nes_unit_test(watermark-manager-test "Runtime/WatermarkManagerTest.cpp")

### Translate Physical Operator Tests ###
add_nes_unit_test(translate-physical-operator-tests "QueryCompiler/LowerLogicalToPhysicalOperatorsTest.cpp")

### AddScanAndEmitPhase Test ###
add_nes_unit_test(add-scan-and-emit-phase-tests "QueryCompiler/AddScanAndEmitPhaseTest.cpp")


### ArrayType Test ###
add_nes_unit_test(array-type-tests "QueryCompiler/ArrayTypeTest.cpp")


### DataTypeFactory Test ###
add_nes_unit_test(data-type-factory-tests "QueryCompiler/DataTypeFactoryTests.cpp")


### Pipelining Tests ###
add_nes_unit_test(pipelining-phase-tests "QueryCompiler/PipeliningPhaseTest.cpp")

### NES Coordinator Test
add_executable(nes-coordinator-test "Components/NesCoordinatorTest.cpp")
target_link_libraries(nes-coordinator-test nes-core-test-util)
gtest_discover_tests(nes-coordinator-test DISCOVERY_TIMEOUT 30)

# ResourcesReaderTest
add_nes_unit_test(resources-reader-test "Monitoring/ResourcesReaderTest.cpp")

# MonitoringQueriesTest
add_nes_integration_test(monitoring-queries-test "Monitoring/MonitoringQueriesTest.cpp")

# MetricCollectorTest
add_nes_unit_test(metric-collector-test "Monitoring/MetricCollectorTest.cpp")

# MetricStoreTest
add_nes_unit_test(metric-store-test "Monitoring/MetricStoreTest.cpp")

# UserDefinedMonitoringTest
add_nes_unit_test(user-defined-monitoring-test "Monitoring/UserDefinedMonitoringTest.cpp")

# NetworkStackUnitTest
add_nes_integration_test(network-stack-test "Network/NetworkStackTest.cpp")

### Config Tests ###
add_nes_unit_test(config-test "Configurations/ConfigTest.cpp")

### Timer Tests ###
#add_nes_unit_test(timer-test "Util/TimerTest.cpp")

### Adaptive Source Tests ###
add_nes_unit_test(adaptive-sensor-tests "Sensors/AdaptiveKFTest.cpp")


### Location Tests ###
add_nes_unit_test(location-unit-tests "Spatial/DataTypes/LocationUnitTest.cpp")

### Async Task Executor ###
add_nes_unit_test(async-task-executor-ut "Runtime/AsyncTaskExecutorTest.cpp")

### Location Provider CSV Tests ###
add_nes_unit_test(location-provider-csv-unit-tests "Spatial/LocationProviderCSVTest.cpp")

### Reconnect Schedule Predictor Test ###
add_nes_unit_test(reconnect-schedule-predictor-tests "Spatial/ReconnectSchedulePredictorTest.cpp")

### Serial Storage Handler Test ###
add_nes_unit_test(serial-access-handle-tests "RequestProcessor/StorageHandles/SerialStorageHandlerTest.cpp")

### 2 Phase Lcoking Storage Handler Test ###
add_nes_unit_test(two-phase-locking-access-handle-tests "RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandlerTest.cpp")

### Unlock Deleter Test ###
add_nes_unit_test(unlock-deleter-tests "RequestProcessor/StorageHandles/UnlockDeleterTest.cpp")

### Fail Query Request Test ###
add_nes_unit_test(fail-query-request-test "RequestProcessor/RequestTypes/FailQueryRequestTests.cpp")

### Add Query Request Test ###
add_nes_unit_test(add-query-request-test "RequestProcessor/RequestTypes/AddQueryRequestTest.cpp")

### Stop Query Request Test ###
add_nes_unit_test(stop-query-request-test "RequestProcessor/RequestTypes/StopQueryRequestTest.cpp")

### Abstract Request Test ###
add_nes_unit_test(abstract-request-test "RequestProcessor/RequestTypes/AbstractRequestTest.cpp")

### Async Request Processor Test ###
add_nes_unit_test(async-request-processor-test "RequestProcessor/AsyncRequestProcessorTest.cpp")

### Request Execution Exception Test ###
add_nes_unit_test(request-execution-exception-test "Exceptions/RequestExecutionExceptionTest.cpp")

### ChangeLog Test ###
add_nes_unit_test(change-log-test "Plans/ChangeLog/ChangeLogTest.cpp")

### ChangeLogEntry Test ###
add_nes_unit_test(change-log-entry-test "Plans/ChangeLog/ChangeLogEntryTest.cpp")

### gRPC Tests
add_nes_unit_test(grpc-test "gRPC/grpcTests.cpp")

### Sensor Bus Tests ###
#add_nes_unit_test(sensor-bus-tests "SensorBusTest.cpp")
#
#

### Global Query Plan Tests ###
add_nes_unit_test(global-query-plan-tests "Plans/Global/Query/GlobalQueryPlanTest.cpp")

### Thread Naming Test ###
add_nes_unit_test(global-execution-plan-tests "Plans/Global/Execution/GlobalExecutionPlanTest.cpp")

### Z3 Validation Test ###
add_nes_unit_test(z3-validation-tests "Z3ValidationTest.cpp")


### Row Layout Tests ###
add_nes_unit_test(row-memory-layout-tests "Runtime/MemoryLayouts/RowMemoryLayoutTest.cpp")


### Row Layout Tests ###
add_nes_unit_test(columnar-memory-layout-tests "Runtime/MemoryLayouts/ColumnarMemoryLayoutTest.cpp")

### Dynamic Memory Layout Tests ###
add_nes_unit_test(dynamic-memory-layout-tests "Runtime/MemoryLayouts/DynamicMemoryLayoutTest.cpp")

### QueryCatalog Tests ###
add_nes_unit_test(query-catalog-service-tests "Services/QueryCatalogServiceTest.cpp")


### LocationService Tests ###
add_nes_unit_test(location-service-tests "Services/LocationServiceTest.cpp")


### LogicalExpressionTest ###
add_nes_unit_test(logical-expression-test "Query/LogicalExpressionTest.cpp")

### TupleBufferTest ###
add_nes_unit_test(tuple-buffer-test "Runtime/TupleBufferTest.cpp")


if (CMAKE_USE_PAPI_PROFILER)
    add_nes_unit_test(papi-profiler-test "Runtime/PAPIProfilerTest.cpp")
endif ()

### LogicalOperatorNodeTest ###
add_nes_unit_test(logical-operator-node-tests
        "Operators/LogicalOperatorNodeTest.cpp")

### MapJavaUDFLogicalOperatorNodeTest ###
add_nes_unit_test(mapjavaudf-logical-operator-node-tests
        "Operators/MapJavaUDFLogicalOperatorNodeTest.cpp")


# ExpressionNodeTest
add_nes_unit_test(expression-node-test "Operators/ExpressionNodeTest.cpp")


# OperatorPropertiesTest
add_nes_unit_test(operator-properties-tests "Operators/OperatorPropertiesTest.cpp")


# QueryTest
add_nes_unit_test(query-tests "API/QueryAPITest.cpp")


# SpatialQueryTest
add_nes_unit_test(spatial-query-tests "API/SpatialQueryTest.cpp")

# SpatialExpressionNodeTest
add_nes_unit_test(spatial-expression-node-test "Operators/SpatialExpressionNodeTest.cpp")

# GeographyExpressionNodeTest
add_nes_unit_test(geography-expression-node-test "Operators/GeographyExpressionNodeTest.cpp")

### Spatial Datatype Tests ###
add_nes_unit_test(circle-datatype-tests "Spatial/DataTypes/CircleTest.cpp")
add_nes_unit_test(point-datatype-tests "Spatial/DataTypes/PointTest.cpp")
add_nes_unit_test(polygon-datatype-tests "Spatial/DataTypes/PolygonTest.cpp")
add_nes_unit_test(rectangle-datatype-tests "Spatial/DataTypes/RectangleTest.cpp")


# QueryPlanIteratorTest
add_nes_unit_test(queryplan-util-iterator-test "Plans/Util/QueryPlanIteratorTest.cpp")


# ConvertLogicalToPhysicalSinkTest
add_nes_unit_test(convert-logical-to-physical-sink-tests "Phases/ConvertLogicalToPhysicalSinkTest.cpp")


# ConvertLogicalToPhysicalSourceTest
add_nes_unit_test(convert-logical-to-physical-source-tests "Phases/ConvertLogicalToPhysicalSourceTest.cpp")



# SerializationUtilTest
add_nes_unit_test(serialize-operator-test "Serialization/SerializationUtilTest.cpp")


### SourceCatalogService Tests ###
add_nes_unit_test(source-catalog-service-tests "Services/SourceCatalogServiceTest.cpp")


### TopologyManagerService Tests ###
add_nes_unit_test(topology-manager-service-tests "Services/TopologyManagerServiceTest.cpp")

### Query Plan Tests ###
add_nes_unit_test(query-plan-tests "Plans/Query/QueryPlanTest.cpp")

### Query Plan Builder Tests ###
add_nes_unit_test(query-plan-builder-tests "Plans/Query/QueryPlanBuilderTest.cpp")


### Pattern Query Plan Tests ###
add_executable(pattern-query-plan-tests "Services/PatternParsingServiceTest.cpp")
target_link_libraries(pattern-query-plan-tests nes-core-test-util)
gtest_discover_tests(pattern-query-plan-tests DISCOVERY_TIMEOUT 30)

### SQL Query Plan Tests ###
add_executable(sql-query-plan-tests "Services/SQLParsingServiceTest.cpp")
target_link_libraries(sql-query-plan-tests nes-core-test-util)
gtest_discover_tests(sql-query-plan-tests DISCOVERY_TIMEOUT 30)

### Lineage Manager Test ###
add_nes_unit_test(lineage-manager-tests "Runtime/LineageManagerTest.cpp")

### Buffer Manager Test ###
add_nes_unit_test(buffer-manager-tests "Runtime/BufferManagerTest.cpp")


### Buffer Storage Test ###
add_nes_unit_test(buffer-storage-tests "Runtime/BufferStorageTest.cpp")

### Sink Tests ###
add_nes_unit_test(sink-tests "Sink/SinkTest.cpp" Sink/SinkTest.cpp)

### Kafka Tests ###
if (NES_USE_KAFKA)
    add_nes_unit_test(kafka-sink-tests "Sink/KafkaSinkTest.cpp")
endif (NES_USE_KAFKA)

### OPC Tests ###
if (NES_USE_OPC)
    add_nes_unit_test(opc-sink-tests "Sink/OPCSinkTest.cpp" Sink/OPCSinkTest.cpp)
endif (NES_USE_OPC)

### MQTT Tests ###
if (NES_USE_MQTT)
    add_nes_unit_test(mqtt-sink-tests "Sink/MQTTSinkTest.cpp")
endif (NES_USE_MQTT)