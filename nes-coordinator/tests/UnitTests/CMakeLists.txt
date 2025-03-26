# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#-------------------------------##############----------------------------------
#------------------------------- # Unit Test Suite --------------------------------

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
add_nes_unit_test(nes-coordinator-test "Components/NesCoordinatorTest.cpp")

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

### Timer Tests ###
#add_nes_unit_test(timer-test "Util/TimerTest.cpp")

### Location Tests ###
add_nes_unit_test(location-unit-tests "Spatial/DataTypes/LocationUnitTest.cpp")

### StatisticRegistry Test ###
add_nes_unit_test(statistic-registry-unit-test "StatisticCollection/StatisticRegistryTest.cpp")

### DefaultStatisticQueryGeneratorTest ###
add_nes_unit_test(default-statistic-query-generator-test "StatisticCollection/DefaultStatisticQueryGeneratorTest.cpp")

### DefaultStatisticStoreTest ###
add_nes_unit_test(default-statistic-store-test "StatisticCollection/DefaultStatisticStoreTest.cpp")

### StatisticIdsExtractorTest ###
add_nes_unit_test(statistic-ids-extractor-test "StatisticCollection/StatisticIdsExtractorTest.cpp")

### Async Task Executor ###
add_nes_unit_test(async-task-executor-ut "Runtime/AsyncTaskExecutorTest.cpp")

### Syntactic query validation ###
add_nes_unit_test(syntactic-query-validation-ut "QueryValidation/SyntacticQueryValidationTest.cpp")

### Location Provider CSV Tests ###
add_nes_unit_test(location-provider-csv-unit-tests "Spatial/LocationProviderCSVTest.cpp")

### Reconnect Schedule Predictor Test ###
add_nes_unit_test(reconnect-schedule-predictor-tests "Spatial/ReconnectSchedulePredictorTest.cpp")

### Serial Storage Handler Test ###
add_nes_unit_test(serial-storage-handler-tests "RequestProcessor/StorageHandles/SerialStorageHandlerTest.cpp")

### 2 Phase Lcoking Storage Handler Test ###
add_nes_unit_test(two-phase-locking-storage-handler-tests "RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandlerTest.cpp")

### Unlock Deleter Test ###
add_nes_unit_test(unlock-deleter-tests "RequestProcessor/StorageHandles/UnlockDeleterTest.cpp")

### Update Source Catalog Request Test ###
add_nes_unit_test(update-source-catalog-request-test "RequestProcessor/RequestTypes/SourceCatalog/UpdateSourceCatalogRequestTest.cpp")

### Get Source Information Request Test ###
add_nes_unit_test(get-source-information-request-test "RequestProcessor/RequestTypes/SourceCatalog/GetSourceCatalogRequestTest.cpp")

### Add Query Request Test ###
add_nes_unit_test(add-query-request-test "RequestProcessor/RequestTypes/AddQueryRequestTest.cpp")

### Stop Query Request Test ###
add_nes_unit_test(stop-query-request-test "RequestProcessor/RequestTypes/StopQueryRequestTest.cpp")

### Abstract Request Test ###
add_nes_unit_test(abstract-request-test "RequestProcessor/RequestTypes/AbstractRequestTest.cpp")

### Abstract MultiRequest Test ###
add_nes_unit_test(abstract-multi-request-test "RequestProcessor/RequestTypes/AbstractMultiRequestTest.cpp")

### Async Request Processor Test ###
add_nes_unit_test(async-request-processor-test "RequestProcessor/AsyncRequestProcessorTest.cpp")

### Request Execution Exception Test ###
add_nes_unit_test(request-execution-exception-test "Exceptions/RequestExecutionExceptionTest.cpp")

### ISQP Query Request Test ###
add_nes_unit_test(isqp-request-test "RequestProcessor/RequestTypes/ISQP/ISQPRequestTest.cpp")

### gRPC Tests
add_nes_unit_test(grpc-test "gRPC/grpcTests.cpp")
add_nes_unit_test(coordinator-rpc-server-test "gRPC/CoordinatorRpcServerTest.cpp")

### Sensor Bus Tests ###
#add_nes_unit_test(sensor-bus-tests "SensorBusTest.cpp")
#
#

### QueryCatalog Tests ###
add_nes_unit_test(query-catalog-tests "Services/QueryCatalogTest.cpp")

### LocationService Tests ###
add_nes_unit_test(location-service-tests "Services/LocationServiceTest.cpp")

### Placement Amendment Handler Tests ###
add_nes_unit_test(placement-amendment-handler-tests "Services/PlacementAmendment/PlacementAmendmentHandlerTest.cpp")

### LogicalExpressionTest ###
add_nes_unit_test(logical-expression-test "Query/LogicalExpressionTest.cpp")

### TupleBufferTest ###
add_nes_unit_test(tuple-buffer-test "Runtime/TupleBufferTest.cpp")

if (CMAKE_USE_PAPI_PROFILER)
    add_nes_unit_test(papi-profiler-test "Runtime/PAPIProfilerTest.cpp")
endif ()

### LogicalOperatorTest ###
add_nes_unit_test(logical-operator-node-tests
        "Operators/LogicalOperatorTest.cpp")

### MapJavaUDFLogicalOperatorTest ###
add_nes_unit_test(mapjavaudf-logical-operator-node-tests
        "Operators/MapJavaUDFLogicalOperatorTest.cpp")

# ExpressionNodeTest
add_nes_unit_test(expression-node-test "Operators/ExpressionNodeTest.cpp")

# OperatorPropertiesTest
add_nes_unit_test(operator-properties-tests "Operators/OperatorPropertiesTest.cpp")

# ConvertLogicalToPhysicalSinkTest
add_nes_unit_test(convert-logical-to-physical-sink-tests "Phases/ConvertLogicalToPhysicalSinkTest.cpp")

# ConvertLogicalToPhysicalSourceTest
add_nes_unit_test(convert-logical-to-physical-source-tests "Phases/ConvertLogicalToPhysicalSourceTest.cpp")

# SerializationUtilTest
add_nes_unit_test(serialize-operator-test "Serialization/SerializationUtilTest.cpp")
# add_nes_unit_test(serialize-operator-test "Serialization/SerializationUtilTest.cpp" "Serialization/TimeWindowSerializationTest.cpp")

### SourceCatalogService Tests ###
add_nes_unit_test(source-catalog-request-tests "Services/SourceCatalogRequestTest.cpp")

### TopologyManagerService Tests ###
add_nes_unit_test(topology-manager-service-tests "Services/TopologyManagerServiceTest.cpp")

### Pattern Query Plan Tests ###
add_nes_unit_test(pattern-query-plan-tests "Services/PatternParsingServiceTest.cpp")

### Lineage Manager Test ###
add_nes_unit_test(lineage-manager-tests "Runtime/LineageManagerTest.cpp")

### Buffer Manager Test ###
add_nes_unit_test(buffer-manager-tests "Runtime/BufferManagerTest.cpp")

### Buffer Storage Test ###
add_nes_unit_test(buffer-storage-tests "Runtime/BufferStorageTest.cpp")

### Sink Tests ###
add_nes_unit_test(sink-tests "Sink/SinkTest.cpp" Sink/SinkTest.cpp)

### Statistic Sink Tests ###
add_nes_unit_test(statistic-sink-tests "Sink/StatisticSinkTest.cpp" Sink/StatisticSinkTest.cpp)

### Topology Node Relocation Request Tests ###
add_nes_unit_test(topology-node-relocation-request-tests "RequestProcessor/RequestTypes/TopologyNodeRelocationRequestTest.cpp" RequestProcessor/RequestTypes/TopologyNodeRelocationRequestTest.cpp)

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

add_subdirectory(Source)