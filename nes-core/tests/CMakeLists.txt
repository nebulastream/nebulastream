# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Google Testing Framework ----------------------------------------------------
include(ExternalProject)

# Copy Test Data --------------------------------------------------------------
file(COPY test_data DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
add_compile_definitions(TEST_DATA_DIRECTORY="${CMAKE_CURRENT_BINARY_DIR}/test_data/")
message(STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}/test_data")

#-------------------------------##############----------------------------------
#------------------------------- # Nes Utils for Test Suite --------------------------------

include_directories(include)
add_library(nes-core-test-util SHARED "src/Util/DummySink.cpp"
        "src/Util/TestHarness/TestHarness.cpp"
        "src/Util/TestHarness/TestHarnessWorkerConfiguration.cpp"
        "src/Util/TestUtils.cpp"
        "src/Util/TestPhaseProvider.cpp"
        "src/Util/NonRunnableDataSource.cpp"
        "src/Util/TestExecutionEngine.cpp"
        "src/Util/TestSourceProvider.cpp"
        "src/Util/TestSourceDescriptor.cpp"
        "src/Util/TestSinkProvider.cpp"
        "src/Util/TestSinkDescriptor.cpp"
        "src/Util/MetricValidator.cpp"
        "src/Util/JavaUdfDescriptorBuilder.cpp"
        "src/Util/TestSink.cpp"
        "src/Util/PythonUDFDescriptorBuilder.cpp")
target_include_directories(nes-core-test-util PUBLIC "./include")

message(STATUS "Test CMAKE_CURRENT_SOURCE_DIR Directory: ${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(nes-core-test-util PUBLIC nes nes-test-util)

if (CODE_COVERAGE)
    target_code_coverage(nes AUTO)
endif ()

#-------------------------------##############----------------------------------
#------------------------------- # Unit Test Suite --------------------------------
add_nes_unit_test(udf-test "UnitTests/Catalogs/UDFCatalogTest.cpp" "UnitTests/Catalogs/JavaUDFDescriptorTest.cpp" "UnitTests/Catalogs/PythonUDFDescriptorTest.cpp")


add_nes_unit_test(hashmap-test "UnitTests/Util/HashMapTest.cpp")


### Node Engine Tests ###
add_nes_integration_test(node-engine-test "UnitTests/Runtime/NodeEngineTest.cpp")


add_nes_unit_test(watermark-manager-test "UnitTests/Runtime/WatermarkManagerTest.cpp")


add_nes_unit_test(lock-free-multi-origin-watermark-processor-test "UnitTests/Windowing/Experimental/LockFreeMultiOriginWatermarkProcessorTest.cpp")


add_nes_unit_test(keyed-thread-local-slice-store-test "UnitTests/Windowing/Experimental/TimeBasedWindow/KeyedThreadLocalSliceStoreTest.cpp")


add_nes_unit_test(window-assigner-test "UnitTests/Windowing/Experimental/WindowAssignerTest.cpp")


### Code Generation Tests ###
add_nes_unit_test(code-generation-tests "UnitTests/CodeGenerator/CodeGenerationTest.cpp")


### Translate Physical Operator Tests ###
add_nes_unit_test(translate-physical-operator-tests "UnitTests/QueryCompiler/LowerLogicalToPhysicalOperatorsTest.cpp")


### AddScanAndEmitPhase Test ###
add_nes_unit_test(add-scan-and-emit-phase-tests "UnitTests/QueryCompiler/AddScanAndEmitPhaseTest.cpp")


### ArrayType Test ###
add_nes_unit_test(array-type-tests "UnitTests/QueryCompiler/ArrayTypeTest.cpp")


### DataTypeFactory Test ###
add_nes_unit_test(data-type-factory-tests "UnitTests/QueryCompiler/DataTypeFactoryTests.cpp")


### Pipelining Tests ###
add_nes_unit_test(pipelining-phase-tests "UnitTests/QueryCompiler/PipeliningPhaseTest.cpp")


### Operator Code Generation Tests ###
add_nes_unit_test(operator-code-generation-tests "UnitTests/CodeGenerator/OperatorCodeGenerationTest.cpp")


### Query Compiler Tests ###
add_nes_unit_test(query-compiler-tests "UnitTests/QueryCompiler/QueryCompilerTest.cpp")


### NES Topology Tests ###
add_nes_unit_test(topology-tests "UnitTests/Topology/TopologyTest.cpp")

#-------------------------------##############----------------------------------
#------------------------------- # Integration Test Suite --------------------------------
### NES Connectivity Controller Tests ###
add_nes_integration_test(connectivity-controller-integration-tests "Integration/REST/Controller/ConnectivityControllerTest.cpp")

### NES Query Catalog Controller Test ###
add_nes_integration_test(query-catalog-controller-integration-tests "Integration/REST/Controller/QueryCatalogControllerTest.cpp")

### NES Monitoring Controller Test ###
add_nes_integration_test(monitoring-controller-tests "Integration/REST/Controller/MonitoringControllerTest.cpp")

### NES Topology Controller Test ###
add_nes_integration_test(topology-controller-integration-tests "Integration/REST/Controller/TopologyControllerTest.cpp")

### NES Udf Catalog Controller Test ###
add_nes_integration_test(udf-catalog-controller-integration-tests "Integration/REST/Controller/UDFCatalogControllerTest.cpp")

### NES Query Controller Test ###
add_nes_integration_test(query-controller-integration-tests "Integration/REST/Controller/QueryControllerTest.cpp")

### NES Source Catalog Controller Test ###
add_nes_integration_test(source-catalog-controller-integration-tests "Integration/REST/Controller/SourceCatalogControllerTest.cpp")

### NES Location Controller Tests ###
add_nes_integration_test(location-controller-integration-tests "Integration/REST/Controller/LocationControllerIntegrationTest.cpp")

### NES TopologyProperty Tests ###
add_nes_unit_test(topology-property-tests "UnitTests/Topology/TopologyPropertyTest.cpp")

### NES TopologyIterator Test ###
add_nes_unit_test(topology-iterator-tests "UnitTests/Topology/TopologyIterationTest.cpp")

### NES Topology Timeline Test ###
add_nes_unit_test(topology-timeline-tests "UnitTests/Topology/Prediction/TopologyTimelineTest.cpp")

### NES Topology Prediction Edge Test ###
add_nes_unit_test(topology-prediction-edge-tests "UnitTests/Topology/Prediction/EdgeTest.cpp")

### NES Topology Changelog Test ###
add_nes_unit_test(topology-changelog-tests "UnitTests/Topology/Prediction/TopologyChangelogTest.cpp")

### NES Topology Prediction Delta Test ###
add_nes_unit_test(topology-delta-tests "UnitTests/Topology/Prediction/TopologyDeltaTest.cpp")

### Lineage Manager Test ###
add_nes_unit_test(lineage-manager-tests "UnitTests/Runtime/LineageManagerTest.cpp")

### Buffer Manager Test ###
add_nes_unit_test(buffer-manager-tests "UnitTests/Runtime/BufferManagerTest.cpp")


### Buffer Storage Test ###
add_nes_unit_test(buffer-storage-tests "UnitTests/Runtime/BufferStorageTest.cpp")


### AbstractWindowHandler Manager Test ###
add_nes_unit_test(window-manager-tests "UnitTests/Windowing/WindowManagerTest.cpp")


### State Manager Test ###
add_nes_unit_test(state-manager-tests "UnitTests/State/StateTest.cpp")


### ZMQ Data Source Sink Tests ###
add_nes_unit_test(zmq-tests "UnitTests/Source/ZMQTest.cpp")

### Source Tests ###
add_nes_integration_test(source-tests "UnitTests/Source/SourceTest.cpp")

### Sink Tests ###
add_nes_unit_test(sink-tests "UnitTests/Sink/SinkTest.cpp" UnitTests/Sink/SinkTest.cpp)

### Kafka Tests ###
if (NES_USE_KAFKA)
    add_nes_unit_test(kafka-source-tests "UnitTests/Source/KafkaSourceTest.cpp")
    add_nes_unit_test(kafka-sink-tests "UnitTests/Sink/KafkaSinkTest.cpp")
endif (NES_USE_KAFKA)

### OPC Tests ###
if (NES_USE_OPC)
    add_nes_unit_test(opc-source-tests "UnitTests/Source/OPCSourceTest.cpp" UnitTests/Source/OPCSourceTest.cpp)
    add_nes_unit_test(opc-sink-tests "UnitTests/Sink/OPCSinkTest.cpp" UnitTests/Sink/OPCSinkTest.cpp)
endif (NES_USE_OPC)

### MQTT Tests ###
if (NES_USE_MQTT)
    add_nes_unit_test(mqtt-source-tests "UnitTests/Source/MQTTSourceTest.cpp" UnitTests/Source/MQTTSourceTest.cpp)
    add_nes_unit_test(mqtt-sink-tests "UnitTests/Sink/MQTTSinkTest.cpp")
endif (NES_USE_MQTT)


### Row Layout Tests ###
add_nes_unit_test(row-memory-layout-tests "UnitTests/Runtime/MemoryLayouts/RowMemoryLayoutTest.cpp")


### Row Layout Tests ###
add_nes_unit_test(columnar-memory-layout-tests "UnitTests/Runtime/MemoryLayouts/ColumnarMemoryLayoutTest.cpp")


### Dynamic Memory Layout Tests ###
add_nes_unit_test(dynamic-memory-layout-tests "UnitTests/Runtime/MemoryLayouts/DynamicMemoryLayoutTest.cpp")


### Source Catalog Tests ###
add_nes_unit_test(source-catalog-test "UnitTests/Catalogs/SourceCatalogTest.cpp")


### QueryCatalog Tests ###
add_nes_unit_test(query-catalog-service-tests "UnitTests/Services/QueryCatalogServiceTest.cpp")


### LocationService Tests ###
add_nes_unit_test(location-service-tests "UnitTests/Services/LocationServiceTest.cpp")


### LogicalExpressionTest ###
add_nes_unit_test(logical-expression-test "UnitTests/Query/LogicalExpressionTest.cpp")


### QueryExecutionTest ###
add_nes_unit_test(legacy-query-execution-test "UnitTests/Query/QueryExecutionTest.cpp")


### GPUPipelineQueryExecutionTest ###
if (NES_USE_GPU)
    add_nes_unit_test(gpu-pipeline-query-execution-test "UnitTests/Query/GPUPipelineQueryExecutionTest.cpp")
endif (NES_USE_GPU)

### TupleBufferTest ###
add_nes_unit_test(tuple-buffer-test "UnitTests/Runtime/TupleBufferTest.cpp")


if (CMAKE_USE_PAPI_PROFILER)
    add_nes_unit_test(papi-profiler-test "UnitTests/Runtime/PAPIProfilerTest.cpp")
endif ()

### LogicalOperatorNodeTest ###
add_nes_unit_test(logical-operator-node-tests
        "UnitTests/Operators/LogicalOperatorNodeTest.cpp")

### MapJavaUDFLogicalOperatorNodeTest ###
add_nes_unit_test(mapjavaudf-logical-operator-node-tests
        "UnitTests/Operators/MapJavaUDFLogicalOperatorNodeTest.cpp")


# ExpressionNodeTest
add_nes_unit_test(expression-node-test "UnitTests/Operators/ExpressionNodeTest.cpp")


# OperatorPropertiesTest
add_nes_unit_test(operator-properties-tests "UnitTests/Operators/OperatorPropertiesTest.cpp")


# QueryTest
add_nes_unit_test(query-tests "UnitTests/API/QueryAPITest.cpp")


# SpatialQueryTest
add_nes_unit_test(spatial-query-tests "UnitTests/API/SpatialQueryTest.cpp")

# SpatialExpressionNodeTest
add_nes_unit_test(spatial-expression-node-test "UnitTests/Operators/SpatialExpressionNodeTest.cpp")

# GeographyExpressionNodeTest
add_nes_unit_test(geography-expression-node-test "UnitTests/Operators/GeographyExpressionNodeTest.cpp")

### Spatial Datatype Tests ###
add_nes_unit_test(circle-datatype-tests "UnitTests/Spatial/DataTypes/CircleTest.cpp")
add_nes_unit_test(point-datatype-tests "UnitTests/Spatial/DataTypes/PointTest.cpp")
add_nes_unit_test(polygon-datatype-tests "UnitTests/Spatial/DataTypes/PolygonTest.cpp")
add_nes_unit_test(rectangle-datatype-tests "UnitTests/Spatial/DataTypes/RectangleTest.cpp")


# QueryPlanIteratorTest
add_nes_unit_test(queryplan-util-iterator-test "UnitTests/Plans/Util/QueryPlanIteratorTest.cpp")


# ConvertLogicalToPhysicalSinkTest
add_nes_unit_test(convert-logical-to-physical-sink-tests "UnitTests/Phases/ConvertLogicalToPhysicalSinkTest.cpp")


# ConvertLogicalToPhysicalSourceTest
add_nes_unit_test(convert-logical-to-physical-source-tests "UnitTests/Phases/ConvertLogicalToPhysicalSourceTest.cpp")


# Type inference test
add_nes_unit_test(type-inference-tests "UnitTests/Optimizer/Phases/TypeInferencePhaseTest.cpp")


# FilterMergeTest
add_nes_unit_test(filter-merge-rule-test "UnitTests/Optimizer/QueryRewrite/FilterMergeRuleTest.cpp")


# FilterPushDownTest
add_nes_unit_test(filter-push-down-rule-test "UnitTests/Optimizer/QueryRewrite/FilterPushDownRuleTest.cpp")


# PredicateReorderingTest
add_nes_unit_test(predicate-reordering-rule-test "UnitTests/Optimizer/QueryRewrite/PredicateReorderingRuleTest.cpp")


# FilterSplitUpTest
add_nes_unit_test(filter-split-up-rule-test "UnitTests/Optimizer/QueryRewrite/FilterSplitUpRuleTest.cpp")


# DistributeWindowRuleTest
add_nes_unit_test(distribute-window-rule-test "UnitTests/Optimizer/QueryRewrite/DistributeWindowRuleTest.cpp")


# DistributeWindowRuleTest
add_nes_unit_test(originid-inference-phase-test "UnitTests/Optimizer/Phases/OriginIdInferencePhaseTest.cpp")


# SerializationUtilTest
add_nes_unit_test(serialize-operator-test "UnitTests/Serialization/SerializationUtilTest.cpp")


### SourceCatalogService Tests ###
add_nes_unit_test(source-catalog-service-tests "UnitTests/Services/SourceCatalogServiceTest.cpp")


### TopologyManagerService Tests ###
add_nes_unit_test(topology-manager-service-tests "UnitTests/Services/TopologyManagerServiceTest.cpp")

### Query Plan Tests ###
add_nes_unit_test(query-plan-tests "UnitTests/Plans/Query/QueryPlanTest.cpp")

### Query Plan Builder Tests ###
add_nes_unit_test(query-plan-builder-tests "UnitTests/Plans/Query/QueryPlanBuilderTest.cpp")


### Pattern Query Plan Tests ###
add_executable(pattern-query-plan-tests "UnitTests/Services/PatternParsingServiceTest.cpp")
target_link_libraries(pattern-query-plan-tests nes-core-test-util)
add_test(NAME pattern-query-plan-tests COMMAND pattern-query-plan-tests)

# SimplePatternTest
add_nes_integration_test(simple-pattern-test "Integration/SimplePatternTest.cpp")


### OrOperatorTest ###
add_nes_integration_test(or-operator-test "Integration/OrOperatorTest.cpp")


### AndOperatorTest ###
add_nes_integration_test(and-operator-test "Integration/AndOperatorTest.cpp")


### AndOperatorTest ###
add_nes_integration_test(seq-operator-test "Integration/SEQOperatorTest.cpp")


### Sensor Bus Tests ###
#add_nes_unit_test(sensor-bus-tests "UnitTests/SensorBusTest.cpp")
#
#

### Global Query Plan Tests ###
add_nes_unit_test(global-query-plan-tests "UnitTests/Plans/Global/Query/GlobalQueryPlanTest.cpp")


### Choose MemLayout Phase Tests ###
add_nes_unit_test(memory-layout-selection-phase-tests "UnitTests/Optimizer/Phases/MemoryLayoutSelectionPhaseTest.cpp")


### Query Merger Phase Tests ###
add_nes_unit_test(global-query-plan-update-phase-tests "UnitTests/Optimizer/Phases/GlobalQueryPlanUpdatePhaseTest.cpp")


### Logical Source Expansion Rule Tests ###
add_nes_unit_test(logical-source-expansion-rule-test "UnitTests/Optimizer/QueryRewrite/LogicalSourceExpansionRuleTest.cpp")

### Map UDFs Open CL Operators Rule Tests ###
add_nes_unit_test(map-udf-to-opencl-rule-test "UnitTests/Optimizer/QueryRewrite/MapUDFsToOpenCLOperatorsRuleTest.cpp")

### Syntax Based Equal Query Merger Rule Test ###
add_nes_unit_test(syntax-based-equal-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/SyntaxBasedCompleteQueryMergerRuleTest.cpp")


### Syntax Based Partial Query Merger Rule Test ###
add_nes_unit_test(syntax-based-partial-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/SyntaxBasedPartialQueryMergerRuleTest.cpp")


### Query Placement Tests ###
add_nes_unit_test(query-placement-test "UnitTests/Optimizer/QueryPlacement/QueryPlacementTest.cpp")

add_nes_unit_test(ilp-placement-test "UnitTests/Optimizer/QueryPlacement/ILPPlacementTest.cpp")

if (NES_USE_TF)
    add_nes_unit_test(ml-heuristic-placement-test "UnitTests/Optimizer/QueryPlacement/MlHeuristicPlacementTest.cpp")
endif ()

add_nes_unit_test(nemo-placement-test "UnitTests/Optimizer/QueryPlacement/NemoPlacementTest.cpp")
#add_executable(nemo-placement-test "UnitTests/Optimizer/QueryPlacement/NemoPlacementTest.cpp")
#target_link_libraries(nemo-placement-test nes-core-test-util)
#add_test(NAME nemo-placement-test COMMAND nemo-placement-test)

### Thread Naming Test ###
add_nes_unit_test(global-execution-plan-tests "UnitTests/Plans/Global/Execution/GlobalExecutionPlanTest.cpp")


### Util Function Test ###
add_nes_unit_test(util-function-tests "UnitTests/Util/UtilFunctionTest.cpp")


### Z3 Validation Test ###
add_nes_unit_test(z3-validation-tests "UnitTests/Z3ValidationTest.cpp")


### Syntactic Query Validation Test ###
add_nes_unit_test(syntactic-query-validation-test "UnitTests/Optimizer/QueryValidation/SyntacticQueryValidationTest.cpp")


### Semantic Query Validation Test ###
add_nes_unit_test(semantic-query-validation-test "UnitTests/Optimizer/QueryValidation/SemanticQueryValidationTest.cpp")
#target_link_libraries(semantic-query-validation-test nes-core-test-util)
#add_test(NAME semantic-query-validation-test COMMAND semantic-query-validation-test)

### Operator To FOL Util Test ###
add_nes_unit_test(query-signature-util-tests "UnitTests/Optimizer/QuerySignatures/QuerySignatureUtilTests.cpp")


### Z3 Signature Inference Phase Test ###
add_nes_unit_test(z3-signature-inference-phase-test "UnitTests/Optimizer/Phases/SignatureInferencePhaseTest.cpp")

### Circular Buffer Tests ###
add_nes_unit_test(circular-buffer-tests "UnitTests/Source/CircularBufferTest.cpp")

### Z3 Signature Based Equal Query Merger Rule Test ###
add_nes_unit_test(z3-signature-based-query-containment-rule-test "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedQueryContainmentRuleTest.cpp")

### Z3 Signature Based Equal Query Merger Rule Test ###
add_nes_unit_test(query-containment-identification-test "UnitTests/Optimizer/QuerySignatures/QueryContainmentIdentificationTest.cpp")

### Z3 Signature Based Equal Query Merger Rule Test ###
add_nes_unit_test(z3-signature-based-equal-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRuleTest.cpp")


### Z3 Signature Based Partial Query Merger Rule Test ###
add_nes_unit_test(z3-signature-based-partial-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRuleTest.cpp")


### Z3 Signature Based Partial Query Merger Rule Test ###
add_nes_unit_test(z3-signature-based-partial-query-merger-bottom-up-rule-test "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerBottomUpRuleTest.cpp")


### String Signature Based Complete Query Merger Rule Test ###
add_nes_unit_test(hash-signature-based-equal-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/HashSignatureBasedCompleteQueryMergerRuleTest.cpp")

### Join Handler Tests ###
add_nes_unit_test(join-handler-tests "UnitTests/JoinHandlerTest.cpp")


### Attribute Sort Rule Test ###
add_nes_unit_test(attribute-sort-rule-test "UnitTests/Optimizer/QueryRewrite/AttributeSortRuleTest.cpp")


### Rename Source To Project Operator Rule Test ###
add_nes_unit_test(rename-source-to-project-operator-rule-test "UnitTests/Optimizer/QueryRewrite/RenameSourceToProjectOperatorRuleTest.cpp")


### Project Before Union Operator Rule Test ###
add_nes_unit_test(project-before-union-operator-rule-test "UnitTests/Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRuleTest.cpp")


### Millisecond Interval Tests ###
add_nes_unit_test(millisecond-interval-tests "UnitTests/Source/MillisecondIntervalTest.cpp")


### Materialized View Tests ###
add_nes_unit_test(materialized-view-tests "Integration/MaterializedViewTest.cpp")


### Binary Operator Sort Rule Tests ###
add_nes_unit_test(binary-operator-sort-rule-tests "UnitTests/Optimizer/QueryRewrite/BinaryOperatorSortRuleTest.cpp")


### Config Tests ###
add_nes_unit_test(config-test "UnitTests/Configurations/ConfigTest.cpp")


### Timer Tests ###
#add_nes_unit_test(timer-test "UnitTests/Util/TimerTest.cpp")

### Adaptive Source Tests ###
add_nes_unit_test(adaptive-sensor-tests "UnitTests/Sensors/AdaptiveKFTest.cpp")


### Location Tests ###
add_nes_unit_test(location-unit-tests "UnitTests/Spatial/DataTypes/LocationUnitTest.cpp")

### Async Task Executor ###
add_nes_unit_test(async-task-executor-ut "UnitTests/Runtime/AsyncTaskExecutorTest.cpp")

### Location Provider CSV Tests ###
add_nes_unit_test(location-provider-csv-unit-tests "UnitTests/Spatial/LocationProviderCSVTest.cpp")

### Reconnect Schedule Predictor Test ###
add_nes_unit_test(reconnect-schedule-predictor-tests "UnitTests/Spatial/ReconnectSchedulePredictorTest.cpp")

### Serial Storage Handler Test ###
add_nes_unit_test(serial-access-handle-tests "UnitTests/RequestProcessor/StorageHandles/SerialStorageHandlerTest.cpp")

### 2 Phase Lcoking Storage Handler Test ###
add_nes_unit_test(two-phase-locking-access-handle-tests "UnitTests/RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandlerTest.cpp")

### Unlock Deleter Test ###
add_nes_unit_test(unlock-deleter-tests "UnitTests/RequestProcessor/StorageHandles/UnlockDeleterTest.cpp")

### Fail Query Request Test ###
add_nes_unit_test(fail-query-request-test "UnitTests/RequestProcessor/RequestTypes/FailQueryRequestTests.cpp")

### Add Query Request Test ###
add_nes_unit_test(add-query-request-test "UnitTests/RequestProcessor/RequestTypes/AddQueryRequestTest.cpp")

### Stop Query Request Test ###
add_nes_unit_test(stop-query-request-test "UnitTests/RequestProcessor/RequestTypes/StopQueryRequestTest.cpp")

### Abstract Request Test ###
add_nes_unit_test(abstract-request-test "UnitTests/RequestProcessor/RequestTypes/AbstractRequestTest.cpp")

### Async Request Processor Test ###
add_nes_unit_test(async-request-processor-test "UnitTests/RequestProcessor/AsyncRequestProcessorTest.cpp")

### Request Execution Exception Test ###
add_nes_unit_test(request-execution-exception-test "UnitTests/Exceptions/RequestExecutionExceptionTest.cpp")

### ChangeLog Test ###
add_nes_unit_test(change-log-test "UnitTests/Plans/ChangeLog/ChangeLogTest.cpp")

### ChangeLogEntry Test ###
add_nes_unit_test(change-log-entry-test "UnitTests/Plans/ChangeLog/ChangeLogEntryTest.cpp")

#------------------------------- # Integration Test Suite --------------------------------
### MultiWorkerTest ###
add_nes_integration_test(multi-worker-test "Integration/MultiWorkerTest.cpp")


### QueryDeploymentTest ###
add_nes_integration_test(query-deployment-test "Integration/QueryDeploymentTest.cpp")


### MQTTSinkDepleymentTest ###
add_nes_integration_test(mqtt-sink-deployment-test "Integration/MQTTSinkDeploymentTest.cpp")


### RenameTest ###
add_nes_integration_test(rename-test "Integration/RenameTest.cpp")

### JoinDeploymentTest ###
add_nes_integration_test(join-deployment-test "Integration/JoinDeploymentTest.cpp")

### windowDeploymentTest ###
add_nes_integration_test(window-deployment-test "Integration/Windowing/WindowDeploymentTest.cpp")
add_nes_integration_test(ysb-deployment-test "Integration/Windowing/YSBDeploymentTest.cpp")
add_nes_integration_test(ktm-deployment-test "Integration/Windowing/KTMDeploymentTest.cpp")


### window integration tests ###
add_nes_integration_test(keyed-tumbling-window-tests "Integration/Windowing/TumblingWindow/KeyedTumblingWindowTests.cpp")
add_nes_integration_test(non-keyed-tumbling-window-tests "Integration/Windowing/TumblingWindow/NotKeyedlTumblingWindowTests.cpp")
add_nes_integration_test(keyed-sliding-window-tests "Integration/Windowing/SlidingWindow/KeyedSlidingWindowTests.cpp")
add_nes_integration_test(non-keyed-sliding-window-tests "Integration/Windowing/SlidingWindow/NonKeyedSlidingWindowTests.cpp")

### distributed window deployment test ###
# Disable this tests currently

#add_nes_integration_test(distributed-window-deployment-test "Integration/Windowing/DistributedWindowDeploymentTest.cpp")


### MultipleWindowsTest ###
add_nes_integration_test(multi-window-test "Integration/MultipleWindowsTest.cpp")


### MultiJoinsTest ###
add_nes_integration_test(multi-join-test "Integration/MultipleJoinsTest.cpp")


### MultiThreadedTest ###
add_nes_integration_test(multi-thread-test "Integration/MultiThreadedTest.cpp")


### AssignWatermarkTest ###
add_nes_integration_test(assignment-watermark-test "Integration/Windowing/AssignWatermarkTest.cpp")


add_nes_integration_test(allowed-lateness-test "Integration/AllowedLatenessTest.cpp")


add_nes_integration_test(complex-sequence-test "Integration/ComplexSequenceTest.cpp")


### unionDeploymentTest ###
add_nes_integration_test(union-deployment-test "Integration/UnionDeploymentTest.cpp")


if (NES_USE_TF)
    ### InferModelDeploymentTest ###
    add_nes_integration_test(infer-model-deployment-test "Integration/Operators/InferModelDeploymentTest.cpp")
endif ()

if (NES_USE_ARROW)
    ### ArrowSourceIntegrationTest ###
    add_nes_integration_test(arrow-source-integration-test "Integration/Source/ArrowSourceIntegrationTest.cpp")
endif ()

### DeepHierarchyTopologyTestDeepHierarchyTopologyTest ###
add_nes_integration_test(deep-hierarchy-test "Integration/DeepHierarchyTopologyTest.cpp")


### WorkerCoordinatorStarter Tests ###
add_nes_integration_test(worker-coordinator-starter-tests "Integration/WorkerCoordinatorStarterTest.cpp")

### WorkerCoordinatorStarter Tests ###
add_nes_integration_test(update-topology-remote-test "Integration/UpdateTopologyRemoteTest.cpp")

# NetworkStackUnitTest
add_nes_integration_test(network-stack-test "UnitTests/Network/NetworkStackTest.cpp")


# NetworkStackIntegrationTest
add_nes_integration_test(network-stack-integration-test "Integration/NetworkStackIntegrationTest.cpp")


# ResourcesReaderTest
add_nes_unit_test(resources-reader-test "UnitTests/Monitoring/ResourcesReaderTest.cpp")


# MonitoringQueriesTest
add_nes_integration_test(monitoring-queries-test "UnitTests/Monitoring/MonitoringQueriesTest.cpp")


# MetricCollectorTest
add_nes_unit_test(metric-collector-test "UnitTests/Monitoring/MetricCollectorTest.cpp")


# MetricStoreTest
add_nes_unit_test(metric-store-test "UnitTests/Monitoring/MetricStoreTest.cpp")

# StatManager-Test
add_nes_unit_test(StatManager-test "UnitTests/StatManager/StatManagerTest.cpp")

# UserDefinedMonitoringTest
add_nes_unit_test(user-defined-monitoring-test "UnitTests/Monitoring/UserDefinedMonitoringTest.cpp")


# NemoIntegrationTest
add_nes_integration_test(nemo-integration-test "Integration/NemoIntegrationTest.cpp")

### StaticDataSourceIntegrationTest ###
#add_nes_integration_test(static-data-source-integration-test "Integration/StaticDataSourceIntegrationTest.cpp")

### MemorySourceIntegrationTest ###
### VariableLengthIntegrationTest ###
### BenchmarkSourceIntegrationTest ###
# TCPSourceIntegrationTest
### ContiniousSourceTest ###
# Lambda Source Integration tests
add_nes_integration_test(source-integration-tests
        "Integration/Source/MemorySourceIntegrationTest.cpp"
        "Integration/Source/VariableLengthIntegrationTest.cpp"
        "Integration/Source/BenchmarkSourceIntegrationTest.cpp"
        "Integration/Source/SourceCatalogRemoteTest.cpp"
        "Integration/Source/TCPSourceIntegrationTest.cpp"
        "Integration/Source/ContinuousSourceTest.cpp"
        "Integration/Source/LambdaSourceIntegrationTest.cpp")
set_tests_properties(source-integration-tests PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(source-integration-tests nesCoordinator nesWorker)


### TestHarnessUtilTest ###
add_nes_integration_test(test-harness-util-test "Integration/TestHarnessUtilTest.cpp")


### StringQueryTest ###
add_nes_integration_test(string-query-test "Integration/StringQueryTest.cpp")


### UpstreamBackup ###
add_nes_integration_test(upstream-backup-test "Integration/UpstreamBackupTest.cpp")

### gRPC Tests
add_nes_integration_test(grpc-test "UnitTests/gRPC/grpcTests.cpp")

### Query Failure Test
add_nes_integration_test(query-failure-test "Integration/QueryFailureTest.cpp")

### NES Coordinator Test
add_executable(nes-coordinator-test "UnitTests/Components/NesCoordinatorTest.cpp")
target_link_libraries(nes-coordinator-test nes-core-test-util)
add_test(NAME nes-coordinator-test COMMAND nes-coordinator-test)

### Location Integration Test ###
add_nes_integration_test(location-integration-tests "Integration/LocationIntegrationTests.cpp")
target_link_libraries(location-integration-tests nes-core-test-util)

add_subdirectory(UnitTests/QueryExecution)

#------------------------------- # E2E Test Suite --------------------------------
### E2E Tests ###
add_executable(e2e-coordinator-single-worker-test "E2e/E2ECoordinatorSingleWorkerTest.cpp")
target_link_libraries(e2e-coordinator-single-worker-test nes-core-test-util)
add_test(NAME e2e-coordinator-single-worker-test COMMAND e2e-coordinator-single-worker-test)
set_tests_properties(e2e-coordinator-single-worker-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(e2e-coordinator-single-worker-test nesCoordinator nesWorker)

add_executable(e2e-coordinator-multi-worker-test "E2e/E2ECoordinatorMultiWorkerTest.cpp")
target_link_libraries(e2e-coordinator-multi-worker-test nes-core-test-util)
add_test(NAME e2e-coordinator-multi-worker-test COMMAND e2e-coordinator-multi-worker-test)
set_tests_properties(e2e-coordinator-multi-worker-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(e2e-coordinator-multi-worker-test nesCoordinator nesWorker)

add_executable(e2e-coordinator-multi-query-test "E2e/E2ECoordinatorMultiQueryTest.cpp")
target_link_libraries(e2e-coordinator-multi-query-test nes-core-test-util)
add_test(NAME e2e-coordinator-multi-query-test COMMAND e2e-coordinator-multi-query-test)
set_tests_properties(e2e-coordinator-multi-query-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(e2e-coordinator-multi-query-test nesCoordinator nesWorker)

add_executable(e2e-monitoring-test "E2e/E2EMonitoringTest.cpp")
target_link_libraries(e2e-monitoring-test nes-core-test-util)
add_test(NAME e2e-monitoring-test COMMAND e2e-monitoring-test)
set_tests_properties(e2e-monitoring-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(e2e-monitoring-test nesCoordinator nesWorker)

set_tests_properties(${Tests} PROPERTIES TIMEOUT 35)