
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

if (CMAKE_CROSSCOMPILING)
    set(CMAKE_ARGS_CROSS -DCMAKE_BUILD_TYPE=Release
            -DCMAKE_TOOLCHAIN_FILE=/opt/toolchain/toolchain-aarch64-llvm.cmake
            -DBoost_NO_SYSTEM_PATHS=TRUE
            -DBoost_INCLUDE_DIR=/opt/sysroots/aarch64-linux-gnu/usr/include/
            -DBoost_LIBRARY_DIR=/opt/sysroots/aarch64-linux-gnu/usr/lib/
            )
else ()
    set(CMAKE_ARGS_CROSS "")
endif ()

# Copy Test Data --------------------------------------------------------------
file(COPY test_data DESTINATION .)
add_compile_definitions(TEST_DATA_DIRECTORY="${CMAKE_CURRENT_BINARY_DIR}/test_data/")
message(STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}/test_data")

#-------------------------------##############----------------------------------
#------------------------------- # Nes Utils for Test Suite --------------------------------

add_library(nes-tests INTERFACE)
target_include_directories(nes-tests INTERFACE ${GTEST_INCLUDE_DIRS} "../include")
target_include_directories(nes-tests INTERFACE "${CMAKE_BINARY_DIR}")
message(STATUS "Test CMAKE_CURRENT_SOURCE_DIR Directory: ${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(nes-tests INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/util/")
target_link_libraries(nes-tests INTERFACE ${GTEST_LIBRARIES} nes nes-tests-util)


#-------------------------------##############----------------------------------
#------------------------------- # Unit Test Suite --------------------------------


### OutputQueueTest ###
add_executable(fault-tolerance-test "UnitTests/Optimizer/QueryPlacement/FaultToleranceTest.cpp" UnitTests/Optimizer/QueryPlacement/FinalTests.cpp)
target_link_libraries(fault-tolerance-test nes-tests)
add_test(NAME fault-tolerance-test COMMAND fault-tolerance-test)



### Query Placement Test Sandbox ###
add_executable(query-placement-test-sandbox "UnitTests/Optimizer/QueryPlacement/QueryPlacementTestSandbox.cpp")
target_link_libraries(query-placement-test-sandbox nes-tests)
add_test(NAME query-placement-test-sandbox COMMAND query-placement-test-sandbox)

add_executable(udf-catalog-controller-test "UnitTests/REST/Controller/UdfCatalogControllerTest.cpp")
target_link_libraries(udf-catalog-controller-test nes-tests)
add_test(NAME udf-catalog-controller-test COMMAND udf-catalog-controller-test)

add_executable(udf-test "UnitTests/Catalogs/UdfCatalogTest.cpp" "UnitTests/Catalogs/JavaUdfDescriptorTest.cpp" "UnitTests/Catalogs/PythonUdfDescriptorTest.cpp")
target_link_libraries(udf-test nes-tests)
add_test(NAME udf-test COMMAND udf-test)

add_executable(hashmap-test "UnitTests/Util/HashMapTest.cpp")
target_link_libraries(hashmap-test nes nes-tests-util ${GTEST_LIBRARIES})
add_test(NAME hashmap-test COMMAND hashmap-test)

### Node Engine Tests ###
add_executable(node-engine-test "UnitTests/Runtime/NodeEngineTest.cpp")
target_link_libraries(node-engine-test nes-tests)
add_test(NAME node-engine-test COMMAND node-engine-test)

add_executable(watermark-manager-test "UnitTests/Runtime/WatermarkManagerTest.cpp")
target_link_libraries(watermark-manager-test nes-tests)
add_test(NAME watermark-manager-test COMMAND watermark-manager-test)

add_executable(lock-free-multi-origin-watermark-processor-test "UnitTests/Windowing/Experimental/LockFreeMultiOriginWatermarkProcessorTest.cpp")
target_link_libraries(lock-free-multi-origin-watermark-processor-test nes nes-tests-util ${GTEST_LIBRARIES})
add_test(NAME lock-free-multi-origin-watermark-processor-test COMMAND lock-free-multi-origin-watermark-processor-test)

add_executable(keyed-thread-local-slice-store-test "UnitTests/Windowing/Experimental/TimeBasedWindow/KeyedThreadLocalSliceStoreTest.cpp")
target_link_libraries(keyed-thread-local-slice-store-test nes nes-tests-util ${GTEST_LIBRARIES})
add_test(NAME keyed-thread-local-slice-store-test COMMAND keyed-thread-local-slice-store-test)

add_executable(window-assigner-test "UnitTests/Windowing/Experimental/WindowAssignerTest.cpp")
target_link_libraries(window-assigner-test nes nes-tests-util ${GTEST_LIBRARIES})
add_test(NAME window-assigner-test COMMAND window-assigner-test)

### Code Generation Tests ###
add_executable(code-generation-tests "UnitTests/CodeGenerator/CodeGenerationTest.cpp")
target_link_libraries(code-generation-tests nes-tests)
add_test(NAME code-generation-tests COMMAND code-generation-tests)

### Translate Physical Operator Tests ###
add_executable(translate-physical-operator-tests "UnitTests/QueryCompiler/LowerLogicalToPhysicalOperatorsTest.cpp")
target_link_libraries(translate-physical-operator-tests nes-tests)
add_test(NAME translate-physical-operator-tests COMMAND translate-physical-operator-tests)

### AddScanAndEmitPhase Test ###
add_executable(add-scan-and-emit-phase-tests "UnitTests/QueryCompiler/AddScanAndEmitPhaseTest.cpp")
target_link_libraries(add-scan-and-emit-phase-tests nes-tests)
add_test(NAME add-scan-and-emit-phase-tests COMMAND add-scan-and-emit-phase-tests)

### ArrayType Test ###
add_executable(array-type-tests "UnitTests/QueryCompiler/ArrayTypeTest.cpp")
target_link_libraries(array-type-tests nes-tests)
add_test(NAME array-type-tests COMMAND array-type-tests)

### DataTypeFactory Test ###
add_executable(data-type-factory-tests "UnitTests/QueryCompiler/DataTypeFactoryTests.cpp")
target_link_libraries(data-type-factory-tests nes-tests)
add_test(NAME data-type-factory-tests COMMAND data-type-factory-tests)

### Pipelining Tests ###
add_executable(pipelining-phase-tests "UnitTests/QueryCompiler/PipeliningPhaseTest.cpp")
target_link_libraries(pipelining-phase-tests nes-tests)
add_test(NAME pipelining-phase-tests COMMAND pipelining-phase-tests)

### Operator Code Generation Tests ###
add_executable(operator-code-generation-tests "UnitTests/CodeGenerator/OperatorCodeGenerationTest.cpp")
target_link_libraries(operator-code-generation-tests nes-tests)
add_test(NAME operator-code-generation-tests COMMAND operator-code-generation-tests)

### Query Compiler Tests ###
add_executable(query-compiler-tests "UnitTests/QueryCompiler/QueryCompilerTest.cpp")
target_link_libraries(query-compiler-tests nes-tests)
add_test(NAME query-compiler-tests COMMAND query-compiler-tests)

### NES Topology Tests ###
add_executable(topology-tests "UnitTests/Topology/TopologyTest.cpp")
target_link_libraries(topology-tests nes-tests)
add_test(NAME topology-tests COMMAND topology-tests)

### NES Query Controller Tests ###
add_executable(query-controller-test "UnitTests/REST/Controller/QueryControllerTest.cpp")
target_link_libraries(query-controller-test nes-tests)
add_test(NAME query-controller-test COMMAND query-controller-test)

### NES Topology Controller Tests ###
add_executable(topology-controller-tests "UnitTests/REST/Controller/TopologyControllerTest.cpp")
target_link_libraries(topology-controller-tests nes-tests)
add_test(NAME topology-controller-tests COMMAND topology-controller-tests)

### NES Connectivity Controller Tests ###
add_executable(connectivity-controller-tests "UnitTests/REST/Controller/ConnectivityControllerTest.cpp")
target_link_libraries(connectivity-controller-tests nes-tests)
add_test(NAME connectivity-controller-tests COMMAND connectivity-controller-tests)

### NES Location Controller Tests ###
add_executable(location-controller-test "UnitTests/REST/Controller/LocationControllerTest.cpp")
target_link_libraries(location-controller-test nes-tests)
add_test(NAME location-controller-test COMMAND location-controller-test)

### NES TopologyProperty Tests ###
add_executable(topology-property-tests "UnitTests/Topology/TopologyPropertyTest.cpp")
target_link_libraries(topology-property-tests nes-tests)
add_test(NAME topology-property-tests COMMAND topology-property-tests)

### NES TopologyIterator Test ###
add_executable(topology-iterator-tests "UnitTests/Topology/TopologyIterationTest.cpp")
target_link_libraries(topology-iterator-tests nes-tests)
add_test(NAME topology-iterator-tests COMMAND topology-iterator-tests)


### Lineage Manager Test ###
add_executable(lineage-manager-tests "UnitTests/Runtime/LineageManagerTest.cpp")
target_link_libraries(lineage-manager-tests nes-tests)
add_test(NAME lineage-manager-tests COMMAND lineage-manager-tests)

### Buffer Manager Test ###
add_executable(buffer-manager-tests "UnitTests/Runtime/BufferManagerTest.cpp")
target_link_libraries(buffer-manager-tests nes-tests)
add_test(NAME buffer-manager-tests COMMAND buffer-manager-tests)

### Buffer Storage Test ###
add_executable(buffer-storage-tests "UnitTests/Runtime/BufferStorageTest.cpp")
target_link_libraries(buffer-storage-tests nes-tests)
add_test(NAME buffer-storage-tests COMMAND buffer-storage-tests)

### AbstractWindowHandler Manager Test ###
add_executable(window-manager-tests "UnitTests/Windowing/WindowManagerTest.cpp")
target_link_libraries(window-manager-tests nes-tests)
add_test(NAME window-manager-tests COMMAND window-manager-tests)

### State Manager Test ###
add_executable(state-manager-tests "UnitTests/State/StateTest.cpp")
target_link_libraries(state-manager-tests nes-tests)
add_test(NAME state-manager-tests COMMAND state-manager-tests)

### ZMQ Data Source Sink Tests ###
add_executable(zmq-tests "UnitTests/Source/ZMQTest.cpp")
target_link_libraries(zmq-tests nes-tests)
add_test(NAME zmq-tests COMMAND zmq-tests)

### Source Tests ###
add_executable(source-tests "UnitTests/Source/SourceTest.cpp")
target_link_libraries(source-tests nes-tests)
add_test(NAME source-tests COMMAND source-tests)

### Sink Tests ###
add_executable(sink-tests "UnitTests/Sink/SinkTest.cpp" UnitTests/Sink/SinkTest.cpp)
target_link_libraries(sink-tests nes-tests)
add_test(NAME sink-tests COMMAND sink-tests)

### Kafka Tests ###
if (NES_USE_KAFKA)
    add_executable(kafka-tests "UnitTests/Source/KafkaTest.cpp")
    message("libraies: ${LIBRARIES}")
    target_link_libraries(kafka-tests nes-tests)
    add_test(NAME kafka-tests COMMAND kafka-tests)
endif (NES_USE_KAFKA)

### OPC Tests ###
if (NES_USE_OPC)
    add_executable(opc-source-tests "UnitTests/Source/OPCSourceTest.cpp" UnitTests/Source/OPCSourceTest.cpp)
    target_link_libraries(opc-source-tests nes-tests)
    add_test(NAME opc-source-tests COMMAND opc-source-tests)
    add_executable(opc-sink-tests "UnitTests/Sink/OPCSinkTest.cpp" UnitTests/Sink/OPCSinkTest.cpp)
    target_link_libraries(opc-sink-tests nes-tests)
    add_test(NAME opc-sink-tests COMMAND opc-sink-tests)
endif (NES_USE_OPC)

### MQTT Tests ###
if (NES_USE_MQTT)
    add_executable(mqtt-source-tests "UnitTests/Source/MQTTSourceTest.cpp" UnitTests/Source/MQTTSourceTest.cpp)
    target_link_libraries(mqtt-source-tests nes-tests)
    add_test(NAME mqtt-source-tests COMMAND mqtt-source-tests)
    add_executable(mqtt-sink-tests "UnitTests/Sink/MQTTSinkTest.cpp")
    target_link_libraries(mqtt-sink-tests nes-tests)
    add_test(NAME mqtt-sink-tests COMMAND mqtt-sink-tests)
endif (NES_USE_MQTT)


### Row Layout Tests ###
add_executable(row-memory-layout-tests "UnitTests/Runtime/MemoryLayouts/RowMemoryLayoutTest.cpp")
target_link_libraries(row-memory-layout-tests nes-tests)
add_test(NAME row-memory-layout-tests COMMAND row-memory-layout-tests)

### Row Layout Tests ###
add_executable(columnar-memory-layout-tests "UnitTests/Runtime/MemoryLayouts/ColumnarMemoryLayoutTest.cpp")
target_link_libraries(columnar-memory-layout-tests nes-tests)
add_test(NAME columnar-memory-layout-tests COMMAND columnar-memory-layout-tests)

### Dynamic Memory Layout Tests ###
add_executable(dynamic-memory-layout-tests "UnitTests/Runtime/MemoryLayouts/DynamicMemoryLayoutTest.cpp")
target_link_libraries(dynamic-memory-layout-tests nes-tests)
add_test(NAME dynamic-memory-layout-tests COMMAND dynamic-memory-layout-tests)

### Source Catalog Tests ###
add_executable(source-catalog-test "UnitTests/Catalogs/SourceCatalogTest.cpp")
target_link_libraries(source-catalog-test nes-tests)
add_test(NAME source-catalog-test COMMAND source-catalog-test)

### QueryCatalog Tests ###
add_executable(query-catalog-service-tests "UnitTests/Services/QueryCatalogServiceTest.cpp")
target_link_libraries(query-catalog-service-tests nes-tests)
add_test(NAME query-catalog-service-tests COMMAND query-catalog-service-tests)

### LocationService Tests ###
add_executable(location-service-tests "UnitTests/Services/LocationServiceTest.cpp")
target_link_libraries(location-service-tests nes-tests)
add_test(NAME location-service-tests COMMAND location-service-tests)

### LogicalExpressionTest ###
add_executable(logical-expression-test "UnitTests/Query/LogicalExpressionTest.cpp")
target_link_libraries(logical-expression-test nes-tests)
add_test(NAME logical-expression-test COMMAND logical-expression-test)

### QueryExecutionTest ###
add_executable(query-execution-test "UnitTests/Query/QueryExecutionTest.cpp")
target_link_libraries(query-execution-test nes-tests)
add_test(NAME query-execution-test COMMAND query-execution-test)

### GPUPipelineQueryExecutionTest ###
if (NES_USE_GPU)
    add_executable(gpu-pipeline-query-execution-test "UnitTests/Query/GPUPipelineQueryExecutionTest.cpp")
    target_link_libraries(gpu-pipeline-query-execution-test nes-tests)
    add_test(NAME gpu-pipeline-query-execution-test COMMAND gpu-pipeline-query-execution-test)
endif (NES_USE_GPU)

### ProjectionTest ###
add_executable(projection-test "UnitTests/Query/ProjectionTest.cpp")
target_link_libraries(projection-test nes-tests)
add_test(NAME projection-test COMMAND projection-test)

### TupleBufferTest ###
add_executable(tuple-buffer-test "UnitTests/Runtime/TupleBufferTest.cpp")
target_link_libraries(tuple-buffer-test nes-tests)
add_test(NAME tuple-buffer-test COMMAND tuple-buffer-test)

if (CMAKE_USE_PAPI_PROFILER)
    add_executable(papi-profiler-test "UnitTests/Runtime/PAPIProfilerTest.cpp")
    target_link_libraries(papi-profiler-test nes-tests)
    add_test(NAME papi-profiler-test COMMAND papi-profiler-test)
endif ()

# LogicalOperatorPlanNodeTest
add_executable(logical-operator-node-tests "UnitTests/Operators/LogicalOperatorNodeTest.cpp")
target_link_libraries(logical-operator-node-tests nes-tests)
add_test(NAME logical-operator-node-tests COMMAND logical-operator-node-tests)

# ExpressionNodeTest
add_executable(expression-node-test "UnitTests/Operators/ExpressionNodeTest.cpp")
target_link_libraries(expression-node-test nes-tests)
add_test(NAME expression-node-test COMMAND expression-node-test)

# OperatorPropertiesTest
add_executable(operator-properties-tests "UnitTests/Operators/OperatorPropertiesTest.cpp")
target_link_libraries(operator-properties-tests nes-tests)
add_test(NAME operator-properties-tests COMMAND operator-properties-tests)

# QueryTest
add_executable(query-tests "UnitTests/API/QueryTest.cpp")
target_link_libraries(query-tests nes-tests)
add_test(NAME query-tests COMMAND query-tests)

# SpatialQueryTest
add_executable(spatial-query-tests "UnitTests/API/SpatialQueryTest.cpp")
target_link_libraries(spatial-query-tests nes-tests)
add_test(NAME spatial-query-tests COMMAND spatial-query-tests)

# QueryPlanIteratorTest
add_executable(queryplan-util-iterator-test "UnitTests/Plans/Util/QueryPlanIteratorTest.cpp")
target_link_libraries(queryplan-util-iterator-test nes-tests)
add_test(NAME queryplan-util-iterator-test COMMAND queryplan-util-iterator-test)

# ConvertLogicalToPhysicalSinkTest
add_executable(convert-logical-to-physical-sink-tests "UnitTests/Phases/ConvertLogicalToPhysicalSinkTest.cpp")
target_link_libraries(convert-logical-to-physical-sink-tests nes-tests)
add_test(NAME convert-logical-to-physical-sink-tests COMMAND convert-logical-to-physical-sink-tests)

# ConvertLogicalToPhysicalSourceTest
add_executable(convert-logical-to-physical-source-tests "UnitTests/Phases/ConvertLogicalToPhysicalSourceTest.cpp")
target_link_libraries(convert-logical-to-physical-source-tests nes-tests)
add_test(NAME convert-logical-to-physical-source-tests COMMAND convert-logical-to-physical-source-tests)

# Type inference test
add_executable(type-inference-tests "UnitTests/Optimizer/Phases/TypeInferencePhaseTest.cpp")
target_link_libraries(type-inference-tests nes-tests)
add_test(NAME type-inference-tests COMMAND type-inference-tests)

# FilterPushDownTest
add_executable(filter-push-down-rule-test "UnitTests/Optimizer/QueryRewrite/FilterPushDownRuleTest.cpp")
target_link_libraries(filter-push-down-rule-test nes-tests)
add_test(NAME filter-push-down-rule-test COMMAND filter-push-down-rule-test)

# DistributeWindowRuleTest
add_executable(distribute-window-rule-test "UnitTests/Optimizer/QueryRewrite/DistributeWindowRuleTest.cpp")
target_link_libraries(distribute-window-rule-test nes-tests)
add_test(NAME distribute-window-rule-test COMMAND distribute-window-rule-test)

# DistributeWindowRuleTest
add_executable(originid-inference-phase-test "UnitTests/Optimizer/Phases/OriginIdInferencePhaseTest.cpp")
target_link_libraries(originid-inference-phase-test nes-tests)
add_test(NAME originid-inference-phase-test COMMAND originid-inference-phase-test)

# SerializationUtilTest
add_executable(serialize-operator-test "UnitTests/Serialization/SerializationUtilTest.cpp")
target_link_libraries(serialize-operator-test nes-tests)
add_test(NAME serialize-operator-test COMMAND serialize-operator-test)

### SourceCatalogService Tests ###
add_executable(source-catalog-service-tests "UnitTests/Services/SourceCatalogServiceTest.cpp")
target_link_libraries(source-catalog-service-tests nes-tests)
add_test(NAME source-catalog-service-tests COMMAND source-catalog-service-tests)

### TopologyManagerService Tests ###
add_executable(topology-manager-service-tests "UnitTests/Services/TopologyManagerServiceTest.cpp")
target_link_libraries(topology-manager-service-tests nes-tests)
add_test(NAME topology-manager-service-tests COMMAND topology-manager-service-tests)

### MaintenanceService Tests ###
add_executable(maintenance-service-tests "UnitTests/Services/MaintenanceServiceTest.cpp")
target_link_libraries(maintenance-service-tests nes-tests)
add_test(NAME maintenance-service-tests COMMAND maintenance-service-tests)

### Query Plan Tests ###
add_executable(query-plan-tests "UnitTests/Plans/Query/QueryPlanTest.cpp")
target_link_libraries(query-plan-tests nes-tests)
add_test(NAME query-plan-tests COMMAND query-plan-tests)


# Lambda Source Integration tests
add_executable(lambda-source-integration-test "Integration/LambdaSourceIntegrationTest.cpp")
target_link_libraries(lambda-source-integration-test nes-tests)
add_test(NAME lambda-source-integration-test COMMAND lambda-source-integration-test)

# SimplePatternTest
add_executable(simple-pattern-test "Integration/SimplePatternTest.cpp")
target_link_libraries(simple-pattern-test nes-tests)
add_test(NAME simple-pattern-test COMMAND simple-pattern-test)

### OrOperatorTest ###
add_executable(or-operator-test "Integration/OrOperatorTest.cpp")
target_link_libraries(or-operator-test nes-tests)
add_test(NAME or-operator-test COMMAND or-operator-test)

### AndOperatorTest ###
add_executable(and-operator-test "Integration/AndOperatorTest.cpp")
target_link_libraries(and-operator-test nes-tests)
add_test(NAME and-operator-test COMMAND and-operator-test)

### AndOperatorTest ###
add_executable(seq-operator-test "Integration/SEQOperatorTest.cpp")
target_link_libraries(seq-operator-test nes-tests)
add_test(NAME seq-operator-test COMMAND seq-operator-test)

### Sensor Bus Tests ###
#add_executable(sensor-bus-tests "UnitTests/SensorBusTest.cpp")
#target_link_libraries(sensor-bus-tests nes-tests)
#add_test(NAME sensor-bus-tests COMMAND sensor-bus-tests)

### Global Query Node Tests ###
add_executable(global-query-node-tests "UnitTests/Plans/Global/Query/GlobalQueryNodeTest.cpp")
target_link_libraries(global-query-node-tests nes-tests)
add_test(NAME global-query-node-tests COMMAND global-query-node-tests)

### Global Query Plan Tests ###
add_executable(global-query-plan-tests "UnitTests/Plans/Global/Query/GlobalQueryPlanTest.cpp")
target_link_libraries(global-query-plan-tests nes-tests)
add_test(NAME global-query-plan-tests COMMAND global-query-plan-tests)

### Choose MemLayout Phase Tests ###
add_executable(memory-layout-selection-phase-tests "UnitTests/Optimizer/Phases/MemoryLayoutSelectionPhaseTest.cpp")
target_link_libraries(memory-layout-selection-phase-tests nes-tests)
add_test(NAME memory-layout-selection-phase-tests COMMAND memory-layout-selection-phase-tests)


### Query Merger Phase Tests ###
add_executable(global-query-plan-update-phase-tests "UnitTests/Optimizer/Phases/GlobalQueryPlanUpdatePhaseTest.cpp")
target_link_libraries(global-query-plan-update-phase-tests nes-tests)
add_test(NAME global-query-plan-update-phase-tests COMMAND global-query-plan-update-phase-tests)

### Logical Source Expansion Rule Tests ###
add_executable(logical-source-expansion-rule-test "UnitTests/Optimizer/QueryRewrite/LogicalSourceExpansionRuleTest.cpp")
target_link_libraries(logical-source-expansion-rule-test nes-tests)
add_test(NAME logical-source-expansion-rule-test COMMAND logical-source-expansion-rule-test)

### Syntax Based Equal Query Merger Rule Test ###
add_executable(syntax-based-equal-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/SyntaxBasedCompleteQueryMergerRuleTest.cpp")
target_link_libraries(syntax-based-equal-query-merger-rule-test nes-tests)
add_test(NAME syntax-based-equal-query-merger-rule-test COMMAND syntax-based-equal-query-merger-rule-test)

### Syntax Based Partial Query Merger Rule Test ###
add_executable(syntax-based-partial-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/SyntaxBasedPartialQueryMergerRuleTest.cpp")
target_link_libraries(syntax-based-partial-query-merger-rule-test nes-tests)
add_test(NAME syntax-based-partial-query-merger-rule-test COMMAND syntax-based-partial-query-merger-rule-test)

### Query Placement Tests ###
add_executable(query-placement-test "UnitTests/Optimizer/QueryPlacement/QueryPlacementTest.cpp")
target_link_libraries(query-placement-test nes-tests)
add_test(NAME query-placement-test COMMAND query-placement-test)

add_executable(ilp-placement-test "UnitTests/Optimizer/QueryPlacement/ILPPlacementTest.cpp" UnitTests/Optimizer/QueryPlacement/FaultToleranceTest.cpp)
target_link_libraries(ilp-placement-test nes-tests)
add_test(NAME ilp-placement-test COMMAND ilp-placement-test)

add_executable(nemo-placement-test "UnitTests/Optimizer/QueryPlacement/NemoPlacementTest.cpp")
target_link_libraries(nemo-placement-test nes-tests)
add_test(NAME nemo-placement-test COMMAND nemo-placement-test)

### Thread Naming Test ###
add_executable(global-execution-plan-tests "UnitTests/Plans/Global/Execution/GlobalExecutionPlanTest.cpp")
target_link_libraries(global-execution-plan-tests nes-tests)
add_test(NAME global-execution-plan-tests COMMAND global-execution-plan-tests)

### Util Function Test ###
add_executable(util-function-tests "UnitTests/Util/UtilFunctionTest.cpp")
target_link_libraries(util-function-tests nes-tests)
add_test(NAME util-function-tests COMMAND util-function-tests)

### Z3 Validation Test ###
add_executable(z3-validation-tests "UnitTests/Z3ValidationTest.cpp")
target_link_libraries(z3-validation-tests nes-tests)
add_test(NAME z3-validation-tests COMMAND z3-validation-tests)

### Syntactic Query Validation Test ###
add_executable(syntactic-query-validation-test "UnitTests/Optimizer/QueryValidation/SyntacticQueryValidationTest.cpp")
target_link_libraries(syntactic-query-validation-test nes-tests)
add_test(NAME syntactic-query-validation-test COMMAND syntactic-query-validation-test)

### Semantic Query Validation Test ###
add_executable(semantic-query-validation-test "UnitTests/Optimizer/QueryValidation/SemanticQueryValidationTest.cpp")
target_link_libraries(semantic-query-validation-test nes-tests)
add_test(NAME semantic-query-validation-test COMMAND semantic-query-validation-test)

### Operator To FOL Util Test ###
add_executable(query-signature-util-tests "UnitTests/Optimizer/QuerySignatures/QuerySignatureUtilTests.cpp")
target_link_libraries(query-signature-util-tests nes-tests)
add_test(NAME query-signature-util-tests COMMAND query-signature-util-tests)

### Z3 Signature Inference Phase Test ###
add_executable(z3-signature-inference-phase-test "UnitTests/Optimizer/Phases/SignatureInferencePhaseTest.cpp")
target_link_libraries(z3-signature-inference-phase-test nes-tests)
add_test(NAME z3-signature-inference-phase-test COMMAND z3-signature-inference-phase-test)

### Circular Buffer Tests ###
add_executable(circular-buffer-tests "UnitTests/Source/CircularBufferTest.cpp")
target_link_libraries(circular-buffer-tests nes-tests)
add_test(NAME circular-buffer-tests COMMAND circular-buffer-tests)

### Z3 Signature Based Equal Query Merger Rule Test ###
add_executable(z3-signature-based-equal-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRuleTest.cpp")
target_link_libraries(z3-signature-based-equal-query-merger-rule-test nes-tests)
add_test(NAME z3-signature-based-equal-query-merger-rule-test COMMAND z3-signature-based-equal-query-merger-rule-test)

### Z3 Signature Based Partial Query Merger Rule Test ###
add_executable(z3-signature-based-partial-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRuleTest.cpp")
target_link_libraries(z3-signature-based-partial-query-merger-rule-test nes-tests)
add_test(NAME z3-signature-based-partial-query-merger-rule-test COMMAND z3-signature-based-partial-query-merger-rule-test)

### Z3 Signature Based Partial Query Merger Rule Test ###
add_executable(z3-signature-based-partial-query-merger-bottom-up-rule-test "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerBottomUpRuleTest.cpp")
target_link_libraries(z3-signature-based-partial-query-merger-bottom-up-rule-test nes-tests)
add_test(NAME z3-signature-based-partial-query-merger-bottom-up-rule-test COMMAND z3-signature-based-partial-query-merger-bottom-up-rule-test)

### String Signature Based Complete Query Merger Rule Test ###
add_executable(string-signature-based-equal-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/HashSignatureBasedCompleteQueryMergerRuleTest.cpp")
target_link_libraries(string-signature-based-equal-query-merger-rule-test nes-tests)
add_test(NAME string-signature-based-equal-query-merger-rule-test COMMAND string-signature-based-equal-query-merger-rule-test)

### String Signature Based Partial Query Merger Rule Test ###
add_executable(string-signature-based-partial-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/StringSignatureBasedPartialQueryMergerRuleTest.cpp")
target_link_libraries(string-signature-based-partial-query-merger-rule-test nes-tests)
add_test(NAME string-signature-based-partial-query-merger-rule-test COMMAND string-signature-based-partial-query-merger-rule-test)

### Join Handler Tests ###
add_executable(join-handler-tests "UnitTests/JoinHandlerTest.cpp")
target_link_libraries(join-handler-tests nes-tests)
add_test(NAME join-handler-tests COMMAND join-handler-tests)

### Attribute Sort Rule Test ###
add_executable(attribute-sort-rule-test "UnitTests/Optimizer/QueryRewrite/AttributeSortRuleTest.cpp")
target_link_libraries(attribute-sort-rule-test nes-tests)
add_test(NAME attribute-sort-rule-test COMMAND attribute-sort-rule-test)

### Rename Source To Project Operator Rule Test ###
add_executable(rename-source-to-project-operator-rule-test "UnitTests/Optimizer/QueryRewrite/RenameSourceToProjectOperatorRuleTest.cpp")
target_link_libraries(rename-source-to-project-operator-rule-test nes-tests)
add_test(NAME rename-source-to-project-operator-rule-test COMMAND rename-source-to-project-operator-rule-test)

### Project Before Union Operator Rule Test ###
add_executable(project-before-union-operator-rule-test "UnitTests/Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRuleTest.cpp")
target_link_libraries(project-before-union-operator-rule-test nes-tests)
add_test(NAME project-before-union-operator-rule-test COMMAND project-before-union-operator-rule-test)

### Millisecond Interval Tests ###
add_executable(millisecond-interval-tests "UnitTests/Source/MillisecondIntervalTest.cpp")
target_link_libraries(millisecond-interval-tests nes-tests)
add_test(NAME millisecond-interval-tests COMMAND millisecond-interval-tests)

### Materialized View Tests ###
add_executable(materialized-view-tests "Integration/MaterializedViewTest.cpp")
target_link_libraries(materialized-view-tests nes-tests)
add_test(NAME materialized-view-tests COMMAND materialized-view-tests)

### Binary Operator Sort Rule Tests ###
add_executable(binary-operator-sort-rule-tests "UnitTests/Optimizer/QueryRewrite/BinaryOperatorSortRuleTest.cpp")
target_link_libraries(binary-operator-sort-rule-tests nes-tests)
add_test(NAME binary-operator-sort-rule-tests COMMAND binary-operator-sort-rule-tests)

### Config Tests ###
add_executable(config-test "UnitTests/Configurations/ConfigTest.cpp")
target_link_libraries(config-test nes-tests)
add_test(NAME config-test COMMAND config-test)

### Timer Tests ###
add_executable(timer-test "UnitTests/Util/TimerTest.cpp")
target_link_libraries(timer-test nes-tests)
add_test(NAME timer-test COMMAND timer-test)

### Adaptive Source Tests ###
add_executable(adaptive-sensor-tests "UnitTests/Sensors/AdaptiveKFTest.cpp")
target_link_libraries(adaptive-sensor-tests nes-tests)
add_test(NAME adaptive-sensor-tests COMMAND adaptive-sensor-tests)

### Location Tests ###
add_executable(location-unit-tests "UnitTests/Common/LocationUnitTest.cpp")
target_link_libraries(location-unit-tests nes-tests)
add_test(NAME location-unit-tests COMMAND location-unit-tests)


### Async Task Executor ###
add_executable(async-task-executor-ut "UnitTests/Runtime/AsyncTaskExecutorTest.cpp")
target_link_libraries(async-task-executor-ut nes-tests)
add_test(NAME async-task-executor-ut COMMAND async-task-executor-ut)

### Location Provider CSV Tests ###
add_executable(location-provider-csv-unit-tests "UnitTests/Spatial/LocationProviderCSVTest.cpp")
target_link_libraries(location-provider-csv-unit-tests nes-tests)
add_test(NAME location-provider-csv-unit-tests COMMAND location-provider-csv-unit-tests)

#------------------------------- # Integration Test Suite --------------------------------
### MultiWorkerTest ###
add_executable(multi-worker-test "Integration/MultiWorkerTest.cpp")
target_link_libraries(multi-worker-test nes-tests)
add_test(NAME multi-worker-test COMMAND multi-worker-test)

### QueryDeploymentTest ###
add_executable(query-deployment-test "Integration/QueryDeploymentTest.cpp")
target_link_libraries(query-deployment-test nes-tests)
add_test(NAME query-deployment-test COMMAND query-deployment-test)

### MQTTSinkDepleymentTest ###
add_executable(mqtt-sink-deployment-test "Integration/MQTTSinkDeploymentTest.cpp")
target_link_libraries(mqtt-sink-deployment-test nes-tests)
add_test(NAME mqtt-sink-deployment-test COMMAND mqtt-sink-deployment-test)

### RenameTest ###
add_executable(rename-test "Integration/RenameTest.cpp")
target_link_libraries(rename-test nes-tests)
add_test(NAME rename-test COMMAND rename-test)

### JoinDeploymentTest ###
add_executable(join-deployment-test "Integration/JoinDeploymentTest.cpp")
target_link_libraries(join-deployment-test nes-tests)
add_test(NAME join-deployment-test COMMAND join-deployment-test)

### windowDeploymentTest ###
add_executable(window-deployment-test "Integration/WindowDeploymentTest.cpp")
target_link_libraries(window-deployment-test nes-tests)
add_test(NAME window-deployment-test COMMAND window-deployment-test)

### windowDeploymentTest ###
add_executable(thread-local-tumbling-window-tests "Integration/Windowing/TumblingWindow/ThreadLocalTumblingWindowTests.cpp")
target_link_libraries(thread-local-tumbling-window-tests nes-tests)
add_test(NAME thread-local-tumbling-window-tests COMMAND thread-local-tumbling-window-tests)

add_executable(thread-local-global-tumbling-window-tests "Integration/Windowing/TumblingWindow/ThreadLocalGlobalTumblingWindowTests.cpp")
target_link_libraries(thread-local-global-tumbling-window-tests nes-tests)
add_test(NAME thread-local-global-tumbling-window-tests COMMAND thread-local-global-tumbling-window-tests)

add_executable(thread-local-sliding-window-tests "Integration/Windowing/SlidingWindow/ThreadLocalKeyedSlidingWindowTests.cpp")
target_link_libraries(thread-local-sliding-window-tests nes-tests)
add_test(NAME thread-local-sliding-window-tests COMMAND thread-local-sliding-window-tests)

add_executable(thread-local-global-sliding-window-tests "Integration/Windowing/SlidingWindow/ThreadLocalGlobalSlidingWindowTests.cpp")
target_link_libraries(thread-local-global-sliding-window-tests nes-tests)
add_test(NAME thread-local-global-sliding-window-tests COMMAND thread-local-global-sliding-window-tests)

### concurrent window deployment test ###
add_executable(concurrent-window-deployment-test "Integration/ConcurrentWindowDeploymentTest.cpp")
target_link_libraries(concurrent-window-deployment-test nes-tests)
add_test(NAME concurrent-window-deployment-test COMMAND concurrent-window-deployment-test)

### MultipleWindowsTest ###
add_executable(multi-window-test "Integration/MultipleWindowsTest.cpp")
target_link_libraries(multi-window-test nes-tests)
add_test(NAME multi-window-test COMMAND multi-window-test)

### MultiJoinsTest ###
add_executable(multi-join-test "Integration/MultipleJoinsTest.cpp")
target_link_libraries(multi-join-test nes-tests)
add_test(NAME multi-join-test COMMAND multi-join-test)

### MultiThreadedTest ###
add_executable(multi-thread-test "Integration/MultiThreadedTest.cpp")
target_link_libraries(multi-thread-test nes-tests)
add_test(NAME multi-thread-test COMMAND multi-thread-test)

### AssignWatermarkTest ###
#TODO: activate for testing
add_executable(assignment-watermark-test "Integration/AssignWatermarkTest.cpp")
target_link_libraries(assignment-watermark-test nes-tests)
add_test(NAME assignment-watermark-test COMMAND assignment-watermark-test)

add_executable(allowed-lateness-test "Integration/AllowedLatenessTest.cpp")
target_link_libraries(allowed-lateness-test nes-tests)
add_test(NAME allowed-lateness-test COMMAND allowed-lateness-test)

add_executable(complex-sequence-test "Integration/ComplexSequenceTest.cpp")
target_link_libraries(complex-sequence-test nes-tests)
add_test(NAME complex-sequence-test COMMAND complex-sequence-test)

### unionDeploymentTest ###
add_executable(union-deployment-test "Integration/UnionDeploymentTest.cpp")
target_link_libraries(union-deployment-test nes-tests)
add_test(NAME union-deployment-test COMMAND union-deployment-test)

### ContiniousSourceTest ###
add_executable(continuous-source-test "Integration/ContinuousSourceTest.cpp")
target_link_libraries(continuous-source-test nes-tests)
add_test(NAME continuous-source-test COMMAND continuous-source-test)

### DeepHierarchyTopologyTestDeepHierarchyTopologyTest ###
add_executable(deep-hierarchy-test "Integration/DeepHierarchyTopologyTest.cpp")
target_link_libraries(deep-hierarchy-test nes-tests)
add_test(NAME deep-hierarchy-test COMMAND deep-hierarchy-test)

### Catalog Remote Tests ###
add_executable(source-catalog-remote-test "Integration/SourceCatalogRemoteTest.cpp")
target_link_libraries(source-catalog-remote-test nes-tests)
add_test(NAME source-catalog-remote-test COMMAND source-catalog-remote-test)

### WorkerCoordinatorStarter Tests ###
add_executable(worker-coordinator-starter-tests "Integration/WorkerCoordinatorStarterTest.cpp")
target_link_libraries(worker-coordinator-starter-tests nes-tests)
add_test(NAME worker-coordinator-starter-tests COMMAND worker-coordinator-starter-tests)

# RESTEndpointTest
add_executable(rest-endpoint-test "Integration/RESTEndpointTest.cpp")
target_link_libraries(rest-endpoint-test nes-tests)
add_test(NAME rest-endpoint-test COMMAND rest-endpoint-test)

### WorkerCoordinatorStarter Tests ###
add_executable(update-topology-remote-test "Integration/UpdateTopologyRemoteTest.cpp")
target_link_libraries(update-topology-remote-test nes-tests)
add_test(NAME update-topology-remote-test COMMAND update-topology-remote-test)

# NetworkStackUnitTest
add_executable(network-stack-test "UnitTests/Network/NetworkStackTest.cpp")
target_link_libraries(network-stack-test nes-tests)
add_test(NAME network-stack-test COMMAND network-stack-test)

# NetworkStackIntegrationTest
add_executable(network-stack-integration-test "Integration/NetworkStackIntegrationTest.cpp")
target_link_libraries(network-stack-integration-test nes-tests)
add_test(NAME network-stack-integration-test COMMAND network-stack-integration-test)

# ResourcesReaderTest
add_executable(resources-reader-test "UnitTests/Monitoring/ResourcesReaderTest.cpp")
target_link_libraries(resources-reader-test nes-tests)
add_test(NAME resources-reader-test COMMAND resources-reader-test)

# MonitoringQueriesTest
add_executable(monitoring-queries-test "UnitTests/Monitoring/MonitoringQueriesTest.cpp")
target_link_libraries(monitoring-queries-test nes-tests)
add_test(NAME monitoring-queries-test COMMAND monitoring-queries-test)

# MetricCollectorTest
add_executable(metric-collector-test "UnitTests/Monitoring/MetricCollectorTest.cpp")
target_link_libraries(metric-collector-test nes-tests)
add_test(NAME metric-collector-test COMMAND metric-collector-test)

# MetricStoreTest
add_executable(metric-store-test "UnitTests/Monitoring/MetricStoreTest.cpp")
target_link_libraries(metric-store-test nes-tests)
add_test(NAME metric-store-test COMMAND metric-store-test)

# UserDefinedMonitoringTest
add_executable(user-defined-monitoring-test "UnitTests/Monitoring/UserDefinedMonitoringTest.cpp")
target_link_libraries(user-defined-monitoring-test nes-tests)
add_test(NAME user-defined-monitoring-test COMMAND user-defined-monitoring-test)

# MonitoringIntegrationTest
add_executable(monitoring-integration-test "Integration/MonitoringIntegrationTest.cpp")
target_link_libraries(monitoring-integration-test nes-tests)
add_test(NAME monitoring-integration-test COMMAND monitoring-integration-test)
set_tests_properties(monitoring-integration-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(monitoring-integration-test nesCoordinator nesWorker)

# NemoIntegrationTest
add_executable(nemo-integration-test "Integration/NemoIntegrationTest.cpp")
target_link_libraries(nemo-integration-test nes-tests)
add_test(NAME nemo-integration-test COMMAND nemo-integration-test)
set_tests_properties(nemo-integration-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(nemo-integration-test nesCoordinator nesWorker)

### MemorySourceIntegrationTest ###
add_executable(memory-source-integration-test "Integration/MemorySourceIntegrationTest.cpp")
target_link_libraries(memory-source-integration-test nes-tests)
add_test(NAME memory-source-integration-test COMMAND memory-source-integration-test)

### StaticDataSourceIntegrationTest ###
add_executable(static-data-source-integration-test "Integration/StaticDataSourceIntegrationTest.cpp")
target_link_libraries(static-data-source-integration-test nes-tests)
add_test(NAME static-data-source-integration-test COMMAND static-data-source-integration-test)

### BenchmarkSourceIntegrationTest ###
add_executable(benchmark-source-integration-test "Integration/BenchmarkSourceIntegrationTest.cpp")
target_link_libraries(benchmark-source-integration-test nes-tests)
add_test(NAME benchmark-source-integration-test COMMAND benchmark-source-integration-test)

### TestHarnessUtilTest ###
add_executable(test-harness-util-test "Integration/TestHarnessUtilTest.cpp")
target_link_libraries(test-harness-util-test nes-tests)
add_test(NAME test-harness-util-test COMMAND test-harness-util-test)

### StringQueryTest ###
add_executable(string-query-test "Integration/StringQueryTest.cpp")
target_link_libraries(string-query-test nes-tests)
add_test(NAME string-query-test COMMAND string-query-test)

### UpstreamBackup ###
add_executable(upstream-backup-test "Integration/UpstreamBackupTest.cpp")
target_link_libraries(upstream-backup-test nes-tests)
add_test(NAME upstream-backup-test COMMAND upstream-backup-test)

### gRPC Tests
add_executable(grpc-test "UnitTests/gRPC/grpcTests.cpp")
target_link_libraries(grpc-test nes-tests)
add_test(NAME grpc-test COMMAND grpc-test)

### Query Failure Test
add_executable(query-failure-test "Integration/QueryFailureTest.cpp")
target_link_libraries(query-failure-test nes-tests)
add_test(NAME query-failure-test COMMAND query-failure-test)

### Location Integration Test ###
if (USE_S2)
    add_executable(location-integration-tests "Integration/LocationIntegrationTests.cpp")
    target_link_libraries(location-integration-tests nes-tests s2)
    add_test(NAME location-integration-tests COMMAND location-integration-tests)
endif ()

#------------------------------- # E2E Test Suite --------------------------------
### E2E Tests ###
add_executable(e2e-coordinator-single-worker-test "E2e/E2ECoordinatorSingleWorkerTest.cpp")
target_link_libraries(e2e-coordinator-single-worker-test nes-tests)
add_test(NAME e2e-coordinator-single-worker-test COMMAND e2e-coordinator-single-worker-test)
set_tests_properties(e2e-coordinator-single-worker-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(e2e-coordinator-single-worker-test nesCoordinator nesWorker)

add_executable(e2e-coordinator-multi-worker-test "E2e/E2ECoordinatorMultiWorkerTest.cpp")
target_link_libraries(e2e-coordinator-multi-worker-test nes-tests)
add_test(NAME e2e-coordinator-multi-worker-test COMMAND e2e-coordinator-multi-worker-test)
set_tests_properties(e2e-coordinator-multi-worker-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(e2e-coordinator-multi-worker-test nesCoordinator nesWorker)

add_executable(e2e-coordinator-multi-query-test "E2e/E2ECoordinatorMultiQueryTest.cpp")
target_link_libraries(e2e-coordinator-multi-query-test nes-tests)
add_test(NAME e2e-coordinator-multi-query-test COMMAND e2e-coordinator-multi-query-test)
set_tests_properties(e2e-coordinator-multi-query-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(e2e-coordinator-multi-query-test nesCoordinator nesWorker)

set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

