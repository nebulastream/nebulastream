# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

ExternalProject_Add(
        gtest
        URL https://github.com/google/googletest/archive/release-1.11.0.zip
        PREFIX ${CMAKE_CURRENT_BINARY_DIR}/gtest
        CMAKE_ARGS ${CMAKE_ARGS_CROSS}
        # Disable install step
        INSTALL_COMMAND ""
        URL_HASH SHA256=353571c2440176ded91c2de6d6cd88ddd41401d14692ec1f99e35d013feda55a
)

# Get GTest source and binary directories from CMake project
ExternalProject_Get_Property(gtest source_dir binary_dir)

# Create a libgtest target to be used as a dependency by test programs
add_library(libgtest IMPORTED STATIC GLOBAL)
add_dependencies(libgtest gtest gtest_main)

# Set libgtest properties
set_target_properties(libgtest PROPERTIES
        "IMPORTED_LOCATION" "${binary_dir}/lib/libgtest.a"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        )

add_library(libgtest_main IMPORTED STATIC GLOBAL)
add_dependencies(libgtest_main gtest_main)

set_target_properties(libgtest_main PROPERTIES
        "IMPORTED_LOCATION" "${binary_dir}/lib/libgtest_main.a"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        )

# Create a libgmock target to be used as a dependency by test programs
add_library(libgmock IMPORTED STATIC GLOBAL)
add_dependencies(libgmock gtest)

# Set libgmock properties
set_target_properties(libgmock PROPERTIES
        "IMPORTED_LOCATION" "${binary_dir}/lib/libgmock.a"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        )

# Set Include Dirs
include_directories("${source_dir}/googletest/include"
        "${source_dir}/googlemock/include")

set(GTEST_LIBRARIES ${LIBRARIES} ${GRPC_LIBRARIES} libgtest libgtest_main libgmock cpprestsdk::cpprest)

# Copy Test Data --------------------------------------------------------------
file(COPY test_data DESTINATION .)
add_definitions(-DTEST_DATA_DIRECTORY="${CMAKE_BINARY_DIR}/tests/test_data")
message(STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}")

#-------------------------------##############----------------------------------
#------------------------------- # Unit Test Suite --------------------------------

add_executable(udf-catalog-controller-test "UnitTests/Rest/UdfCatalogControllerTest.cpp")
target_link_libraries(udf-catalog-controller-test nes ${GTEST_LIBRARIES})
add_test(NAME udf-catalog-controller-test COMMAND udf-catalog-controller-test)

add_executable(udf-test "UnitTests/Catalogs/UdfCatalogTest.cpp" "UnitTests/Catalogs/JavaUdfDescriptorTest.cpp")
target_link_libraries(udf-test nes ${GTEST_LIBRARIES})
add_test(NAME udf-test COMMAND udf-test)

add_executable(compiler-test "UnitTests/Compiler/JITCompilerTest.cpp")
target_link_libraries(compiler-test nes ${GTEST_LIBRARIES})
add_test(NAME compiler-test COMMAND compiler-test)

add_executable(compiler-sharedlibrary-test "UnitTests/Compiler/SharedLibraryTest.cpp")
target_link_libraries(compiler-sharedlibrary-test nes ${GTEST_LIBRARIES})
add_test(NAME compiler-sharedlibrary-test COMMAND compiler-sharedlibrary-test)

### Node Engine Tests ###
add_executable(node-engine-test "UnitTests/Runtime/NodeEngineTest.cpp")
target_link_libraries(node-engine-test nes ${GTEST_LIBRARIES})
add_test(NAME node-engine-test COMMAND node-engine-test)

add_executable(watermark-manager-test "UnitTests/Runtime/WatermarkManagerTest.cpp")
target_link_libraries(watermark-manager-test nes ${GTEST_LIBRARIES})
add_test(NAME watermark-manager-test COMMAND watermark-manager-test)

### Code Generation Tests ###
add_executable(code-generation-tests "UnitTests/CodeGenerator/CodeGenerationTest.cpp")
target_link_libraries(code-generation-tests nes ${GTEST_LIBRARIES})
add_test(NAME code-generation-tests COMMAND code-generation-tests)

### Translate Physical Operator Tests ###
add_executable(translate-physical-operator-tests "UnitTests/QueryCompiler/TranslateToPhysicalOperatorPhaseTest.cpp")
target_link_libraries(translate-physical-operator-tests nes ${GTEST_LIBRARIES})
add_test(NAME translate-physical-operator-tests COMMAND translate-physical-operator-tests)

### AddScanAndEmitPhase Test ###
add_executable(add-scan-and-emit-phase-tests "UnitTests/QueryCompiler/AddScanAndEmitPhaseTest.cpp")
target_link_libraries(add-scan-and-emit-phase-tests nes ${GTEST_LIBRARIES})
add_test(NAME add-scan-and-emit-phase-tests COMMAND add-scan-and-emit-phase-tests)

### ArrayType Test ###
add_executable(array-type-tests "UnitTests/QueryCompiler/ArrayTypeTest.cpp")
target_link_libraries(array-type-tests nes ${GTEST_LIBRARIES})
add_test(NAME array-type-tests COMMAND array-type-tests)

### DataTypeFactory Test ###
add_executable(data-type-factory-tests "UnitTests/QueryCompiler/DataTypeFactoryTests.cpp")
target_link_libraries(data-type-factory-tests nes ${GTEST_LIBRARIES})
add_test(NAME data-type-factory-tests COMMAND data-type-factory-tests)

### Pipelining Tests ###
add_executable(pipelining-phase-tests "UnitTests/QueryCompiler/PipeliningPhaseTest.cpp")
target_link_libraries(pipelining-phase-tests nes ${GTEST_LIBRARIES})
add_test(NAME pipelining-phase-tests COMMAND pipelining-phase-tests)

### Operator Code Generation Tests ###
add_executable(operator-code-generation-tests "UnitTests/CodeGenerator/OperatorCodeGenerationTest.cpp")
target_link_libraries(operator-code-generation-tests nes ${GTEST_LIBRARIES})
add_test(NAME operator-code-generation-tests COMMAND operator-code-generation-tests)

### Query Compiler Tests ###
add_executable(query-compiler-tests "UnitTests/QueryCompiler/QueryCompilerTest.cpp")
target_link_libraries(query-compiler-tests  nes ${GTEST_LIBRARIES})
add_test(NAME query-compiler-tests  COMMAND query-compiler-tests )

### NES Topology Tests ###
add_executable(topology-tests "UnitTests/Topology/TopologyTest.cpp")
target_link_libraries(topology-tests nes ${GTEST_LIBRARIES})
add_test(NAME topology-tests COMMAND topology-tests)

### NES Topology Controller Tests ###
add_executable(topology-controller-tests "UnitTests/Rest/TopologyControllerTest.cpp")
target_link_libraries(topology-controller-tests nes ${GTEST_LIBRARIES})
add_test(NAME topology-controller-tests COMMAND topology-controller-tests)

### NES TopologyProperty Tests ###
add_executable(topology-property-tests "UnitTests/Topology/TopologyPropertyTest.cpp")
target_link_libraries(topology-property-tests nes ${GTEST_LIBRARIES})
add_test(NAME topology-property-tests COMMAND topology-property-tests)

### NES TopologyIterator Test ###
add_executable(topology-iterator-tests "UnitTests/Topology/TopologyIterationTest.cpp")
target_link_libraries(topology-iterator-tests nes ${GTEST_LIBRARIES})
add_test(NAME topology-iterator-tests COMMAND topology-iterator-tests)

### Buffer Storage Test ###
add_executable(buffer-storage-tests "UnitTests/Runtime/BufferStorageTest.cpp" UnitTests/Runtime/BufferStorageTest.cpp)
target_link_libraries(buffer-storage-tests nes ${GTEST_LIBRARIES})
add_test(NAME buffer-storage-tests COMMAND buffer-storage-tests)

### Buffer Manager Test ###
add_executable(buffer-manager-tests "UnitTests/Runtime/BufferManagerTest.cpp" UnitTests/Runtime/BufferStorageTest.cpp)
target_link_libraries(buffer-manager-tests nes ${GTEST_LIBRARIES})
add_test(NAME buffer-manager-tests COMMAND buffer-manager-tests)

### AbstractWindowHandler Manager Test ###
add_executable(window-manager-tests "UnitTests/Windowing/WindowManagerTest.cpp")
target_link_libraries(window-manager-tests nes ${GTEST_LIBRARIES})
add_test(NAME window-manager-tests COMMAND window-manager-tests)

### State Manager Test ###
add_executable(state-manager-tests "UnitTests/State/StateTest.cpp")
target_link_libraries(state-manager-tests nes ${GTEST_LIBRARIES})
add_test(NAME state-manager-tests COMMAND state-manager-tests)

### ZMQ Data Source Sink Tests ###
add_executable(zmq-tests "UnitTests/Source/ZMQTest.cpp")
target_link_libraries(zmq-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME zmq-tests COMMAND zmq-tests)

### Source Tests ###
add_executable(source-tests "UnitTests/Source/SourceTest.cpp")
target_link_libraries(source-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME source-tests COMMAND source-tests)

### Sink Tests ###
add_executable(sink-tests "UnitTests/Sink/SinkTest.cpp" UnitTests/Sink/SinkTest.cpp)
target_link_libraries(sink-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
add_test(NAME sink-tests COMMAND sink-tests)

### Kafka Tests ###
if (NES_USE_KAFKA)
    add_executable(kafka-tests "UnitTests/Source/KafkaTest.cpp")
    message("libraies: ${LIBRARIES}")
    target_link_libraries(kafka-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
    add_test(NAME kafka-tests COMMAND kafka-tests)
endif (NES_USE_KAFKA)

### OPC Tests ###
if (NES_USE_OPC)
    add_executable(opc-source-tests "UnitTests/Source/OPCSourceTest.cpp" UnitTests/Source/OPCSourceTest.cpp)
    target_link_libraries(opc-source-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
    add_test(NAME opc-source-tests COMMAND opc-source-tests)
    add_executable(opc-sink-tests "UnitTests/Sink/OPCSinkTest.cpp" UnitTests/Sink/OPCSinkTest.cpp)
    target_link_libraries(opc-sink-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
    add_test(NAME opc-sink-tests COMMAND opc-sink-tests)
endif (NES_USE_OPC)

### MQTT Tests ###
if (NES_USE_MQTT)
    add_executable(mqtt-source-tests "UnitTests/Source/MQTTSourceTest.cpp" UnitTests/Source/MQTTSourceTest.cpp)
    add_executable(mqtt-sink-tests "UnitTests/Sink/MQTTSinkTest.cpp")
    target_link_libraries(mqtt-source-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
    target_link_libraries(mqtt-sink-tests nes ${LIBRARIES} ${GTEST_LIBRARIES})
    add_test(NAME mqtt-source-tests COMMAND mqtt-source-tests)
    add_test(NAME mqtt-sink-tests COMMAND mqtt-sink-tests)
endif (NES_USE_MQTT)

### Dynamic Memory Layout Tests ###
add_executable(dynamic-memory-layout-tests "UnitTests/DynamicMemoryLayoutTest.cpp")
target_link_libraries(dynamic-memory-layout-tests nes ${GTEST_LIBRARIES})
add_test(NAME dynamic-memory-layout-tests COMMAND dynamic-memory-layout-tests)

### Stream Catalog Tests ###
add_executable(stream-catalog-test "UnitTests/Catalogs/StreamCatalogTest.cpp")
target_link_libraries(stream-catalog-test nes ${GTEST_LIBRARIES})
add_test(NAME stream-catalog-test COMMAND stream-catalog-test)

### QueryCatalog Tests ###
add_executable(query-catalog-tests "UnitTests/ServiceTests/QueryCatalogTest.cpp")
target_link_libraries(query-catalog-tests nes ${GTEST_LIBRARIES})
add_test(NAME query-catalog-tests COMMAND query-catalog-tests)

### LogicalExpressionTest ###
add_executable(logical-expression-test "UnitTests/Query/LogicalExpressionTest.cpp")
target_link_libraries(logical-expression-test nes ${GTEST_LIBRARIES})
add_test(NAME logical-expression-test COMMAND logical-expression-test)

### QueryExecutionTest ###
add_executable(query-execution-test "UnitTests/Query/QueryExecutionTest.cpp")
target_link_libraries(query-execution-test nes ${GTEST_LIBRARIES})
add_test(NAME query-execution-test COMMAND query-execution-test)

### ProjectionTest ###
add_executable(projection-test "UnitTests/Query/ProjectionTest.cpp")
target_link_libraries(projection-test nes ${GTEST_LIBRARIES})
add_test(NAME projection-test COMMAND projection-test)

### TupleBufferTest ###
add_executable(tuple-buffer-test "UnitTests/Runtime/TupleBufferTest.cpp")
target_link_libraries(tuple-buffer-test nes ${GTEST_LIBRARIES})
add_test(NAME tuple-buffer-test COMMAND tuple-buffer-test)

# LogicalOperatorPlanNodeTest
add_executable(logical-operator-node-tests "UnitTests/Operators/LogicalOperatorNodeTest.cpp")
target_link_libraries(logical-operator-node-tests nes ${GTEST_LIBRARIES})
add_test(NAME logical-operator-node-tests COMMAND logical-operator-node-tests)

# ExpressionNodeTest
add_executable(expression-node-test "UnitTests/Operators/ExpressionNodeTest.cpp")
target_link_libraries(expression-node-test nes ${GTEST_LIBRARIES})
add_test(NAME expression-node-test COMMAND expression-node-test)

# OperatorPropertiesTest
add_executable(operator-properties-tests "UnitTests/Operators/OperatorPropertiesTest.cpp")
target_link_libraries(operator-properties-tests nes ${GTEST_LIBRARIES})
add_test(NAME operator-properties-tests COMMAND operator-properties-tests)

# QueryTest
add_executable(query-tests "UnitTests/API/QueryTest.cpp")
target_link_libraries(query-tests nes ${GTEST_LIBRARIES})
add_test(NAME query-tests COMMAND query-tests)

# QueryPlanIteratorTest
add_executable(queryplan-util-iterator-test "UnitTests/Plans/Util/QueryPlanIteratorTest.cpp")
target_link_libraries(queryplan-util-iterator-test nes ${GTEST_LIBRARIES})
add_test(NAME queryplan-util-iterator-test COMMAND queryplan-util-iterator-test)

# ConvertLogicalToPhysicalSinkTest
add_executable(convert-logical-to-physical-sink-tests "UnitTests/Phases/ConvertLogicalToPhysicalSinkTest.cpp")
target_link_libraries(convert-logical-to-physical-sink-tests nes ${GTEST_LIBRARIES})
add_test(NAME convert-logical-to-physical-sink-tests COMMAND convert-logical-to-physical-sink-tests)

# ConvertLogicalToPhysicalSourceTest
add_executable(convert-logical-to-physical-source-tests "UnitTests/Phases/ConvertLogicalToPhysicalSourceTest.cpp")
target_link_libraries(convert-logical-to-physical-source-tests nes ${GTEST_LIBRARIES})
add_test(NAME convert-logical-to-physical-source-tests COMMAND convert-logical-to-physical-source-tests)

# Type inference test
add_executable(type-inference-tests "UnitTests/Optimizer/Phases/TypeInferencePhaseTest.cpp")
target_link_libraries(type-inference-tests nes ${GTEST_LIBRARIES})
add_test(NAME type-inference-tests COMMAND type-inference-tests)

# FilterPushDownTest
add_executable(filter-push-down-rule-test "UnitTests/Optimizer/QueryRewrite/FilterPushDownRuleTest.cpp")
target_link_libraries(filter-push-down-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME filter-push-down-rule-test COMMAND filter-push-down-rule-test)

# DistributeWindowRuleTest
add_executable(window-distr-rule-test "UnitTests/Optimizer/QueryRewrite/DistributeWindowRuleTest.cpp")
target_link_libraries(window-distr-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME window-distr-rule-test COMMAND window-distr-rule-test)

# SerializationUtilTest
add_executable(serialize-operator-test "UnitTests/Serialization/SerializationUtilTest.cpp")
target_link_libraries(serialize-operator-test nes ${GTEST_LIBRARIES})
add_test(NAME serialize-operator-test COMMAND serialize-operator-test)

### StreamCatalogService Tests ###
add_executable(stream-catalog-service-tests "UnitTests/ServiceTests/StreamCatalogServiceTest.cpp")
target_link_libraries(stream-catalog-service-tests nes ${GTEST_LIBRARIES})
add_test(NAME stream-catalog-service-tests COMMAND stream-catalog-service-tests)

### TopologyManagerService Tests ###
add_executable(topology-manager-service-tests "UnitTests/ServiceTests/TopologyManagerServiceTest.cpp")
target_link_libraries(topology-manager-service-tests  nes ${GTEST_LIBRARIES})
add_test(NAME topology-manager-service-tests COMMAND topology-manager-service-tests)

### Query Plan Tests ###
add_executable(query-plan-tests "UnitTests/Plans/Query/QueryPlanTest.cpp")
target_link_libraries(query-plan-tests nes ${GTEST_LIBRARIES})
add_test(NAME query-plan-tests COMMAND query-plan-tests)

# SimplePatternTest
add_executable(simple-pattern-test "Integration/SimplePatternTest.cpp")
target_link_libraries(simple-pattern-test nes ${GTEST_LIBRARIES})
add_test(NAME simple-pattern-test COMMAND simple-pattern-test)

### Sensor Bus Tests ###
#add_executable(sensor-bus-tests "UnitTests/SensorBusTest.cpp")
#target_link_libraries(sensor-bus-tests nes ${GTEST_LIBRARIES})
#add_test(NAME sensor-bus-tests COMMAND sensor-bus-tests)

### Adaptive Source Tests ###
add_executable(adaptive-source-tests "UnitTests/Source/AdaptiveSourceTest.cpp")
target_link_libraries(adaptive-source-tests nes ${GTEST_LIBRARIES})
add_test(NAME adaptive-source-tests COMMAND adaptive-source-tests)

### Global Query Node Tests ###
add_executable(global-query-node-tests "UnitTests/Plans/Global/Query/GlobalQueryNodeTest.cpp")
target_link_libraries(global-query-node-tests nes ${GTEST_LIBRARIES})
add_test(NAME global-query-node-tests COMMAND global-query-node-tests)

### Global Query Plan Tests ###
add_executable(global-query-plan-tests "UnitTests/Plans/Global/Query/GlobalQueryPlanTest.cpp")
target_link_libraries(global-query-plan-tests nes ${GTEST_LIBRARIES})
add_test(NAME global-query-plan-tests COMMAND global-query-plan-tests)

### Query Merger Phase Tests ###
add_executable(global-query-plan-update-phase-tests "UnitTests/Optimizer/Phases/GlobalQueryPlanUpdatePhaseTest.cpp")
target_link_libraries(global-query-plan-update-phase-tests nes ${GTEST_LIBRARIES})
add_test(NAME global-query-plan-update-phase-tests COMMAND global-query-plan-update-phase-tests)

### Logical Source Expansion Rule Tests ###
add_executable(logical-source-expansion-rule-test "UnitTests/Optimizer/QueryRewrite/LogicalSourceExpansionRuleTest.cpp")
target_link_libraries(logical-source-expansion-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME logical-source-expansion-rule-test COMMAND logical-source-expansion-rule-test)

### Syntax Based Equal Query Merger Rule Test ###
add_executable(syntax-based-equal-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/SyntaxBasedCompleteQueryMergerRuleTest.cpp")
target_link_libraries(syntax-based-equal-query-merger-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME syntax-based-equal-query-merger-rule-test COMMAND syntax-based-equal-query-merger-rule-test)

### Syntax Based Partial Query Merger Rule Test ###
add_executable(syntax-based-partial-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/SyntaxBasedPartialQueryMergerRuleTest.cpp")
target_link_libraries(syntax-based-partial-query-merger-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME syntax-based-partial-query-merger-rule-test COMMAND syntax-based-partial-query-merger-rule-test)

### Query Placement Tests ###
add_executable(query-placement-test "UnitTests/Optimizer/QueryPlacement/QueryPlacementTest.cpp")
target_link_libraries(query-placement-test nes ${GTEST_LIBRARIES})
add_test(NAME query-placement-test COMMAND query-placement-test)

### Thread Naming Test ###
add_executable(thread-naming-tests "UnitTests/Util/ThreadNamingTest.cpp")
target_link_libraries(thread-naming-tests nes ${GTEST_LIBRARIES})
add_test(NAME thread-naming-tests COMMAND thread-naming-tests)

### Thread Naming Test ###
add_executable(global-execution-plan-tests "UnitTests/Plans/Global/Execution/GlobalExecutionPlanTest.cpp")
target_link_libraries(global-execution-plan-tests nes ${GTEST_LIBRARIES})
add_test(NAME global-execution-plan-tests COMMAND global-execution-plan-tests)

### Util Function Test ###
add_executable(util-function-tests "UnitTests/Util/UtilFunctionTest.cpp")
target_link_libraries(util-function-tests nes ${GTEST_LIBRARIES})
add_test(NAME util-function-tests COMMAND util-function-tests)

### Z3 Validation Test ###
add_executable(z3-validation-tests "UnitTests/Z3ValidationTest.cpp")
target_link_libraries(z3-validation-tests nes ${GTEST_LIBRARIES})
add_test(NAME z3-validation-tests COMMAND z3-validation-tests)

### Syntactic Query Validation Test ###
add_executable(syntactic-query-validation-test "UnitTests/Optimizer/QueryValidation/SyntacticQueryValidationTest.cpp")
target_link_libraries(syntactic-query-validation-test nes ${GTEST_LIBRARIES})
add_test(NAME syntactic-query-validation-test COMMAND syntactic-query-validation-test)

### Semantic Query Validation Test ###
add_executable(semantic-query-validation-test "UnitTests/Optimizer/QueryValidation/SemanticQueryValidationTest.cpp")
target_link_libraries(semantic-query-validation-test nes ${GTEST_LIBRARIES})
add_test(NAME semantic-query-validation-test COMMAND semantic-query-validation-test)

### Operator To FOL Util Test ###
add_executable(query-signature-util-tests "UnitTests/Optimizer/Utils/QuerySignatureUtilTests.cpp")
target_link_libraries(query-signature-util-tests nes ${GTEST_LIBRARIES})
add_test(NAME query-signature-util-tests COMMAND query-signature-util-tests)

### Z3 Signature Inference Phase Test ###
add_executable(z3-signature-inference-phase-test "UnitTests/Optimizer/Phases/SignatureInferencePhaseTest.cpp")
target_link_libraries(z3-signature-inference-phase-test nes ${GTEST_LIBRARIES})
add_test(NAME z3-signature-inference-phase-test COMMAND z3-signature-inference-phase-test)

### Circular Buffer Tests ###
add_executable(circular-buffer-tests "UnitTests/Source/CircularBufferTest.cpp")
target_link_libraries(circular-buffer-tests nes ${GTEST_LIBRARIES})
add_test(NAME circular-buffer-tests COMMAND circular-buffer-tests)

### Z3 Signature Based Equal Query Merger Rule Test ###
add_executable(z3-signature-based-equal-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRuleTest.cpp")
target_link_libraries(z3-signature-based-equal-query-merger-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME z3-signature-based-equal-query-merger-rule-test COMMAND z3-signature-based-equal-query-merger-rule-test)

### Z3 Signature Based Partial Query Merger Rule Test ###
add_executable(z3-signature-based-partial-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRuleTest.cpp")
target_link_libraries(z3-signature-based-partial-query-merger-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME z3-signature-based-partial-query-merger-rule-test COMMAND z3-signature-based-partial-query-merger-rule-test)

### Z3 Signature Based Partial Query Merger Rule Test ###
add_executable(z3-signature-based-partial-query-merger-bottom-up-rule-test "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerBottomUpRuleTest.cpp")
target_link_libraries(z3-signature-based-partial-query-merger-bottom-up-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME z3-signature-based-partial-query-merger-bottom-up-rule-test COMMAND z3-signature-based-partial-query-merger-bottom-up-rule-test)

### String Signature Based Complete Query Merger Rule Test ###
add_executable(string-signature-based-equal-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/HashSignatureBasedCompleteQueryMergerRuleTest.cpp")
target_link_libraries(string-signature-based-equal-query-merger-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME string-signature-based-equal-query-merger-rule-test COMMAND string-signature-based-equal-query-merger-rule-test)

### String Signature Based Partial Query Merger Rule Test ###
add_executable(string-signature-based-partial-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/StringSignatureBasedPartialQueryMergerRuleTest.cpp")
target_link_libraries(string-signature-based-partial-query-merger-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME string-signature-based-partial-query-merger-rule-test COMMAND string-signature-based-partial-query-merger-rule-test)

### Join Handler Tests ###
add_executable(join-handler-tests "UnitTests/JoinHandlerTest.cpp")
target_link_libraries(join-handler-tests nes ${GTEST_LIBRARIES})
add_test(NAME join-handler-tests COMMAND join-handler-tests)

### Attribute Sort Rule Test ###
add_executable(attribute-sort-rule-test "UnitTests/Optimizer/QueryRewrite/AttributeSortRuleTest.cpp")
target_link_libraries(attribute-sort-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME attribute-sort-rule-test COMMAND attribute-sort-rule-test)

### Rename Stream To Project Operator Rule Test ###
add_executable(rename-stream-to-project-operator-rule-test "UnitTests/Optimizer/QueryRewrite/RenameStreamToProjectOperatorRuleTest.cpp")
target_link_libraries(rename-stream-to-project-operator-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME rename-stream-to-project-operator-rule-test COMMAND rename-stream-to-project-operator-rule-test)

### Project Before Union Operator Rule Test ###
add_executable(project-before-union-operator-rule-test "UnitTests/Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRuleTest.cpp")
target_link_libraries(project-before-union-operator-rule-test nes ${GTEST_LIBRARIES})
add_test(NAME project-before-union-operator-rule-test COMMAND project-before-union-operator-rule-test)

### Millisecond Interval Tests ###
add_executable(millisecond-interval-tests "UnitTests/Source/MillisecondIntervalTest.cpp")
target_link_libraries(millisecond-interval-tests nes ${GTEST_LIBRARIES})
add_test(NAME millisecond-interval-tests COMMAND millisecond-interval-tests)

### Binary Operator Sort Rule Tests ###
add_executable(binary-operator-sort-rule-tests "UnitTests/Optimizer/QueryRewrite/BinaryOperatorSortRuleTest.cpp")
target_link_libraries(binary-operator-sort-rule-tests nes ${GTEST_LIBRARIES})
add_test(NAME binary-operator-sort-rule-tests COMMAND binary-operator-sort-rule-tests)

### Config Tests ###
add_executable(config-test "UnitTests/Configurations/ConfigTest.cpp")
target_link_libraries(config-test nes ${GTEST_LIBRARIES})
add_test(NAME config-test COMMAND config-test)

### Timer Tests ###
add_executable(timer-test "UnitTests/Util/TimerTest.cpp")
target_link_libraries(timer-test nes ${GTEST_LIBRARIES})
add_test(NAME timer-test COMMAND timer-test)

#------------------------------- # Integration Test Suite --------------------------------
### MultiWorkerTest ###
add_executable(multi-worker-test "Integration/MultiWorkerTest.cpp")
target_link_libraries(multi-worker-test nes ${GTEST_LIBRARIES})
add_test(NAME multi-worker-test COMMAND multi-worker-test)

### QueryDeploymentTest ###
add_executable(query-deployment-test "Integration/QueryDeploymentTest.cpp")
target_link_libraries(query-deployment-test nes ${GTEST_LIBRARIES})
add_test(NAME query-deployment-test COMMAND query-deployment-test)

### MQTTSinkDepleymentTest ###
add_executable(mqtt-sink-deployment-test "Integration/MQTTSinkDeploymentTest.cpp")
target_link_libraries(mqtt-sink-deployment-test nes ${GTEST_LIBRARIES})
add_test(NAME mqtt-sink-deployment-test COMMAND mqtt-sink-deployment-test)

### RenameTest ###
add_executable(rename-test "Integration/RenameTest.cpp")
target_link_libraries(rename-test nes ${GTEST_LIBRARIES})
add_test(NAME rename-test COMMAND rename-test)

### JoinDeploymentTest ###
add_executable(join-deployment-test "Integration/JoinDeploymentTest.cpp")
target_link_libraries(join-deployment-test nes ${GTEST_LIBRARIES})
add_test(NAME join-deployment-test COMMAND join-deployment-test)

### windowDeploymentTest ###
add_executable(window-deployment-test "Integration/WindowDeploymentTest.cpp")
target_link_libraries(window-deployment-test nes ${GTEST_LIBRARIES})
add_test(NAME window-deployment-test COMMAND window-deployment-test)

### windowDeploymentTest ###
add_executable(concurrent-window-deployment-test "Integration/ConcurrentWindowDeploymentTest.cpp")
target_link_libraries(concurrent-window-deployment-test nes ${GTEST_LIBRARIES})
add_test(NAME concurrent-window-deployment-test COMMAND concurrent-window-deployment-test)

### MultipleWindowsTest ###
add_executable(multi-window-test "Integration/MultipleWindowsTest.cpp")
target_link_libraries(multi-window-test nes ${GTEST_LIBRARIES})
add_test(NAME multi-window-test COMMAND multi-window-test)

### MultiJoinsTest ###
add_executable(multi-join-test "Integration/MultipleJoinsTest.cpp")
target_link_libraries(multi-join-test nes ${GTEST_LIBRARIES})
add_test(NAME multi-join-test COMMAND multi-join-test)

### MultiThreadedTest ###
add_executable(multi-thread-test "Integration/MultiThreadedTest.cpp")
target_link_libraries(multi-thread-test nes ${GTEST_LIBRARIES})
add_test(NAME multi-thread-test COMMAND multi-thread-test)

### AssignWatermarkTest ###
#TODO: activate for testing
add_executable(assignment-watermark-test "Integration/AssignWatermarkTest.cpp")
target_link_libraries(assignment-watermark-test nes ${GTEST_LIBRARIES})
add_test(NAME assignment-watermark-test COMMAND assignment-watermark-test)

add_executable(allowed-lateness-test "Integration/AllowedLatenessTest.cpp")
target_link_libraries(allowed-lateness-test nes ${GTEST_LIBRARIES})
add_test(NAME allowed-lateness-test COMMAND allowed-lateness-test)

add_executable(complex-sequence-test "Integration/ComplexSequenceTest.cpp")
target_link_libraries(complex-sequence-test nes ${GTEST_LIBRARIES})
add_test(NAME complex-sequence-test COMMAND complex-sequence-test)

### unionDeploymentTest ###
add_executable(union-deployment-test "Integration/UnionDeploymentTest.cpp")
target_link_libraries(union-deployment-test nes ${GTEST_LIBRARIES})
add_test(NAME union-deployment-test COMMAND union-deployment-test)

### ContiniousSourceTest ###
add_executable(continious-source-test "Integration/ContinuousSourceTest.cpp")
target_link_libraries(continious-source-test nes ${GTEST_LIBRARIES})
add_test(NAME continious-source-test COMMAND continious-source-test)

### DeepHierarchyTopologyTestDeepHierarchyTopologyTest ###
add_executable(deep-hierarchy-test "Integration/DeepHierarchyTopologyTest.cpp")
target_link_libraries(deep-hierarchy-test nes ${GTEST_LIBRARIES})
add_test(NAME deep-hierarchy-test COMMAND deep-hierarchy-test)

### Catalog Remote Tests ###
add_executable(stream-catalog-remote-test "Integration/StreamCatalogRemoteTest.cpp")
target_link_libraries(stream-catalog-remote-test nes ${GTEST_LIBRARIES})
add_test(NAME stream-catalog-remote-test COMMAND stream-catalog-remote-test)

### WorkerCoordinatorStarter Tests ###
add_executable(worker-coordinator-starter-tests "Integration/WorkerCoordinatorStarterTest.cpp")
target_link_libraries(worker-coordinator-starter-tests nes ${GTEST_LIBRARIES})
add_test(NAME worker-coordinator-starter-tests COMMAND worker-coordinator-starter-tests)

# RESTEndpointTest
add_executable(rest-endpoint-test "Integration/RESTEndpointTest.cpp")
target_link_libraries(rest-endpoint-test nes ${GTEST_LIBRARIES})
add_test(NAME rest-endpoint-test COMMAND rest-endpoint-test)

### WorkerCoordinatorStarter Tests ###
add_executable(update-topology-remote-test "Integration/UpdateTopologyRemoteTest.cpp")
target_link_libraries(update-topology-remote-test nes ${GTEST_LIBRARIES})
add_test(NAME update-topology-remote-test COMMAND update-topology-remote-test)

# NetworkStackTest
add_executable(network-stack-test "UnitTests/Network/NetworkStackTest.cpp")
target_link_libraries(network-stack-test nes ${GTEST_LIBRARIES})
add_test(NAME network-stack-test COMMAND network-stack-test)

# MonitoringStackTest
add_executable(monitoring-stack-test "UnitTests/Monitoring/MonitoringStackTest.cpp")
target_link_libraries(monitoring-stack-test nes ${GTEST_LIBRARIES})
add_test(NAME monitoring-stack-test COMMAND monitoring-stack-test)

# MonitoringSerializationTest
add_executable(monitoring-serialization-test "UnitTests/Monitoring/MonitoringSerializationTest.cpp")
target_link_libraries(monitoring-serialization-test nes ${GTEST_LIBRARIES})
add_test(NAME monitoring-serialization-test COMMAND monitoring-serialization-test)

# MonitoringIntegrationTest
add_executable(monitoring-integration-test "Integration/MonitoringIntegrationTest.cpp")
target_link_libraries(monitoring-integration-test nes ${GTEST_LIBRARIES})
add_test(NAME monitoring-integration-test COMMAND monitoring-integration-test)

### MemorySourceIntegrationTest ###
add_executable(memory-source-integration-test "Integration/MemorySourceIntegrationTest.cpp")
target_link_libraries(memory-source-integration-test nes ${GTEST_LIBRARIES})
add_test(NAME memory-source-integration-test COMMAND memory-source-integration-test)

### BenchmarkSourceIntegrationTest ###
add_executable(benchmark-source-integration-test "Integration/BenchmarkSourceIntegrationTest.cpp")
target_link_libraries(benchmark-source-integration-test nes ${GTEST_LIBRARIES})
add_test(NAME benchmark-source-integration-test COMMAND benchmark-source-integration-test)

### TestHarnessUtilTest ###
add_executable(test-harness-util-test "Integration/TestHarnessUtilTest.cpp")
target_link_libraries(test-harness-util-test nes ${GTEST_LIBRARIES})
add_test(NAME test-harness-util-test COMMAND test-harness-util-test)

### StringQueryTest ###
add_executable(string-query-test "Integration/StringQueryTest.cpp")
target_link_libraries(string-query-test nes ${GTEST_LIBRARIES})
add_test(NAME string-query-test COMMAND string-query-test)

#------------------------------- # E2E Test Suite --------------------------------
### E2E Tests ###
add_executable(e2e-coordinator-single-worker-test "E2e/E2ECoordinatorSingleWorkerTest.cpp")
target_link_libraries(e2e-coordinator-single-worker-test nes ${GTEST_LIBRARIES})
add_test(NAME e2e-coordinator-single-worker-test COMMAND e2e-coordinator-single-worker-test)
set_tests_properties(e2e-coordinator-single-worker-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(e2e-coordinator-single-worker-test nesCoordinator nesWorker)

add_executable(e2e-coordinator-multi-worker-test "E2e/E2ECoordinatorMultiWorkerTest.cpp")
target_link_libraries(e2e-coordinator-multi-worker-test nes ${GTEST_LIBRARIES})
add_test(NAME e2e-coordinator-multi-worker-test COMMAND e2e-coordinator-multi-worker-test)
set_tests_properties(e2e-coordinator-multi-worker-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(e2e-coordinator-multi-worker-test nesCoordinator nesWorker)

add_executable(e2e-coordinator-multi-query-test "E2e/E2ECoordinatorMultiQueryTest.cpp")
target_link_libraries(e2e-coordinator-multi-query-test nes ${GTEST_LIBRARIES})
add_test(NAME e2e-coordinator-multi-query-test COMMAND e2e-coordinator-multi-query-test)
set_tests_properties(e2e-coordinator-multi-query-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(e2e-coordinator-multi-query-test nesCoordinator nesWorker)

set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

# Custom test commands --------------------------------------------------------
add_custom_target(test_debug COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure DEPENDS ${UNITTESTS})