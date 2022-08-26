/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifdef USE_BABELFISH
#include <Experimental/Babelfish/BabelfishPipelineCompilerBackend.hpp>
#endif
#include "Experimental/Interpreter/Expressions/ArithmeticalExpression/AddExpression.hpp"
#include "Experimental/Interpreter/Expressions/ArithmeticalExpression/MulExpression.hpp"
#include "Experimental/Interpreter/Expressions/ArithmeticalExpression/SubExpression.hpp"
#include "Experimental/Interpreter/Expressions/LogicalExpressions/AndExpression.hpp"
#include "Experimental/Interpreter/Operators/Aggregation/AvgFunction.hpp"
#include "Experimental/Interpreter/Operators/GroupedAggregation.hpp"
#include "Util/Timer.hpp"
#include "Util/UtilityFunctions.hpp"
#include <API/Schema.hpp>
#include <Experimental/ExecutionEngine/CompilationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Utility/TPCHUtil.hpp>
#ifdef USE_FLOUNDER
#include <Experimental/Flounder/FlounderPipelineCompilerBackend.hpp>
#endif
#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Expressions/ConstantIntegerExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Experimental/Interpreter/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/Expressions/UDFCallExpression.hpp>
#include <Experimental/Interpreter/Expressions/WriteFieldExpression.hpp>
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/Operators/Aggregation.hpp>
#include <Experimental/Interpreter/Operators/Aggregation/AggregationFunction.hpp>
#include <Experimental/Interpreter/Operators/Emit.hpp>
#include <Experimental/Interpreter/Operators/Join/JoinBuild.hpp>
#include <Experimental/Interpreter/Operators/Join/JoinProbe.hpp>
#include <Experimental/Interpreter/Operators/Map.hpp>
#include <Experimental/Interpreter/Operators/Scan.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#ifdef USE_MLIR
#include <Experimental/MLIR/MLIRPipelineCompilerBackend.hpp>
#include <Experimental/MLIR/MLIRUtility.hpp>
#endif
#include <Experimental/NESIR/Phases/LoopInferencePhase.hpp>
#include <Experimental/Runtime/RuntimeExecutionContext.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Experimental/Trace/ExecutionTrace.hpp>
#include <Experimental/Trace/Phases/SSACreationPhase.hpp>
#include <Experimental/Trace/Phases/TraceToIRConversionPhase.hpp>
#include <Experimental/Trace/TraceContext.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <execinfo.h>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>

namespace NES::ExecutionEngine::Experimental::Interpreter {

/**
 * @brief This test tests query execution using th mlir backend
 */
class QueryExecutionTest : public testing::Test, public ::testing::WithParamInterface<std::string> {
  public:
    Trace::SSACreationPhase ssaCreationPhase;
    Trace::TraceToIRConversionPhase irCreationPhase;
    IR::LoopInferencePhase loopInferencePhase;
    std::shared_ptr<ExecutionEngine::Experimental::PipelineCompilerBackend> backend;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup QueryExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        auto param = this->GetParam();
        std::cout << "Setup QueryExecutionTest test case." << param << std::endl;
        if (param == "MLIR") {
#ifdef USE_MLIR
            backend = std::make_shared<MLIRPipelineCompilerBackend>();
#endif
        } else if (param == "FLOUNDER") {
#ifdef USE_FLOUNDER
            backend = std::make_shared<FlounderPipelineCompilerBackend>();
#endif
        } else if (param == "BABELFISH") {
#ifdef USE_BABELFISH
            backend = std::make_shared<BabelfishPipelineCompilerBackend>();
#endif
        }
        if (backend == nullptr) {
            GTEST_SKIP_("No compiler backend found");
        }
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down QueryExecutionTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down QueryExecutionTest test class." << std::endl; }
};

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext()
        : PipelineExecutionContext(
            0,
            0,
            nullptr,
            [](Runtime::TupleBuffer&, Runtime::WorkerContextRef) {

            },
            [](Runtime::TupleBuffer&) {
            },
            std::vector<Runtime::Execution::OperatorHandlerPtr>()){};
};

TEST_P(QueryExecutionTest, longAggregationQueryTest) {

    auto bm = std::make_shared<Runtime::BufferManager>();

    // use 100 mb buffer
    auto buffer = bm->getUnpooledBuffer(1024 * 1024 * 100).value();
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    Scan scan = Scan(memoryLayout);
    auto aggField = std::make_shared<ReadFieldExpression>(0);
    auto sumAggFunction = std::make_shared<SumFunction>(aggField, IR::Types::StampFactory::createUInt64Stamp());
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    scan.setChild(aggregation);
    auto executionEngine = CompilationBasedPipelineExecutionEngine(backend);
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine.compile(pipeline);

    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (auto i = 0ul; i < dynamicBuffer.getCapacity() - 1; i++) {
        dynamicBuffer[i]["f1"].write((uint64_t) 1);
    }
    dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity() - 1);
    executablePipeline->setup();
    auto globalState = (GlobalAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(0);
    auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();
    for (auto i = 0; i < 10; i++) {
        Timer timer("QueryExecutionTime");
        timer.start();
        sumState->sum = 0;
        executablePipeline->setup();
        executablePipeline->execute(*runtimeWorkerContext, buffer);
        std::cout << "Result " << sumState->sum;
        timer.snapshot("QueryExecutionTime");
        timer.pause();
        NES_INFO("QueryExecutionTime: " << timer);
    }
}

TEST_F(QueryExecutionTest, longAggregationUDFQueryTest) {

    auto bm = std::make_shared<Runtime::BufferManager>();

    // use 100 mb buffer
    auto buffer = bm->getUnpooledBuffer(1024 * 1024 * 100).value();
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    Scan scan = Scan(memoryLayout);

    // map
    auto field_0 = std::make_shared<ReadFieldExpression>(0);
    auto udfCallExpression = std::make_shared<UDFCallExpression>(field_0);
    auto writeExpression = std::make_shared<WriteFieldExpression>(0, udfCallExpression);
    auto mapOperator = std::make_shared<Map>(writeExpression);
    scan.setChild(mapOperator);
    auto aggField = std::make_shared<ReadFieldExpression>(0);
    auto sumAggFunction = std::make_shared<SumFunction>(aggField, IR::Types::StampFactory::createInt64Stamp());
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    mapOperator->setChild(aggregation);
    //backend = std::make_shared<MLIRPipelineCompilerBackend>();
    //backend = std::make_shared<FlounderPipelineCompilerBackend>();
    auto executionEngine = CompilationBasedPipelineExecutionEngine(backend);
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine.compile(pipeline);

    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (auto i = 0ul; i < dynamicBuffer.getCapacity() - 1; i++) {
        dynamicBuffer[i]["f1"].write((int64_t) 1);
    }
    dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity() - 1);

    for (auto i = 0; i < 100; i++) {
        Timer timer("QueryExecutionTime");
        timer.start();
        executablePipeline->setup();
        executablePipeline->execute(*runtimeWorkerContext, buffer);
        auto globalState = (GlobalAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(0);
        auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();
        std::cout << "Result " << sumState->sum;
        timer.snapshot("QueryExecutionTime");
        timer.pause();
        NES_INFO("QueryExecutionTime: " << timer);
    }
}

TEST_P(QueryExecutionTest, groupedAggQueryTest) {
    auto bm = std::make_shared<Runtime::BufferManager>(1000);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("key", BasicType::INT64);
    schema->addField("value", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    Scan scan = Scan(memoryLayout);
    auto keyField = std::make_shared<ReadFieldExpression>(0);
    auto aggField = std::make_shared<ReadFieldExpression>(1);
    auto sumAggFunction = std::make_shared<SumFunction>(aggField, IR::Types::StampFactory::createInt64Stamp());
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    std::vector<ExpressionPtr> keys = {keyField, keyField};
    NES::Experimental::HashMapFactory factory = NES::Experimental::HashMapFactory(bm, 16, 8, 1000);
    auto aggregation = std::make_shared<GroupedAggregation>(factory, keys, functions);
    scan.setChild(aggregation);
    auto executionEngine = CompilationBasedPipelineExecutionEngine(backend);
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine.compile(pipeline);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (auto i = 0; i < 10; i++) {
        dynamicBuffer[i]["key"].write((int64_t) i % 2);
        dynamicBuffer[i]["value"].write((int64_t) 1);
    }
    dynamicBuffer.setNumberOfTuples(10);
    executablePipeline->setup();
    executablePipeline->execute(*runtimeWorkerContext, buffer);

    auto globalState = (GroupedAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(0);
    auto currentSize = globalState->threadLocalAggregationSlots[0].get()->numberOfEntries();
    ASSERT_EQ(currentSize, (int64_t) 2);
}

TEST_P(QueryExecutionTest, joinBuildQueryTest) {
    auto bm = std::make_shared<Runtime::BufferManager>(1000);
    auto executionEngine = CompilationBasedPipelineExecutionEngine(backend);

    auto buildSideSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    buildSideSchema->addField("key", BasicType::INT64);
    buildSideSchema->addField("value", BasicType::INT64);
    auto buildSideMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(buildSideSchema, bm->getBufferSize());

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    Scan buildSideScan = Scan(buildSideMemoryLayout);
    std::vector<ExpressionPtr> joinBuildKeys = {std::make_shared<ReadFieldExpression>(0)};
    std::vector<ExpressionPtr> joinBuildValues = {std::make_shared<ReadFieldExpression>(1)};
    NES::Experimental::HashMapFactory factory = NES::Experimental::HashMapFactory(bm, 16, 8, 1000);
    auto map = factory.createPtr();
    std::shared_ptr<NES::Experimental::Hashmap> sharedHashMap = std::move(map);
    auto joinBuild = std::make_shared<JoinBuild>(sharedHashMap, joinBuildKeys, joinBuildValues);
    buildSideScan.setChild(joinBuild);
    auto buildPipeline = std::make_shared<PhysicalOperatorPipeline>();
    buildPipeline->setRootOperator(&buildSideScan);
    auto buildSidePipeline = executionEngine.compile(buildPipeline);

    auto buildSideBuffer = bm->getBufferBlocking();
    auto buildSideDynBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(buildSideMemoryLayout, buildSideBuffer);
    for (auto i = 0; i < 10; i++) {
        buildSideDynBuffer[i]["key"].write((int64_t) i % 2);
        buildSideDynBuffer[i]["value"].write((int64_t) 1);
    }
    buildSideDynBuffer.setNumberOfTuples(10);
    buildSidePipeline->setup();
    buildSidePipeline->execute(*runtimeWorkerContext, buildSideBuffer);

    auto currentSize = sharedHashMap->numberOfEntries();
    ASSERT_EQ(currentSize, (int64_t) 10);
}

TEST_P(QueryExecutionTest, joinBuildAndPropeQueryTest) {
    auto bm = std::make_shared<Runtime::BufferManager>(1000);
    auto executionEngine = CompilationBasedPipelineExecutionEngine(backend);

    auto buildSideSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    buildSideSchema->addField("key", BasicType::INT64);
    buildSideSchema->addField("valueLeft", BasicType::INT64);
    auto buildSideMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(buildSideSchema, bm->getBufferSize());

    auto probSideSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    probSideSchema->addField("key", BasicType::INT64);
    probSideSchema->addField("valueRight", BasicType::INT64);
    auto probeSideMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(probSideSchema, bm->getBufferSize());

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    Scan buildSideScan = Scan(buildSideMemoryLayout);
    std::vector<ExpressionPtr> joinBuildKeys = {std::make_shared<ReadFieldExpression>(0)};
    std::vector<ExpressionPtr> joinBuildValues = {std::make_shared<ReadFieldExpression>(1)};
    NES::Experimental::HashMapFactory factory = NES::Experimental::HashMapFactory(bm, 16, 8, 1000);
    std::shared_ptr<NES::Experimental::Hashmap> sharedHashMap = factory.createPtr();
    auto joinBuild = std::make_shared<JoinBuild>(sharedHashMap, joinBuildKeys, joinBuildValues);
    buildSideScan.setChild(joinBuild);
    auto buildPipeline = std::make_shared<PhysicalOperatorPipeline>();
    buildPipeline->setRootOperator(&buildSideScan);
    auto buildSidePipeline = executionEngine.compile(buildPipeline);

    Scan probSideScan = Scan(probeSideMemoryLayout);
    std::vector<ExpressionPtr> joinProbeKeys = {std::make_shared<ReadFieldExpression>(0)};
    std::vector<ExpressionPtr> joinProbeValues = {std::make_shared<ReadFieldExpression>(1)};
    auto joinProb = std::make_shared<JoinProbe>(sharedHashMap, joinBuildKeys);
    probSideScan.setChild(joinProb);
    auto probePipeline = std::make_shared<PhysicalOperatorPipeline>();
    probePipeline->setRootOperator(&probSideScan);
    auto executablePropePipeline = executionEngine.compile(probePipeline);
    auto buildSideBuffer = bm->getBufferBlocking();
    auto buildSideDynBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(buildSideMemoryLayout, buildSideBuffer);
    for (auto i = 0; i < 10; i++) {
        buildSideDynBuffer[i]["key"].write((int64_t) i % 2);
        buildSideDynBuffer[i]["valueLeft"].write((int64_t) 1);
    }
    buildSideDynBuffer.setNumberOfTuples(10);

    auto pronbSideBuffer = bm->getBufferBlocking();
    auto pronbSideDynBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(probeSideMemoryLayout, pronbSideBuffer);
    for (auto i = 0; i < 10; i++) {
        pronbSideDynBuffer[i]["key"].write((int64_t) i);
        pronbSideDynBuffer[i]["valueRight"].write((int64_t) 1);
    }
    buildSideDynBuffer.setNumberOfTuples(10);

    buildSidePipeline->setup();
    buildSidePipeline->execute(*runtimeWorkerContext, buildSideBuffer);

    executablePropePipeline->setup();
    executablePropePipeline->execute(*runtimeWorkerContext, pronbSideBuffer);

    auto currentSize = sharedHashMap->numberOfEntries();
    ASSERT_EQ(currentSize, (int64_t) 10);
}

TEST_P(QueryExecutionTest, aggQueryTest) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    Scan scan = Scan(memoryLayout);
    auto aggField = std::make_shared<ReadFieldExpression>(0);
    auto sumAggFunction = std::make_shared<SumFunction>(aggField, IR::Types::StampFactory::createUInt64Stamp());
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    scan.setChild(aggregation);
    auto executionEngine = CompilationBasedPipelineExecutionEngine(backend);
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine.compile(pipeline);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (auto i = 0; i < 10; i++) {
        dynamicBuffer[i]["f1"].write((uint64_t) 1);
    }
    dynamicBuffer.setNumberOfTuples(10);
    executablePipeline->setup();
    executablePipeline->execute(*runtimeWorkerContext, buffer);

    auto globalState = (GlobalAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(0);
    auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();
    ASSERT_EQ(sumState->sum, (int64_t) 10);
}

INSTANTIATE_TEST_CASE_P(testSingleNodeConcurrentTumblingWindowTest,
                        QueryExecutionTest,
                        ::testing::Values("MLIR"),
                        [](const testing::TestParamInfo<QueryExecutionTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::ExecutionEngine::Experimental::Interpreter