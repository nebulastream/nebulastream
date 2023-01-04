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
#include <API/Schema.hpp>
#include <Experimental/ExecutionEngine/CompilationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/InterpretationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/Expressions/ArithmeticalExpression/AddExpression.hpp>
#include <Experimental/Interpreter/Expressions/ArithmeticalExpression/MulExpression.hpp>
#include <Experimental/Interpreter/Expressions/ArithmeticalExpression/SubExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Experimental/Interpreter/Operators/Aggregation/AvgFunction.hpp>
#include <Experimental/Interpreter/Operators/GroupedAggregation.hpp>
#include <Experimental/Utility/TPCHUtil.hpp>
#include <Util/Timer.hpp>
#include <Util/UtilityFunctions.hpp>
#ifdef USE_FLOUNDER
#include <Experimental/Flounder/FlounderPipelineCompilerBackend.hpp>
#endif
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/UDFCallExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Operators/Aggregation.hpp>
#include <Experimental/Interpreter/Operators/Aggregation/AggregationFunction.hpp>
#include <Experimental/Interpreter/Operators/Emit.hpp>
#include <Experimental/Interpreter/Operators/Join/JoinBuild.hpp>
#include <Experimental/Interpreter/Operators/Join/JoinProbe.hpp>
#include <Experimental/Interpreter/Operators/Map.hpp>
#include <Experimental/Interpreter/Operators/Scan.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#ifdef USE_MLIR
#include <Nautilus/Backends/MLIR/MLIRPipelineCompilerBackend.hpp>
#include <Nautilus/Backends/MLIR/MLIRUtility.hpp>
#endif
#include <Experimental/Runtime/RuntimeExecutionContext.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
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

using namespace NES::ExecutionEngine::Experimental;
namespace NES::Nautilus {

/**
* @brief This test tests query execution using th mlir backend
*/
class JoinOperatorTest : public Testing::NESBaseTest, public ::testing::WithParamInterface<std::string> {
  public:
    Tracing::SSACreationPhase ssaCreationPhase;
    Tracing::TraceToIRConversionPhase irCreationPhase;
    std::shared_ptr<ExecutionEngine::Experimental::PipelineExecutionEngine> executionEngine;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryExecutionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        auto param = this->GetParam();
        NES_INFO("Setup QueryExecutionTest test case." << param);
        if (param == "INTERPRETER") {
            executionEngine = std::make_shared<InterpretationBasedPipelineExecutionEngine>();
        } else if (param == "MLIR") {
#ifdef USE_MLIR
            auto backend = std::make_shared<Nautilus::Backends::MLIR::MLIRPipelineCompilerBackend>();
            executionEngine = std::make_shared<CompilationBasedPipelineExecutionEngine>(backend);
#endif
        } else if (param == "FLOUNDER") {
#ifdef USE_FLOUNDER
            auto backend = std::make_shared<FlounderPipelineCompilerBackend>();
            executionEngine = std::make_shared<CompilationBasedPipelineExecutionEngine>(backend);
#endif
        } else if (param == "BABELFISH") {
#ifdef USE_BABELFISH
            auto backend = std::make_shared<BabelfishPipelineCompilerBackend>();
            executionEngine = std::make_shared<CompilationBasedPipelineExecutionEngine>(backend);
#endif
        }
        if (executionEngine == nullptr) {
            GTEST_SKIP_("No backend found");
        }
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down QueryExecutionTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down QueryExecutionTest test class."); }
};

class CollectPipeline : public ExecutablePipeline {
  public:
    CollectPipeline() : ExecutablePipeline(nullptr, nullptr) {}
    void execute(Runtime::WorkerContext&, Runtime::TupleBuffer& buffer) override { receivedBuffers.emplace_back(buffer); }
    std::vector<Runtime::TupleBuffer> receivedBuffers;
};

TEST_P(JoinOperatorTest, joinBuildQueryTest) {
    auto bm = std::make_shared<Runtime::BufferManager>(1000);

    auto buildSideSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    buildSideSchema->addField("key", BasicType::INT64);
    buildSideSchema->addField("value", BasicType::INT64);
    auto buildSideMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(buildSideSchema, bm->getBufferSize());

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    Scan buildSideScan = Scan(buildSideMemoryLayout);
    std::vector<ExpressionPtr> joinBuildKeys = {std::make_shared<ReadFieldExpression>("key")};
    std::vector<ExpressionPtr> joinBuildValues = {std::make_shared<ReadFieldExpression>("value")};
    NES::Experimental::HashMapFactory factory = NES::Experimental::HashMapFactory(bm, 16, 8, 1000);
    auto map = factory.createPtr();
    std::shared_ptr<NES::Experimental::Hashmap> sharedHashMap = std::move(map);
    auto joinBuild = std::make_shared<JoinBuild>(sharedHashMap, joinBuildKeys, joinBuildValues);
    buildSideScan.setChild(joinBuild);
    auto buildPipeline = std::make_shared<PhysicalOperatorPipeline>();
    buildPipeline->setRootOperator(&buildSideScan);
    auto buildSidePipeline = executionEngine->compile(buildPipeline);

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

TEST_P(JoinOperatorTest, joinBuildAndPropeQueryTest) {
    auto bm = std::make_shared<Runtime::BufferManager>(1000);

    auto buildSideSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    buildSideSchema->addField("key", BasicType::INT64);
    buildSideSchema->addField("valueLeft", BasicType::INT64);
    auto buildSideMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(buildSideSchema, bm->getBufferSize());

    auto probSideSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    probSideSchema->addField("key", BasicType::INT64);
    probSideSchema->addField("valueRight", BasicType::INT64);
    auto probeSideMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(probSideSchema, bm->getBufferSize());

    auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    resultSchema->addField("key", BasicType::INT64);
    resultSchema->addField("valueLeft", BasicType::INT64);
    resultSchema->addField("valueRight", BasicType::INT64);
    auto resultMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    Scan buildSideScan = Scan(buildSideMemoryLayout);
    std::vector<ExpressionPtr> joinBuildKeys = {std::make_shared<ReadFieldExpression>("key")};
    std::vector<ExpressionPtr> joinBuildValues = {std::make_shared<ReadFieldExpression>("valueLeft")};
    NES::Experimental::HashMapFactory factory = NES::Experimental::HashMapFactory(bm, 8, 8, 1000);
    std::shared_ptr<NES::Experimental::Hashmap> sharedHashMap = factory.createPtr();
    auto joinBuild = std::make_shared<JoinBuild>(sharedHashMap, joinBuildKeys, joinBuildValues);
    buildSideScan.setChild(joinBuild);
    auto buildPipeline = std::make_shared<PhysicalOperatorPipeline>();
    buildPipeline->setRootOperator(&buildSideScan);
    auto buildSidePipeline = executionEngine->compile(buildPipeline);

    Scan probSideScan = Scan(probeSideMemoryLayout);
    std::vector<IR::Types::StampPtr> keyStamps = {IR::Types::StampFactory::createInt64Stamp()};
    std::vector<IR::Types::StampPtr> valueStamps = {IR::Types::StampFactory::createInt64Stamp()};
    std::vector<ExpressionPtr> joinProbeKeys = {std::make_shared<ReadFieldExpression>("key")};
    std::vector<ExpressionPtr> joinProbeValues = {std::make_shared<ReadFieldExpression>("valueRight")};
    std::vector<Record::RecordFieldIdentifier> joinProbeResultFields = {"key", "valueLeft", "valueRight"};
    auto joinProb =
        std::make_shared<JoinProbe>(sharedHashMap, joinProbeResultFields, joinProbeKeys, joinProbeValues, keyStamps, valueStamps);
    probSideScan.setChild(joinProb);
    auto emit = std::make_shared<Emit>(resultMemoryLayout);
    joinProb->setChild(emit);
    auto probePipeline = std::make_shared<PhysicalOperatorPipeline>();
    probePipeline->setRootOperator(&probSideScan);
    auto executablePropePipeline = executionEngine->compile(probePipeline);

    auto collectPipelines = std::make_shared<CollectPipeline>();
    executablePropePipeline->getExecutionContext()->addSuccessorPipeline(collectPipelines);

    auto buildSideBuffer = bm->getBufferBlocking();
    auto buildSideDynBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(buildSideMemoryLayout, buildSideBuffer);
    for (auto i = 0; i < 10; i++) {
        buildSideDynBuffer[i]["key"].write((int64_t) i);
        buildSideDynBuffer[i]["valueLeft"].write((int64_t) 666);
    }
    buildSideDynBuffer.setNumberOfTuples(10);

    auto probeSideBuffer = bm->getBufferBlocking();
    auto probeSideDynBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(probeSideMemoryLayout, probeSideBuffer);
    for (auto i = 0; i < 10; i++) {
        probeSideDynBuffer[i]["key"].write((int64_t) i % 5);
        probeSideDynBuffer[i]["valueRight"].write((int64_t) 42);
    }
    probeSideDynBuffer.setNumberOfTuples(10);

    buildSidePipeline->setup();
    buildSidePipeline->execute(*runtimeWorkerContext, buildSideBuffer);

    auto currentSize = sharedHashMap->numberOfEntries();
    ASSERT_EQ(currentSize, (int64_t) 10);

    executablePropePipeline->setup();
    executablePropePipeline->execute(*runtimeWorkerContext, probeSideBuffer);

    ASSERT_EQ(collectPipelines->receivedBuffers.size(), 1);
    auto resultBuffer = collectPipelines->receivedBuffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 10);
    auto resultDynBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(resultMemoryLayout, resultBuffer);
    ASSERT_EQ(resultDynBuffer[0][0].read<int64_t>(), 0);
    ASSERT_EQ(resultDynBuffer[0][1].read<int64_t>(), 666);
    ASSERT_EQ(resultDynBuffer[0][2].read<int64_t>(), 42);
    ASSERT_EQ(resultDynBuffer[1][0].read<int64_t>(), 1);
    ASSERT_EQ(resultDynBuffer[1][1].read<int64_t>(), 666);
    ASSERT_EQ(resultDynBuffer[1][2].read<int64_t>(), 42);
    ASSERT_EQ(resultDynBuffer[6][0].read<int64_t>(), 1);
    ASSERT_EQ(resultDynBuffer[6][1].read<int64_t>(), 666);
    ASSERT_EQ(resultDynBuffer[6][2].read<int64_t>(), 42);
}

INSTANTIATE_TEST_CASE_P(testJoinOperator,
                        JoinOperatorTest,
                        ::testing::Values("INTERPRETER", "MLIR", "FLOUNDER"),
                        [](const testing::TestParamInfo<JoinOperatorTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Nautilus