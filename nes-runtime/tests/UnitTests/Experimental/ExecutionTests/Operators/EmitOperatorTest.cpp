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
class EmitOperatorTest : public Testing::NESBaseTest, public ::testing::WithParamInterface<std::string> {
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

TEST_P(EmitOperatorTest, scanAndEmitTest) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    Scan scan = Scan(memoryLayout);
    auto emit = std::make_shared<Emit>(memoryLayout);
    scan.setChild(emit);

    auto memRef = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    RecordBuffer recordBuffer = RecordBuffer(memRef);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine->compile(pipeline);
    auto collectPipelines = std::make_shared<CollectPipeline>();
    executablePipeline->getExecutionContext()->addSuccessorPipeline(collectPipelines);
    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (auto i = 0; i < 10; i++) {
        dynamicBuffer[i]["f1"].write((int64_t) i % 2);
        dynamicBuffer[i]["f2"].write((int64_t) 1);
    }
    dynamicBuffer.setNumberOfTuples(10);

    executablePipeline->setup();

    Timer timer("QueryExecutionTime");
    timer.start();
    executablePipeline->execute(*runtimeWorkerContext, buffer);
    timer.snapshot("QueryExecutionTime");
    timer.pause();
    NES_INFO("QueryExecutionTime: " << timer);
    ASSERT_EQ(collectPipelines->receivedBuffers.size(), 1);
    ASSERT_EQ(collectPipelines->receivedBuffers[0].getNumberOfTuples(), 10);
}

INSTANTIATE_TEST_CASE_P(testEmitOperator,
                        EmitOperatorTest,
                        ::testing::Values("INTERPRETER", "MLIR", "FLOUNDER"),
                        [](const testing::TestParamInfo<EmitOperatorTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Nautilus