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

#include <API/Schema.hpp>
#include <Experimental/Interpreter/DataValue/Integer.hpp>
#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Experimental/Interpreter/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/Operators/Aggregation.hpp>
#include <Experimental/Interpreter/Operators/AggregationFunction.hpp>
#include <Experimental/Interpreter/Operators/Emit.hpp>
#include <Experimental/Interpreter/Operators/Scan.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Experimental/NESIR/Phases/LoopInferencePhase.hpp>
#include <Experimental/Runtime/ExecutionContext.hpp>
#include <Experimental/Runtime/PipelineContext.hpp>
#include <Experimental/ExecutionEngine/CompilationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
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
#include <execinfo.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::ExecutionEngine::Experimental::Interpreter {

/**
 * @brief This test tests query execution using th mlir backend
 */
class QueryExecutionTest : public testing::Test {
  public:
    Trace::SSACreationPhase ssaCreationPhase;
    Trace::TraceToIRConversionPhase irCreationPhase;
    IR::LoopInferencePhase loopInferencePhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup QueryExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup QueryExecutionTest test case." << std::endl; }

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

TEST_F(QueryExecutionTest, emitQueryTest) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);

    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    Scan scan = Scan();
    auto emit = std::make_shared<Emit>(memoryLayout);
    scan.setChild(emit);

    auto memRef = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    RecordBuffer recordBuffer = RecordBuffer(memoryLayout, memRef);

    auto memRefPCTX = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    memRefPCTX.ref = Trace::ValueRef(INT32_MAX, 0, IR::Operations::INT8PTR);
    auto wctxRefPCTX = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    wctxRefPCTX.ref = Trace::ValueRef(INT32_MAX, 1, IR::Operations::INT8PTR);
    ExecutionContext executionContext = ExecutionContext(memRefPCTX);

    auto execution = Trace::traceFunctionSymbolically([&scan, &executionContext, &recordBuffer]() {
        scan.open(executionContext, recordBuffer);
        scan.close(executionContext, recordBuffer);
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution.get() << std::endl;
    auto ir = irCreationPhase.apply(execution);
    std::cout << ir->toString() << std::endl;
    loopInferencePhase.apply(ir);
    auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    MLIR::MLIRUtility::DebugFlags df = {false, false, false};
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, false);
}

TEST_F(QueryExecutionTest, aggQueryTest) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    Scan scan = Scan();
    auto aggField = std::make_shared<ReadFieldExpression>(0);
    auto sumAggFunction = std::make_shared<SumFunction>(aggField);
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    scan.setChild(aggregation);

    auto memRef = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    memRef.ref = Trace::ValueRef(INT32_MAX, 0, IR::Operations::INT8PTR);
    RecordBuffer recordBuffer = RecordBuffer(memoryLayout, memRef);

    auto runtimePipelineContext = std::make_shared<Runtime::Execution::PipelineContext>();
    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    auto runtimeExecutionContext = Runtime::Execution::ExecutionContext(runtimeWorkerContext.get(), runtimePipelineContext.get());
    auto runtimeExecutionContextRef = Value<MemRef>(std::make_unique<MemRef>(MemRef((int8_t*) &runtimeExecutionContext)));
    runtimeExecutionContextRef.ref = Trace::ValueRef(INT32_MAX, 3, IR::Operations::INT8PTR);
    ExecutionContext executionContext = ExecutionContext(runtimeExecutionContextRef);

    aggregation->setup(executionContext);

    auto execution = Trace::traceFunctionSymbolically([&scan, &executionContext, &recordBuffer]() {
        scan.open(executionContext, recordBuffer);
        scan.close(executionContext, recordBuffer);
    });
    execution = ssaCreationPhase.apply(std::move(execution));
    std::cout << *execution.get() << std::endl;
    auto ir = irCreationPhase.apply(execution);
    //std::cout << ir->toString() << std::endl;
    //ir = loopInferencePhase.apply(ir);
    std::cout << ir->toString() << std::endl;
    auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    MLIR::MLIRUtility::DebugFlags df = {false, false, false};
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, false);
    auto engine = mlirUtility->prepareEngine();

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (auto i = 0; i <= 10; i++) {
        dynamicBuffer[i]["f1"].write((uint64_t) 1);
    }
    dynamicBuffer.setNumberOfTuples(10);

    auto pipelineExecutionContext = MockedPipelineExecutionContext();

    auto function = (void (*)(void*, void*)) engine->lookup("execute").get();

    auto globalState = (GlobalAggregationState*) runtimePipelineContext->getGlobalOperatorState(0);
    auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();
    ASSERT_EQ(sumState->sum, (int64_t) 0);

    function((void*) &runtimeExecutionContext, std::addressof(buffer));

    ASSERT_EQ(sumState->sum, (int64_t) 10);
}

TEST_F(QueryExecutionTest, aggQueryTest2) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    Scan scan = Scan();
    auto aggField = std::make_shared<ReadFieldExpression>(0);
    auto sumAggFunction = std::make_shared<SumFunction>(aggField);
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    scan.setChild(aggregation);

    auto executionEngine = CompilationBasedPipelineExecutionEngine();
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);
    auto executablePipeline = executionEngine.compile(pipeline);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (auto i = 0; i <= 10; i++) {
        dynamicBuffer[i]["f1"].write((uint64_t) 1);
    }
    dynamicBuffer.setNumberOfTuples(10);

    //function((void*) &runtimeExecutionContext, std::addressof(buffer));
    executablePipeline->execute(*runtimeWorkerContext, buffer);
    //ASSERT_EQ(sumState->sum, (int64_t) 10);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter