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

#include "Experimental/Interpreter/Expressions/LogicalExpressions/AndExpression.hpp"
#include "Util/Timer.hpp"
#include "Util/UtilityFunctions.hpp"
#include <API/Schema.hpp>
#include <Experimental/ExecutionEngine/CompilationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/DataValue/Integer.hpp>
#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Expressions/ConstantIntegerExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/LessThanExpression.hpp>
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
    Scan scan = Scan(memoryLayout);
    auto emit = std::make_shared<Emit>(memoryLayout);
    scan.setChild(emit);

    auto memRef = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    RecordBuffer recordBuffer = RecordBuffer(memRef);

    auto memRefPCTX = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    memRefPCTX.ref = Trace::ValueRef(INT32_MAX, 0, IR::Operations::INT8PTR);
    auto wctxRefPCTX = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    wctxRefPCTX.ref = Trace::ValueRef(INT32_MAX, 1, IR::Operations::INT8PTR);
    RuntimeExecutionContext executionContext = RuntimeExecutionContext(memRefPCTX);

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

    Scan scan = Scan(memoryLayout);
    auto aggField = std::make_shared<ReadFieldExpression>(0);
    auto sumAggFunction = std::make_shared<SumFunction>(aggField);
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    scan.setChild(aggregation);

    auto memRef = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    memRef.ref = Trace::ValueRef(INT32_MAX, 0, IR::Operations::INT8PTR);
    RecordBuffer recordBuffer = RecordBuffer(memRef);

    auto runtimePipelineContext = std::make_shared<Runtime::Execution::RuntimePipelineContext>();
    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    auto runtimeExecutionContext =
        Runtime::Execution::RuntimeExecutionContext(runtimeWorkerContext.get(), runtimePipelineContext.get());
    auto runtimeExecutionContextRef = Value<MemRef>(std::make_unique<MemRef>(MemRef((int8_t*) &runtimeExecutionContext)));
    runtimeExecutionContextRef.ref = Trace::ValueRef(INT32_MAX, 3, IR::Operations::INT8PTR);
    RuntimeExecutionContext executionContext = RuntimeExecutionContext(runtimeExecutionContextRef);

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

Runtime::MemoryLayouts::DynamicTupleBuffer loadLineItemTable(std::shared_ptr<Runtime::BufferManager> bm) {
    std::ifstream inFile("/home/pgrulich/projects/tpch-dbgen/lineitem.tbl");
    uint64_t linecount = 0;
    std::string line;
    while (std::getline(inFile, line)) {
        // using printf() in all tests for consistency
        linecount++;
    }
    NES_DEBUG("LOAD lineitem with " << linecount << " lines");
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);

    // orderkey
    // partkey
    // suppkey
    // linenumber
    schema->addField("l_quantity", BasicType::INT64);
    schema->addField("l_extendedprice", BasicType::INT64);
    schema->addField("l_discount", BasicType::INT64);
    // tax
    // returnflag
    // linestatus
    schema->addField("l_shipdate", BasicType::INT64);
    // commitdate
    // receiptdate
    // shipinstruct
    // shipmode
    // comment
    auto targetBufferSize = schema->getSchemaSizeInBytes() * linecount;
    auto buffer = bm->getUnpooledBuffer(targetBufferSize).value();
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);

    inFile.clear();// clear fail and eof bits
    inFile.seekg(0, std::ios::beg);

    while (std::getline(inFile, line)) {
        // using printf() in all tests for consistency
        auto index = dynamicBuffer.getNumberOfTuples();
        auto strings = NES::Util::splitWithStringDelimiter<std::string>(line, "|");

        auto l_quantityString = strings[4];
        int64_t l_quantity = std::stoi(l_quantityString);
        dynamicBuffer[index][0].write(l_quantity);

        auto l_extendedpriceString = strings[5];
        int64_t l_extendedprice = std::stof(l_extendedpriceString) * 100;
        dynamicBuffer[index][1].write(l_extendedprice);

        auto l_discountString = strings[6];
        int64_t l_discount = std::stof(l_discountString) * 100;
        dynamicBuffer[index][2].write(l_discount);

        auto l_shipdateString = strings[10];
        NES::Util::findAndReplaceAll(l_shipdateString, "-", "");
        int64_t l_shipdate = std::stoi(l_shipdateString);
        dynamicBuffer[index][3].write(l_shipdate);
        dynamicBuffer.setNumberOfTuples(index + 1);
    }
    inFile.close();
    NES_DEBUG("Loading of Lineitem done");
    return dynamicBuffer;
}

TEST_F(QueryExecutionTest, aggQueryTest2) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    Scan scan = Scan(memoryLayout);
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
    executablePipeline->setup();
    executablePipeline->execute(*runtimeWorkerContext, buffer);

    auto globalState = (GlobalAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(0);
    auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();
    ASSERT_EQ(sumState->sum, (int64_t) 10);
}

TEST_F(QueryExecutionTest, tpchQ6) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = loadLineItemTable(bm);

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);
    Scan scan = Scan(lineitemBuffer.getMemoryLayout());
    /*
     *   l_shipdate >= date '1994-01-01'
     *   and l_shipdate < date '1995-01-01'
     */
    auto const_1994_01_01 = std::make_shared<ConstantIntegerExpression>(19940101);
    auto const_1995_01_01 = std::make_shared<ConstantIntegerExpression>(19950101);
    auto readShipdate = std::make_shared<ReadFieldExpression>(3);
    auto lessThanExpression1 = std::make_shared<LessThanExpression>(const_1994_01_01, readShipdate);
    auto lessThanExpression2 = std::make_shared<LessThanExpression>(readShipdate, const_1995_01_01);
    auto andExpression = std::make_shared<AndExpression>(lessThanExpression1, lessThanExpression2);

    // l_discount between 0.06 - 0.01 and 0.06 + 0.01
    auto readDiscount = std::make_shared<ReadFieldExpression>(2);
    auto const_0_05 = std::make_shared<ConstantIntegerExpression>(4);
    auto const_0_07 = std::make_shared<ConstantIntegerExpression>(8);
    auto lessThanExpression3 = std::make_shared<LessThanExpression>(const_0_05, readDiscount);
    auto lessThanExpression4 = std::make_shared<LessThanExpression>(readDiscount, const_0_07);
    auto andExpression2 = std::make_shared<AndExpression>(lessThanExpression3, lessThanExpression4);

    // l_quantity < 24
    auto const_24 = std::make_shared<ConstantIntegerExpression>(24);
    auto readQuantity = std::make_shared<ReadFieldExpression>(0);
    auto lessThanExpression5 = std::make_shared<LessThanExpression>(readQuantity, const_24);
    auto andExpression3 = std::make_shared<AndExpression>(andExpression, andExpression2);
    auto andExpression4 = std::make_shared<AndExpression>(andExpression3, lessThanExpression5);

    auto selection = std::make_shared<Selection>(andExpression4);
    scan.setChild(selection);

    // sum(l_extendedprice)
    auto aggField = std::make_shared<ReadFieldExpression>(1);
    auto sumAggFunction = std::make_shared<SumFunction>(aggField);
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    selection->setChild(aggregation);

    auto executionEngine = CompilationBasedPipelineExecutionEngine();
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine.compile(pipeline);

    executablePipeline->setup();

    auto buffer = lineitemBuffer.getBuffer();
    Timer timer("QueryExecutionTime");
    timer.start();
    executablePipeline->execute(*runtimeWorkerContext, buffer);
    timer.snapshot("QueryExecutionTime");
    timer.pause();
    NES_INFO("QueryExecutionTime: " << timer);

    auto globalState = (GlobalAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(0);
    auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();
    ASSERT_EQ(sumState->sum, (int64_t) 204783021253);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter