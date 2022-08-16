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
#include <Experimental/Interpreter/Operators/AggregationFunction.hpp>
#include <Experimental/Interpreter/Operators/Emit.hpp>
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

#ifdef USE_MLIR
TEST_P(QueryExecutionTest, emitQueryTest) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);

    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::UINT64);
    schema->addField("f2", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    Scan scan = Scan(memoryLayout);
    auto emit = std::make_shared<Emit>(memoryLayout);
    scan.setChild(emit);

    auto memRef = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    RecordBuffer recordBuffer = RecordBuffer(memRef);

    auto memRefPCTX = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    memRefPCTX.ref = Trace::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    auto wctxRefPCTX = Value<MemRef>(std::make_unique<MemRef>(MemRef(0)));
    wctxRefPCTX.ref = Trace::ValueRef(INT32_MAX, 1, IR::Types::StampFactory::createAddressStamp());
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
    auto mlirUtility = new MLIR::MLIRUtility("", false);
    MLIR::MLIRUtility::DebugFlags df = {false, false, false};
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, false);
}
#endif

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

TEST_P(QueryExecutionTest, tpchQ1) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = TPCHUtil::getLineitems("/home/pgrulich/projects/tpch-dbgen/", bm, true);

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);
    Scan scan = Scan(lineitemBuffer.first);

    /*
     *   l_shipdate <= date '1998-12-01' - interval '90' day
     *
     *   1998-09-02
     */
    auto const_1998_09_02 = std::make_shared<ConstantIntegerExpression>(19980902);
    auto readShipdate = std::make_shared<ReadFieldExpression>(10);
    auto lessThanExpression1 = std::make_shared<LessThanExpression>(readShipdate, const_1998_09_02);
    auto selection = std::make_shared<Selection>(lessThanExpression1);
    scan.setChild(selection);

    /**
     *
     * group by
        l_returnflag,
        l_linestatus

        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
     */
    auto l_returnflagField = std::make_shared<ReadFieldExpression>(8);
    auto l_linestatusFiled = std::make_shared<ReadFieldExpression>(9);

    //  sum(l_quantity) as sum_qty,
    auto l_quantityField = std::make_shared<ReadFieldExpression>(4);
    auto sumAggFunction1 = std::make_shared<SumFunction>(l_quantityField, IR::Types::StampFactory::createInt64Stamp());

    // sum(l_extendedprice) as sum_base_price,
    auto l_extendedpriceField = std::make_shared<ReadFieldExpression>(5);
    auto sumAggFunction2 = std::make_shared<SumFunction>(l_extendedpriceField, IR::Types::StampFactory::createInt64Stamp());

    // sum(l_extendedprice * (1 - l_discount)) as sum_disc_price
    auto l_discountField = std::make_shared<ReadFieldExpression>(6);
    auto oneConst = std::make_shared<ConstantIntegerExpression>(1);
    auto subExpression = std::make_shared<SubExpression>(oneConst, l_discountField);
    auto mulExpression = std::make_shared<MulExpression>(l_extendedpriceField, subExpression);
    auto sumAggFunction3 = std::make_shared<SumFunction>(mulExpression, IR::Types::StampFactory::createInt64Stamp());

    //  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    auto l_taxField = std::make_shared<ReadFieldExpression>(6);
    auto addExpression = std::make_shared<AddExpression>(oneConst, l_taxField);
    auto mulExpression2 = std::make_shared<MulExpression>(mulExpression, addExpression);
    auto sumAggFunction4 = std::make_shared<SumFunction>(mulExpression2, IR::Types::StampFactory::createInt64Stamp());

    // avg(l_quantity) as avg_qty,
    auto sumAggFunction5 = std::make_shared<SumFunction>(l_quantityField, IR::Types::StampFactory::createInt64Stamp());

    //  avg(l_extendedprice) as avg_price,
    auto sumAggFunction6 = std::make_shared<SumFunction>(l_quantityField, IR::Types::StampFactory::createInt64Stamp());

    //  avg(l_discount) as avg_price,
    auto sumAggFunction7 = std::make_shared<SumFunction>(l_quantityField, IR::Types::StampFactory::createInt64Stamp());

    //  count() as avg_price,
    auto sumAggFunction8 = std::make_shared<SumFunction>(l_quantityField, IR::Types::StampFactory::createInt64Stamp());

    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction1,
                                                                   sumAggFunction2,
                                                                   sumAggFunction3,
                                                                   sumAggFunction4,
                                                                   sumAggFunction5,
                                                                   sumAggFunction6,
                                                                   sumAggFunction7,
                                                                   sumAggFunction8};
    std::vector<ExpressionPtr> keys = {l_returnflagField, l_linestatusFiled};
    NES::Experimental::HashMapFactory factory = NES::Experimental::HashMapFactory(bm, 16, 64, 1000);
    auto aggregation = std::make_shared<GroupedAggregation>(factory, keys, functions);
    selection->setChild(aggregation);

    auto executionEngine = CompilationBasedPipelineExecutionEngine(backend);
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine.compile(pipeline);

    executablePipeline->setup();

    auto buffer = lineitemBuffer.second.getBuffer();

    Timer timer("QueryExecutionTime");
    timer.start();
    executablePipeline->execute(*runtimeWorkerContext, buffer);
    timer.snapshot("QueryExecutionTime");
    timer.pause();
    NES_INFO("QueryExecutionTime: " << timer);

    auto globalState = (GroupedAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(0);
    auto currentSize = globalState->threadLocalAggregationSlots[0].get()->numberOfEntries();
    ASSERT_EQ(currentSize, (int64_t) 4);

    auto entryBuffer = globalState->threadLocalAggregationSlots[0].get()->getEntries().get()[0];

}

TEST_P(QueryExecutionTest, tpchQ6_agg) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = TPCHUtil::getLineitems("/home/pgrulich/projects/tpch-dbgen/", bm);

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);
    Scan scan = Scan(lineitemBuffer.first);
    /*
     *   l_shipdate >= date '1994-01-01'
     *   and l_shipdate < date '1995-01-01'
     */
    auto const_1994_01_01 = std::make_shared<ConstantIntegerExpression>(19940101);
    auto const_1995_01_01 = std::make_shared<ConstantIntegerExpression>(19950101);
    auto readShipdate = std::make_shared<ReadFieldExpression>(3);
    auto lessThanExpression1 = std::make_shared<LessThanExpression>(const_1994_01_01, readShipdate);
    auto selection1 = std::make_shared<Selection>(lessThanExpression1);
    scan.setChild(selection1);
    auto lessThanExpression2 = std::make_shared<LessThanExpression>(readShipdate, const_1995_01_01);
    auto selection2 = std::make_shared<Selection>(lessThanExpression2);
    selection1->setChild(selection2);

    // l_discount between 0.06 - 0.01 and 0.06 + 0.01
    auto readDiscount = std::make_shared<ReadFieldExpression>(2);
    auto const_0_05 = std::make_shared<ConstantIntegerExpression>(4);
    auto const_0_07 = std::make_shared<ConstantIntegerExpression>(8);
    auto lessThanExpression3 = std::make_shared<LessThanExpression>(const_0_05, readDiscount);
    auto selection3 = std::make_shared<Selection>(lessThanExpression3);
    selection2->setChild(selection3);
    auto lessThanExpression4 = std::make_shared<LessThanExpression>(readDiscount, const_0_07);
    auto selection4 = std::make_shared<Selection>(lessThanExpression4);
    selection3->setChild(selection4);

    // l_quantity < 24
    auto const_24 = std::make_shared<ConstantIntegerExpression>(24);
    auto readQuantity = std::make_shared<ReadFieldExpression>(0);
    auto lessThanExpression5 = std::make_shared<LessThanExpression>(readQuantity, const_24);
    auto selection5 = std::make_shared<Selection>(lessThanExpression5);
    selection4->setChild(selection5);

    // sum(l_extendedprice)
    auto aggField = std::make_shared<ReadFieldExpression>(1);
    auto sumAggFunction = std::make_shared<SumFunction>(aggField, IR::Types::StampFactory::createInt64Stamp());
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    selection5->setChild(aggregation);

    auto executionEngine = CompilationBasedPipelineExecutionEngine(backend);
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine.compile(pipeline);

    executablePipeline->setup();

    auto buffer = lineitemBuffer.second.getBuffer();

    for (auto i = 0; i < 10; i++) {
        Timer timer("QueryExecutionTime");
        timer.start();
        executablePipeline->execute(*runtimeWorkerContext, buffer);
        timer.snapshot("QueryExecutionTime");
        timer.pause();
        NES_INFO("QueryExecutionTime: " << timer);
    }

    auto globalState = (GlobalAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(0);
    auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();
    ASSERT_EQ(sumState->sum, (int64_t) 19599269581);
}

TEST_F(QueryExecutionTest, tpchQ6) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = TPCHUtil::getLineitems("/home/pgrulich/projects/tpch-dbgen/", bm);

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);
    Scan scan = Scan(lineitemBuffer.first);
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
    auto sumAggFunction = std::make_shared<SumFunction>(aggField, IR::Types::StampFactory::createInt64Stamp());
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    selection->setChild(aggregation);

    auto executionEngine = CompilationBasedPipelineExecutionEngine(backend);
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine.compile(pipeline);

    executablePipeline->setup();

    auto buffer = lineitemBuffer.second.getBuffer();
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

TEST_P(QueryExecutionTest, tpchQ6and) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = TPCHUtil::getLineitems("/home/pgrulich/projects/tpch-dbgen/", bm);

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);
    Scan scan = Scan(lineitemBuffer.first);
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
    auto selection1 = std::make_shared<Selection>(andExpression);
    scan.setChild(selection1);

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
    auto andExpression3 = std::make_shared<AndExpression>(andExpression2, lessThanExpression5);

    auto selection2 = std::make_shared<Selection>(andExpression3);
    selection1->setChild(selection2);
    // sum(l_extendedprice)
    auto aggField = std::make_shared<ReadFieldExpression>(1);
    auto sumAggFunction = std::make_shared<SumFunction>(aggField, IR::Types::StampFactory::createInt64Stamp());
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    selection2->setChild(aggregation);

    auto executionEngine = CompilationBasedPipelineExecutionEngine(backend);
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine.compile(pipeline);

    executablePipeline->setup();

    auto buffer = lineitemBuffer.second.getBuffer();
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

INSTANTIATE_TEST_CASE_P(testSingleNodeConcurrentTumblingWindowTest,
                        QueryExecutionTest,
                        ::testing::Values("MLIR", "Flounder"),
                        [](const testing::TestParamInfo<QueryExecutionTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::ExecutionEngine::Experimental::Interpreter