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
class Query3Test : public Testing::NESBaseTest,
                   public ::testing::WithParamInterface<std::tuple<std::string, Schema::MemoryLayoutType>> {
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
        auto compiler = std::get<0>(param);
        NES_INFO("Setup Query6Test test case." << compiler);
        if (compiler == "INTERPRETER") {
            executionEngine = std::make_shared<InterpretationBasedPipelineExecutionEngine>();
        } else if (compiler == "MLIR") {
#ifdef USE_MLIR
            auto backend = std::make_shared<Nautilus::Backends::MLIR::MLIRPipelineCompilerBackend>();
            executionEngine = std::make_shared<CompilationBasedPipelineExecutionEngine>(backend);
#endif
        } else if (compiler == "FLOUNDER") {
#ifdef USE_FLOUNDER
            auto backend = std::make_shared<FlounderPipelineCompilerBackend>();
            executionEngine = std::make_shared<CompilationBasedPipelineExecutionEngine>(backend);
#endif
        } else if (compiler == "BABELFISH") {
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

TEST_P(Query3Test, tpchQ3) {
    Timer compilationTimer("QueryCompilationTime");
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto customersBuffer = TPCHUtil::getCustomers("/home/pgrulich/projects/tpch-dbgen/", bm, std::get<1>(this->GetParam()), true);

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);

    /**
     * Pipeline 1 with scan customers -> selection -> JoinBuild
     */
    Scan customersScan = Scan(customersBuffer.first);

    // c_mksegment = 'BUILDING' -> currently modeled as 1
    auto BUILDING = std::make_shared<ConstantIntegerExpression>(1);
    auto readC_mktsegment = std::make_shared<ReadFieldExpression>("c_mksegment");
    auto equalsExpression = std::make_shared<EqualsExpression>(readC_mktsegment, BUILDING);
    auto selection = std::make_shared<Selection>(equalsExpression);
    customersScan.setChild(selection);

    //  c_custkey = o_custkey
    // JoinBuild
    std::vector<ExpressionPtr> customersJoinBuildKeys = {std::make_shared<ReadFieldExpression>("c_custkey")};
    std::vector<ExpressionPtr> customersJoinBuildValues = {};
    NES::Experimental::HashMapFactory factory = NES::Experimental::HashMapFactory(bm, 8, 0, 100000);
    std::shared_ptr<NES::Experimental::Hashmap> customersHashMap = factory.createPtr();
    auto customersJoinBuild = std::make_shared<JoinBuild>(customersHashMap, customersJoinBuildKeys, customersJoinBuildValues);
    selection->setChild(customersJoinBuild);

    auto pipeline1 = std::make_shared<PhysicalOperatorPipeline>();
    pipeline1->setRootOperator(&customersScan);

    compilationTimer.start();
    auto executablePipeline1 = executionEngine->compile(pipeline1);
    compilationTimer.snapshot("Compile P1");
    compilationTimer.pause();

    /**
     * Pipeline 2 with scan orders -> selection -> JoinPrope with customers from pipeline 1
     */
    auto ordersBuffer = TPCHUtil::getOrders("/home/pgrulich/projects/tpch-dbgen/", bm, std::get<1>(this->GetParam()), true);
    Scan orderScan = Scan(ordersBuffer.first);

    //  o_orderdate < date '1995-03-15'
    auto const_1995_03_15 = std::make_shared<ConstantIntegerExpression>(19950315);
    auto readO_orderdate = std::make_shared<ReadFieldExpression>("o_orderdate");
    auto orderDateSelection =
        std::make_shared<Selection>(std::make_shared<LessThanExpression>(readO_orderdate, const_1995_03_15));
    orderScan.setChild(orderDateSelection);

    // join probe with customers
    std::vector<IR::Types::StampPtr> keyStamps = {IR::Types::StampFactory::createInt64Stamp()};
    std::vector<IR::Types::StampPtr> valueStamps = {};
    std::vector<ExpressionPtr> ordersProbeKeys = {std::make_shared<ReadFieldExpression>("o_custkey")};
    std::vector<ExpressionPtr> orderProbeValues = {std::make_shared<ReadFieldExpression>("o_orderkey"),
                                                   std::make_shared<ReadFieldExpression>("o_orderdate"),
                                                   std::make_shared<ReadFieldExpression>("o_shippriority")};

    std::vector<Record::RecordFieldIdentifier> joinProbeResults = {"o_custkey", "o_orderkey", "o_orderdate", "o_shippriority"};
    auto customersJoinProbe = std::make_shared<JoinProbe>(customersHashMap,
                                                          joinProbeResults,
                                                          ordersProbeKeys,
                                                          orderProbeValues,
                                                          keyStamps,
                                                          valueStamps);
    orderDateSelection->setChild(customersJoinProbe);

    // join build for order_customers
    std::vector<ExpressionPtr> order_customersJoinBuildKeys = {std::make_shared<ReadFieldExpression>("o_custkey")};
    std::vector<ExpressionPtr> order_customersJoinBuildValues = {std::make_shared<ReadFieldExpression>("o_orderdate"),
                                                                 std::make_shared<ReadFieldExpression>("o_shippriority")};
    NES::Experimental::HashMapFactory order_customersfactory = NES::Experimental::HashMapFactory(bm, 8, 16, 100000);
    std::shared_ptr<NES::Experimental::Hashmap> order_customersHashMap = order_customersfactory.createPtr();
    auto order_customersJoinBuild =
        std::make_shared<JoinBuild>(order_customersHashMap, order_customersJoinBuildKeys, order_customersJoinBuildValues);
    customersJoinProbe->setChild(order_customersJoinBuild);

    auto pipeline2 = std::make_shared<PhysicalOperatorPipeline>();
    pipeline2->setRootOperator(&orderScan);

    compilationTimer.start();
    auto executablePipeline2 = executionEngine->compile(pipeline2);
    compilationTimer.snapshot("Compile P2");
    compilationTimer.pause();

    /**
     * Pipeline 3 with scan lineitem -> selection -> JoinPrope with order_customers from pipeline 2 -> aggregation
     */
    auto lineitemsBuffer = TPCHUtil::getLineitems("/home/pgrulich/projects/tpch-dbgen/", bm, std::get<1>(this->GetParam()), true);
    Scan lineitemsScan = Scan(lineitemsBuffer.first);
    //   date '1995-03-15' < l_shipdate
    auto readL_shipdate = std::make_shared<ReadFieldExpression>("l_shipdate");
    auto shipDateSelection = std::make_shared<Selection>(std::make_shared<LessThanExpression>(const_1995_03_15, readL_shipdate));
    lineitemsScan.setChild(shipDateSelection);

    // join probe
    std::vector<IR::Types::StampPtr> order_customersKeyStamps = {IR::Types::StampFactory::createInt64Stamp()};
    std::vector<IR::Types::StampPtr> order_customersValueStamps = {IR::Types::StampFactory::createInt64Stamp(),
                                                                   IR::Types::StampFactory::createInt64Stamp()};
    //  l_orderkey,
    std::vector<ExpressionPtr> lineitemProbeKeys = {std::make_shared<ReadFieldExpression>("l_orderkey")};
    //  sum(l_extendedprice * (1 - l_discount)) as revenue,
    std::vector<ExpressionPtr> lineitemProbeValues = {std::make_shared<ReadFieldExpression>("l_extendedprice"),
                                                      std::make_shared<ReadFieldExpression>("l_discount")};
    std::vector<Record::RecordFieldIdentifier> lineItemjoinProbeResults = {"l_orderkey",
                                                                           "leftv1",
                                                                           "leftv2",
                                                                           "l_extendedprice",
                                                                           "l_discount"};
    auto lineitemJoinProbe = std::make_shared<JoinProbe>(order_customersHashMap,
                                                         lineItemjoinProbeResults,
                                                         lineitemProbeKeys,
                                                         lineitemProbeValues,
                                                         order_customersKeyStamps,
                                                         order_customersValueStamps);
    shipDateSelection->setChild(lineitemJoinProbe);

    NES::Experimental::HashMapFactory groupedAggregationFactory = NES::Experimental::HashMapFactory(bm, 24, 8, 100000);

    auto l_extendedpriceField = std::make_shared<ReadFieldExpression>("l_extendedprice");
    auto l_discountField = std::make_shared<ReadFieldExpression>("l_discount");
    auto oneConst = std::make_shared<ConstantIntegerExpression>(1);
    auto subExpression = std::make_shared<SubExpression>(oneConst, l_discountField);
    auto mulExpression = std::make_shared<MulExpression>(l_extendedpriceField, subExpression);
    auto sumAggFunction = std::make_shared<SumFunction>(mulExpression, IR::Types::StampFactory::createInt64Stamp());

    std::vector<ExpressionPtr> keys = {std::make_shared<ReadFieldExpression>("l_orderkey")};
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<GroupedAggregation>(groupedAggregationFactory, keys, functions);
    lineitemJoinProbe->setChild(aggregation);

    auto pipeline3 = std::make_shared<PhysicalOperatorPipeline>();
    pipeline3->setRootOperator(&lineitemsScan);
    compilationTimer.start();
    auto executablePipeline3 = executionEngine->compile(pipeline3);
    compilationTimer.snapshot("Compile P3");
    compilationTimer.pause();

    executablePipeline1->setup();
    executablePipeline2->setup();
    executablePipeline3->setup();
#ifdef USE_BABELFISH

    uint64_t warmup = 10000;
    for (auto i = 0ull; i < warmup; i++) {
        Timer timer("QueryExecutionTime");
        timer.start();
        auto buffer1 = customersBuffer.second.getBuffer();
        executablePipeline1->execute(*runtimeWorkerContext, buffer1);
        auto buffer2 = ordersBuffer.second.getBuffer();
        executablePipeline2->execute(*runtimeWorkerContext, buffer2);
        auto buffer3 = lineitemsBuffer.second.getBuffer();
        executablePipeline3->execute(*runtimeWorkerContext, buffer3);
        timer.snapshot("Execute Warmup");
        timer.pause();
        NES_INFO("QueryExecutionTime Warmup: " << timer);
        customersHashMap->clear();
        order_customersHashMap->clear();
        auto tag = *((int64_t*) aggregation.get());
        auto globalState = (GroupedAggregationState*) executablePipeline3->getExecutionContext()->getGlobalOperatorState(tag);
        globalState->threadLocalAggregationSlots[0]->clear();
    }

#endif

    Timer timer("QueryExecutionTime");

    {
        auto buffer = customersBuffer.second.getBuffer();
        timer.start();
        executablePipeline1->execute(*runtimeWorkerContext, buffer);
        timer.snapshot("Execute P1");
        timer.pause();
        EXPECT_EQ(customersHashMap->numberOfEntries(), 30142);
    }
    {
        auto buffer = ordersBuffer.second.getBuffer();
        timer.start();
        executablePipeline2->execute(*runtimeWorkerContext, buffer);
        timer.snapshot("Execute P2");
        timer.pause();
        EXPECT_EQ(order_customersHashMap->numberOfEntries(), 147126);
    }

    {
        auto buffer = lineitemsBuffer.second.getBuffer();
        timer.start();
        executablePipeline3->execute(*runtimeWorkerContext, buffer);
        timer.snapshot("Execute P3");
        timer.pause();
        auto tag = *((int64_t*) aggregation.get());
        auto globalState = (GroupedAggregationState*) executablePipeline3->getExecutionContext()->getGlobalOperatorState(tag);
        auto currentSize = globalState->threadLocalAggregationSlots[0].get()->numberOfEntries();
        EXPECT_EQ(currentSize, (int64_t) 11620);
    }

    NES_INFO("QueryCompilationTime: " << compilationTimer);
    NES_INFO("QueryExecutionTime: " << timer);
}

#ifdef USE_BABELFISH
INSTANTIATE_TEST_CASE_P(testTPCHQ3,
                        Query3Test,
                        ::testing::Combine(::testing::Values("BABELFISH"),
                                           ::testing::Values(Schema::MemoryLayoutType::ROW_LAYOUT,
                                                             Schema::MemoryLayoutType::COLUMNAR_LAYOUT)),
                        [](const testing::TestParamInfo<Query3Test::ParamType>& info) {
                            auto layout = std::get<1>(info.param);
                            if (layout == Schema::ROW_LAYOUT) {
                                return std::get<0>(info.param) + "_ROW";
                            } else {
                                return std::get<0>(info.param) + "_COLUMNAR";
                            }
                        });

#else

INSTANTIATE_TEST_CASE_P(testTPCHQ3,
                        Query3Test,
                        ::testing::Combine(::testing::Values("INTERPRETER", "MLIR", "FLOUNDER"),
                                           ::testing::Values(Schema::MemoryLayoutType::ROW_LAYOUT,
                                                             Schema::MemoryLayoutType::COLUMNAR_LAYOUT)),
                        [](const testing::TestParamInfo<Query3Test::ParamType>& info) {
                            auto layout = std::get<1>(info.param);
                            if (layout == Schema::ROW_LAYOUT) {
                                return std::get<0>(info.param) + "_ROW";
                            } else {
                                return std::get<0>(info.param) + "_COLUMNAR";
                            }
                        });
#endif

}// namespace NES::Nautilus