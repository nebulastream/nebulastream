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
class Query1Test : public Testing::NESBaseTest,
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

TEST_P(Query1Test, tpchQ1) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto lineitemBuffer = TPCHUtil::getLineitems("/home/pgrulich/projects/tpch-dbgen/", bm, std::get<1>(this->GetParam()), true);
    auto ordersBuffer = TPCHUtil::getOrders("/home/pgrulich/projects/tpch-dbgen/", bm, std::get<1>(this->GetParam()), true);

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);
    Scan scan = Scan(lineitemBuffer.first);

    /*
     *   l_shipdate <= date '1998-12-01' - interval '90' day
     *
     *   1998-09-02
     */
    auto const_1998_09_02 = std::make_shared<ConstantIntegerExpression>(19980831);
    auto readShipdate = std::make_shared<ReadFieldExpression>("l_shipdate");
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
    auto l_returnflagField = std::make_shared<ReadFieldExpression>("l_returnflag");
    auto l_linestatusFiled = std::make_shared<ReadFieldExpression>("l_linestatus");

    //  sum(l_quantity) as sum_qty,
    auto l_quantityField = std::make_shared<ReadFieldExpression>("l_quantity");
    auto sumAggFunction1 = std::make_shared<SumFunction>(l_quantityField, IR::Types::StampFactory::createInt64Stamp());

    // sum(l_extendedprice) as sum_base_price,
    auto l_extendedpriceField = std::make_shared<ReadFieldExpression>("l_extendedprice");
    auto sumAggFunction2 = std::make_shared<SumFunction>(l_extendedpriceField, IR::Types::StampFactory::createInt64Stamp());

    // sum(l_extendedprice * (1 - l_discount)) as sum_disc_price
    auto l_discountField = std::make_shared<ReadFieldExpression>("l_discount");
    auto oneConst = std::make_shared<ConstantIntegerExpression>(1);
    auto subExpression = std::make_shared<SubExpression>(oneConst, l_discountField);
    auto mulExpression = std::make_shared<MulExpression>(l_extendedpriceField, subExpression);
    auto sumAggFunction3 = std::make_shared<SumFunction>(mulExpression, IR::Types::StampFactory::createInt64Stamp());

    //  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    auto l_taxField = std::make_shared<ReadFieldExpression>("l_tax");
    auto addExpression = std::make_shared<AddExpression>(oneConst, l_taxField);
    auto mulExpression2 = std::make_shared<MulExpression>(mulExpression, addExpression);
    auto sumAggFunction4 = std::make_shared<SumFunction>(mulExpression2, IR::Types::StampFactory::createInt64Stamp());

    auto sumAggFunction5 = std::make_shared<SumFunction>(l_discountField, IR::Types::StampFactory::createInt64Stamp());

    //  count() as avg_price,
    auto countFunction = std::make_shared<CountFunction>();

    std::vector<std::shared_ptr<AggregationFunction>> functions =
        {sumAggFunction1, sumAggFunction2, sumAggFunction3, sumAggFunction4, sumAggFunction5, countFunction};
    std::vector<ExpressionPtr> keys = {l_returnflagField, l_linestatusFiled};
    // 5 * 3 + 3 * 16 = 88 value size
    NES::Experimental::HashMapFactory factory = NES::Experimental::HashMapFactory(bm, 16, 48, 1000);
    auto aggregation = std::make_shared<GroupedAggregation>(factory, keys, functions);
    scan.setChild(aggregation);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine->compile(pipeline);

    executablePipeline->setup();

    auto buffer = lineitemBuffer.second.getBuffer();

#ifdef USE_BABELFISH
    uint64_t warmup = 1000;
    for (auto i = 0ull; i < warmup; i++) {
        executablePipeline->execute(*runtimeWorkerContext, buffer);
    }
    {
        auto tag = *((int64_t*) aggregation.get());
        auto globalState = (GroupedAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(tag);
        globalState->threadLocalAggregationSlots[0].get()->clear();
    }
#endif

    Timer timer("QueryExecutionTime");
    timer.start();
    timer.snapshot("Start");
    executablePipeline->execute(*runtimeWorkerContext, buffer);
    timer.snapshot("Execute");
    timer.pause();
    NES_INFO("QueryExecutionTime: " << timer);
    auto tag = *((int64_t*) aggregation.get());
    auto globalState = (GroupedAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(tag);
    auto currentSize = globalState->threadLocalAggregationSlots[0].get()->numberOfEntries();
    ASSERT_EQ(currentSize, (int64_t) 4);

    auto entryBuffer = globalState->threadLocalAggregationSlots[0].get()->getEntries().get()[0];

    struct REntry {
        void* next;
        std::int64_t hash;
        std::int64_t k1;
        std::int64_t k2;
        GlobalSumState ag1;
        GlobalSumState ag2;
        GlobalSumState ag3;
        GlobalSumState ag4;
        GlobalCountState ag8;
    };

    auto entries = entryBuffer[0].getBuffer<REntry>();
    for (auto i = 0; i < 4; i++) {
        NES_DEBUG("K1 " << entries[i].k1);
        NES_DEBUG("K2 " << entries[i].k2);
        NES_DEBUG("ag1 " << entries[i].ag1.sum);
        NES_DEBUG("ag2 " << entries[i].ag2.sum);
        NES_DEBUG("ag3 " << entries[i].ag3.sum);
        NES_DEBUG("ag4 " << entries[i].ag4.sum);
        NES_DEBUG("ag8 " << entries[i].ag8.count);
        NES_DEBUG("\n");
        NES_DEBUG("\n");
    }

    //ASSERT_EQ(entries[0].ag1, 37719753);
    //ASSERT_EQ(entries[0].ag2, 5656804138090);
}
#ifdef USE_BABELFISH
INSTANTIATE_TEST_CASE_P(testTPCHQ1,
                        Query1Test,
                        ::testing::Combine(::testing::Values("BABELFISH"),
                                           ::testing::Values(Schema::MemoryLayoutType::ROW_LAYOUT,
                                                             Schema::MemoryLayoutType::COLUMNAR_LAYOUT)),
                        [](const testing::TestParamInfo<Query1Test::ParamType>& info) {
                            auto layout = std::get<1>(info.param);
                            if (layout == Schema::ROW_LAYOUT) {
                                return std::get<0>(info.param) + "_ROW";
                            } else {
                                return std::get<0>(info.param) + "_COLUMNAR";
                            }
                        });

#else
INSTANTIATE_TEST_CASE_P(testTPCHQ1,
                        Query1Test,
                        ::testing::Combine(::testing::Values("INTERPRETER", "MLIR", "FLOUNDER"),
                                           ::testing::Values(Schema::MemoryLayoutType::ROW_LAYOUT,
                                                             Schema::MemoryLayoutType::COLUMNAR_LAYOUT)),
                        [](const testing::TestParamInfo<Query1Test::ParamType>& info) {
                            auto layout = std::get<1>(info.param);
                            if (layout == Schema::ROW_LAYOUT) {
                                return std::get<0>(info.param) + "_ROW";
                            } else {
                                return std::get<0>(info.param) + "_COLUMNAR";
                            }
                        });
#endif
}// namespace NES::Nautilus