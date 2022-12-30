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
class Query6Test : public Testing::NESBaseTest,
                   public ::testing::WithParamInterface<std::tuple<std::string, Schema::MemoryLayoutType>> {
  public:
    Tracing::SSACreationPhase ssaCreationPhase;
    Tracing::TraceToIRConversionPhase irCreationPhase;
    std::shared_ptr<ExecutionEngine::Experimental::PipelineExecutionEngine> executionEngine;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("Query6Test.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup Query6Test test class.");
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

TEST_P(Query6Test, tpchQ6) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = TPCHUtil::getLineitems("/home/pgrulich/projects/tpch-dbgen/", bm, std::get<1>(this->GetParam()), true);
    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);
    Scan scan = Scan(lineitemBuffer.first, {"l_shipdate", "l_discount", "l_quantity", "l_extendedprice"});
    /*
     *   l_shipdate >= date '1994-01-01'
     *   and l_shipdate < date '1995-01-01'
     */
    auto const_1994_01_01 = std::make_shared<ConstantIntegerExpression>(19940101);
    auto const_1995_01_01 = std::make_shared<ConstantIntegerExpression>(19950101);
    auto readShipdate = std::make_shared<ReadFieldExpression>("l_shipdate");
    auto lessThanExpression1 = std::make_shared<LessThanExpression>(const_1994_01_01, readShipdate);
    auto lessThanExpression2 = std::make_shared<LessThanExpression>(readShipdate, const_1995_01_01);
    auto andExpression = std::make_shared<AndExpression>(lessThanExpression1, lessThanExpression2);

    // l_discount between 0.06 - 0.01 and 0.06 + 0.01
    auto readDiscount = std::make_shared<ReadFieldExpression>("l_discount");
    auto const_0_05 = std::make_shared<ConstantIntegerExpression>(4);
    auto const_0_07 = std::make_shared<ConstantIntegerExpression>(8);
    auto lessThanExpression3 = std::make_shared<LessThanExpression>(const_0_05, readDiscount);
    auto lessThanExpression4 = std::make_shared<LessThanExpression>(readDiscount, const_0_07);
    auto andExpression2 = std::make_shared<AndExpression>(lessThanExpression3, lessThanExpression4);

    // l_quantity < 24
    auto const_24 = std::make_shared<ConstantIntegerExpression>(24);
    auto readQuantity = std::make_shared<ReadFieldExpression>("l_quantity");
    auto lessThanExpression5 = std::make_shared<LessThanExpression>(readQuantity, const_24);
    auto andExpression3 = std::make_shared<AndExpression>(andExpression, andExpression2);
    auto andExpression4 = std::make_shared<AndExpression>(andExpression3, lessThanExpression5);

    auto selection = std::make_shared<Selection>(andExpression4);
    scan.setChild(selection);

    // sum(l_extendedprice)
    auto aggField = std::make_shared<ReadFieldExpression>("l_extendedprice");
    auto sumAggFunction = std::make_shared<SumFunction>(aggField, IR::Types::StampFactory::createInt64Stamp());
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    selection->setChild(aggregation);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine->compile(pipeline);

    executablePipeline->setup();

    auto buffer = lineitemBuffer.second.getBuffer();
#ifdef USE_BABELFISH
    uint64_t warmup = 100;
    for (auto i = 0ull; i < warmup; i++) {
        executablePipeline->execute(*runtimeWorkerContext, buffer);
    }
    {
        auto tag = *((int64_t*) aggregation.get());
        auto globalState = (GlobalAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(tag);
        auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();
        sumState->sum = 0;
    }
#endif

    NES_INFO("Start Execution");
    Timer timer("QueryExecutionTime");
    timer.start();
    executablePipeline->execute(*runtimeWorkerContext, buffer);
    timer.snapshot("QueryExecutionTime");
    timer.pause();
    NES_INFO("QueryExecutionTime: " << timer);
    auto tag = *((int64_t*) aggregation.get());
    auto globalState = (GlobalAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(tag);
    auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();

    ASSERT_EQ(sumState->sum, (int64_t) 204783021253);
}

TEST_P(Query6Test, DISABLED_tpchQ6and) {
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = TPCHUtil::getLineitems("/home/pgrulich/projects/tpch-dbgen/", bm, std::get<1>(this->GetParam()), true);

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);
    Scan scan = Scan(lineitemBuffer.first);
    /*
     *   l_shipdate >= date '1994-01-01'
     *   and l_shipdate < date '1995-01-01'
     */
    auto const_1994_01_01 = std::make_shared<ConstantIntegerExpression>(19940101);
    auto const_1995_01_01 = std::make_shared<ConstantIntegerExpression>(19950101);
    auto readShipdate = std::make_shared<ReadFieldExpression>("l_shipdate");
    auto lessThanExpression1 = std::make_shared<LessThanExpression>(const_1994_01_01, readShipdate);
    auto lessThanExpression2 = std::make_shared<LessThanExpression>(readShipdate, const_1995_01_01);
    auto andExpression = std::make_shared<AndExpression>(lessThanExpression1, lessThanExpression2);
    auto selection1 = std::make_shared<Selection>(andExpression);
    scan.setChild(selection1);

    // l_discount between 0.06 - 0.01 and 0.06 + 0.01
    auto readDiscount = std::make_shared<ReadFieldExpression>("l_discount");
    auto const_0_05 = std::make_shared<ConstantIntegerExpression>(4);
    auto const_0_07 = std::make_shared<ConstantIntegerExpression>(8);
    auto lessThanExpression3 = std::make_shared<LessThanExpression>(const_0_05, readDiscount);
    auto lessThanExpression4 = std::make_shared<LessThanExpression>(readDiscount, const_0_07);
    auto andExpression2 = std::make_shared<AndExpression>(lessThanExpression3, lessThanExpression4);

    // l_quantity < 24
    auto const_24 = std::make_shared<ConstantIntegerExpression>(24);
    auto readQuantity = std::make_shared<ReadFieldExpression>("l_quantity");
    auto lessThanExpression5 = std::make_shared<LessThanExpression>(readQuantity, const_24);
    auto andExpression3 = std::make_shared<AndExpression>(andExpression2, lessThanExpression5);

    auto selection2 = std::make_shared<Selection>(andExpression3);
    selection1->setChild(selection2);
    // sum(l_extendedprice)
    auto aggField = std::make_shared<ReadFieldExpression>("l_extendedprice");
    auto sumAggFunction = std::make_shared<SumFunction>(aggField, IR::Types::StampFactory::createInt64Stamp());
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    selection2->setChild(aggregation);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(&scan);

    auto executablePipeline = executionEngine->compile(pipeline);

    executablePipeline->setup();
    auto buffer = lineitemBuffer.second.getBuffer();
#ifdef USE_BABELFISH
    uint64_t warmup = 100;
    for (auto i = 0ull; i < warmup; i++) {
        executablePipeline->execute(*runtimeWorkerContext, buffer);
    }
    {
        auto tag = *((int64_t*) aggregation.get());
        auto globalState = (GlobalAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(tag);
        auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();
        sumState->sum = 0;
    }
#endif

    NES_INFO("Start Execution");

    Timer timer("QueryExecutionTime");
    timer.start();
    executablePipeline->execute(*runtimeWorkerContext, buffer);
    timer.snapshot("QueryExecutionTime");
    timer.pause();
    NES_INFO("QueryExecutionTime: " << timer);
    auto tag = *((int64_t*) aggregation.get());
    auto globalState = (GlobalAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(tag);
    auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();
    ASSERT_EQ(sumState->sum, (int64_t) 204783021253);
}
#ifdef USE_BABELFISH
INSTANTIATE_TEST_CASE_P(testTPCHQ6,
                        Query6Test,
                        ::testing::Combine(::testing::Values("BABELFISH"),
                                           ::testing::Values(Schema::MemoryLayoutType::ROW_LAYOUT,
                                                             Schema::MemoryLayoutType::COLUMNAR_LAYOUT)),
                        [](const testing::TestParamInfo<Query6Test::ParamType>& info) {
                            auto layout = std::get<1>(info.param);
                            if (layout == Schema::ROW_LAYOUT) {
                                return std::get<0>(info.param) + "_ROW";
                            } else {
                                return std::get<0>(info.param) + "_COLUMNAR";
                            }
                        });

#else
INSTANTIATE_TEST_CASE_P(testTPCHQ6,
                        Query6Test,
                        ::testing::Combine(::testing::Values("INTERPRETER"),
                                           // ::testing::Combine(::testing::Values("INTERPRETER", "MLIR", "FLOUNDER"),
                                           ::testing::Values(Schema::MemoryLayoutType::ROW_LAYOUT)),
                        //  Schema::MemoryLayoutType::COLUMNAR_LAYOUT)),
                        [](const testing::TestParamInfo<Query6Test::ParamType>& info) {
                            auto layout = std::get<1>(info.param);
                            if (layout == Schema::ROW_LAYOUT) {
                                return std::get<0>(info.param) + "_ROW";
                            } else {
                                return std::get<0>(info.param) + "_COLUMNAR";
                            }
                        });
#endif
}// namespace NES::Nautilus