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
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/MulExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/SubExpression.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationScan.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TPCH/Query6.hpp>
#include <TPCH/TPCHTableGenerator.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {
using namespace Expressions;
using namespace Operators;
class TPCH_Q6 : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {

  public:
    TPCH_Scale_Factor targetScaleFactor = TPCH_Scale_Factor::F0_01;
    Nautilus::CompilationOptions options;
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::BufferManager> table_bm;
    std::shared_ptr<WorkerContext> wc;
    std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>> tables;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TPCH_Q6.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("Setup TPCH_Q6 test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup TPCH_Q6 test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        table_bm = std::make_shared<Runtime::BufferManager>(8 * 1024 * 1024, 1000);
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
        tables = TPCHTableGenerator(table_bm, targetScaleFactor).generate();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TPCH_Q6 test class."); }
};

/**
 * @brief Emit operator that emits a row oriented tuple buffer.
 */
TEST_P(TPCH_Q6, aggregationPipeline) {

    auto& lineitems = tables[TPCHTable::LineItem];

    auto plan = TPCH_Query6::getPipelinePlan(tables, bm);

    // process table
    NES_INFO("Process {} chunks", lineitems->getChunks().size());
    Timer timer("Q6");
    timer.start();
    auto pipeline1 = plan.getPipeline(0);
    auto pipeline2 = plan.getPipeline(1);
    auto aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
    auto emitExecutablePipeline = provider->create(pipeline2.pipeline, options);
    aggExecutablePipeline->setup(*pipeline1.ctx);
    emitExecutablePipeline->setup(*pipeline2.ctx);
    timer.snapshot("setup");
    for (auto& chunk : lineitems->getChunks()) {
        aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
    }
    timer.snapshot("execute agg");
    auto dummyBuffer = TupleBuffer();
    emitExecutablePipeline->execute(dummyBuffer, *pipeline2.ctx, *wc);
    timer.snapshot("execute emit");

    aggExecutablePipeline->stop(*pipeline1.ctx);
    emitExecutablePipeline->stop(*pipeline2.ctx);
    timer.snapshot("stop");
    timer.pause();
    std::stringstream timerAsString;
    timerAsString << timer;
    NES_INFO("Query Runtime:\n{}", timerAsString.str());
    // compare results
    auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    resultSchema->addField("revenue", BasicType::FLOAT32);
    auto resultLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(resultLayout, pipeline2.ctx->buffers[0]);
    if (targetScaleFactor == TPCH_Scale_Factor::F1) {
        NES_INFO("{:f}", resultDynamicBuffer[0][0].read<float>());
        EXPECT_NEAR(resultDynamicBuffer[0][0].read<float>(), 122817720.0f, 200);
    } else if (targetScaleFactor == TPCH_Scale_Factor::F0_01) {
        NES_INFO("{:f}", resultDynamicBuffer[0][0].read<float>());
        EXPECT_NEAR(resultDynamicBuffer[0][0].read<float>(), 1192973.625f, 200);
    } else {
        GTEST_FAIL();
    }
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        TPCH_Q6,
                        ::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<TPCH_Q6::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution