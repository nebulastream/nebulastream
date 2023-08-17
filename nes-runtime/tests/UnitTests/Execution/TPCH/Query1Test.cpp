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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchKeyedAggregationHandler.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TPCH/Query1.hpp>
#include <TPCH/TPCHTableGenerator.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {
using namespace Expressions;
using namespace Operators;
class TPCH_Q1 : public Testing::TestWithErrorHandling, public AbstractPipelineExecutionTest {

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
        NES::Logger::setupLogging("TPCH_Q1.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("Setup TPCH_Q1 test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        NES_INFO("Setup TPCH_Q1 test case.");
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
 * select
 * l_returnflag
 * l_linestatus,
 * sum(l_quantity) as sum_qty,
 * sum(l_extendedprice) as sum_base_price,
 * sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
 * sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
 * avg(l_quantity) as avg_qty,
 * avg(l_extendedprice) as avg_price,
 * avg(l_discount) as avg_disc,
 * count(*) as count_order
 * from  lineite
 * where l_shipdate <= date '1998-12-01' - interval '90' day
 * group by l_returnflag,  l_linestatus
 */
TEST_P(TPCH_Q1, aggregationPipeline) {
    auto plan = TPCH_Query1::getPipelinePlan(tables, bm);
    auto& lineitems = tables[TPCHTable::LineItem];

    // process table
    NES_INFO("Process {} chunks", lineitems->getChunks().size());
    Timer timer("Q1");
    timer.start();
    auto pipeline1 = plan.getPipeline(0);
    auto aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
    aggExecutablePipeline->setup(*pipeline1.ctx);
    timer.snapshot("setup");
    for (auto& chunk : lineitems->getChunks()) {
        aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
    }
    timer.snapshot("execute agg");
    aggExecutablePipeline->stop(*pipeline1.ctx);
    timer.snapshot("stop");
    timer.pause();
    std::stringstream timerAsString;
    timerAsString << timer;
    NES_INFO("Query Runtime:\n{}", timerAsString.str());
    // compare results
    auto aggregationHandler = pipeline1.ctx->getOperatorHandler<BatchKeyedAggregationHandler>(0);
    // TODO extend for multi thread support
    auto hashTable = aggregationHandler->getThreadLocalStore(wc->getId());
    auto numberOfKeys = hashTable->getCurrentSize();
    EXPECT_EQ(numberOfKeys, 4);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        TPCH_Q1,
                        ::testing::Values("BCInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<TPCH_Q1::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution