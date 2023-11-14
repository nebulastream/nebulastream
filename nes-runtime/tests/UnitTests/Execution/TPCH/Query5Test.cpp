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
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/MulExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/SubExpression.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchKeyedAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchKeyedAggregationHandler.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinBuild.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinHandler.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinProbe.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TPCH/Query5.hpp>
#include <TPCH/TPCHTableGenerator.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {
using namespace Expressions;
using namespace Operators;
class TPCH_Q5 : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {

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
        NES::Logger::setupLogging("TPCH_Q5.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("Setup TPCH_Q5 test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup TPCH_Q5 test case.");
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
    static void TearDownTestCase() { NES_INFO("Tear down TPCH_Q5 test class."); }
};

TEST_P(TPCH_Q5, joinPipeline) {
    auto& customers = tables[TPCHTable::Customer];
    auto& orders = tables[TPCHTable::Orders];
    auto& lineItems = tables[TPCHTable::LineItem];
    auto& suppliers = tables[TPCHTable::Supplier];
    auto& nations = tables[TPCHTable::Nation];
    auto& regions = tables[TPCHTable::Region];

    // Get the pipeline plan of the query
    auto plan = TPCH_Query5::getPipelinePlan(tables, bm);

    // Get the pipelines from the plan
    auto customerPipeline = plan.getPipeline(0);
    auto orderPipeline = plan.getPipeline(1);
    auto lineItemPipeline = plan.getPipeline(2);
    auto supplierPipeline = plan.getPipeline(3);
    auto nationPipeline = plan.getPipeline(4);
    auto regionPipeline = plan.getPipeline(5);

    // Create executable pipelines stages (EPS) from the plan
    auto customerEps = provider->create(customerPipeline.pipeline, options);
    auto orderEps = provider->create(orderPipeline.pipeline, options);
    auto lineItemEps = provider->create(lineItemPipeline.pipeline, options);
    auto supplierEps = provider->create(supplierPipeline.pipeline, options);
    auto nationEps = provider->create(nationPipeline.pipeline, options);
    auto regionEps = provider->create(regionPipeline.pipeline, options);

    // Setup each pipeline
    customerEps->setup(*customerPipeline.ctx);
    orderEps->setup(*orderPipeline.ctx);
    lineItemEps->setup(*lineItemPipeline.ctx);
    supplierEps->setup(*supplierPipeline.ctx);
    nationEps->setup(*nationPipeline.ctx);
    regionEps->setup(*regionPipeline.ctx);

    // == Execute and assert the first pipeline (Customer) == //
    for (auto& customerChunk : customers->getChunks()) {
        customerEps->execute(customerChunk, *customerPipeline.ctx, *wc);
    }
    // Assert the content of the hash map
    auto customerJoinHandler = customerPipeline.ctx->getOperatorHandler<BatchJoinHandler>(0);
    auto customerJoinNumberOfKeys = customerJoinHandler->getThreadLocalState(wc->getId())->getNumberOfEntries();
    EXPECT_EQ(customerJoinNumberOfKeys, 1500);
    auto customerJoinHashMap = customerJoinHandler->mergeState();
    EXPECT_EQ(customerJoinHashMap->getCurrentSize(), 1500);

    // == Execute and assert the second pipeline (Order) == //
    for (auto& orderChunk : orders->getChunks()) {
        orderEps->execute(orderChunk, *orderPipeline.ctx, *wc);
    }
    // Assert the content of the hash map
    auto orderJoinHandler = orderPipeline.ctx->getOperatorHandler<BatchJoinHandler>(1); // the build handler is at index 1
    auto orderJoinNumberOfKeys = orderJoinHandler->getThreadLocalState(wc->getId())->getNumberOfEntries();
    EXPECT_EQ(orderJoinNumberOfKeys, 4570);
    auto orderJoinHashMap = orderJoinHandler->mergeState();
    EXPECT_EQ(orderJoinHashMap->getCurrentSize(), 4570);

    // == Execute and assert the third pipeline (LineItem) == //
    for (auto& lineItemChunk : lineItems->getChunks()) {
        lineItemEps->execute(lineItemChunk, *lineItemPipeline.ctx, *wc);
    }
    // Assert the content of the hash map
    auto lineItemJoinHandler = lineItemPipeline.ctx->getOperatorHandler<BatchJoinHandler>(1);  // the build handler is at index 1
    auto lineItemJoinNumberOfKeys = lineItemJoinHandler->getThreadLocalState(wc->getId())->getNumberOfEntries();
    EXPECT_EQ(lineItemJoinNumberOfKeys, 18444);
    auto lineItemJoinHashMap = lineItemJoinHandler->mergeState();
    EXPECT_EQ(lineItemJoinHashMap->getCurrentSize(), 18444);

    // == Execute and assert the fourth pipeline (Supplier) == //
    for (auto& supplierChunk : suppliers->getChunks()) {
        supplierEps->execute(supplierChunk, *supplierPipeline.ctx, *wc);
    }
    // Assert the content of the hash map
    auto supplierJoinHandler = supplierPipeline.ctx->getOperatorHandler<BatchJoinHandler>(1);  // the build handler is at index 1
    auto supplierJoinNumberOfKeys = supplierJoinHandler->getThreadLocalState(wc->getId())->getNumberOfEntries();
    EXPECT_EQ(supplierJoinNumberOfKeys, 100);
    auto supplierJoinHashMap = supplierJoinHandler->mergeState();
    EXPECT_EQ(supplierJoinHashMap->getCurrentSize(), 100);

    // == Execute and assert the fifth pipeline (Nation) == //
    for (auto& nationChunk : nations->getChunks()) {
        nationEps->execute(nationChunk, *nationPipeline.ctx, *wc);
    }
    // Assert the content of the hash map
    auto nationJoinHandler = nationPipeline.ctx->getOperatorHandler<BatchJoinHandler>(1);  // the build handler is at index 1
    auto nationJoinNumberOfKeys = nationJoinHandler->getThreadLocalState(wc->getId())->getNumberOfEntries();
    EXPECT_EQ(nationJoinNumberOfKeys, 25);
    auto nationJoinHashMap = nationJoinHandler->mergeState();
    EXPECT_EQ(nationJoinHashMap->getCurrentSize(), 25);

    // == Execute and assert the sixth pipeline (Region) == //
    for (auto& regionChunk : regions->getChunks()) {
        regionEps->execute(regionChunk, *regionPipeline.ctx, *wc);
    }
    // Assert the content in the aggregation handler
    auto aggHandler = regionPipeline.ctx->getOperatorHandler<BatchKeyedAggregationHandler>(1); // the aggregation handler is at index 1
    EXPECT_EQ(aggHandler->getThreadLocalStore(0)->getCurrentSize(), 0);         // TODO 4240: not sure if we should expect 0
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        TPCH_Q5,
                        ::testing::Values("BCInterpreter", "PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<TPCH_Q5::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution