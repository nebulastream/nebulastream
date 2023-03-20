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
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TPCH/TPCHTableGenerator.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {
using namespace Expressions;
using namespace Operators;
class TPCH_Q3 : public Testing::NESBaseTest, public AbstractPipelineExecutionTest {

  public:
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
        Testing::NESBaseTest::SetUp();
        NES_INFO("Setup TPCH_Q6 test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        table_bm = std::make_shared<Runtime::BufferManager>(8 * 1024 * 1024, 1000);
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
        tables = TPCHTableGenerator(table_bm, 1.0f).generate();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TPCH_Q6 test class."); }
};

/**
// select
//  l_orderkey,
//  sum(l_extendedprice * (1 - l_discount)) as revenue,
//  o_orderdate,
//  o_shippriority
// from
//  customer,
//  orders,
//  lineitem
// where
//  c_mktsegment = 'BUILDING'
//  and c_custkey = o_custkey
//  and l_orderkey = o_orderkey
//  and o_orderdate < date '1995-03-15'
//  and l_shipdate > date '1995-03-15'
// group by
//   l_orderkey,
//   o_orderdate,
//   o_shippriority
 */
TEST_P(TPCH_Q3, joinPipeline) {
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
    PhysicalTypePtr uintegerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());
    PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());
    auto& customers = tables[TPCHTable::Customer];
    auto& orders = tables[TPCHTable::Orders];
    auto& lineitems = tables[TPCHTable::LineItem];

    auto c_scanMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
        std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(customers->getLayout()));
    std::vector<Nautilus::Record::RecordFieldIdentifier> customersProjection = {"c_mksegment", "c_custkey"};
    auto customersScan = std::make_shared<Operators::Scan>(std::move(c_scanMemoryProviderPtr), customersProjection);

    // c_mksegment = 'BUILDING' -> currently modeled as 1
    auto BUILDING = std::make_shared<ConstantInt32ValueExpression>(1);
    auto readC_mktsegment = std::make_shared<ReadFieldExpression>("c_mksegment");
    auto equalsExpression = std::make_shared<EqualsExpression>(readC_mktsegment, BUILDING);
    auto selection = std::make_shared<Selection>(equalsExpression);
    customersScan->setChild(selection);

    // build ht for first join
    auto readC_key = std::make_shared<ReadFieldExpression>("c_custkey");
    auto joinOp = std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                              std::vector<Expressions::ExpressionPtr>{readC_key},
                                                              std::vector<PhysicalTypePtr>{integerType},
                                                              std::vector<Expressions::ExpressionPtr>(),
                                                              std::vector<PhysicalTypePtr>(),
                                                              std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
    selection->setChild(joinOp);

    // create customerJoinBuildPipeline pipeline
    auto customerJoinBuildPipeline = std::make_shared<PhysicalOperatorPipeline>();
    customerJoinBuildPipeline->setRootOperator(customersScan);
    auto aggExecutablePipeline = provider->create(customerJoinBuildPipeline);
    auto joinHandler = std::make_shared<Operators::BatchJoinHandler>();
    auto pipeline1Context = MockedPipelineExecutionContext({joinHandler});

    /**
     * Pipeline 2 with scan orders -> selection -> JoinPrope with customers from pipeline 1
     */
    auto ordersMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
        std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(orders->getLayout()));
    std::vector<Nautilus::Record::RecordFieldIdentifier> ordersProjection = {"o_orderdate",
                                                                             "o_shippriority",
                                                                             "o_custkey",
                                                                             "o_orderkey"};
    auto orderScan = std::make_shared<Operators::Scan>(std::move(ordersMemoryProviderPtr), ordersProjection);

    //  o_orderdate < date '1995-03-15'
    auto const_1995_03_15 = std::make_shared<ConstantInt32ValueExpression>(19950315);
    auto readO_orderdate = std::make_shared<ReadFieldExpression>("o_orderdate");
    auto orderDateSelection =
        std::make_shared<Selection>(std::make_shared<LessThanExpression>(readO_orderdate, const_1995_03_15));
    orderScan->setChild(orderDateSelection);

    // join probe with customers
    std::vector<IR::Types::StampPtr> keyStamps = {IR::Types::StampFactory::createInt64Stamp()};
    std::vector<IR::Types::StampPtr> valueStamps = {};
    std::vector<ExpressionPtr> ordersProbeKeys = {std::make_shared<ReadFieldExpression>("o_custkey")};

    std::vector<Record::RecordFieldIdentifier> joinProbeResults = {"o_custkey", "o_orderkey", "o_orderdate", "o_shippriority"};
    auto customersJoinProbe = std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                                               ordersProbeKeys,
                                                               std::vector<PhysicalTypePtr>{integerType},
                                                               std::vector<Record::RecordFieldIdentifier>(),
                                                               std::vector<PhysicalTypePtr>(),
                                                               std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
    orderDateSelection->setChild(customersJoinProbe);

    // join build for order_customers
    std::vector<ExpressionPtr> order_customersJoinBuildKeys = {std::make_shared<ReadFieldExpression>("o_orderkey")};
    std::vector<ExpressionPtr> order_customersJoinBuildValues = {std::make_shared<ReadFieldExpression>("o_orderdate"),
                                                                 std::make_shared<ReadFieldExpression>("o_shippriority")};

    auto order_customersJoinBuild =
        std::make_shared<Operators::BatchJoinBuild>(1 /*handler index*/,
                                                    order_customersJoinBuildKeys,
                                                    std::vector<PhysicalTypePtr>{integerType},
                                                    order_customersJoinBuildValues,
                                                    std::vector<PhysicalTypePtr>{integerType, integerType},
                                                    std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

    customersJoinProbe->setChild(order_customersJoinBuild);

    // create order_customersJoinBuild pipeline
    auto orderCustomersJoinBuild = std::make_shared<PhysicalOperatorPipeline>();
    orderCustomersJoinBuild->setRootOperator(orderScan);
    auto orderCustomersJoinBuildPipeline = provider->create(orderCustomersJoinBuild);
    auto joinHandler2 = std::make_shared<Operators::BatchJoinHandler>();
    auto pipeline2Context = MockedPipelineExecutionContext({joinHandler, joinHandler2});

    /**
     * Pipeline 3 with scan lineitem -> selection -> JoinPrope with order_customers from pipeline 2 -> aggregation
     */
    auto lineitemsMP = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
        std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(lineitems->getLayout()));
    std::vector<Nautilus::Record::RecordFieldIdentifier> lineItemProjection = {"l_orderkey",
                                                                               "l_extendedprice",
                                                                               "l_discount",
                                                                               "l_shipdate"};
    auto lineitemsScan = std::make_shared<Operators::Scan>(std::move(lineitemsMP), lineItemProjection);

    //   date '1995-03-15' < l_shipdate
    auto readL_shipdate = std::make_shared<ReadFieldExpression>("l_shipdate");
    auto shipDateSelection = std::make_shared<Selection>(std::make_shared<LessThanExpression>(const_1995_03_15, readL_shipdate));
    lineitemsScan->setChild(shipDateSelection);

    // join probe with customers

    //  l_orderkey,
    auto l_orderkey = std::make_shared<ReadFieldExpression>("l_orderkey");
    std::vector<ExpressionPtr> lineitemProbeKeys = {l_orderkey};

    //std::vector<IR::Types::StampPtr> keyStamps = {IR::Types::StampFactory::createInt64Stamp()};
    //std::vector<IR::Types::StampPtr> valueStamps = {};
    //std::vector<ExpressionPtr> ordersProbeKeys = {std::make_shared<ReadFieldExpression>("o_custkey")};
    std::vector<Record::RecordFieldIdentifier> orderProbeFieldNames = {"o_shippriority", "o_orderdate"};

    auto lineitemJoinProbe = std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                                              lineitemProbeKeys,
                                                              std::vector<PhysicalTypePtr>{integerType},
                                                              orderProbeFieldNames,
                                                              std::vector<PhysicalTypePtr>{integerType, integerType},
                                                              std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
    shipDateSelection->setChild(lineitemJoinProbe);

    //  sum(l_extendedprice * (1 - l_discount)) as revenue,
    auto l_extendedpriceField = std::make_shared<ReadFieldExpression>("l_extendedprice");
    auto l_discountField = std::make_shared<ReadFieldExpression>("l_discount");
    auto oneConst = std::make_shared<ConstantFloatValueExpression>(1.0f);
    auto subExpression = std::make_shared<SubExpression>(oneConst, l_discountField);
    auto revenueExpression = std::make_shared<MulExpression>(l_extendedpriceField, subExpression);
    auto sumRevenue = std::make_shared<Aggregation::SumAggregationFunction>(floatType, floatType);

    std::vector<Expressions::ExpressionPtr> keyFields = {l_orderkey,
                                                         readO_orderdate,
                                                         std::make_shared<ReadFieldExpression>("o_shippriority")};
    std::vector<Expressions::ExpressionPtr> aggregationExpressions = {revenueExpression};
    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {sumRevenue};

    PhysicalTypePtr smallType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt8());

    auto aggregation =
        std::make_shared<Operators::BatchKeyedAggregation>(1 /*handler index*/,
                                                           keyFields,
                                                           std::vector<PhysicalTypePtr>{integerType, integerType, integerType},
                                                           aggregationExpressions,
                                                           aggregationFunctions,
                                                           std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

    lineitemJoinProbe->setChild(aggregation);

    // create lineitems_ordersJoinBuild pipeline
    auto lineitems_ordersJoinBuild = std::make_shared<PhysicalOperatorPipeline>();
    lineitems_ordersJoinBuild->setRootOperator(lineitemsScan);
    auto lineitems_ordersJoinBuildPipeline = provider->create(lineitems_ordersJoinBuild);
    auto aggHandler = std::make_shared<Operators::BatchKeyedAggregationHandler>();
    auto pipeline3Context = MockedPipelineExecutionContext({joinHandler2, aggHandler});

    // process table
    NES_INFO2("Process {} chunks", customers->getChunks().size());
    Timer timer("Q3");
    timer.start();

    aggExecutablePipeline->setup(pipeline1Context);
    timer.snapshot("setup p1");
    orderCustomersJoinBuildPipeline->setup(pipeline2Context);
    timer.snapshot("setup p2");
    lineitems_ordersJoinBuildPipeline->setup(pipeline3Context);
    timer.snapshot("setup p3");

    for (auto& chunk : customers->getChunks()) {
        aggExecutablePipeline->execute(chunk, pipeline1Context, *wc);
    }
    timer.snapshot("execute p1: scan");

    auto numberOfKeys = joinHandler->getThreadLocalState(wc->getId())->getNumberOfEntries();
    EXPECT_EQ(numberOfKeys, 30142);
    auto hm = joinHandler->mergeState();
    EXPECT_EQ(hm->getCurrentSize(), 30142);
    timer.snapshot("execute p1: mergeState");

    for (auto& chunk : orders->getChunks()) {
        orderCustomersJoinBuildPipeline->execute(chunk, pipeline2Context, *wc);
    }
    timer.snapshot("execute p2: scan");

    auto numberOfKeys2 = joinHandler2->getThreadLocalState(wc->getId())->getNumberOfEntries();
    EXPECT_EQ(numberOfKeys2, 147126);
    auto hm2 = joinHandler2->mergeState();
    EXPECT_EQ(hm2->getCurrentSize(), 147126);
    timer.snapshot("execute p2: mergeState");

    for (auto& chunk : lineitems->getChunks()) {
        lineitems_ordersJoinBuildPipeline->execute(chunk, pipeline3Context, *wc);
    }
    timer.snapshot("execute p3: scan");
    EXPECT_EQ(aggHandler->getThreadLocalStore(0)->getCurrentSize(), 11620);

    aggExecutablePipeline->stop(pipeline1Context);
    orderCustomersJoinBuildPipeline->stop(pipeline2Context);
    orderCustomersJoinBuildPipeline->stop(pipeline3Context);
    timer.snapshot("stop");
    timer.pause();
    NES_INFO("Query Runtime:\n" << timer);
    // compare results
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        TPCH_Q3,
                        ::testing::Values("BCInterpreter", "PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<TPCH_Q3::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution