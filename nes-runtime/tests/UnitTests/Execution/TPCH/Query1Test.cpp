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
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationScan.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchKeyedAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchKeyedAggregationHandler.hpp>
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
class TPCH_Q1 : public Testing::NESBaseTest, public AbstractPipelineExecutionTest {

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
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
    PhysicalTypePtr uintegerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());
    PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());
    auto& lineitems = tables[TPCHTable::LineItem];

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
        std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(lineitems->getLayout()));
    std::vector<std::string> projections = {"l_shipdate", "l_discount", "l_quantity", "l_extendedprice"};
    auto scan = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    /*
     * l_shipdate <= date '1998-12-01' - interval '90' day
     *
     *  1998-09-02
     */
    auto const_1998_09_02 = std::make_shared<ConstantInt32ValueExpression>(19980831);
    auto readShipdate = std::make_shared<ReadFieldExpression>("l_shipdate");
    auto lessThanExpression1 = std::make_shared<LessThanExpression>(readShipdate, const_1998_09_02);
    auto selection = std::make_shared<Selection>(lessThanExpression1);
    scan->setChild(selection);

    /*
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

        rewrite to
        sum(l_quantity) as sum_qty
        sum(l_extendedprice)
        disc_price = l_extendedprice * (1 - l_discount)
        sum(disc_price)
        sum(disc_price * (one + l_tax[i]))
        count(*)
     */
    auto l_returnflagField = std::make_shared<ReadFieldExpression>("l_returnflag");
    auto l_linestatusFiled = std::make_shared<ReadFieldExpression>("l_linestatus");

    //  sum(l_quantity) as sum_qty,
    auto l_quantityField = std::make_shared<ReadFieldExpression>("l_quantity");
    auto sumAggFunction1 = std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType);

    // sum(l_extendedprice) as sum_base_price,
    auto l_extendedpriceField = std::make_shared<ReadFieldExpression>("l_extendedprice");
    auto sumAggFunction2 = std::make_shared<Aggregation::SumAggregationFunction>(floatType, floatType);

    // disc_price = l_extendedprice * (1 - l_discount)
    auto l_discountField = std::make_shared<ReadFieldExpression>("l_discount");
    auto oneConst = std::make_shared<ConstantFloatValueExpression>(1.0f);
    auto subExpression = std::make_shared<SubExpression>(oneConst, l_discountField);
    auto mulExpression = std::make_shared<MulExpression>(l_extendedpriceField, subExpression);
    auto disc_priceExpression = std::make_shared<WriteFieldExpression>("disc_price", mulExpression);
    auto map = std::make_shared<Map>(disc_priceExpression);
    selection->setChild(map);

    //  sum(disc_price)
    auto disc_price = std::make_shared<ReadFieldExpression>("disc_price");
    auto sumAggFunction3 = std::make_shared<Aggregation::SumAggregationFunction>(floatType, floatType);

    //  sum(disc_price * (one + l_tax[i]))
    auto l_taxField = std::make_shared<ReadFieldExpression>("l_tax");
    auto addExpression = std::make_shared<AddExpression>(oneConst, l_taxField);
    auto mulExpression2 = std::make_shared<AddExpression>(disc_price, addExpression);
    auto sumAggFunction4 = std::make_shared<Aggregation::SumAggregationFunction>(floatType, floatType);

    //   count(*)
    auto countAggFunction5 = std::make_shared<Aggregation::CountAggregationFunction>(uintegerType, uintegerType);

    std::vector<Expressions::ExpressionPtr> keyFields = {l_returnflagField, l_linestatusFiled};
    std::vector<Expressions::ExpressionPtr> aggregationExpressions = {l_quantityField,
                                                                      l_extendedpriceField,
                                                                      disc_price,
                                                                      mulExpression2,
                                                                      l_quantityField};
    std::vector<std::string> resultFields = {"sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "count_order"};
    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {sumAggFunction1,
                                                                                           sumAggFunction2,
                                                                                           sumAggFunction3,
                                                                                           sumAggFunction4,
                                                                                           countAggFunction5};

    PhysicalTypePtr smallType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt8());
    std::vector<PhysicalTypePtr> types = {smallType, smallType};
    auto aggregation =
        std::make_shared<Operators::BatchKeyedAggregation>(0 /*handler index*/,
                                                           keyFields,
                                                           types,
                                                           aggregationExpressions,
                                                           aggregationFunctions,
                                                           std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

    map->setChild(aggregation);

    // create aggregation pipeline
    auto aggregationPipeline = std::make_shared<PhysicalOperatorPipeline>();
    aggregationPipeline->setRootOperator(scan);
    auto aggExecutablePipeline = provider->create(aggregationPipeline);
    auto aggregationHandler = std::make_shared<Operators::BatchKeyedAggregationHandler>();
    auto pipeline1Context = MockedPipelineExecutionContext({aggregationHandler});

    //   auto aggScan = std::make_shared<BatchKeyedAggregation>(0 /*handler index*/, aggregationFunctions, resultFields);
    // emit operator
    //  auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    // resultSchema->addField("revenue", BasicType::FLOAT32);
    // auto resultLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
    //auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(resultLayout);
    //auto emit = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    //aggScan->setChild(emit);

    // create aggregation pipeline
    //auto emitPipeline = std::make_shared<PhysicalOperatorPipeline>();
    //emitPipeline->setRootOperator(aggScan);
    //auto emitExecutablePipeline = provider->create(emitPipeline);
    //auto pipeline2Context = MockedPipelineExecutionContext({aggregationHandler});

    // process table
    NES_INFO2("Process {} chunks", lineitems->getChunks().size());
    Timer timer("Q1");
    timer.start();
    aggExecutablePipeline->setup(pipeline1Context);
    // emitExecutablePipeline->setup(pipeline2Context);
    timer.snapshot("setup");
    for (auto& chunk : lineitems->getChunks()) {
        aggExecutablePipeline->execute(chunk, pipeline1Context, *wc);
    }
    timer.snapshot("execute agg");
    //auto dummyBuffer = TupleBuffer();
    //emitExecutablePipeline->execute(dummyBuffer, pipeline2Context, *wc);
    //timer.snapshot("execute emit");

    aggExecutablePipeline->stop(pipeline1Context);
    //emitExecutablePipeline->stop(pipeline2Context);
    timer.snapshot("stop");
    timer.pause();
    NES_INFO("Query Runtime:\n" << timer);
    // compare results

    auto numberOfKeys = aggregationHandler->getThreadLocalStore(wc->getId())->getCurrentSize();
    EXPECT_EQ(numberOfKeys, 4);

    //auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(resultLayout, pipeline2Context.buffers[0]);
    //NES_INFO2("{:f}", resultDynamicBuffer[0][0].read<float>());
    //EXPECT_NEAR(resultDynamicBuffer[0][0].read<float>(), 122629680.0f, 200);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        TPCH_Q1,
                        ::testing::Values( "BCInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<TPCH_Q1::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution