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
#include <Execution/Expressions/ArithmeticalExpressions/MulExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/SubExpression.hpp>
#include <Execution/Expressions/ConstantFloatExpression.hpp>
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/BatchAggregation.hpp>
#include <Execution/Operators/Relational/BatchAggregationHandler.hpp>
#include <Execution/Operators/Relational/BatchAggregationScan.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
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
class TPCH_Q6 : public Testing::NESBaseTest, public AbstractPipelineExecutionTest {

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
 */
TEST_P(TPCH_Q6, aggregationPipeline) {

    auto& lineitems = tables[TPCHTable::LineItem];

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
        std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(lineitems->getLayout()));
    std::vector<std::string> projections = {"l_shipdate", "l_discount", "l_quantity", "l_extendedprice"};
    auto scan = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr), projections);

    /* TODO use >= instead of LessThan
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
    scan->setChild(selection1);

    // l_discount between 0.06 - 0.01 and 0.06 + 0.01
    auto readDiscount = std::make_shared<ReadFieldExpression>("l_discount");
    auto const_0_05 = std::make_shared<ConstantFloatExpression>(0.04);
    auto const_0_07 = std::make_shared<ConstantFloatExpression>(0.08);
    auto lessThanExpression3 = std::make_shared<LessThanExpression>(const_0_05, readDiscount);
    auto lessThanExpression4 = std::make_shared<LessThanExpression>(readDiscount, const_0_07);
    auto andExpression2 = std::make_shared<AndExpression>(lessThanExpression3, lessThanExpression4);

    // l_quantity < 24
    auto const_24 = std::make_shared<ConstantIntegerExpression>(24);
    auto readQuantity = std::make_shared<ReadFieldExpression>("l_quantity");
    auto lessThanExpression5 = std::make_shared<LessThanExpression>(readQuantity, const_24);
    auto andExpression3 = std::make_shared<AndExpression>(andExpression, andExpression2);
    auto andExpression4 = std::make_shared<AndExpression>(andExpression3, lessThanExpression5);

    auto selection2 = std::make_shared<Selection>(andExpression4);
    selection1->setChild(selection2);

    // sum(l_extendedprice * l_discount)
    auto l_extendedprice = std::make_shared<Expressions::ReadFieldExpression>("l_extendedprice");
    auto l_discount = std::make_shared<Expressions::ReadFieldExpression>("l_discount");
    auto revenue = std::make_shared<Expressions::MulExpression>(l_extendedprice, l_discount);
    auto aggregationResultFieldName = "revenue";
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());
    std::vector<Expressions::ExpressionPtr> aggregationFields = {revenue};
    std::vector<std::string> resultFields = {aggregationResultFieldName};
    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
        std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType)};
    auto aggregation =
        std::make_shared<Operators::BatchAggregation>(0 /*handler index*/, aggregationFields, aggregationFunctions, resultFields);
    selection2->setChild(aggregation);

    // create aggregation pipeline
    auto aggregationPipeline = std::make_shared<PhysicalOperatorPipeline>();
    aggregationPipeline->setRootOperator(scan);
    auto aggExecutablePipeline = provider->create(aggregationPipeline);
    auto aggregationHandler = std::make_shared<Operators::BatchAggregationHandler>();
    auto pipeline1Context = MockedPipelineExecutionContext({aggregationHandler});

    auto aggScan = std::make_shared<BatchAggregationScan>(0 /*handler index*/, aggregationFunctions, resultFields);
    // emit operator
    auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    resultSchema->addField("revenue", BasicType::FLOAT32);
    auto resultLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(resultLayout);
    auto emit = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    aggScan->setChild(emit);

    // create aggregation pipeline
    auto emitPipeline = std::make_shared<PhysicalOperatorPipeline>();
    emitPipeline->setRootOperator(aggScan);
    auto emitExecutablePipeline = provider->create(emitPipeline);
    auto pipeline2Context = MockedPipelineExecutionContext({aggregationHandler});

    // process table
    NES_INFO2("Process {} chunks", lineitems->getChunks().size());
    Timer timer("Q6");
    timer.start();
    aggExecutablePipeline->setup(pipeline1Context);
    emitExecutablePipeline->setup(pipeline2Context);
    timer.snapshot("setup");
    for (auto& chunk : lineitems->getChunks()) {
        aggExecutablePipeline->execute(chunk, pipeline1Context, *wc);
    }
    timer.snapshot("execute agg");
    auto dummyBuffer = TupleBuffer();
    emitExecutablePipeline->execute(dummyBuffer, pipeline2Context, *wc);
    timer.snapshot("execute emit");

    aggExecutablePipeline->stop(pipeline1Context);
    emitExecutablePipeline->stop(pipeline2Context);
    timer.snapshot("stop");
    timer.pause();
    NES_INFO("Query Runtime:\n" << timer);
    // compare results
    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(resultLayout, pipeline2Context.buffers[0]);
    NES_INFO2("{:f}", resultDynamicBuffer[0][0].read<float>());
    EXPECT_NEAR(resultDynamicBuffer[0][0].read<float>(), 122629680.0f, 200);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        TPCH_Q6,
                        ::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<TPCH_Q6::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution