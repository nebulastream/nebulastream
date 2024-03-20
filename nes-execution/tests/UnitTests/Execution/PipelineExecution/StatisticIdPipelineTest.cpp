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
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedWindowEmitAction.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <random>
#include <utility>

namespace NES::Runtime::Execution {

class StatisticIdPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;
    Nautilus::CompilationOptions options;

    Operators::OperatorPtr scanOperator;
    Operators::ExecutableOperatorPtr emitOperator;
    SchemaPtr schema;
    StatisticId statisticIdScan = 1, statisticIdFilter = 2, statisticIdMap = 3;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StatisticIdPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatisticIdPipelineTest test class.");
    }
    
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup StatisticIdPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);

        schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                     ->addField("f1", BasicType::INT64)
                     ->addField("f2", BasicType::INT64);

        // Creating a scan and an emit operator, as we need this for all tests
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));
        scanOperator->setStatisticId(statisticIdScan);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        emitOperator->setStatisticId(statisticIdScan);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down StatisticIdPipelineTest test class."); }


    /**
 * @brief This method creates numBuffers buffers (default = 100) with some arbitrary data
 */
    std::vector<TupleBuffer> createData(uint64_t numberOfBuffers = 20) {
        std::vector<TupleBuffer> retBuffers;
        for (uint64_t bufCnt = 0; bufCnt < numberOfBuffers; ++bufCnt) {
            auto buffer = bm->getBufferBlocking();
            auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);
            for (auto i = 0_u64; i < testBuffer.getCapacity(); ++i) {
                testBuffer[i]["f1"].write<int64_t>(i);
                testBuffer[i]["f2"].write(+1_s64);
                testBuffer.setNumberOfTuples(i + 1);
            }
            buffer.setSequenceNumber(bufCnt + 1);
            buffer.setChunkNumber(1);
            buffer.setLastChunk(true);
            // As we do not have a physical source, we set the statisticId to the id of the scan operator.
            // In the actual system, this would be set to the id of the predecessor pipeline
            buffer.setStatisticId(statisticIdScan);
            retBuffers.emplace_back(buffer);
        }

        return retBuffers;
    }
};


/**
 * @brief Tests if the statistic id is set correctly for the emitted tuple buffer of a pipeline consisting of a
 * scan --> filter --> emit. The statistic id of the filter operator should be set
 */
TEST_P(StatisticIdPipelineTest, testScanFilterEmitPipeline) {
    // Creating a filter operator that checks for f1 == 5
    auto readF1 = std::make_shared<Expressions::ConstantInt64ValueExpression>(5);
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto equalsExpression = std::make_shared<Expressions::EqualsExpression>(readF1, readF2);
    auto selectionOperator = std::make_shared<Operators::Selection>(equalsExpression);
    selectionOperator->setStatisticId(statisticIdFilter);
    emitOperator->setStatisticId(statisticIdFilter);

    // Building the pipeline
    scanOperator->setChild(selectionOperator);
    selectionOperator->setChild(emitOperator);
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);
    auto executablePipeline = provider->create(pipeline, options);

    auto pipelineContext = MockedPipelineExecutionContext();
    executablePipeline->setup(pipelineContext);
    for (auto& buf : createData()) {
        executablePipeline->execute(buf, pipelineContext, *wc);
    }
    executablePipeline->stop(pipelineContext);

    // Checking if all buffers have the statisticId of the filterOperator
    bool statisticIdCheck = std::accumulate(pipelineContext.buffers.begin(), pipelineContext.buffers.end(),
                                            true, [this](const bool cur, const TupleBuffer& buf) {
                                                return cur && (buf.getStatisticId() == statisticIdFilter);
                                            });
    EXPECT_TRUE(statisticIdCheck);
}

/**
 * @brief Tests if the statistic id is set correctly for the emitted tuple buffer of a pipeline consisting of a
 * scan --> filter --> map --> emit. The statistic id of the filter operator should be set
 */
TEST_P(StatisticIdPipelineTest, testScanFilterMapEmitPipeline) {
    // Creating a filter operator that checks for f1 == 5
    auto readF1 = std::make_shared<Expressions::ConstantInt64ValueExpression>(5);
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto equalsExpression = std::make_shared<Expressions::EqualsExpression>(readF1, readF2);
    auto selectionOperator = std::make_shared<Operators::Selection>(equalsExpression);
    selectionOperator->setStatisticId(statisticIdFilter);
    emitOperator->setStatisticId(statisticIdFilter);

    // Creating a map operator that performs f1 = f1 + f2
    auto addExpression = std::make_shared<Expressions::AddExpression>(readF1, readF2);
    auto writeF3 = std::make_shared<Expressions::WriteFieldExpression>("f1", addExpression);
    auto mapOperator = std::make_shared<Operators::Map>(writeF3);
    mapOperator->setStatisticId(statisticIdMap);
    emitOperator->setStatisticId(statisticIdMap);

    // Building the pipeline
    scanOperator->setChild(selectionOperator);
    selectionOperator->setChild(mapOperator);
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);
    auto executablePipeline = provider->create(pipeline, options);

    auto pipelineContext = MockedPipelineExecutionContext();
    executablePipeline->setup(pipelineContext);
    for (auto& buf : createData()) {
        executablePipeline->execute(buf, pipelineContext, *wc);
    }
    executablePipeline->stop(pipelineContext);

    // Checking if all buffers have the statisticId of the filterOperator
    bool statisticIdCheck = std::accumulate(pipelineContext.buffers.begin(), pipelineContext.buffers.end(),
                                            true, [this](const bool cur, const TupleBuffer& buf) {
                                                return cur && (buf.getStatisticId() == statisticIdMap);
                                            });
    EXPECT_TRUE(statisticIdCheck);
}


INSTANTIATE_TEST_CASE_P(testStatisticIdCompilation,
                        StatisticIdPipelineTest,
                        ::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<StatisticIdPipelineTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Runtime::Execution