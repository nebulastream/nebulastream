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

#include <BaseUnitTest.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/EquiWidthHistogram/EquiWidthHistogramBuild.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/EquiWidthHistogram/EquiWidthHistogramOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Nautilus/Util/CompilationOptions.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/EquiWidthHistogramDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyASAP.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/StatisticCollection/AbstractStatisticFormat.hpp>
#include <Sinks/Formats/StatisticCollection/StatisticFormatFactory.hpp>
#include <StatisticCollection/StatisticStorage/AbstractStatisticStore.hpp>
#include <StatisticCollection/StatisticStorage/DefaultStatisticStore.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>

namespace NES::Runtime::Execution {

class EquiWidthHistPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    EquiWidthHistPipelineExecutionContext(BufferManagerPtr bufferManager, OperatorHandlerPtr operatorHandler)
        : PipelineExecutionContext(
              PipelineId(1),         // mock pipeline id
              DecomposedQueryPlanId(1),         // mock query id
              bufferManager,
              1, // numberOfWorkerThreads
              [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                  this->emittedBuffers.emplace_back(std::move(buffer));
              },
              [this](TupleBuffer& buffer) {
                  this->emittedBuffers.emplace_back(std::move(buffer));
              },
              {operatorHandler}){};
    
    std::vector<Runtime::TupleBuffer> emittedBuffers;
};


class EquiWidthHistPipelineTest : public Testing::BaseUnitTest, public ::testing::WithParamInterface<std::tuple<std::string, Statistic::StatisticDataCodec>> {
  public:
    ExecutablePipelineProvider* provider;
    BufferManagerPtr bufferManager;
    WorkerContextPtr workerContext;
    std::shared_ptr<EquiWidthHistPipelineExecutionContext> pipelineExecutionContext;
    Nautilus::CompilationOptions options;
    SchemaPtr inputSchema, outputSchema;
    const std::string fieldToBuildEquiWidthHistOver = "f1";
    const std::string timestampFieldName = "ts";
    Statistic::StatisticStorePtr testStatisticStore;
    Statistic::SendingPolicyPtr sendingPolicy;
    Statistic::StatisticFormatPtr statisticFormat;
    Statistic::StatisticMetricHash metricHash;
    Statistic::StatisticDataCodec sinkDataCodec;
    Runtime::MemoryLayouts::MemoryLayoutPtr sampleMemoryLayout;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("EquiWidthHistPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup EquiWidthHistPipelineTest test class.");
    }
    
    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseUnitTest::SetUp();
        NES_INFO("Setup EquiWidthHistPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(std::get<0>(GetParam()))) {
            GTEST_SKIP();
        }
        // Creating class members for being able to use them in all test cases
        provider = ExecutablePipelineProviderRegistry::getPlugin(std::get<0>(GetParam())).get();
        sinkDataCodec = std::get<1>(GetParam());
        bufferManager = std::make_shared<Runtime::BufferManager>();
        workerContext = std::make_shared<WorkerContext>(0, bufferManager, 100);
        inputSchema = Schema::create()
                          ->addField(fieldToBuildEquiWidthHistOver, BasicType::INT64)
                          ->addField(timestampFieldName, BasicType::UINT64);
        outputSchema = Schema::create()->addField(Statistic::BASE_FIELD_NAME_START, BasicType::UINT64)
                           ->addField(Statistic::BASE_FIELD_NAME_END, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_HASH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::WIDTH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_DATA_FIELD_NAME, BasicType::TEXT);
        testStatisticStore = Statistic::DefaultStatisticStore::create();
        sendingPolicy = Statistic::SendingPolicyASAP::create(sinkDataCodec);
        statisticFormat = Statistic::StatisticFormatFactory::createFromSchema(outputSchema, bufferManager->getBufferSize(), Statistic::StatisticSynopsisType::COUNT_MIN, sinkDataCodec);
        metricHash = 42; // Just some arbitrary number

        // Creating the sampleMemoryLayout
        const auto sampleSchema = Schema::create()->addField(fieldToBuildEquiWidthHistOver, BasicType::UINT64);
        if (sampleSchema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
            sampleMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(sampleSchema, bufferManager->getBufferSize());
        } else if (sampleSchema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
            sampleMemoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(sampleSchema, bufferManager->getBufferSize());
        } else {
            NES_NOT_IMPLEMENTED();
        }
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down EquiWidthHistPipelineTest test case.");
        BaseUnitTest::TearDown();
    }

/**
     * @brief Creates an executable pipeline Scan --> EquiWidthHistogramSampleBuildOperator
     * @param windowSize
     * @param windowSlide
     * @param binWidth
     * @param inputOrigins
     * @return ExecutablePipelineStage
     */
std::unique_ptr<ExecutablePipelineStage> createExecutablePipeline(uint64_t windowSize, uint64_t windowSlide,
                                                                  uint64_t binWidth,
                                                                  const std::vector<OriginId>& inputOrigins) {
    // 1. Creating the scan operator
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bufferManager->getBufferSize());
    auto scanMemoryProvider = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProvider));
    
    // 2. Creating the reservoir sample operator handler
    auto reservoirSampleOperatorHandler = Operators::EquiWidthHistogramOperatorHandler::create(windowSize,
                                                                                            windowSlide,
                                                                                            sendingPolicy,
                                                                                            binWidth,
                                                                                            statisticFormat,
                                                                                            inputOrigins);
    pipelineExecutionContext = std::make_shared<EquiWidthHistPipelineExecutionContext>(bufferManager, reservoirSampleOperatorHandler);
    reservoirSampleOperatorHandler->start(pipelineExecutionContext, 0);
    constexpr auto operatorHandlerIndex = 0;

    // 3. Building the reservoir sample operator
    const auto readTsField = std::make_shared<Expressions::ReadFieldExpression>(timestampFieldName);
    auto timeFunction = std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsField, Windowing::TimeUnit::Milliseconds());
    auto reservoirSampleOperator = std::make_shared<Operators::EquiWidthHistogramBuild>(operatorHandlerIndex,
                                                                                     fieldToBuildEquiWidthHistOver,
                                                                                     binWidth,
                                                                                     metricHash,
                                                                                     std::move(timeFunction));

    // 4. Building the pipeline and creating an executable version
    auto pipelineBuild = std::make_shared<PhysicalOperatorPipeline>();
    scanOperator->setChild(reservoirSampleOperator);
    pipelineBuild->setRootOperator(scanOperator);
    return provider->create(pipelineBuild, options);
}
};
/**
 * @brief Here we test, if we create a equi width histogram sample  for a single input tuple
 */
TEST_P(EquiWidthHistPipelineTest, singleInputTuple){
    constexpr auto windowSize = 10, windowSlide = 10, binWidth = 10;
    const std::vector<OriginId> inputOrigins = {OriginId(1)};
    auto executablePipeline = createExecutablePipeline(windowSize, windowSlide, binWidth, inputOrigins);

    auto inputBuffers = Util::createDataForOneFieldAndTimeStamp(1, *bufferManager, inputSchema, fieldToBuildEquiWidthHistOver, timestampFieldName);
    executablePipeline->setup(*pipelineExecutionContext);
    for (auto& buf : inputBuffers) {
        executablePipeline->execute(buf, *pipelineExecutionContext, *workerContext);

        // Now doing the same thing for the test reservoir sample
        auto dynamicBuffer = MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buf, inputSchema);
        Util::updateTestEquiWidthHistogramStatistic(dynamicBuffer, testStatisticStore, metricHash, windowSize, windowSlide, binWidth, fieldToBuildEquiWidthHistOver, timestampFieldName);
    }

    for (auto& emitBuf : pipelineExecutionContext->emittedBuffers) {
        auto createdReservoirSampleStatistics = statisticFormat->readStatisticsFromBuffer(emitBuf);
        for (auto& [statisticHash, reservoirSampleStatistic] : createdReservoirSampleStatistics) {
            auto expectedStatistics = testStatisticStore->getStatistics(statisticHash, reservoirSampleStatistic->getStartTs(), reservoirSampleStatistic->getEndTs());
            EXPECT_EQ(expectedStatistics.size(), 1);
            EXPECT_TRUE(expectedStatistics[0]->equal(*reservoirSampleStatistic));
        }
    }
}

/**
 * @brief Here we test, if we create multiple reservoir sample for multiple input buffers
 */
TEST_P(EquiWidthHistPipelineTest, multipleInputBuffers) {
    constexpr auto windowSize = 1000, windowSlide = 1000, binWidth = 1000;
    const std::vector<OriginId> inputOrigins = {OriginId(1)};
    auto executablePipeline = createExecutablePipeline(windowSize, windowSlide, binWidth, inputOrigins);

    auto inputBuffers = Util::createDataForOneFieldAndTimeStamp(1, *bufferManager, inputSchema, fieldToBuildEquiWidthHistOver, timestampFieldName);
    executablePipeline->setup(*pipelineExecutionContext);
    for (auto& buf : inputBuffers) {
        executablePipeline->execute(buf, *pipelineExecutionContext, *workerContext);

        // Now doing the same thing for the test reservoir sample
        auto dynamicBuffer = MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buf, inputSchema);
        Util::updateTestEquiWidthHistogramStatistic(dynamicBuffer, testStatisticStore, metricHash, windowSize, windowSlide, binWidth, fieldToBuildEquiWidthHistOver, timestampFieldName);
    }

    for (auto& emitBuf : pipelineExecutionContext->emittedBuffers) {
        auto createdReservoirSampleStatistics = statisticFormat->readStatisticsFromBuffer(emitBuf);
        for (auto& [statisticHash, reservoirSampleStatistic] : createdReservoirSampleStatistics) {
            auto expectedStatistics = testStatisticStore->getStatistics(statisticHash, reservoirSampleStatistic->getStartTs(), reservoirSampleStatistic->getEndTs());
            EXPECT_EQ(expectedStatistics.size(), 1);
            EXPECT_TRUE(expectedStatistics[0]->equal(*reservoirSampleStatistic));
        }
    }
}

INSTANTIATE_TEST_CASE_P(testEquiWidthHistPipeline,
                        EquiWidthHistPipelineTest,
                        ::testing::Combine(
                            ::testing::Values("PipelineInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                            ::testing::ValuesIn(magic_enum::enum_values<Statistic::StatisticDataCodec>())
                                ),
                        [](const testing::TestParamInfo<EquiWidthHistPipelineTest::ParamType>& info) {
                            const auto param = info.param;
                            return std::get<0>(param) + "_sinkDataCodec_" + std::string(magic_enum::enum_name(std::get<1>(param)));
                        });


} // namespace NES::Runtime::Execution