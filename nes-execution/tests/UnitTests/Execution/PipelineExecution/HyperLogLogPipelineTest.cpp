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
#include <BaseUnitTest.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/HyperLogLog/HyperLogLogBuild.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/HyperLogLog/HyperLogLogOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Nautilus/Util/CompilationOptions.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyASAP.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/StatisticCollection/HyperLogLogStatisticFormat.hpp>
#include <StatisticCollection/StatisticStorage/DefaultStatisticStore.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES::Runtime::Execution {

class HyperLogLogPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    HyperLogLogPipelineExecutionContext(BufferManagerPtr bufferManager, OperatorHandlerPtr operatorHandler)
        : PipelineExecutionContext(
              1,         // mock pipeline id
              1,         // mock query id
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


class HyperLogLogPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    BufferManagerPtr bufferManager;
    WorkerContextPtr workerContext;
    std::shared_ptr<HyperLogLogPipelineExecutionContext> pipelineExecutionContext;
    Nautilus::CompilationOptions options;
    SchemaPtr inputSchema, outputSchema;
    const std::string fieldToBuildCountMinOver = "f1";
    const std::string timestampFieldName = "ts";
    Statistic::AbstractStatisticStorePtr testStatisticStore;
    Statistic::SendingPolicyPtr sendingPolicy;
    Statistic::AbstractStatisticFormatPtr statisticFormat;
    Statistic::StatisticMetricHash metricHash;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("HyperLogLogPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup HyperLogLogPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseUnitTest::SetUp();
        NES_INFO("Setup HyperLogLogPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        // Creating class members for being able to use them in all test cases
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bufferManager = std::make_shared<Runtime::BufferManager>();
        workerContext = std::make_shared<WorkerContext>(0, bufferManager, 100);
        inputSchema = Schema::create()->addField(fieldToBuildCountMinOver, BasicType::UINT64)->addField(timestampFieldName, BasicType::UINT64);
        outputSchema = Schema::create()->addField(Statistic::BASE_FIELD_NAME_START, BasicType::UINT64)
                           ->addField(Statistic::BASE_FIELD_NAME_END, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_HASH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::WIDTH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::ESTIMATE_FIELD_NAME, BasicType::FLOAT64)
                           ->addField(Statistic::STATISTIC_DATA_FIELD_NAME, BasicType::TEXT);
        testStatisticStore = Statistic::DefaultStatisticStore::create();
        sendingPolicy = Statistic::SendingPolicyASAP::create();
        statisticFormat = Statistic::HyperLogLogStatisticFormat::create(Runtime::MemoryLayouts::RowLayout::create(outputSchema, bufferManager->getBufferSize()));
        metricHash = 42; // Just some arbitrary number
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down HyperLogLogPipelineTest test case.");
        BaseUnitTest::TearDown();
    }

    /**
     * @brief Creates an executable pipeline Scan --> HyperLogLogBuild
     * @param windowSize
     * @param windowSlide
     * @param width
     * @param inputOrigins
     * @param numberOfBitsInKey
     * @return ExecutablePipelineStage
     */
    std::unique_ptr<ExecutablePipelineStage> createExecutablePipeline(uint64_t windowSize, uint64_t windowSlide,
                                                                      uint64_t width,
                                                                      const std::vector<OriginId>& inputOrigins) {
        // 1. Creating the scan operator
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bufferManager->getBufferSize());
        auto scanMemoryProvider = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProvider));

        // 2. Creating the hyper log log operator handler
        auto countMinOperatorHandler = Operators::HyperLogLogOperatorHandler::create(windowSize,
                                                                                     windowSlide,
                                                                                     sendingPolicy,
                                                                                     statisticFormat,
                                                                                     width,
                                                                                     inputOrigins);
        pipelineExecutionContext = std::make_shared<HyperLogLogPipelineExecutionContext>(bufferManager, countMinOperatorHandler);
        countMinOperatorHandler->start(pipelineExecutionContext, 0);
        constexpr auto operatorHandlerIndex = 0;

        // 3. Building the count min operator
        const auto readTsField = std::make_shared<Expressions::ReadFieldExpression>(timestampFieldName);
        auto timeFunction = std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsField);
        auto countMinOperator = std::make_shared<Operators::HyperLogLogBuild>(operatorHandlerIndex,
                                                                           fieldToBuildCountMinOver,
                                                                           metricHash,
                                                                           std::move(timeFunction));

        // 4. Building the pipeline and creating an executable version
        auto pipelineBuild = std::make_shared<PhysicalOperatorPipeline>();
        scanOperator->setChild(countMinOperator);
        pipelineBuild->setRootOperator(scanOperator);
        return provider->create(pipelineBuild, options);
    }
};

/**
 * @brief Here we test, if we create a hyper loglog sketches for a single input tuple
 */
TEST_P(HyperLogLogPipelineTest, singleInputTuple) {
    constexpr auto windowSize = 10, windowSlide = 10, width = 4;
    const std::vector<OriginId> inputOrigins = {0};
    auto executablePipeline = createExecutablePipeline(windowSize, windowSlide, width, inputOrigins);
    
    auto inputBuffers = Util::createDataForOneFieldAndTimeStamp(1, *bufferManager, inputSchema, fieldToBuildCountMinOver, timestampFieldName);
    executablePipeline->setup(*pipelineExecutionContext);
    for (auto& buf : inputBuffers) {
        executablePipeline->execute(buf, *pipelineExecutionContext, *workerContext);

        // Now doing the same thing for the test count min
        auto dynamicBuffer = MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buf, inputSchema);
        Util::updateTestHyperLogLogStatistic(dynamicBuffer, testStatisticStore, metricHash, windowSize, windowSlide, width, fieldToBuildCountMinOver, timestampFieldName);
    }

    for (auto& emitBuf : pipelineExecutionContext->emittedBuffers) {
        auto createdCountMinStatistics = statisticFormat->readStatisticsFromBuffer(emitBuf);
        for (auto& [statisticHash, countMinStatistic] : createdCountMinStatistics) {
            auto expectedStatistics = testStatisticStore->getStatistics(statisticHash, countMinStatistic->getStartTs(), countMinStatistic->getEndTs());
            EXPECT_EQ(expectedStatistics.size(), 1);
            EXPECT_TRUE(expectedStatistics[0]->equal(*countMinStatistic));
        }
    }
}

/**
 * @brief Here we test, if we create multiple hyper loglog sketches for multiple input buffers, but also for larger sketches
 */
TEST_P(HyperLogLogPipelineTest, multipleInputBuffers) {
    constexpr auto windowSize = 1000, windowSlide = 1000, width = 8;
    const std::vector<OriginId> inputOrigins = {0};
    auto executablePipeline = createExecutablePipeline(windowSize, windowSlide, width, inputOrigins);

    auto inputBuffers = Util::createDataForOneFieldAndTimeStamp(1, *bufferManager, inputSchema, fieldToBuildCountMinOver, timestampFieldName);
    executablePipeline->setup(*pipelineExecutionContext);
    for (auto& buf : inputBuffers) {
        executablePipeline->execute(buf, *pipelineExecutionContext, *workerContext);

        // Now doing the same thing for the test count min
        auto dynamicBuffer = MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buf, inputSchema);
        Util::updateTestHyperLogLogStatistic(dynamicBuffer, testStatisticStore, metricHash, windowSize, windowSlide, width, fieldToBuildCountMinOver, timestampFieldName);
    }

    for (auto& emitBuf : pipelineExecutionContext->emittedBuffers) {
        auto createdCountMinStatistics = statisticFormat->readStatisticsFromBuffer(emitBuf);
        for (auto& [statisticHash, countMinStatistic] : createdCountMinStatistics) {
            auto expectedStatistics = testStatisticStore->getStatistics(statisticHash, countMinStatistic->getStartTs(), countMinStatistic->getEndTs());
            EXPECT_EQ(expectedStatistics.size(), 1);
            EXPECT_TRUE(expectedStatistics[0]->equal(*countMinStatistic));
        }
    }
}


INSTANTIATE_TEST_CASE_P(testHyperLogLogPipeline,
                        HyperLogLogPipelineTest,
                        ::testing::Values("PipelineInterpreter",
                                          "PipelineCompiler",
                                          "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<HyperLogLogPipelineTest::ParamType>& info) {
                            return info.param;
                        });



} // namespace NES::Runtime::Execution