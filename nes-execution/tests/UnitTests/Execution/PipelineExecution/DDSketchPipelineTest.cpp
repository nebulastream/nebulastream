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

#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Schema.hpp>
#include <BaseUnitTest.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/DDSketch/DDSketchBuild.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/DDSketch/DDSketchOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Nautilus/Util/CompilationOptions.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyASAP.hpp>
#include <QueryCompiler/Phases/Translations/ExpressionProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/StatisticCollection/AbstractStatisticFormat.hpp>
#include <Sinks/Formats/StatisticCollection/StatisticFormatFactory.hpp>
#include <StatisticCollection/StatisticStorage/DefaultStatisticStore.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/StdInt.hpp>

namespace NES::Runtime::Execution {

class DDSketchPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    DDSketchPipelineExecutionContext(BufferManagerPtr bufferManager, OperatorHandlerPtr operatorHandler)
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

class DDSketchPipelineTest : public Testing::BaseUnitTest, public ::testing::WithParamInterface<std::tuple<std::string, Statistic::StatisticDataCodec>> {
  public:
    ExecutablePipelineProvider* provider;
    BufferManagerPtr bufferManager;
    WorkerContextPtr workerContext;
    std::shared_ptr<DDSketchPipelineExecutionContext> pipelineExecutionContext;
    Nautilus::CompilationOptions options;
    SchemaPtr inputSchema, outputSchema;
    const std::string fieldToBuildDDSketchOver = "f1";
    const std::string timestampFieldName = "ts";
    Statistic::StatisticStorePtr testStatisticStore;
    Statistic::SendingPolicyPtr sendingPolicy;
    Statistic::StatisticFormatPtr statisticFormat;
    Statistic::StatisticMetricHash metricHash;
    Statistic::StatisticDataCodec sinkDataCodec;
    QueryCompilation::ExpressionProvider expressionProvider;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("DDSketchPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DDSketchPipelineTest test class.");
    }
    
    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseUnitTest::SetUp();
        NES_INFO("Setup DDSketchPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(std::get<0>(GetParam()))) {
            GTEST_SKIP();
        }
        // Creating class members for being able to use them in all test cases
        provider = ExecutablePipelineProviderRegistry::getPlugin(std::get<0>(GetParam())).get();
        sinkDataCodec = std::get<1>(GetParam());
        bufferManager = std::make_shared<Runtime::BufferManager>();
        workerContext = std::make_shared<WorkerContext>(WorkerThreadId(0), bufferManager, 100);
        inputSchema = Schema::create()->addField(fieldToBuildDDSketchOver, BasicType::INT64)->addField(timestampFieldName, BasicType::UINT64);
        outputSchema = Schema::create()->addField(Statistic::BASE_FIELD_NAME_START, BasicType::UINT64)
                           ->addField(Statistic::BASE_FIELD_NAME_END, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_HASH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::WIDTH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::DEPTH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_DATA_FIELD_NAME, BasicType::TEXT);
        testStatisticStore = Statistic::DefaultStatisticStore::create();
        sendingPolicy = Statistic::SendingPolicyASAP::create(sinkDataCodec);
        statisticFormat = Statistic::StatisticFormatFactory::createFromSchema(outputSchema, bufferManager->getBufferSize(), Statistic::StatisticSynopsisType::DD_SKETCH, sinkDataCodec);
        metricHash = 42; // Just some arbitrary number
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down DDSketchPipelineTest test case.");
        BaseUnitTest::TearDown();
    }

    /**
     * @brief Creates an executable pipeline Scan --> DDSketchBuildOperator
     * @param windowSize
     * @param windowSlide
     * @param width
     * @param depth
     * @param inputOrigins
     * @param numberOfBitsInKey
     * @return ExecutablePipelineStage
     */
    std::unique_ptr<ExecutablePipelineStage> createExecutablePipeline(uint64_t windowSize, uint64_t windowSlide,
                                                                      uint64_t numberOfPreAllocatedBuckets, double gamma,
                                                                      const std::vector<OriginId>& inputOrigins) {
        // 1. Creating the scan operator
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bufferManager->getBufferSize());
        auto scanMemoryProvider = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProvider));

        // 2. Creating the DD-Sketch operator handler
        auto ddSketchOperatorHandler = Operators::DDSketchOperatorHandler::create(windowSize,
                                                                                  windowSlide,
                                                                                  sendingPolicy,
                                                                                  numberOfPreAllocatedBuckets,
                                                                                  gamma,
                                                                                  statisticFormat,
                                                                                  inputOrigins);
        pipelineExecutionContext = std::make_shared<DDSketchPipelineExecutionContext>(bufferManager, ddSketchOperatorHandler);
        ddSketchOperatorHandler->start(pipelineExecutionContext, 0);
        constexpr auto operatorHandlerIndex = 0;

        // 3. Building the DD-Sketch operator
        // 3.1 Building the necessary expressions, log10(abs(fieldToTrackFieldName)) / log10(gamma)
        const auto& field = Attribute(fieldToBuildDDSketchOver);
        const auto gammaValueExpression = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::FLOAT64, std::to_string(gamma)));
        auto calcLogFloorIndexExpressions = expressionProvider.lowerExpression(LOG10(ABS(field)) / LOG10(gammaValueExpression));

        // 3.2. Building field < 0 and field > 0 expressions
        auto greaterThanZeroExpression = expressionProvider.lowerExpression(Attribute(fieldToBuildDDSketchOver) > 0_u64);
        auto lessThanZeroExpression = expressionProvider.lowerExpression(Attribute(fieldToBuildDDSketchOver) < 0_u64);
        const auto readTsField = std::make_shared<Expressions::ReadFieldExpression>(timestampFieldName);

        // 3.3. Creating the time function and then the operator
        auto timeFunction = std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsField, Windowing::TimeUnit::Milliseconds());
        auto ddSketchOperator = std::make_shared<Operators::DDSketchBuild>(operatorHandlerIndex,
                                                                           fieldToBuildDDSketchOver,
                                                                           metricHash,
                                                                           std::move(timeFunction),
                                                                           calcLogFloorIndexExpressions,
                                                                           greaterThanZeroExpression,
                                                                           lessThanZeroExpression);

        // 4. Building the pipeline and creating an executable version
        auto pipelineBuild = std::make_shared<PhysicalOperatorPipeline>();
        scanOperator->setChild(ddSketchOperator);
        pipelineBuild->setRootOperator(scanOperator);
        return provider->create(pipelineBuild, options);
    }
};

/**
 * @brief Here we test, if we create a DD-Sketch sketch for a single input tuple
 */
TEST_P(DDSketchPipelineTest, singleInputTuple) {
    constexpr auto windowSize = 10, windowSlide = 10, numberOfPreAllocatedBuckets = 1024;
    constexpr auto gamma = 0.05;
    constexpr auto numberOfTuples = 1;
    const std::vector inputOrigins = {OriginId(1)};
    auto executablePipeline = createExecutablePipeline(windowSize, windowSlide, numberOfPreAllocatedBuckets, gamma, inputOrigins);

    auto inputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples, *bufferManager, inputSchema, fieldToBuildDDSketchOver, timestampFieldName);
    executablePipeline->setup(*pipelineExecutionContext);
    for (auto& buf : inputBuffers) {
        executablePipeline->execute(buf, *pipelineExecutionContext, *workerContext);

        // Now doing the same thing for the test DD-Sketch
        auto dynamicBuffer = MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buf, inputSchema);
        Util::updateTestDDSketchStatistic(dynamicBuffer, testStatisticStore, metricHash, windowSize, windowSlide, numberOfPreAllocatedBuckets, gamma, fieldToBuildDDSketchOver, timestampFieldName);
    }

    for (auto& emitBuf : pipelineExecutionContext->emittedBuffers) {
        auto createdDDSketchStatistics = statisticFormat->readStatisticsFromBuffer(emitBuf);
        for (auto& [statisticHash, ddSketchStatistic] : createdDDSketchStatistics) {
            auto expectedStatistics = testStatisticStore->getStatistics(statisticHash, ddSketchStatistic->getStartTs(), ddSketchStatistic->getEndTs());
            EXPECT_EQ(expectedStatistics.size(), 1);
            EXPECT_TRUE(expectedStatistics[0]->equal(*ddSketchStatistic));
        }
    }
}

/**
 * @brief Here we test, if we create multiple DD-Sketch sketches for multiple input buffers, but also for larger sketches
 */
TEST_P(DDSketchPipelineTest, multipleInputBuffers) {
    constexpr auto windowSize = 1000, windowSlide = 1000, numberOfPreAllocatedBuckets = 1024;
    constexpr auto gamma = 0.05;
    constexpr auto numberOfTuples = 100'000;
    const std::vector inputOrigins = {OriginId(1)};
    auto executablePipeline = createExecutablePipeline(windowSize, windowSlide, numberOfPreAllocatedBuckets, gamma, inputOrigins);

    auto inputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples, *bufferManager, inputSchema, fieldToBuildDDSketchOver, timestampFieldName);
    executablePipeline->setup(*pipelineExecutionContext);
    for (auto& buf : inputBuffers) {
        executablePipeline->execute(buf, *pipelineExecutionContext, *workerContext);

        // Now doing the same thing for the test DD-Sketch
        auto dynamicBuffer = MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buf, inputSchema);
        Util::updateTestDDSketchStatistic(dynamicBuffer, testStatisticStore, metricHash, windowSize, windowSlide, numberOfPreAllocatedBuckets, gamma, fieldToBuildDDSketchOver, timestampFieldName);
    }

    for (auto& emitBuf : pipelineExecutionContext->emittedBuffers) {
        auto createdDDSketchStatistics = statisticFormat->readStatisticsFromBuffer(emitBuf);
        for (auto& [statisticHash, ddSketchStatistic] : createdDDSketchStatistics) {
            auto expectedStatistics = testStatisticStore->getStatistics(statisticHash, ddSketchStatistic->getStartTs(), ddSketchStatistic->getEndTs());
            EXPECT_EQ(expectedStatistics.size(), 1);
            EXPECT_TRUE(expectedStatistics[0]->equal(*ddSketchStatistic));
        }
    }
}


INSTANTIATE_TEST_CASE_P(testDDSketchPipeline,
                        DDSketchPipelineTest,
                        ::testing::Combine(
                            ::testing::Values("PipelineInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                            ::testing::ValuesIn(magic_enum::enum_values<Statistic::StatisticDataCodec>())
                                ),
                        [](const testing::TestParamInfo<DDSketchPipelineTest::ParamType>& info) {
                            const auto param = info.param;
                            return std::get<0>(param) + "_sinkDataCodec_" + std::string(magic_enum::enum_name(std::get<1>(param)));
                        });


} // namespace NES::Runtime::Execution