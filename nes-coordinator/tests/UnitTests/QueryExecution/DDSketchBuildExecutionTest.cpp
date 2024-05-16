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
#include <Operators/LogicalOperators/Sinks/StatisticSinkDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/DDSketchDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyASAP.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/NeverTrigger.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Sinks/Formats/StatisticCollection/StatisticFormatFactory.hpp>
#include <StatisticCollection/StatisticStorage/DefaultStatisticStore.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/Common.hpp>

namespace NES::Runtime::Execution {
using namespace std::chrono_literals;
constexpr auto queryCompilerDumpMode = NES::QueryCompilation::DumpMode::NONE;

class DDSketchBuildExecutionTest : public Testing::BaseUnitTest,
                                   public ::testing::WithParamInterface<std::tuple<uint64_t, uint64_t, double, Statistic::StatisticDataCodec>> {
  public:
    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    static constexpr uint64_t defaultDecomposedQueryPlanId = 0;
    static constexpr uint64_t defaultSharedQueryId = 0;
    uint64_t numberOfPreAllocatedBuckets;
    double relativeError;
    SchemaPtr inputSchema, outputSchema;
    std::string fieldToBuildDDSketchOver = "f1";
    std::string timestampFieldName = "ts";
    Statistic::StatisticFormatPtr statisticFormat;
    Statistic::StatisticMetricHash metricHash;
    Statistic::StatisticStorePtr testStatisticStore;
    Statistic::StatisticDataCodec statisticDataCodec;
    Statistic::SendingPolicyPtr sendingPolicy;
    Statistic::TriggerConditionPtr triggerCondition;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("DDSketchBuildExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("QueryExecutionTest: Setup DDSketchBuildExecutionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup DDSketchBuildExecutionTest test class.");
        Testing::BaseUnitTest::SetUp();
        const auto curTestCaseParams = DDSketchBuildExecutionTest::GetParam();
        const auto numWorkerThreads = std::get<0>(DDSketchBuildExecutionTest::GetParam());
        numberOfPreAllocatedBuckets = std::get<1>(DDSketchBuildExecutionTest::GetParam());
        relativeError = std::get<2>(DDSketchBuildExecutionTest::GetParam());
        statisticDataCodec = std::get<3>(DDSketchBuildExecutionTest::GetParam());

        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompilerDumpMode, numWorkerThreads);

        inputSchema = Schema::create()
                          ->addField(fieldToBuildDDSketchOver, BasicType::INT64)
                          ->addField(timestampFieldName, BasicType::UINT64)
                          ->updateSourceName("test");
        fieldToBuildDDSketchOver =
            inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + fieldToBuildDDSketchOver;
        timestampFieldName = inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + timestampFieldName;

        outputSchema = Schema::create()
                           ->addField(Statistic::BASE_FIELD_NAME_START, BasicType::UINT64)
                           ->addField(Statistic::BASE_FIELD_NAME_END, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_HASH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::WIDTH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::GAMMA_FIELD_NAME, BasicType::FLOAT64)
                           ->addField(Statistic::STATISTIC_DATA_FIELD_NAME, BasicType::TEXT)
                           ->updateSourceName("test");
        testStatisticStore = Statistic::DefaultStatisticStore::create();
        statisticFormat =
            Statistic::StatisticFormatFactory::createFromSchema(outputSchema,
                                                                executionEngine->getBufferManager()->getBufferSize(),
                                                                Statistic::StatisticSynopsisType::DD_SKETCH,
                                                                statisticDataCodec);
        metricHash = 42;// Just some arbitrary number
        sendingPolicy = Statistic::SendingPolicyASAP::create(statisticDataCodec);
        triggerCondition = Statistic::NeverTrigger::create();
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("QueryExecutionTest: Tear down DDSketchBuildExecutionTest test case.");
        EXPECT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /**
     * @brief Runs the statistic query and checks if the created DDSketch sketches are correct by creating
     * DDSketch sketches on our own and then comparing expected versus actual
     * @param testSourceDescriptor
     * @param testSinkDescriptor
     * @param ddSketchDescriptor
     * @param windowSize
     * @param windowSlide
     * @param allInputBuffers
     */
    void runQueryAndCheckCorrectness(SourceDescriptorPtr testSourceDescriptor,
                                     SinkDescriptorPtr testSinkDescriptor,
                                     Statistic::WindowStatisticDescriptorPtr ddSketchDescriptor,
                                     uint64_t windowSize,
                                     uint64_t windowSlide,
                                     TimeCharacteristicPtr timeCharacteristic,
                                     std::vector<TupleBuffer> allInputBuffers) {

        // Creating the query
        auto window =
            SlidingWindow::of(timeCharacteristic, Milliseconds(windowSize), Milliseconds(windowSlide));
        auto query = TestQuery::from(testSourceDescriptor)
                         .buildStatistic(window, ddSketchDescriptor, metricHash, sendingPolicy, triggerCondition)
                         .sink(testSinkDescriptor);

        // Creating query and submitting it to the execution engine
        NES_INFO("Submitting query: {}", query.getQueryPlan()->toString())
        auto decomposedQueryPlan = DecomposedQueryPlan::create(DecomposedQueryPlanId(defaultDecomposedQueryPlanId),
                                                               SharedQueryId(defaultSharedQueryId),
                                                               INVALID_WORKER_NODE_ID,
                                                               query.getQueryPlan()->getRootOperators());
        auto plan = executionEngine->submitQuery(decomposedQueryPlan);

        // Emitting the input buffers and creating the expected DDSketch sketches in testStatisticStore
        auto source = executionEngine->getDataSource(plan, 0);
        for (auto buf : allInputBuffers) {
            // We call here emit work, as we do not want the metadata of the buffer to change, due to as setting it
            // in Util::createDataForOneFieldAndTimeStamp()
            source->emitWork(buf);

            // Now creating the expected DDSketch sketches in testStatisticStore
            auto dynamicBuffer = MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buf, inputSchema);
            const auto gamma = ddSketchDescriptor->as<Statistic::DDSketchDescriptor>()->calculateGamma();
            Util::updateTestDDSketchStatistic(dynamicBuffer, testStatisticStore, metricHash, windowSize, windowSlide, numberOfPreAllocatedBuckets, gamma, fieldToBuildDDSketchOver, timestampFieldName);
        }

        // Giving the execution engine time to process the tuples, so that we do not just test our terminate() implementation
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Stopping query and waiting until the test sink has received the expected number of tuples
        NES_INFO("Stopping query now!!!");
        EXPECT_TRUE(executionEngine->stopQuery(plan, Runtime::QueryTerminationType::Graceful));
        const auto nodeEngine = executionEngine->getNodeEngine();
        const auto nodeEngineStatisticStore = nodeEngine->getStatisticManager()->getStatisticStore();

        while (nodeEngineStatisticStore->getAllStatistics().size() != testStatisticStore->getAllStatistics().size()) {
            NES_DEBUG("Waiting till all statistics have been built. Currently {} and expecting {}...",
                      nodeEngineStatisticStore->getAllStatistics().size(),
                      testStatisticStore->getAllStatistics().size());
            std::this_thread::sleep_for(100ms);
        }

        // Checking the correctness
        NES_DEBUG("Built all statistics. Now checking for correctness...");
        for (auto& [statisticHash, ddSketchStatistic] : nodeEngineStatisticStore->getAllStatistics()) {
            NES_DEBUG("Searching for statisticHash = {} ddSketchStatistic = {}", statisticHash, ddSketchStatistic->toString());
            auto expectedStatistics =
                testStatisticStore->getStatistics(statisticHash, ddSketchStatistic->getStartTs(), ddSketchStatistic->getEndTs());
            EXPECT_EQ(expectedStatistics.size(), 1);
            NES_DEBUG("Found for statisticHash = {} expectedStatistics = {}", statisticHash, expectedStatistics[0]->toString());
            if (!expectedStatistics[0]->equal(*ddSketchStatistic)) {
                std::ofstream file("expectedStatistics.txt");
                file << expectedStatistics[0]->toString() << std::endl;
                file.close();

                std::ofstream file2("actualStatistics.txt");
                file2 << ddSketchStatistic->toString() << std::endl;
                file2.close();
            }
            ASSERT_TRUE(expectedStatistics[0]->equal(*ddSketchStatistic));
        }
    }
};

/**
 * @brief Here we test, if we create a DDSketch sketch for a single input tuple
 */
TEST_P(DDSketchBuildExecutionTest, singleInputTuple) {
    using namespace Statistic;
    constexpr auto windowSize = 10;
    constexpr auto numberOfTuples = 1;
    const auto timeCharacteristic = EventTime(Attribute(timestampFieldName));
    const auto isIngestionTime = false;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildDDSketchOver,
                                                                   timestampFieldName,
                                                                   isIngestionTime);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSynopsisType::DD_SKETCH, statisticDataCodec);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the DDSketch descriptor and running the query
    auto ddSketchDescriptor = DDSketchDescriptor::create(Over(fieldToBuildDDSketchOver), relativeError, numberOfPreAllocatedBuckets);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                ddSketchDescriptor,
                                windowSize,
                                windowSize,
                                timeCharacteristic,
                                allInputBuffers);
}

/**
 * @brief Here we test, if we create multiple DDSketch sketches for multiple input buffers, but also for larger sketches
 */
TEST_P(DDSketchBuildExecutionTest, multipleInputBuffers) {
    using namespace Statistic;
    constexpr auto windowSize = 1000;
    constexpr auto numberOfTuples = 1'000;
    const auto timeCharacteristic = EventTime(Attribute(timestampFieldName));
    const auto isIngestionTime = false;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildDDSketchOver,
                                                                   timestampFieldName,
                                                                   isIngestionTime);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSynopsisType::DD_SKETCH, statisticDataCodec);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the DDSketch descriptor and running the query
    auto ddSketchDescriptor = DDSketchDescriptor::create(Over(fieldToBuildDDSketchOver), relativeError, numberOfPreAllocatedBuckets);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                ddSketchDescriptor,
                                windowSize,
                                windowSize,
                                timeCharacteristic,
                                allInputBuffers);
}

/**
 * @brief Here we test, if we create multiple DDSketch sketches for multiple input buffers, but also for larger sketches
 * and for sliding windows (we create one sketch per slice)
 */
TEST_P(DDSketchBuildExecutionTest, multipleInputBuffersSlidingWindow) {
    // Will be enabled with issue #4865
    GTEST_SKIP();
    using namespace Statistic;
    constexpr auto windowSize = 1000;
    constexpr auto windowSlide = 500;
    constexpr auto numberOfTuples = 10'000;
    const auto timeCharacteristic = EventTime(Attribute(timestampFieldName));
    const auto isIngestionTime = false;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildDDSketchOver,
                                                                   timestampFieldName,
                                                                   isIngestionTime);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSynopsisType::DD_SKETCH, statisticDataCodec);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the DDSketch descriptor and running the query
    auto ddSketchDescriptor = DDSketchDescriptor::create(Over(fieldToBuildDDSketchOver), relativeError, numberOfPreAllocatedBuckets);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                ddSketchDescriptor,
                                windowSize,
                                windowSlide,
                                timeCharacteristic,
                                allInputBuffers);
}

/**
* @brief Here we test, if we create multiple DDSketch sketches for multiple input buffers, but also for larger sketches
 * The difference is that we use the ingestion time instead of an event time
 */
TEST_P(DDSketchBuildExecutionTest, multipleInputBuffersIngestionTime) {
    using namespace Statistic;
    constexpr auto windowSize = 1000;
    constexpr auto numberOfTuples = 1'000;
    const auto timeCharacteristic = EventTime(Attribute(timestampFieldName));
    const auto isIngestionTime = true;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildDDSketchOver,
                                                                   timestampFieldName,
                                                                   isIngestionTime);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSynopsisType::DD_SKETCH, statisticDataCodec);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the DDSketch descriptor and running the query
    auto ddSketchDescriptor = DDSketchDescriptor::create(Over(fieldToBuildDDSketchOver), relativeError, numberOfPreAllocatedBuckets);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                ddSketchDescriptor,
                                windowSize,
                                windowSize,
                                timeCharacteristic,
                                allInputBuffers);
}

INSTANTIATE_TEST_CASE_P(testDDSketch,
                        DDSketchBuildExecutionTest,
                        ::testing::Combine(::testing::Values(1, 4, 8),             // numWorkerThread
                                           ::testing::Values(10, 2048),            // numberOfPreAllocatedBuckets
                                           ::testing::Values(0.5, 0.25, 0.1, 0.01),// relative error
                                           ::testing::ValuesIn(                    // All possible statistic sink datatype
                                               magic_enum::enum_values<Statistic::StatisticDataCodec>())),
                        [](const testing::TestParamInfo<DDSketchBuildExecutionTest::ParamType>& info) {
                            const auto numWorkerThread = std::get<0>(info.param);
                            const auto numberOfPreAllocatedBuckets = std::get<1>(info.param);
                            auto relativeError = std::to_string(std::get<2>(info.param));
                            Util::findAndReplaceAll(relativeError, ".", std::string("_"));
                            const auto dataCodec = std::get<3>(info.param);
                            return std::to_string(numWorkerThread) + "Threads_" + std::to_string(numberOfPreAllocatedBuckets) + "numberOfPreAllocatedBuckets_"
                                + relativeError + "relativeError_"  +
                                std::string(magic_enum::enum_name(dataCodec));
                        });
}// namespace NES::Runtime::Execution
