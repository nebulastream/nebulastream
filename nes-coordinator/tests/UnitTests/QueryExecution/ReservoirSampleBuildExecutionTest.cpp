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
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/ReservoirSampleDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/NeverTrigger.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyASAP.hpp>
#include <Statistics/StatisticUtil.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Sinks/Formats/StatisticCollection/StatisticFormatFactory.hpp>
#include <StatisticCollection/StatisticStorage/DefaultStatisticStore.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/Core.hpp>

namespace NES::Runtime::Execution {
using namespace std::chrono_literals;
constexpr auto queryCompilerDumpMode = NES::QueryCompilation::DumpMode::NONE;

class ReservoirSampleBuildExecutionTest : public Testing::BaseUnitTest,
                                          public ::testing::WithParamInterface<std::tuple<uint64_t,
                                                                                          uint64_t,
                                                                                          Statistic::StatisticDataCodec,
                                                                                          bool>> {
  public:
    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    uint64_t sampleSize;
    SchemaPtr inputSchema, outputSchema;
    std::string fieldToBuildReservoirSampleOver = "f1";
    std::string timestampFieldName = "ts";
    Statistic::StatisticFormatPtr statisticFormat;
    Statistic::StatisticMetricHash metricHash;
    Statistic::StatisticStorePtr testStatisticStore;
    Statistic::StatisticDataCodec statisticDataCodec;
    Statistic::SendingPolicyPtr sendingPolicy;
    Statistic::TriggerConditionPtr triggerCondition;
    Runtime::MemoryLayouts::MemoryLayoutPtr sampleMemoryLayout;
    bool keepOnlyRequiredField;


    static void SetUpTestCase() {
        NES::Logger::setupLogging("ReservoirSampleBuildExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("QueryExecutionTest: Setup ReservoirSampleBuildExecutionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup ReservoirSampleBuildExecutionTest test class.");
        Testing::BaseUnitTest::SetUp();
        const auto curTestCaseParams = ReservoirSampleBuildExecutionTest::GetParam();
        const auto numWorkerThreads = std::get<0>(ReservoirSampleBuildExecutionTest::GetParam());
        sampleSize = std::get<1>(ReservoirSampleBuildExecutionTest::GetParam());
        statisticDataCodec = std::get<2>(ReservoirSampleBuildExecutionTest::GetParam());
        keepOnlyRequiredField = std::get<3>(GetParam());

        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompilerDumpMode, numWorkerThreads);
        inputSchema = Schema::create()
                          ->addField(fieldToBuildReservoirSampleOver, BasicType::INT64)
                          ->addField(timestampFieldName, BasicType::UINT64)
                          ->updateSourceName("test");
        fieldToBuildReservoirSampleOver =
            inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + fieldToBuildReservoirSampleOver;
        timestampFieldName = inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + timestampFieldName;
        outputSchema = Schema::create()
                           ->addField(Statistic::BASE_FIELD_NAME_START, BasicType::UINT64)
                           ->addField(Statistic::BASE_FIELD_NAME_END, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_HASH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::WIDTH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_DATA_FIELD_NAME, BasicType::TEXT);
        if (keepOnlyRequiredField) {
            outputSchema->addField(fieldToBuildReservoirSampleOver, BasicType::UINT64);
        } else {
            outputSchema->copyFields(inputSchema);
        }
        outputSchema = outputSchema->updateSourceName("test");

        testStatisticStore = Statistic::DefaultStatisticStore::create();
        statisticFormat = Statistic::StatisticFormatFactory::createFromSchema(outputSchema, executionEngine->getBufferManager()->getBufferSize(), Statistic::StatisticSynopsisType::RESERVOIR_SAMPLE, statisticDataCodec);
        metricHash = 42;// Just some arbitrary number
        sendingPolicy = Statistic::SendingPolicyASAP::create(statisticDataCodec);
        triggerCondition = Statistic::NeverTrigger::create();

        // Creating the sampleMemoryLayout
        const auto sampleSchema = keepOnlyRequiredField ? Schema::create()->addField(fieldToBuildReservoirSampleOver, BasicType::UINT64) : Statistic::StatisticUtil::createSampleSchema(inputSchema);
        sampleMemoryLayout = NES::Util::createMemoryLayout(sampleSchema, sampleSize * sampleSchema->getSchemaSizeInBytes());
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("QueryExecutionTest: Tear down ReservoirSampleBuildExecutionTest test case.");
        EXPECT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /**
     * @brief Runs the statistic query and checks if the created reservoir sample are correct by creating
     * reservoir sample on our own and then comparing expected versus actual
     * @param testSourceDescriptor
     * @param testSinkDescriptor
     * @param reservoirSampleDescriptor
     * @param windowSize
     * @param windowSlide
     * @param allInputBuffers
     */
    void runQueryAndCheckCorrectness(SourceDescriptorPtr testSourceDescriptor,
                                     SinkDescriptorPtr testSinkDescriptor,
                                     Statistic::WindowStatisticDescriptorPtr reservoirSampleDescriptor,
                                     uint64_t windowSize,
                                     uint64_t windowSlide,
                                     TimeCharacteristicPtr timeCharacteristic,
                                     std::vector<TupleBuffer> allInputBuffers) {

        // Creating the query
        auto window =
            SlidingWindow::of(timeCharacteristic, Milliseconds(windowSize), Milliseconds(windowSlide));
        auto query =
            TestQuery::from(testSourceDescriptor)
                .buildStatistic(window, reservoirSampleDescriptor, metricHash, sendingPolicy, triggerCondition)
                .sink(testSinkDescriptor);

        // Creating query and submitting it to the execution engine
        NES_INFO("Submitting query: {}", query.getQueryPlan()->toString())
        auto decomposedQueryPlan = DecomposedQueryPlan::create(DecomposedQueryPlanId(0),
                                                               SharedQueryId(0),
                                                               INVALID_WORKER_NODE_ID,
                                                               query.getQueryPlan()->getRootOperators());
        auto plan = executionEngine->submitQuery(decomposedQueryPlan);

        // Emitting the input buffers and creating the expected reservoir sample  in testStatisticStore
        auto source = executionEngine->getDataSource(plan, 0);
        for (auto buf : allInputBuffers) {
            // We call here emit buffer without changing the metadata of the buffer, due to as setting it
            // in Util::createDataForOneFieldAndTimeStamp()
            source->emitBuffer(buf, false);

            // Now creating the expected reservoir sample  in testStatisticStore
            auto dynamicBuffer = MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buf, inputSchema);
            Util::updateTestReservoirSampleStatistic(dynamicBuffer, testStatisticStore, metricHash, windowSize, windowSlide, sampleSize, timestampFieldName, sampleMemoryLayout);
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
        for (auto& [statisticHash, reservoirSampleStatistic] : nodeEngineStatisticStore->getAllStatistics()) {
            NES_DEBUG("Searching for statisticHash = {} reservoirSampleStatistic = {}", statisticHash, reservoirSampleStatistic->toString());
            auto expectedStatistics =
                testStatisticStore->getStatistics(statisticHash, reservoirSampleStatistic->getStartTs(), reservoirSampleStatistic->getEndTs());
            EXPECT_EQ(expectedStatistics.size(), 1);
            NES_DEBUG("Found for statisticHash = {} expectedStatistics = {}", statisticHash, expectedStatistics[0]->toString());
            EXPECT_TRUE(expectedStatistics[0]->equal(*reservoirSampleStatistic));
        }
    }
};

/**
 * @brief Here we test, if we create a reservoir sample sketch for a single input tuple
 */
TEST_P(ReservoirSampleBuildExecutionTest, singleInputTuple) {
    using namespace Statistic;
    constexpr auto windowSize = 10;
    constexpr auto numberOfTuples = 1;
    const auto timeCharacteristic = EventTime(Attribute(timestampFieldName));
    const auto isIngestionTime = false;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildReservoirSampleOver,
                                                                   timestampFieldName,
                                                                   isIngestionTime);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSynopsisType::RESERVOIR_SAMPLE, statisticDataCodec);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the reservoir sample descriptor and running the query
    auto reservoirSampleDescriptor = ReservoirSampleDescriptor::create(Over(fieldToBuildReservoirSampleOver), sampleSize, keepOnlyRequiredField);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                reservoirSampleDescriptor,
                                windowSize,
                                windowSize,
                                timeCharacteristic,
                                allInputBuffers);
}

/**
 * @brief Here we test, if we create multiple reservoir sample for multiple input buffers, but also for larger
 */
TEST_P(ReservoirSampleBuildExecutionTest, multipleInputBuffers) {
    using namespace Statistic;
    constexpr auto windowSize = 1000;
    constexpr auto numberOfTuples = 10'000;
    const auto timeCharacteristic = EventTime(Attribute(timestampFieldName));
    const auto isIngestionTime = false;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildReservoirSampleOver,
                                                                   timestampFieldName,
                                                                   isIngestionTime);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSynopsisType::RESERVOIR_SAMPLE, statisticDataCodec);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the reservoir sample descriptor and running the query
    auto reservoirSampleDescriptor = ReservoirSampleDescriptor::create(Over(fieldToBuildReservoirSampleOver), sampleSize, keepOnlyRequiredField);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                reservoirSampleDescriptor,
                                windowSize,
                                windowSize,
                                timeCharacteristic,
                                allInputBuffers);
}

/**
 * @brief Here we test, if we create multiple reservoir sample  for multiple input buffers, but also for larger 
 * and for sliding windows (we create one sketch per slice)
 */
TEST_P(ReservoirSampleBuildExecutionTest, multipleInputBuffersSlidingWindow) {
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
                                                                   fieldToBuildReservoirSampleOver,
                                                                   timestampFieldName,
                                                                   isIngestionTime);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSynopsisType::RESERVOIR_SAMPLE, statisticDataCodec);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the reservoir sample descriptor and running the query
    auto reservoirSampleDescriptor = ReservoirSampleDescriptor::create(Over(fieldToBuildReservoirSampleOver), sampleSize, keepOnlyRequiredField);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                reservoirSampleDescriptor,
                                windowSize,
                                windowSlide,
                                timeCharacteristic,
                                allInputBuffers);
}

/**
 * @brief Here we test, if we create multiple reservoir sample  for multiple input buffers, but also for larger samples
 * The difference is that we use the ingestion time instead of an event time
 */
TEST_P(ReservoirSampleBuildExecutionTest, multipleInputBuffersIngestionTime) {
    using namespace Statistic;
    constexpr auto windowSize = 1000;
    constexpr auto numberOfTuples = 1'000;
    const auto timeCharacteristic = EventTime(Attribute(timestampFieldName));
    const auto isIngestionTime = true;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildReservoirSampleOver,
                                                                   timestampFieldName,
                                                                   isIngestionTime);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSynopsisType::RESERVOIR_SAMPLE, statisticDataCodec);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the count min descriptor and running the query
    auto reservoirSampleDescriptor = ReservoirSampleDescriptor::create(Over(fieldToBuildReservoirSampleOver), sampleSize, keepOnlyRequiredField);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                reservoirSampleDescriptor,
                                windowSize,
                                windowSize,
                                timeCharacteristic,
                                allInputBuffers);
}

INSTANTIATE_TEST_CASE_P(testReservoirSample,
                        ReservoirSampleBuildExecutionTest,
                        ::testing::Combine(::testing::Values(1),     // numWorkerThread, we can only test with one thread, as the merging + building of local samples is not deterministic
                                           ::testing::Values(1, 100, 5000),// sampleSize
                                           ::testing::ValuesIn(            // All possible statistic data codecs
                                               magic_enum::enum_values<Statistic::StatisticDataCodec>()),
                                           ::testing::Values(false, true) // Keeping all fields in the output schema
                                           ),
                        [](const testing::TestParamInfo<ReservoirSampleBuildExecutionTest::ParamType>& info) {
                            const auto numWorkerThread = std::get<0>(info.param);
                            const auto sampleSize = std::get<1>(info.param);
                            const auto dataCodec = std::get<2>(info.param);
                            const auto keepOnlyRequiredField = std::get<3>(info.param) ? "1FIELD": "ALL_FIELDS";
                            return std::to_string(numWorkerThread) + "Threads_" + std::to_string(sampleSize) + "Width_" +
                                std::string(magic_enum::enum_name(dataCodec)) + "_" +
                                keepOnlyRequiredField;
                        });
}// namespace NES::Runtime::Execution