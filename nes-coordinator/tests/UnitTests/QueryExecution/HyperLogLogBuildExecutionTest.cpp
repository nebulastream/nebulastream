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
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/HyperLogLogDescriptor.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Sinks/Formats/StatisticCollection/HyperLogLogStatisticFormat.hpp>
#include <StatisticCollection/StatisticStorage/DefaultStatisticStore.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>

namespace NES::Runtime::Execution {
using namespace std::chrono_literals;
constexpr auto queryCompilerDumpMode = NES::QueryCompilation::DumpMode::NONE;

class HyperLogLogBuildExecutionTest : public Testing::BaseUnitTest,
                                      public ::testing::WithParamInterface<std::tuple<uint64_t, uint64_t>> {
  public:
    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    static constexpr uint64_t defaultDecomposedQueryPlanId = 0;
    static constexpr uint64_t defaultSharedQueryId = 0;
    uint64_t sketchWidth;
    SchemaPtr inputSchema, outputSchema;
    std::string fieldToBuildHyperLogLogOver = "f1";
    std::string timestampFieldName = "ts";
    Statistic::AbstractStatisticFormatPtr statisticFormat;
    Statistic::StatisticMetricHash metricHash;
    Statistic::AbstractStatisticStorePtr testStatisticStore;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("HyperLogLogBuildExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("QueryExecutionTest: Setup HyperLogLogBuildExecutionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup HyperLogLogBuildExecutionTest test class.");
        Testing::BaseUnitTest::SetUp();
        const auto numWorkerThreads = std::get<0>(HyperLogLogBuildExecutionTest::GetParam());
        sketchWidth = std::get<1>(HyperLogLogBuildExecutionTest::GetParam());

        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompilerDumpMode, numWorkerThreads);

        inputSchema = Schema::create()
                          ->addField(fieldToBuildHyperLogLogOver, BasicType::UINT64)
                          ->addField(timestampFieldName, BasicType::UINT64)
                          ->updateSourceName("test");
        fieldToBuildHyperLogLogOver =
            inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + fieldToBuildHyperLogLogOver;
        timestampFieldName = inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + timestampFieldName;

        outputSchema = Schema::create()
                           ->addField(Statistic::BASE_FIELD_NAME_START, BasicType::UINT64)
                           ->addField(Statistic::BASE_FIELD_NAME_END, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_HASH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::WIDTH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::ESTIMATE_FIELD_NAME, BasicType::FLOAT64)
                           ->addField(Statistic::STATISTIC_DATA_FIELD_NAME, BasicType::TEXT)
                           ->updateSourceName("test");
        testStatisticStore = Statistic::DefaultStatisticStore::create();
        statisticFormat = Statistic::HyperLogLogStatisticFormat::create(
            Runtime::MemoryLayouts::RowLayout::create(outputSchema, executionEngine->getBufferManager()->getBufferSize()));
        metricHash = 42;// Just some arbitrary number
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("QueryExecutionTest: Tear down HyperLogLogBuildExecutionTest test case.");
        EXPECT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /**
     * @brief Runs the statistic query and checks if the created hyperloglog sketches are correct by creating
     * hyperloglog sketches on our own and then comparing expected versus actual
     * @param testSourceDescriptor
     * @param testSinkDescriptor
     * @param hyperLogLogDescriptor
     * @param windowSize
     * @param windowSlide
     * @param allInputBuffers
     */
    void runQueryAndCheckCorrectness(SourceDescriptorPtr testSourceDescriptor,
                                     SinkDescriptorPtr testSinkDescriptor,
                                     Statistic::WindowStatisticDescriptorPtr hyperLogLogDescriptor,
                                     uint64_t windowSize,
                                     uint64_t windowSlide,
                                     std::vector<TupleBuffer> allInputBuffers) {

        // Creating the query
        auto window =
            SlidingWindow::of(EventTime(Attribute(timestampFieldName)), Milliseconds(windowSize), Milliseconds(windowSlide));
        auto query = TestQuery::from(testSourceDescriptor)
                         .buildStatistic(window, hyperLogLogDescriptor, metricHash)
                         .sink(testSinkDescriptor);

        // Creating query and submitting it to the execution engine
        NES_INFO("Submitting query: {}", query.getQueryPlan()->toString())
        auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                               defaultSharedQueryId,
                                                               INVALID_WORKER_NODE_ID,
                                                               query.getQueryPlan()->getRootOperators());
        auto plan = executionEngine->submitQuery(decomposedQueryPlan);

        // Emitting the input buffers and creating the expected hyperloglog sketches in testStatisticStore
        auto source = executionEngine->getDataSource(plan, 0);
        for (auto buf : allInputBuffers) {
            source->emitBuffer(buf);

            // Now creating the expected hyperloglog sketches in testStatisticStore
            auto dynamicBuffer = MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buf, inputSchema);
            Util::updateTestHyperLogLogStatistic(dynamicBuffer,
                                                 testStatisticStore,
                                                 metricHash,
                                                 windowSize,
                                                 windowSlide,
                                                 sketchWidth,
                                                 fieldToBuildHyperLogLogOver,
                                                 timestampFieldName);
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
        for (auto& [statisticHash, hyperLogLogStatistic] : nodeEngineStatisticStore->getAllStatistics()) {
            NES_DEBUG("Searching for statisticHash = {} hyperLogLogStatistic = {}",
                      statisticHash,
                      hyperLogLogStatistic->toString());
            auto expectedStatistics = testStatisticStore->getStatistics(statisticHash,
                                                                        hyperLogLogStatistic->getStartTs(),
                                                                        hyperLogLogStatistic->getEndTs());
            EXPECT_EQ(expectedStatistics.size(), 1);
            NES_DEBUG("Found for statisticHash = {} expectedStatistics = {}", statisticHash, expectedStatistics[0]->toString());
            EXPECT_TRUE(expectedStatistics[0]->equal(*hyperLogLogStatistic));
        }
    }
};

/**
 * @brief Here we test, if we create a hyperloglog sketch for a single input tuple
 */
TEST_P(HyperLogLogBuildExecutionTest, singleInputTuple) {
    using namespace Statistic;
    constexpr auto windowSize = 10;
    constexpr auto numberOfTuples = 1;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildHyperLogLogOver,
                                                                   timestampFieldName);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSinkFormatType::HLL);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the hyperloglog descriptor and running the query
    auto hyperLogLogDescriptor = HyperLogLogDescriptor::create(Over(fieldToBuildHyperLogLogOver), sketchWidth);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                hyperLogLogDescriptor,
                                windowSize,
                                windowSize,
                                allInputBuffers);
}

/**
 * @brief Here we test, if we create multiple hyperloglog sketches for multiple input buffers, but also for larger sketches
 */
TEST_P(HyperLogLogBuildExecutionTest, multipleInputBuffers) {
    using namespace Statistic;
    constexpr auto windowSize = 1000;
    constexpr auto numberOfTuples = 1'000;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildHyperLogLogOver,
                                                                   timestampFieldName);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSinkFormatType::HLL);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the hyperloglog descriptor and running the query
    auto hyperLogLogDescriptor = HyperLogLogDescriptor::create(Over(fieldToBuildHyperLogLogOver), sketchWidth);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                hyperLogLogDescriptor,
                                windowSize,
                                windowSize,
                                allInputBuffers);
}

/**
 * @brief Here we test, if we create multiple hyperloglog sketches for multiple input buffers, but also for larger sketches
 * and for sliding windows (we create one sketch per slice)
 */
TEST_P(HyperLogLogBuildExecutionTest, multipleInputBuffersSlidingWindow) {
    using namespace Statistic;
    constexpr auto windowSize = 1000;
    constexpr auto windowSlide = 500;
    constexpr auto numberOfTuples = 1'000;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildHyperLogLogOver,
                                                                   timestampFieldName);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSinkFormatType::HLL);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the hyperloglog descriptor and running the query
    auto hyperLogLogDescriptor = HyperLogLogDescriptor::create(Over(fieldToBuildHyperLogLogOver), sketchWidth);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                hyperLogLogDescriptor,
                                windowSize,
                                windowSlide,
                                allInputBuffers);
}

INSTANTIATE_TEST_CASE_P(testHyperLogLog,
                        HyperLogLogBuildExecutionTest,
                        ::testing::Combine(::testing::Values(1, 4, 8),              // numWorkerThread
                                           ::testing::Values(4, 5, 6, 8, 10, 15, 20)// sketchWidth
                                           ),
                        [](const testing::TestParamInfo<HyperLogLogBuildExecutionTest::ParamType>& info) {
                            const auto numWorkerThread = std::get<0>(info.param);
                            const auto sketchWidth = std::get<1>(info.param);
                            return std::to_string(numWorkerThread) + "Threads_" + std::to_string(sketchWidth) + "Width";
                        });

}// namespace NES::Runtime::Execution