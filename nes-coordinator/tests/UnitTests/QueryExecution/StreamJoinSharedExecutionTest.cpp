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

#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <algorithm>
#include <gmock/gmock-matchers.h>
#include <random>
#include <vector>

/**
 * This class tests that joins when they are shared by multiple queries, which is the master thesis project right now (e.g. task #5113)
 * In the end it would be the best if both NLJ and HJ configurations are tested with all sharedJoinApproaches in one
 * parameterized test. Bucketing will most likely never be supported
 */
namespace NES::Runtime::Execution {

#define ALL_JOIN_SHARING_APPROACHES                                                                                              \
    ::testing::Values(SharedJoinApproach::APPROACH_PROBING,                                                                      \
                      SharedJoinApproach::APPROACH_DELETING,                                                                     \
                      SharedJoinApproach::APPROACH_TOMBSTONE)
#define JOIN_SHARING_CONFIGURATIONS ::testing::Combine(ALL_JOIN_STRATEGIES, ALL_JOIN_SHARING_APPROACHES)

using namespace std::chrono_literals;
constexpr auto queryCompilerDumpMode = QueryCompilation::DumpMode::NONE;

class StreamJoinQuerySharedExecutionTest
    : public Testing::BaseUnitTest,
      public ::testing::WithParamInterface<std::tuple<QueryCompilation::StreamJoinStrategy, SharedJoinApproach>> {
  public:
    static constexpr DecomposedQueryId defaultDecomposedQueryId = INVALID_DECOMPOSED_QUERY_PLAN_ID;
    static constexpr SharedQueryId defaultSharedQueryId = INVALID_SHARED_QUERY_ID;
    static constexpr QueryCompilation::WindowingStrategy windowingStrategy = QueryCompilation::WindowingStrategy::SLICING;
    QueryCompilation::StreamJoinStrategy joinStrategy;
    SharedJoinApproach sharedJoinApproach;
    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    std::shared_ptr<NodeEngine> nodeEngine;

    static void SetUpTestCase() {
        Logger::setupLogging("StreamJoinQueryExecutionTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("QueryExecutionTest: Setup StreamJoinQueryExecutionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup StreamJoinQueryExecutionTest test class.");
        BaseUnitTest::SetUp();
        constexpr auto numWorkerThreads = 1;
        joinStrategy = std::get<0>(GetParam());
        sharedJoinApproach = std::get<1>(GetParam());
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompilerDumpMode,
                                                                         numWorkerThreads,
                                                                         joinStrategy,
                                                                         windowingStrategy);
        nodeEngine = executionEngine->getNodeEngine();
    }

    /* Will be called after a test was executed. */
    void TearDown() override {
        NES_INFO("QueryExecutionTest: Tear down StreamJoinQueryExecutionTest test case.");
        EXPECT_TRUE(executionEngine->stop());
        BaseUnitTest::TearDown();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t windowStart;
        uint64_t windowEnd;
        uint64_t idLeft;
        uint64_t joinValueLeft;
        uint64_t tsLeft;
        uint64_t idRight;
        uint64_t joinValueRight;
        uint64_t tsRight;

        bool operator==(const ResultRecord& rhs) const {
            return windowStart == rhs.windowStart && windowEnd == rhs.windowEnd && idLeft == rhs.idLeft
                && joinValueLeft == rhs.joinValueLeft && tsLeft == rhs.tsLeft && idRight == rhs.idRight
                && joinValueRight == rhs.joinValueRight && tsRight == rhs.tsRight;
        }

        std::string to_string() const {
            std::ostringstream oss;
            oss << "ResultRecord { " << windowStart << ", " << windowEnd << ", " << idLeft << ", " << joinValueLeft << ", "
                << tsLeft << ", " << idRight << ", " << joinValueRight << ", " << tsRight << " }";
            return oss.str();
        }
    };

    struct InputRecord {
        uint64_t id;
        uint64_t join_value;
        uint64_t ts;
        std::string to_string() const {
            std::ostringstream ossTmp;
            ossTmp << id << "," << join_value << "," << ts;
            return ossTmp.str();
        }
    };

    const SchemaPtr leftSchemaTest = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                         ->addField("f1_left", BasicType::UINT64)
                                         ->addField("f2_left", BasicType::UINT64)
                                         ->addField("timestamp", BasicType::UINT64)
                                         ->updateSourceName(*srcName);

    const SchemaPtr rightSchemaTest = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                          ->addField("f1_right", BasicType::UINT64)
                                          ->addField("f2_right", BasicType::UINT64)
                                          ->addField("timestamp", BasicType::UINT64)
                                          ->updateSourceName(*srcName);

    static std::vector<InputRecord>
    shuffleInputVector(std::vector<InputRecord>& inputRecords,
                       const std::vector<std::tuple<QueryId, uint64_t, bool>>& queriesTimesDeploys,
                       const std::pair<uint64_t, uint64_t>& windowSizeSlide);

    static std::vector<InputRecord> miniShuffleHelper(std::vector<InputRecord>& shuffledVector,
                                                      uint64_t guaranteeFrom,
                                                      uint64_t guaranteeTo,
                                                      uint64_t vectorFrom,
                                                      uint64_t vectorTo);

    std::vector<ResultRecord>
    createExpectedResultVector(const std::vector<InputRecord>& leftRecords,
                               const std::vector<InputRecord>& rightRecords,
                               const std::pair<uint64_t, uint64_t>& windowSizeSlide,
                               const std::vector<std::tuple<QueryId, uint64_t, bool>>& queriesTimesDeploy,
                               const SchemaPtr& resultSchema) const;

    static std::pair<std::vector<InputRecord>, std::vector<InputRecord>> createInputRecords(uint64_t numberOfRecords,
                                                                                            uint64_t valueModulo);

    std::vector<TupleBuffer> createExpectedBufferFromInputRecords(const std::vector<InputRecord>& inputRecords,
                                                                  const SchemaPtr& schema,
                                                                  uint64_t fromItem,
                                                                  uint64_t toItem) const;

    void printDifference(std::vector<ResultRecord>& actualResults, std::vector<ResultRecord>& expectedResults) const;

    /**
     * Runs a shared join query for some time. The joinOpHandler contains the deploymentTime of one or more queries. Running for
     * some time is emulated by only emitting tuples from that time. We have one sorted side and one out-of-order side.
     * (some cases allow both sides to be out-of-order in which case the emitting order doesn't matter). The tuples from
     * the sorted side will have the timestamp corresponding to the emulated time in which the ExecutableQueryPlan runs
     * with this configuration. Configuration means, how many queries share this EQP and are managed by the same StreamJoinOperatorHandler.
     * (logic for this done in caller)
     * @param executableQueryPlan the EQP. It will be adjusted with the addition of a new query or the deletion of a deployed
     * query first and then the tuples for the next time frame will be emitted.
     * @param queryTimeDeployOrStop the next time a query gets deployed or stopped for the EQP.
     * @param leftInputBuffers tuples that will be emitted by the left source
     * @param rightInputBuffers tuples that will be emitted by the right source
     * @param leftSideShuffled if the left side is shuffled. If true emit left tuples first, otherwise emit right tuples first.
     */
    void runSharedQueryMockedForSomeTime(const ExecutableQueryPlanPtr& executableQueryPlan,
                                         const std::tuple<QueryId, uint64_t, bool>& queryTimeDeployOrStop,
                                         const std::vector<TupleBuffer>& leftInputBuffers,
                                         const std::vector<TupleBuffer>& rightInputBuffers,
                                         const bool leftSideShuffled) const {

        //add or remove a query from the shared StreamJoinOperatorHandler
        if (std::get<2>(queryTimeDeployOrStop)) {
            const auto queryId = std::get<0>(queryTimeDeployOrStop);
            const auto deployTime = std::get<1>(queryTimeDeployOrStop);
            // !Add query to configuration! (pipeline 0 should be the build pipeline in this case.)
            executableQueryPlan->getPipelines()[0]->addQueryToSharedJoin(queryId, deployTime, sharedJoinApproach);
        } else {
            const auto queryId = std::get<0>(queryTimeDeployOrStop);
            // !Add query to configuration! (pipeline 0 should be the build pipeline in this case.)
            executableQueryPlan->getPipelines()[0]->stopQueryFromSharedJoin(queryId);
        }

        // Emitting the input buffers that contain the tuples that would be emitted while the handler has this particular configuration of deployed queries.
        // We need to care for watermarks to not sent windows if tuples are missing. For this we have one sorted side and one out-of-order side.
        // We need to emit the out-of-order side first, because otherwise we will just get the max watermark from the sorted side
        // and then emit tuples in a random order in the unsorted side which will lead to uncontrollable watermarks.
        auto dataSourceCount = leftSideShuffled ? 0_u64 : 1_u64;// left is 0 and right is 1
        for (int i = 0; i < 2; i++) {
            auto inputBuffers = dataSourceCount == 0_u64 ? leftInputBuffers : rightInputBuffers;
            auto source = executionEngine->getDataSource(executableQueryPlan, dataSourceCount);
            dataSourceCount = (dataSourceCount + 1) % 2;
            for (auto buf : inputBuffers) {
                source->emitBuffer(buf);
            }
        }

        // Let it do the work of processing the emitted tuples before we add another query or remove one to this shared EQP
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    };

    /** Emulates running multiple queries that share the same EQP. The time when a query is deployed or removed is specified in queriesTimesDeploys.
     * The input data was split into multiple chunks that each represent the time the EQP runs while being shared by the momentarily active queries.
     * In the time in which the configuration of the EQP does not change in the regard of which queries share it, the corresponding chunk of data will be emitted by the sources.
     * @note each line in the input csv increments the time by 1
     * @param allInputBuffersLeft input buffers which the left source will emit
     * @param allInputBuffersRight input buffers which the right source will emit
     * @param testSink the sink that will collect the resulting tuples
     * @param query contains the LQP, which is connected to the testSink.
     * @param queriesTimesDeploys a triplet to specify when a new query should be (un)deployed. if the bool is true the query should be deployed.
     * @param leftShuffled the shuffled side has to be emitted first
     * @return a vector with every tuple that was produced and sent to the sink
    */
    std::vector<ResultRecord>
    runMultipleEqualQueriesDeployedDifferentTime(const std::vector<std::vector<TupleBuffer>>& allInputBuffersLeft,
                                                 const std::vector<std::vector<TupleBuffer>>& allInputBuffersRight,
                                                 const std::shared_ptr<CollectTestSink<ResultRecord>>& testSink,
                                                 const Query& query,
                                                 const std::vector<std::tuple<QueryId, uint64_t, bool>>& queriesTimesDeploys,
                                                 const bool leftShuffled) const {

        std::vector<ResultRecord> resultRecords;
        NES_INFO("Creating query: {}", query.getQueryPlan()->toString())
        auto decomposedQueryPlanQueryOne = DecomposedQueryPlan::create(defaultDecomposedQueryId,
                                                                       defaultSharedQueryId,
                                                                       INVALID_WORKER_NODE_ID,
                                                                       query.getQueryPlan()->getRootOperators());

        auto executableQueryPlan = executionEngine->submitQuery(decomposedQueryPlanQueryOne);

        for (uint64_t i = 0; i < queriesTimesDeploys.size(); i++) {
            runSharedQueryMockedForSomeTime(executableQueryPlan,
                                            queriesTimesDeploys[i],
                                            allInputBuffersLeft[i],
                                            allInputBuffersRight[i],
                                            leftShuffled);
        }

        NES_ASSERT(
            nodeEngine->stopDecomposedQueryPlan(defaultSharedQueryId, defaultDecomposedQueryId, QueryTerminationType::Graceful),
            "didn't stop");
        NES_ASSERT(nodeEngine->unregisterDecomposedQueryPlan(defaultSharedQueryId, defaultDecomposedQueryId), "no unregister");

        return testSink->getResult();
    }

    /**
     * Prepares the query to be run. For that it creates a query with a test sink to collect results, as well as sources to emit
     * tuples. It further creates TupleBuffers that contain all InputRecords that will be emitted. For each timeframe in which
     * no new query is deployed or stopped the InputRecords from that time are grouped into one chunk. The tuples of one side will be
     * out of order, so the timestamp of the tuples does not need to correspond to this timeframe. These buffers will be
     * emitted by sources once the StreamJoinOperatorHandler has deployed or stopped all active queries at that time.
     * @param numOfRecords number of records
     * @param joinValueModulo joinValue increments by one for each record, but the value is taken modulo this val
     * @param joinParams contains input and output schemas
     * @param joinWindow window over which the join samples tuples
     * @param keyLeft The join condition is that this left key is equal to the right key
     * @param keyRight The join condition is that this right key is equal to the left key
     * @param queriesTimesDeploys a triplet to specify when a new query should be (un)deployed. if the bool is true the query should be deployed.
     * @param windowSizeSlide the windows size and slide
     * @return resultRecords that a sink wrote. I.e., some probe joined them and a sink stored the result.
     */
    std::vector<ResultRecord>
    prepareRunTwoEqualQueriesDeployedDifferentTime(const uint64_t numOfRecords,
                                                   const uint64_t joinValueModulo,
                                                   const TestUtils::JoinParams& joinParams,
                                                   const WindowTypePtr& joinWindow,
                                                   const std::string& keyLeft,
                                                   const std::string& keyRight,
                                                   const std::vector<std::tuple<QueryId, uint64_t, bool>>& queriesTimesDeploys,
                                                   const std::pair<uint64_t, uint64_t>& windowSizeSlide) const {

        auto [leftRecords, rightRecords] = createInputRecords(numOfRecords, joinValueModulo);

        const auto bufferManager = executionEngine->getBufferManager();

        // Creating a test sink to which the computed results will be written
        const auto testSink = executionEngine->createCollectSink<ResultRecord>(joinParams.outputSchema);
        const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
        const auto testSourceDescriptorLeft = executionEngine->createDataSource(joinParams.inputSchemas[0]);
        const auto testSourceDescriptorRight = executionEngine->createDataSource(joinParams.inputSchemas[1]);

        // Creating a simple join query with the sink attached to it
        const auto query = TestQuery::from(testSourceDescriptorLeft)
                               .joinWith(TestQuery::from(testSourceDescriptorRight))
                               .where(Attribute(keyLeft) == Attribute(keyRight))
                               .window(joinWindow)
                               .sink(testSinkDescriptor);

        // We can not shuffle both sides because of the Watermarks. The watermark will be determined by the biggest tuple seen in both join sides.
        // Therefore, we want one side to be sorted, so its biggest tuple can be controlled and that it remains low.
        std::random_device randomDevice;
        const auto seed = 0;// randomDevice(); for random number
        std::mt19937 generator(seed);
        std::bernoulli_distribution bernoulliDistribution(0.5);
        const bool shuffleLeft = bernoulliDistribution(generator);
        // We don't need any order ingesting tuples if no tuples are emitted before the test ends. Except when queries
        // are removed, but this is handled in the shuffling.
        const bool shuffleBoth = windowSizeSlide.first > numOfRecords;
        NES_INFO("print seed {} which leads to {} side shuffled", seed, shuffleBoth ? "both" : shuffleLeft ? "left" : "right");

        if (shuffleLeft || shuffleBoth) {
            leftRecords = shuffleInputVector(leftRecords, queriesTimesDeploys, windowSizeSlide);
        }
        if (!shuffleLeft || shuffleBoth) {
            rightRecords = shuffleInputVector(rightRecords, queriesTimesDeploys, windowSizeSlide);
        }

        // create TupleBuffers from the created stream
        std::vector<std::vector<TupleBuffer>> allInputBuffersLeft;
        std::vector<std::vector<TupleBuffer>> allInputBuffersRight;
        for (uint64_t i = 0; i < queriesTimesDeploys.size(); ++i) {
            //this might not be the whole time the query runs, but at the next time a different query might be added/removed.
            auto fromItem = std::get<1>(queriesTimesDeploys[i]);
            auto toItem = i < queriesTimesDeploys.size() - 1 ? std::get<1>(queriesTimesDeploys[i + 1]) - 1 : 999;

            allInputBuffersLeft.emplace_back(
                createExpectedBufferFromInputRecords(leftRecords, joinParams.inputSchemas[0], fromItem, toItem));

            allInputBuffersRight.emplace_back(
                createExpectedBufferFromInputRecords(rightRecords, joinParams.inputSchemas[1], fromItem, toItem));
        }

        // Running the query
        auto resultRecords = runMultipleEqualQueriesDeployedDifferentTime(allInputBuffersLeft,
                                                                          allInputBuffersRight,
                                                                          testSink,
                                                                          query,
                                                                          queriesTimesDeploys,
                                                                          shuffleLeft);

        auto expectedResultVector =
            createExpectedResultVector(leftRecords, rightRecords, windowSizeSlide, queriesTimesDeploys, joinParams.outputSchema);

        EXPECT_EQ(resultRecords.size(), expectedResultVector.size());
        EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedResultVector));
        printDifference(resultRecords, expectedResultVector);
        // We return the result records, as someone might want to print them to std::out or do something else
        return resultRecords;
    }
};

TEST_P(StreamJoinQuerySharedExecutionTest, streamJoinSharedExecutionTestSameQueryDifferentDeploymentTime) {
    if (joinStrategy != QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN
        || (sharedJoinApproach != SharedJoinApproach::APPROACH_PROBING
            && sharedJoinApproach != SharedJoinApproach::APPROACH_DELETING)) {
        // Other configurations are not implemented yet. Approaches are described in #5113
        GTEST_SKIP();
    }

    const auto windowSize = Milliseconds(20);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);
    const TestUtils::JoinParams joinParams({leftSchemaTest, rightSchemaTest});
    constexpr std::tuple<QueryId, uint64_t, bool> t1 = {QueryId(0), 0, true};
    constexpr std::tuple<QueryId, uint64_t, bool> t2 = {QueryId(3), 3, true};

    const auto resultRecords =
        prepareRunTwoEqualQueriesDeployedDifferentTime(20,
                                                       10,
                                                       joinParams,
                                                       window,
                                                       "f2_left",
                                                       "f2_right",
                                                       {t1, t2},
                                                       std::make_pair(windowSize.getTime(), windowSize.getTime()));
}

TEST_P(StreamJoinQuerySharedExecutionTest, streamJoinSharedExecutionTestSameQueryDifferentDeploymentTimeSlidingWindow) {
    if (joinStrategy != QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN
        || (sharedJoinApproach != SharedJoinApproach::APPROACH_PROBING
            && sharedJoinApproach != SharedJoinApproach::APPROACH_DELETING)) {
        // Other configurations are not implemented yet. Approaches are described in #5113
        GTEST_SKIP();
    }

    const auto windowSize = Milliseconds(15);
    const auto windowSlide = Milliseconds(5);
    const auto window = SlidingWindow::of(EventTime(Attribute("timestamp")), windowSize, windowSlide);
    const TestUtils::JoinParams joinParams({leftSchemaTest, rightSchemaTest});
    constexpr std::tuple<QueryId, uint64_t, bool> t1 = {QueryId(0), 0, true};
    constexpr std::tuple<QueryId, uint64_t, bool> t2 = {QueryId(3), 3, true};

    const auto resultRecords =
        prepareRunTwoEqualQueriesDeployedDifferentTime(20,
                                                       10,
                                                       joinParams,
                                                       window,
                                                       "f2_left",
                                                       "f2_right",
                                                       {t1, t2},
                                                       std::make_pair(windowSize.getTime(), windowSlide.getTime()));
}

TEST_P(StreamJoinQuerySharedExecutionTest, streamJoinSharedExecutionTestSameQueryThreeDifferentDeploymentTimes) {
    if (joinStrategy != QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN
        || (sharedJoinApproach != SharedJoinApproach::APPROACH_PROBING
            && sharedJoinApproach != SharedJoinApproach::APPROACH_DELETING)) {
        // Other configurations are not implemented yet. Approaches are described in #5113
        GTEST_SKIP();
    }

    const auto windowSize = Milliseconds(20);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);
    const TestUtils::JoinParams joinParams({leftSchemaTest, rightSchemaTest});
    constexpr std::tuple<QueryId, uint64_t, bool> t1 = {QueryId(0), 0, true};
    constexpr std::tuple<QueryId, uint64_t, bool> t2 = {QueryId(3), 3, true};
    constexpr std::tuple<QueryId, uint64_t, bool> t3 = {QueryId(6), 6, true};

    const auto resultRecords =
        prepareRunTwoEqualQueriesDeployedDifferentTime(20,
                                                       10,
                                                       joinParams,
                                                       window,
                                                       "f2_left",
                                                       "f2_right",
                                                       {t1, t2, t3},
                                                       std::make_pair(windowSize.getTime(), windowSize.getTime()));
}

TEST_P(StreamJoinQuerySharedExecutionTest,
       streamJoinSharedExecutionTestSameQueryDifferentDeploymentTimeAfterFirstWindowTriggered) {
    if (joinStrategy != QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN
        || (sharedJoinApproach != SharedJoinApproach::APPROACH_PROBING
            && sharedJoinApproach != SharedJoinApproach::APPROACH_DELETING)) {
        // Other configurations are not implemented yet. Approaches are described in #5113
        GTEST_SKIP();
    }

    const auto windowSize = Milliseconds(5);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);
    const TestUtils::JoinParams joinParams({leftSchemaTest, rightSchemaTest});
    constexpr std::tuple<QueryId, uint64_t, bool> t1 = {QueryId(0), 0, true};
    constexpr std::tuple<QueryId, uint64_t, bool> t2 = {QueryId(7), 7, true};

    const auto resultRecords =
        prepareRunTwoEqualQueriesDeployedDifferentTime(20,
                                                       10,
                                                       joinParams,
                                                       window,
                                                       "f2_left",
                                                       "f2_right",
                                                       {t1, t2},
                                                       std::make_pair(windowSize.getTime(), windowSize.getTime()));
}

TEST_P(StreamJoinQuerySharedExecutionTest, streamJoinSharedExecutionTestSameQueryDifferentDeploymentTimeAndUndeploySecondQuery) {
    if (joinStrategy != QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN
        || (sharedJoinApproach != SharedJoinApproach::APPROACH_PROBING
            && sharedJoinApproach != SharedJoinApproach::APPROACH_DELETING)) {
        // Other configurations are not implemented yet. Approaches are described in #5113
        GTEST_SKIP();
    }

    const auto windowSize = Milliseconds(10);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);
    const TestUtils::JoinParams joinParams({leftSchemaTest, rightSchemaTest});
    constexpr std::tuple<QueryId, uint64_t, bool> t1 = {QueryId(0), 0, true};
    constexpr std::tuple<QueryId, uint64_t, bool> t2 = {QueryId(3), 3, true};
    constexpr std::tuple<QueryId, uint64_t, bool> t3 = {QueryId(3), 13, false};

    const auto resultRecords =
        prepareRunTwoEqualQueriesDeployedDifferentTime(20,
                                                       10,
                                                       joinParams,
                                                       window,
                                                       "f2_left",
                                                       "f2_right",
                                                       {t1, t2, t3},
                                                       std::make_pair(windowSize.getTime(), windowSize.getTime()));
}

TEST_P(StreamJoinQuerySharedExecutionTest, streamJoinSharedExecutionTestSameQueryDifferentDeploymentTimeLargeSlidingWindow) {
    if (joinStrategy != QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN
        || (sharedJoinApproach != SharedJoinApproach::APPROACH_PROBING
            && sharedJoinApproach != SharedJoinApproach::APPROACH_DELETING)) {
        // Other configurations are not implemented yet. Approaches are described in #5113
        GTEST_SKIP();
    }

    const auto windowSize = Milliseconds(50);
    const auto windowSlide = Milliseconds(40);
    const auto timestampFieldName = "timestamp";
    const auto window = SlidingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize, windowSlide);
    const TestUtils::JoinParams joinParams({leftSchemaTest, rightSchemaTest});
    constexpr std::tuple<QueryId, uint64_t, bool> t1 = {QueryId(0), 0, true};
    constexpr std::tuple<QueryId, uint64_t, bool> t2 = {QueryId(68), 68, true};

    const auto resultRecords =
        prepareRunTwoEqualQueriesDeployedDifferentTime(200,
                                                       20,
                                                       joinParams,
                                                       window,
                                                       "f2_left",
                                                       "f2_right",
                                                       {t1, t2},
                                                       std::make_pair(windowSize.getTime(), windowSlide.getTime()));
}

/**
 * The shuffling starts random but every query deployment time is inspected and if some tuples need to be ingested
 * before a change, it will be guaranteed that these tuples are ingested before that change.
 * 1. If a query arrives later than the end time of a window, we need to have sent all tuples that are part of the emitted windows.
 * 2. If a query is un-deployed, all tuples until this point have to have been sent
 * @param inputRecords the records to shuffle
 * @param queriesTimesDeploys triplet that contains each time a query is deployed or un-deployed
 * @param windowSizeSlide the window size and slide
 * @return a shuffled vector with the same InputRecords as the vector inputRecords.
 */
std::vector<StreamJoinQuerySharedExecutionTest::InputRecord> StreamJoinQuerySharedExecutionTest::shuffleInputVector(
    std::vector<InputRecord>& inputRecords,
    const std::vector<std::tuple<QueryId, uint64_t, bool>>& queriesTimesDeploys,
    const std::pair<uint64_t, uint64_t>& windowSizeSlide) {

    std::random_device randomDevice;
    const auto seed = 0;//randomDevice();
    NES_INFO("print seed determining std::shuffle() {}", seed);
    std::mt19937 generator(seed);
    std::shuffle(inputRecords.begin(), inputRecords.end(), generator);

    // Loop starts from last entry and will overflow after it has reached the first entry and therefore stop.
    for (uint64_t i = queriesTimesDeploys.size() - 1; i < queriesTimesDeploys.size(); i--) {
        const auto changingTimeQuery = std::get<1>(queriesTimesDeploys[i]);
        const auto deployQuery = std::get<2>(queriesTimesDeploys[i]);
        // If a query un-deploys all records up until this point need to have been sent
        if (!deployQuery) {
            inputRecords = miniShuffleHelper(inputRecords, 0, changingTimeQuery, 0, changingTimeQuery);
        }
        // If a query deploys records only need to be sorted if windows where triggered before this query
        else if (windowSizeSlide.first < changingTimeQuery) {
            auto guaranteeTo = windowSizeSlide.first;
            while (guaranteeTo + windowSizeSlide.second < changingTimeQuery) {
                guaranteeTo += windowSizeSlide.second;
            }
            inputRecords = miniShuffleHelper(inputRecords, 0, guaranteeTo, 0, changingTimeQuery);
        }
    }

    int cnt = 0;
    for (auto record : inputRecords) {
        NES_INFO("Shuffled vector at pos {}: {}", cnt++, record.to_string())
    }
    return inputRecords;
}

/**
 * Receives a shuffled vector and returns a vector in which the guaranteed timestamp range will be in the specified vector range.
 * Tuples that don't need to be changed to achieve this will stay in the same order as in the shuffled vector.
 * @param shuffledVector the vector with tuples of the format InputRecord
 * @param guaranteeFrom every ts equal or bigger than this has to be in the specified vector range
 * @param guaranteeTo every ts smaller than this has to be in the specified vector range
 * @param vectorFrom including
 * @param vectorTo excluding
 * @return the shuffled vector in which some elements were probably changed, so now the guarantees hold true in this vector.
 */
std::vector<StreamJoinQuerySharedExecutionTest::InputRecord>
StreamJoinQuerySharedExecutionTest::miniShuffleHelper(std::vector<InputRecord>& shuffledVector,
                                                      uint64_t guaranteeFrom,
                                                      uint64_t guaranteeTo,
                                                      uint64_t vectorFrom,
                                                      uint64_t vectorTo) {
    NES_INFO("Shuffling input vector tuples guaranteed from {} to {}, in range of vector from {} to {}",
             guaranteeFrom,
             guaranteeTo,
             vectorFrom,
             vectorTo)
    std::vector<uint64_t> hasToChangePos;
    std::vector<uint64_t> cantChangePos;

    for (uint64_t i = 0; i < shuffledVector.size(); ++i) {
        const auto inputRecord = shuffledVector[i];
        if (inputRecord.ts >= guaranteeFrom && inputRecord.ts < guaranteeTo) {
            if (i >= vectorFrom && i < vectorTo) {
                cantChangePos.push_back(i);
            } else {
                hasToChangePos.push_back(i);
            }
        }
    }
    uint64_t swapPos = vectorTo - 1;
    for (uint64_t i = 0; i < hasToChangePos.size(); ++i) {
        while (std::find(cantChangePos.begin(), cantChangePos.end(), swapPos) != cantChangePos.end()) {
            swapPos--;
        }
        std::swap(shuffledVector[hasToChangePos[i]], shuffledVector[swapPos]);
        NES_INFO("swapped positions {} ({}) and {} ({})",
                 swapPos,
                 shuffledVector[swapPos].to_string(),
                 hasToChangePos[i],
                 shuffledVector[hasToChangePos[i]].to_string())
        swapPos--;
    }

    return shuffledVector;
}

/**
 * Prints the difference between the computed tuples (actual results) and the expected result tuples that we wanted (which we
 * compute with createExpectedResultVector()). Each tuple that should not be present will be printed. Each
 * tuple that should be present but isn't will be printed. Each tuple that was created too many times will be printed. Finally,
 * each tuple that was created to few times will be printed
 * @param actualResults the systems computed results
 * @param expectedResults the results that were expected
 */
void StreamJoinQuerySharedExecutionTest::printDifference(std::vector<ResultRecord>& actualResults,
                                                         std::vector<ResultRecord>& expectedResults) const {

    auto bufferManager = executionEngine->getBufferManager();

    std::vector<ResultRecord> wronglyCreated;
    for (auto actualRecord : actualResults) {
        if (std::find(expectedResults.begin(), expectedResults.end(), actualRecord) == expectedResults.end()) {
            NES_WARNING("Created record that should not have been created {}", actualRecord.to_string())
            wronglyCreated.push_back(actualRecord);
        }
    }

    std::vector<ResultRecord> missingRecords;
    for (auto expectedRecord : expectedResults) {
        if (std::find(actualResults.begin(), actualResults.end(), expectedRecord) == actualResults.end()) {
            NES_WARNING("Did not create Record that was expected to be created {}", expectedRecord.to_string())
            missingRecords.push_back((expectedRecord));
        }
    }

    auto expectedResultsCopy = expectedResults;
    for (auto actualRecord : actualResults) {
        auto it = std::find(expectedResultsCopy.begin(), expectedResultsCopy.end(), actualRecord);
        if (it != expectedResultsCopy.end()) {
            expectedResultsCopy.erase(it);
        } else {
            if (std::find(wronglyCreated.begin(), wronglyCreated.end(), actualRecord) == wronglyCreated.end()) {
                NES_WARNING("Created this Record multiple times {}", actualRecord.to_string())
            }
        }
    }

    for (auto expectedRecord : expectedResults) {
        auto it = std::find(actualResults.begin(), actualResults.end(), expectedRecord);
        if (it != actualResults.end()) {
            actualResults.erase(it);
        } else {
            if (std::find(missingRecords.begin(), missingRecords.end(), expectedRecord) == missingRecords.end()) {
                NES_WARNING("Did not create this Record often enough {}", expectedRecord.to_string())
            }
        }
    }
}

/**
 * Method calculates all expected ResultRecord. For this purpose it calculates all possible windows from the biggest timestamp
 * and for each window it checks all InputRecords from both sides to see if they are a part of this window. It uses
 * queriesTimesDeploy to get the windows for each individual query
 * @param leftRecords the input records from the left side
 * @param rightRecords the input records from the right side
 * @param windowSizeSlide the window size and the window slide
 * @param queriesTimesDeploy Entry for each time a query gets deployed or un-deployed
 * @param resultSchema the schema which the resultRecords use
 * @return a vector with the expected result for this query configuration
 */
std::vector<StreamJoinQuerySharedExecutionTest::ResultRecord> StreamJoinQuerySharedExecutionTest::createExpectedResultVector(
    const std::vector<InputRecord>& leftRecords,
    const std::vector<InputRecord>& rightRecords,
    const std::pair<uint64_t, uint64_t>& windowSizeSlide,
    const std::vector<std::tuple<QueryId, uint64_t, bool>>& queriesTimesDeploy,
    const SchemaPtr& resultSchema) const {

    const uint64_t windowSize = windowSizeSlide.first;
    const uint64_t windowSlide = windowSizeSlide.second;

    // Compute the max timestamp over both input files
    const auto maxItTsLeft =
        std::max_element(leftRecords.begin(), leftRecords.end(), [](const InputRecord& a, const InputRecord& b) {
            return a.ts < b.ts;
        });
    const auto maxTsLeft = maxItTsLeft->ts;
    const auto maxItTsRight =
        std::max_element(rightRecords.begin(), rightRecords.end(), [](const InputRecord& a, const InputRecord& b) {
            return a.ts < b.ts;
        });
    const auto maxTsRight = maxItTsRight->ts;
    const auto maxTs = std::max(maxTsLeft, maxTsRight);
    NES_INFO("max ts left {} right {}", maxTsLeft, maxTsRight)

    // Compute windows that may potentially contain tuples
    std::vector<std::pair<uint64_t, uint64_t>> windows;
    for (auto queryTimeDeploy : queriesTimesDeploy) {
        // New Query gets deployed. Create all windows for this query.
        if (std::get<2>(queryTimeDeploy)) {
            auto windowStart = std::get<1>(queryTimeDeploy);
            while (windowStart <= maxTs) {
                windows.emplace_back(windowStart, windowStart + windowSize);
                windowStart += windowSlide;
            }
        } else {
            // Old query gets removed. Find deploymentTime of removed query, by finding the same query in queriesTimesDeploy when the bool is true
            auto queryId = std::get<0>(queryTimeDeploy);
            auto deployTime = 0_u64;
            for (auto tempQueryTimeDeploy : queriesTimesDeploy) {
                if (std::get<0>(tempQueryTimeDeploy) == queryId && std::get<2>(tempQueryTimeDeploy)) {
                    deployTime = std::get<1>(tempQueryTimeDeploy);
                }
            }
            // Erase windows that won't be created. These are all windows that were created for this query and that have a start time later than the removal time
            for (auto it = windows.begin(); it != windows.end();) {
                if (it->first >= std::get<1>(queryTimeDeploy) && (it->first - deployTime) % windowSize == 0) {
                    it = windows.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }

    // Sort windows just so the print is prettier. Order does not influence the tests
    std::sort(windows.begin(), windows.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });

    // For each window iterate over all left and right tuples and check for each pair if they are part of the windows. Result is first stored as a string
    std::stringstream joinedRecordsAsStringStream;
    for (auto [window_start, window_end] : windows) {
        for (auto leftRecord : leftRecords) {
            for (auto rightRecord : rightRecords) {

                if (leftRecord.ts < window_start || leftRecord.ts >= window_end) {
                    continue;
                }
                if (rightRecord.ts < window_start || rightRecord.ts >= window_end) {
                    continue;
                }

                if (leftRecord.join_value == rightRecord.join_value) {
                    joinedRecordsAsStringStream << window_start << "," << window_end << "," << leftRecord.to_string() << ","
                                                << rightRecord.to_string() << "\n";
                }
            }
        }
    }

    // Use TestUtil function to create the vector of ResultRecords from the constructed result string
    std::vector<ResultRecord> res;
    auto buffs =
        TestUtils::createExpectedBufferFromStream(joinedRecordsAsStringStream, resultSchema, executionEngine->getBufferManager());
    for (auto buf : buffs) {
        auto tempVec = TestUtils::createVecFromTupleBuffer<ResultRecord>(buf);
        res.insert(res.end(), tempVec.begin(), tempVec.end());
    }
    return res;
}

/**
 * constructs numOfRecords input records for both left and right side in a sorted manner
 * @param numberOfRecords how many records to produce
 * @param valueModulo how often the join value is repeated (joinValue = numberOfCurrentRecord % valueModulo
 * @return a pair of vectors with the input records for the left and right side
 */
std::pair<std::vector<StreamJoinQuerySharedExecutionTest::InputRecord>,
          std::vector<StreamJoinQuerySharedExecutionTest::InputRecord>>
StreamJoinQuerySharedExecutionTest::createInputRecords(const uint64_t numberOfRecords, const uint64_t valueModulo) {
    std::vector<InputRecord> leftRecords;
    std::vector<InputRecord> rightRecords;

    for (auto i = 0_u64; i < numberOfRecords; i++) {
        leftRecords.push_back(InputRecord(i, (i % valueModulo), i));
        rightRecords.push_back(InputRecord(1000 + i, (i % valueModulo), i));
    }

    return std::make_pair(leftRecords, rightRecords);
}

/**
 * creates a vector of tupleBuffers from a part of a vector of InputRecords. The part of the vector is determined by the variables
 * fromItem and toItem and the actual calculations closely resemble the function in TestUtils createExpectedBufferFromStreamSpecificLines()
 * @param inputRecords a vector of all InputRecords
 * @param schema the schema of each InputRecord
 * @param fromItem the first item considered from InputRecords (inclusive)
 * @param toItem the last item considered from InputRecords (inclusive)
 * @return 
 */
std::vector<TupleBuffer>
StreamJoinQuerySharedExecutionTest::createExpectedBufferFromInputRecords(const std::vector<InputRecord>& inputRecords,
                                                                         const SchemaPtr& schema,
                                                                         const uint64_t fromItem,
                                                                         const uint64_t toItem) const {

    const auto bufferManager = executionEngine->getBufferManager();
    std::vector<PhysicalTypePtr> physicalTypes;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    std::vector<TupleBuffer> allBuffers;
    auto tupleCount = 0_u64;
    auto parser = std::make_shared<CSVParser>(schema->fields.size(), physicalTypes, ",");
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto maxTuplePerBuffer = bufferManager->getBufferSize() / schema->getSchemaSizeInBytes();

    for (auto i = 0_u64; i < inputRecords.size(); i++) {
        if (i < fromItem || i > toItem) {
            continue;
        }

        auto testBuffer = MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(tupleBuffer, schema);
        const auto [id, join_value, ts] = inputRecords[i];

        //write tuple
        for (auto j = 0_u64; j < schema->fields.size(); j++) {
            std::string val;
            switch (j) {
                case 0: val = std::to_string(id); break;
                case 1: val = std::to_string(join_value); break;
                case 2: val = std::to_string(ts); break;
                default: NES_ERROR("schema for an input record should have 3 fields")
            }
            parser->writeFieldValueToTupleBuffer(val, j, testBuffer, schema, tupleCount, bufferManager);
        }

        tupleCount++;
        if (tupleCount >= maxTuplePerBuffer) {
            tupleBuffer.setNumberOfTuples(tupleCount);
            allBuffers.emplace_back(tupleBuffer);
            tupleCount = 0;

            tupleBuffer = bufferManager->getBufferBlocking();
        }
    }

    if (tupleCount > 0) {
        tupleBuffer.setNumberOfTuples(tupleCount);
        allBuffers.emplace_back(tupleBuffer);
    }
    return allBuffers;
}

INSTANTIATE_TEST_CASE_P(testStreamJoinQueries,
                        StreamJoinQuerySharedExecutionTest,
                        JOIN_SHARING_CONFIGURATIONS,
                        [](const testing::TestParamInfo<StreamJoinQuerySharedExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(std::get<0>(info.param))) + "_"
                                + std::string(magic_enum::enum_name(std::get<1>(info.param)));
                        });

}// namespace NES::Runtime::Execution
