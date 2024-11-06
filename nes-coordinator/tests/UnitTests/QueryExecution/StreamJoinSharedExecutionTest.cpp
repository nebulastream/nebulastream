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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinProbeOperator.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <gmock/gmock-matchers.h>
#include <vector>

namespace NES::Runtime::Execution {

using namespace std::chrono_literals;
constexpr auto queryCompilerDumpMode = NES::QueryCompilation::DumpMode::NONE;

/*
 * This method is inspired by StreamJoinExecutionTest.cpp. Design decisions have been taken from the other class as a starting point.
 * !!! Note that the tests are only green, because the test inputs provide ordered tuples. If tuples arrive out of order the content of slices
 * needs to be changed, which will be addressed in a later PR. Issue #5113!!!
 */
class StreamJoinQuerySharedExecutionTest
    : public Testing::BaseUnitTest,
      public ::testing::WithParamInterface<
          std::tuple<QueryCompilation::StreamJoinStrategy, QueryCompilation::WindowingStrategy>> {
  public:
    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    std::shared_ptr<NodeEngine> nodeEngine;
    static constexpr DecomposedQueryId defaultDecomposedQueryId = INVALID_DECOMPOSED_QUERY_PLAN_ID;
    static constexpr SharedQueryId defaultSharedQueryId = INVALID_SHARED_QUERY_ID;
    static constexpr std::chrono::milliseconds defaultTimeout = 1s;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("StreamJoinQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("QueryExecutionTest: Setup StreamJoinQueryExecutionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup StreamJoinQueryExecutionTest test class.");
        Testing::BaseUnitTest::SetUp();
        joinStrategy = std::get<0>(NES::Runtime::Execution::StreamJoinQuerySharedExecutionTest::GetParam());
        windowingStrategy = std::get<1>(NES::Runtime::Execution::StreamJoinQuerySharedExecutionTest::GetParam());
        const auto numWorkerThreads = 1;
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompilerDumpMode,
                                                                         numWorkerThreads,
                                                                         joinStrategy,
                                                                         windowingStrategy);
        nodeEngine = executionEngine->getNodeEngine();
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("QueryExecutionTest: Tear down StreamJoinQueryExecutionTest test case.");
        EXPECT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /**
     * Runs a shared join query for some time. The joinOpHandler contains the deploymentTime of one or more queries. Running for some time
     * is emulated by only emitting tuples from that time. (logic for this done in caller)
     * @tparam ResultRecord
     * @param testSink
     * @param query
     * @param deploymentTimes
     * @param leftInputBuffer
     * @param rightInputBuffer
     * @param deleteQueryTime
     * @param notLast
     * @return the tuples that were fully processed while this query was running. This means if we receive a watermark that triggers a window
     * the tuples will be written to this sink. When a window does not trigger and this is not the last deployment (notLast=true) the state (slices)
     * are stored and can be triggered once the next watermark arrives or the last deployment finishes.
     */
    template<typename ResultRecord>
    std::vector<ResultRecord> runSharedQueryMockedForSomeTime(const std::shared_ptr<CollectTestSink<ResultRecord>> testSink,
                                                              const Query& query,
                                                              const std::vector<uint64_t> deploymentTimes,
                                                              const std::vector<Runtime::TupleBuffer> leftInputBuffer,
                                                              const std::vector<Runtime::TupleBuffer> rightInputBuffer,
                                                              const std::tuple<QueryId, uint64_t, bool> deleteQueryTime,
                                                              const bool notLast) {

        NES_INFO("Creating query: {}", query.getQueryPlan()->toString())
        auto decomposedQueryPlanQueryOne = DecomposedQueryPlan::create(defaultDecomposedQueryId,
                                                                       defaultSharedQueryId,
                                                                       INVALID_WORKER_NODE_ID,
                                                                       query.getQueryPlan()->getRootOperators());

        //add deployment times and flag to keep opHandler for next deployTime (not for the last deployTime as this leads to an error when trying to free up the buffers in the teardown)
        auto logicalJoinOperator = decomposedQueryPlanQueryOne->getOperatorByType<LogicalJoinOperator>()[0];
        logicalJoinOperator->setFlagKeepOperator(notLast);
        for (auto deploymentTime : deploymentTimes) {
            logicalJoinOperator->setDeployAgainAtTime(QueryId(deploymentTime), deploymentTime);
        }

        // Iff true a deploymentTime that was still running the last time should be removed. To emulate a flow scenario that is a bit more realistic, we will create the streamJoinOpHandler with this time and call remove() before the tuples will be sent
        if (std::get<2>(deleteQueryTime)) {
            logicalJoinOperator->setDeployAgainAtTime(std::get<0>(deleteQueryTime), std::get<1>(deleteQueryTime));
        }
        auto planQueryOne = executionEngine->submitQuery(decomposedQueryPlanQueryOne);
        if (std::get<2>(deleteQueryTime)) {//remove a deploymentTime
            auto phyProbe = decomposedQueryPlanQueryOne
                                ->getOperatorByType<QueryCompilation::PhysicalOperators::PhysicalStreamJoinOperator>()[0];
            phyProbe->getJoinOperatorHandler()->removeQueryFromSharedJoin(std::get<0>(deleteQueryTime));
        }

        // Emitting the input buffers that contain the tuples that would be while the handler has this particular configuration of deployed queries.
        auto dataSourceCount = 0_u64;
        for (auto inputBuffers : {leftInputBuffer, rightInputBuffer}) {
            auto source = executionEngine->getDataSource(planQueryOne, dataSourceCount);
            dataSourceCount++;
            for (auto buf : inputBuffers) {
                source->emitBuffer(buf);
            }
        }

        // Let it do the work of processing the emitted tuples before we stop it and start it again with multiple window definitions
        // (same scenario as if a new query got added and this planQueryOne was stopped -> changed -> started again.) (would probably work without the sleep)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // need to stop and unregister, in contrast to just stopping with the execution engine. (Maybe just because I am using the same decomposedQueryPlanId)
        // This does hopefully ensure that the sink receives all tuples, as the tests depend on it and this does not seem to be the case with executionEngine.stop()
        NES_ASSERT(
            nodeEngine->stopDecomposedQueryPlan(defaultSharedQueryId, defaultDecomposedQueryId, QueryTerminationType::Graceful),
            "didnt stop");
        NES_ASSERT(nodeEngine->unregisterDecomposedQueryPlan(defaultSharedQueryId, defaultDecomposedQueryId), "no unregister");

        return testSink->getResult();
    };

    /**
     * Runs multiple equal queries one after the other. The time when a query is deployed or removed is specified in queriesTimesDeploys.
     * Deploying a query means that the first query gets undeployed and another query with the same LQP will be started. This new query
     * will contain all the deploymentTimes of the first query plus its own deploymentTime. This means each query runs until another query
     * is added or removed. We split up the input data to multiple chunks that each represent the time one query runs (Each
     * line in the input csv increments the time by 1). While a query is running its chunk will be emitted and processed. So the whole
     * setup emulates a scenario where the query is not stopped at all and instead the joinOpHandler is just adjusted on the fly.
     * @tparam ResultRecord
     * @param inputs name of input csv files. content is stored in two files for each side. these have the same name with a "1" or a "2" as a prefix
     * @param testSinks each query will write to a particular sink once it is deployed. we collect the results from all sinks
     * @param queries contains the LQP, which contains to which sink it is connected.
     * @param queriesTimesDeploys a triplet to tell the test when a new query should be deployed. if the bool is true the query should be deployed.
     * If the bool is false this means a query should be removed. To this the same QueryId needed to be deployed before.
     * @return
    */
    template<typename ResultRecord>
    std::vector<ResultRecord>
    runTwoEqualQueriesDeployedDifferentTime(const std::vector<std::pair<SchemaPtr, std::string>>& inputs,
                                            const std::vector<std::shared_ptr<CollectTestSink<ResultRecord>>> testSinks,
                                            const std::vector<Query> queries,
                                            const std::vector<std::tuple<QueryId, uint64_t, bool>> queriesTimesDeploys) {

        // Creating the input buffers
        auto bufferManager = executionEngine->getBufferManager();

        std::vector<std::vector<Runtime::TupleBuffer>> allInputBuffersLeft;
        std::vector<std::vector<Runtime::TupleBuffer>> allInputBuffersRight;
        allInputBuffersLeft.reserve(queriesTimesDeploys.size());
        allInputBuffersRight.reserve(queriesTimesDeploys.size());

        for (uint64_t i = 0; i < queriesTimesDeploys.size(); ++i) {
            auto fromTime = std::get<1>(queriesTimesDeploys[i]);
            auto toTime = i < queriesTimesDeploys.size() - 1 ? std::get<1>(queriesTimesDeploys[i + 1]) - 1 : 999;
            allInputBuffersLeft.emplace_back(TestUtils::createExpectedBuffersFromCsvSpecificLines(inputs[0].second,
                                                                                                  inputs[0].first,
                                                                                                  bufferManager,
                                                                                                  fromTime,
                                                                                                  toTime));
            allInputBuffersRight.emplace_back(TestUtils::createExpectedBuffersFromCsvSpecificLines(inputs[1].second,
                                                                                                   inputs[1].first,
                                                                                                   bufferManager,
                                                                                                   fromTime,
                                                                                                   toTime));
        }

        std::vector<ResultRecord> resultRecords;
        std::vector<uint64_t> deployTimes;
        for (uint64_t i = 0; i < queriesTimesDeploys.size(); ++i) {
            std::tuple<QueryId, uint64_t, bool> deleteQuery = {QueryId(0), 3, false};//dummy dont delete anything (false)
            if (std::get<2>(queriesTimesDeploys[i])) {
                deployTimes.push_back(std::get<1>(queriesTimesDeploys[i]));
            } else {
                for (auto qTD : queriesTimesDeploys) {
                    if (std::get<0>(qTD) == std::get<0>(queriesTimesDeploys[i])
                        && std::get<1>(qTD) != std::get<1>(queriesTimesDeploys[i])) {
                        deleteQuery = qTD;
                        //vec.erase(std::remove(vec.begin(), vec.end(), 2), vec.end());
                        deployTimes.erase(std::remove(deployTimes.begin(), deployTimes.end(), std::get<1>(qTD)));
                    }
                }
            }

            auto tuplesInSink = runSharedQueryMockedForSomeTime(testSinks[i],
                                                                queries[i],
                                                                deployTimes,
                                                                allInputBuffersLeft[i],
                                                                allInputBuffersRight[i],
                                                                deleteQuery,
                                                                i != queriesTimesDeploys.size() - 1);
            NES_INFO("ran query {} and this wrote {} tuples to its sink", i, tuplesInSink.size());
            resultRecords.insert(resultRecords.end(), tuplesInSink.begin(), tuplesInSink.end());
        }

        return resultRecords;
    }

    /**
     * prepares identical queries with "identical" sinks (same variables for the sink but distinct objects). Furthermore gets the
     * expected results and performs the actual test if the actual and expected results are the same
     * @tparam ResultRecord
     * @param csvFileParams
     * @param joinParams
     * @param joinWindow
     * @param keyLeft
     * @param keyRight
     * @param deployTimes
     * @return resultRecords that a sink wrote. I.e., some probe joined them and a sink stored the result.
     */
    template<typename ResultRecord>
    std::vector<ResultRecord>
    prepareRunTwoEqualQueriesDeployedDifferentTime(const TestUtils::CsvFileParams& csvFileParams,
                                                   const TestUtils::JoinParams& joinParams,
                                                   const WindowTypePtr& joinWindow,
                                                   const std::string keyLeft,
                                                   const std::string keyRight,
                                                   const std::vector<std::tuple<QueryId, uint64_t, bool>> deployTimes) {

        // Getting the expected output tuples
        auto bufferManager = executionEngine->getBufferManager();

        std::vector<ResultRecord> expectedSinkVector;
        const auto expectedSinkBuffers =
            TestUtils::createExpectedBuffersFromCsv(csvFileParams.expectedFile, joinParams.outputSchema, bufferManager);

        for (const auto& buf : expectedSinkBuffers) {
            const auto tmpVec = TestUtils::createVecFromTupleBuffer<ResultRecord>(buf);
            expectedSinkVector.insert(expectedSinkVector.end(), tmpVec.begin(), tmpVec.end());
        }

        std::vector<std::shared_ptr<CollectTestSink<ResultRecord>>> testSinks;
        std::vector<Query> queries;
        for (uint64_t i = 0; i < deployTimes.size(); i++) {
            auto testSink = executionEngine->createCollectSink<ResultRecord>(joinParams.outputSchema);
            auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
            auto testSourceDescriptorLeft = executionEngine->createDataSource(joinParams.inputSchemas[0]);
            auto testSourceDescriptorRight = executionEngine->createDataSource(joinParams.inputSchemas[1]);

            auto query = TestQuery::from(testSourceDescriptorLeft)
                             .joinWith(TestQuery::from(testSourceDescriptorRight))
                             .where(Attribute(keyLeft) == Attribute(keyRight))
                             .window(joinWindow)
                             .sink(testSinkDescriptor);

            queries.push_back(query);
            testSinks.push_back(testSink);
        }

        // Running the query
        auto resultRecords =
            runTwoEqualQueriesDeployedDifferentTime<ResultRecord>({{joinParams.inputSchemas[0], csvFileParams.inputCsvFiles[0]},
                                                                   {joinParams.inputSchemas[1], csvFileParams.inputCsvFiles[1]}},
                                                                  testSinks,
                                                                  queries,
                                                                  deployTimes);

        // Checking for correctness
        EXPECT_EQ(resultRecords.size(), expectedSinkVector.size());
        EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedSinkVector));
        // We return the result records, as someone might want to print them to std::out or do something else
        return resultRecords;
    }

    QueryCompilation::StreamJoinStrategy joinStrategy;
    QueryCompilation::WindowingStrategy windowingStrategy;
};

TEST_P(StreamJoinQuerySharedExecutionTest, streamJoinExecutionTestSameQueryDifferentDeploymentTime) {
    if (joinStrategy != QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN
        || windowingStrategy != QueryCompilation::WindowingStrategy::SLICING) {
        //started for nlj with slicing. Afterward all hj strategies should be implemented with slicing as well. Bucketing will
        //most likely not be implemented, as my information is that it is supposed to be discontinued.
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;

        uint64_t test1f1_left;
        uint64_t test1f2_left;
        uint64_t test1timestamp;
        uint64_t test2f1_right;
        uint64_t test2f2_right;
        uint64_t test2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1f1_left == rhs.test1f1_left && test1f2_left == rhs.test1f2_left && test1timestamp == rhs.test1timestamp
                && test2f1_right == rhs.test2f1_right && test2f2_right == rhs.test2f2_right
                && test2timestamp == rhs.test2timestamp;
        }

        std::string to_string() const {
            std::ostringstream oss;
            oss << "ResultRecord { "
                << "wStart: " << test1test2Start << ", "
                << "wEnd: " << test1test2End << ", "
                << "lValue: " << test1f1_left << ", "
                << "lJoinValue: " << test1f2_left << ", "
                << "lTs: " << test1timestamp << ", "
                << "rValue: " << test2f1_right << ", "
                << "rJoinValue: " << test2f2_right << ", "
                << "rTs: " << test2timestamp << " }";
            return oss.str();
        }
    };

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64)
                                ->updateSourceName(*srcName);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64)
                                 ->updateSourceName(*srcName);

    const auto windowSize = Milliseconds(20);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);
    //const auto window = SlidingWindow::of(EventTime(Attribute(timestampFieldName)), Milliseconds(10), Milliseconds(5));

    TestUtils::CsvFileParams csvFileParams("stream_join_shared_left.csv",
                                           "stream_join_shared_right.csv",
                                           "stream_join_shared_sink1.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    std::tuple<QueryId, uint64_t, bool> t1 = {QueryId(0), 0, true};
    std::tuple<QueryId, uint64_t, bool> t2 = {QueryId(3), 3, true};
    const auto resultRecords = prepareRunTwoEqualQueriesDeployedDifferentTime<ResultRecord>(csvFileParams,
                                                                                            joinParams,
                                                                                            window,
                                                                                            "f2_left",
                                                                                            "f2_right",
                                                                                            {t1, t2});

    int i = 0;
    for (auto record : resultRecords) {
        NES_INFO("record {}: {}", i, record.to_string());
        i++;
    }
}

TEST_P(StreamJoinQuerySharedExecutionTest, streamJoinExecutionTestSameQueryDifferentDeploymentTimeSliding) {
    if (joinStrategy != QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN
        || windowingStrategy != QueryCompilation::WindowingStrategy::SLICING) {
        //started for nlj with slicing. Afterward all hj strategies should be implemented with slicing as well. Bucketing will
        //most likely not be implemented, as my information is that it is supposed to be discontinued.
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;

        uint64_t test1f1_left;
        uint64_t test1f2_left;
        uint64_t test1timestamp;
        uint64_t test2f1_right;
        uint64_t test2f2_right;
        uint64_t test2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1f1_left == rhs.test1f1_left && test1f2_left == rhs.test1f2_left && test1timestamp == rhs.test1timestamp
                && test2f1_right == rhs.test2f1_right && test2f2_right == rhs.test2f2_right
                && test2timestamp == rhs.test2timestamp;
        }

        std::string to_string() const {
            std::ostringstream oss;
            oss << "ResultRecord { "
                << "wStart: " << test1test2Start << ", "
                << "wEnd: " << test1test2End << ", "
                << "lValue: " << test1f1_left << ", "
                << "lJoinValue: " << test1f2_left << ", "
                << "lTs: " << test1timestamp << ", "
                << "rValue: " << test2f1_right << ", "
                << "rJoinValue: " << test2f2_right << ", "
                << "rTs: " << test2timestamp << " }";
            return oss.str();
        }
    };

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64)
                                ->updateSourceName(*srcName);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64)
                                 ->updateSourceName(*srcName);

    const auto window = SlidingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(15), Milliseconds(5));

    TestUtils::CsvFileParams csvFileParams("stream_join_shared_left.csv",
                                           "stream_join_shared_right.csv",
                                           "stream_join_shared_sink2.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});

    std::tuple<QueryId, uint64_t, bool> t1 = {QueryId(0), 0, true};
    std::tuple<QueryId, uint64_t, bool> t2 = {QueryId(3), 3, true};
    const auto resultRecords = prepareRunTwoEqualQueriesDeployedDifferentTime<ResultRecord>(csvFileParams,
                                                                                            joinParams,
                                                                                            window,
                                                                                            "f2_left",
                                                                                            "f2_right",
                                                                                            {t1, t2});

    int i = 0;
    for (auto record : resultRecords) {
        NES_INFO("record {}: {}", i, record.to_string());
        i++;
    }
}

TEST_P(StreamJoinQuerySharedExecutionTest, streamJoinExecutionTestSameQueryThreeDifferentDeploymentTime) {
    if (joinStrategy != QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN
        || windowingStrategy != QueryCompilation::WindowingStrategy::SLICING) {
        //started for nlj with slicing. Afterward all hj strategies should be implemented with slicing as well. Bucketing will
        //most likely not be implemented, as my information is that it is supposed to be discontinued.
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;

        uint64_t test1f1_left;
        uint64_t test1f2_left;
        uint64_t test1timestamp;
        uint64_t test2f1_right;
        uint64_t test2f2_right;
        uint64_t test2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1f1_left == rhs.test1f1_left && test1f2_left == rhs.test1f2_left && test1timestamp == rhs.test1timestamp
                && test2f1_right == rhs.test2f1_right && test2f2_right == rhs.test2f2_right
                && test2timestamp == rhs.test2timestamp;
        }

        std::string to_string() const {
            std::ostringstream oss;
            oss << "ResultRecord { "
                << "wStart: " << test1test2Start << ", "
                << "wEnd: " << test1test2End << ", "
                << "lValue: " << test1f1_left << ", "
                << "lJoinValue: " << test1f2_left << ", "
                << "lTs: " << test1timestamp << ", "
                << "rValue: " << test2f1_right << ", "
                << "rJoinValue: " << test2f2_right << ", "
                << "rTs: " << test2timestamp << " }";
            return oss.str();
        }
    };

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64)
                                ->updateSourceName(*srcName);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64)
                                 ->updateSourceName(*srcName);

    const auto windowSize = Milliseconds(20);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);
    //const auto window = SlidingWindow::of(EventTime(Attribute(timestampFieldName)), Milliseconds(10), Milliseconds(5));

    TestUtils::CsvFileParams csvFileParams("stream_join_shared_left.csv",
                                           "stream_join_shared_right.csv",
                                           "stream_join_shared_sink3.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    std::tuple<QueryId, uint64_t, bool> t1 = {QueryId(0), 0, true};
    std::tuple<QueryId, uint64_t, bool> t2 = {QueryId(3), 3, true};
    std::tuple<QueryId, uint64_t, bool> t3 = {QueryId(6), 6, true};
    const auto resultRecords = prepareRunTwoEqualQueriesDeployedDifferentTime<ResultRecord>(csvFileParams,
                                                                                            joinParams,
                                                                                            window,
                                                                                            "f2_left",
                                                                                            "f2_right",
                                                                                            {t1, t2, t3});

    int i = 0;
    for (auto record : resultRecords) {
        NES_INFO("record {}: {}", i, record.to_string());
        i++;
    }
}

TEST_P(StreamJoinQuerySharedExecutionTest, streamJoinExecutionTestSameQueryDifferentDeploymentTimeAfterFirstWindowTriggered) {
    if (joinStrategy != QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN
        || windowingStrategy != QueryCompilation::WindowingStrategy::SLICING) {
        //started for nlj with slicing. Afterward all hj strategies should be implemented with slicing as well. Bucketing will
        //most likely not be implemented, as my information is that it is supposed to be discontinued.
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;

        uint64_t test1f1_left;
        uint64_t test1f2_left;
        uint64_t test1timestamp;
        uint64_t test2f1_right;
        uint64_t test2f2_right;
        uint64_t test2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1f1_left == rhs.test1f1_left && test1f2_left == rhs.test1f2_left && test1timestamp == rhs.test1timestamp
                && test2f1_right == rhs.test2f1_right && test2f2_right == rhs.test2f2_right
                && test2timestamp == rhs.test2timestamp;
        }

        std::string to_string() const {
            std::ostringstream oss;
            oss << "ResultRecord { "
                << "wStart: " << test1test2Start << ", "
                << "wEnd: " << test1test2End << ", "
                << "lValue: " << test1f1_left << ", "
                << "lJoinValue: " << test1f2_left << ", "
                << "lTs: " << test1timestamp << ", "
                << "rValue: " << test2f1_right << ", "
                << "rJoinValue: " << test2f2_right << ", "
                << "rTs: " << test2timestamp << " }";
            return oss.str();
        }
    };

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64)
                                ->updateSourceName(*srcName);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64)
                                 ->updateSourceName(*srcName);

    const auto windowSize = Milliseconds(5);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);
    //const auto window = SlidingWindow::of(EventTime(Attribute(timestampFieldName)), Milliseconds(10), Milliseconds(5));

    TestUtils::CsvFileParams csvFileParams("stream_join_shared_left.csv",
                                           "stream_join_shared_right.csv",
                                           "stream_join_shared_sink4.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    std::tuple<QueryId, uint64_t, bool> t1 = {QueryId(0), 0, true};
    std::tuple<QueryId, uint64_t, bool> t2 = {QueryId(7), 7, true};
    const auto resultRecords = prepareRunTwoEqualQueriesDeployedDifferentTime<ResultRecord>(csvFileParams,
                                                                                            joinParams,
                                                                                            window,
                                                                                            "f2_left",
                                                                                            "f2_right",
                                                                                            {t1, t2});

    int i = 0;
    for (auto record : resultRecords) {
        NES_INFO("record {}: {}", i, record.to_string());
        i++;
    }
}

TEST_P(StreamJoinQuerySharedExecutionTest, streamJoinExecutionTestSameQueryDifferentDeploymentTimeAndUndeployBeforeWindowCloses) {
    if (joinStrategy != QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN
        || windowingStrategy != QueryCompilation::WindowingStrategy::SLICING) {
        //started for nlj with slicing. Afterward all hj strategies should be implemented with slicing as well. Bucketing will
        //most likely not be implemented, as my information is that it is supposed to be discontinued.
        GTEST_SKIP();
    }

    /*
     * Test only works because the window of the undeployed query is triggered at the time as the undeployment of the query.
     */

    struct __attribute__((packed)) ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;

        uint64_t test1f1_left;
        uint64_t test1f2_left;
        uint64_t test1timestamp;
        uint64_t test2f1_right;
        uint64_t test2f2_right;
        uint64_t test2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1f1_left == rhs.test1f1_left && test1f2_left == rhs.test1f2_left && test1timestamp == rhs.test1timestamp
                && test2f1_right == rhs.test2f1_right && test2f2_right == rhs.test2f2_right
                && test2timestamp == rhs.test2timestamp;
        }

        std::string to_string() const {
            std::ostringstream oss;
            oss << "ResultRecord { "
                << "wStart: " << test1test2Start << ", "
                << "wEnd: " << test1test2End << ", "
                << "lValue: " << test1f1_left << ", "
                << "lJoinValue: " << test1f2_left << ", "
                << "lTs: " << test1timestamp << ", "
                << "rValue: " << test2f1_right << ", "
                << "rJoinValue: " << test2f2_right << ", "
                << "rTs: " << test2timestamp << " }";
            return oss.str();
        }
    };

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64)
                                ->updateSourceName(*srcName);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64)
                                 ->updateSourceName(*srcName);

    const auto windowSize = Milliseconds(10);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);
    //const auto window = SlidingWindow::of(EventTime(Attribute(timestampFieldName)), Milliseconds(10), Milliseconds(5));

    TestUtils::CsvFileParams csvFileParams("stream_join_shared_left.csv",
                                           "stream_join_shared_right.csv",
                                           "stream_join_shared_sink5.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    std::tuple<QueryId, uint64_t, bool> t1 = {QueryId(0), 0, true};
    std::tuple<QueryId, uint64_t, bool> t2 = {QueryId(3), 3, true};
    std::tuple<QueryId, uint64_t, bool> t3 = {QueryId(3), 13, false};

    const auto resultRecords = prepareRunTwoEqualQueriesDeployedDifferentTime<ResultRecord>(csvFileParams,
                                                                                            joinParams,
                                                                                            window,
                                                                                            "f2_left",
                                                                                            "f2_right",
                                                                                            {t1, t2, t3});

    int i = 0;
    for (auto record : resultRecords) {
        NES_INFO("record {}: {}", i, record.to_string());
        i++;
    }
}

INSTANTIATE_TEST_CASE_P(testStreamJoinQueries,
                        StreamJoinQuerySharedExecutionTest,
                        JOIN_STRATEGIES_WINDOW_STRATEGIES,
                        [](const testing::TestParamInfo<StreamJoinQuerySharedExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(std::get<0>(info.param))) + "_"
                                + std::string(magic_enum::enum_name(std::get<1>(info.param)));
                        });

}// namespace NES::Runtime::Execution
