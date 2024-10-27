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

#include <iterator>
#include <memory>
#include <random>
#include <ranges>
#include <utility>
#include <Listeners/QueryLog.hpp>
#include <Util/Core.hpp>
#include <gmock/gmock-function-mocker.h>
#include <BaseUnitTest.hpp>
#include <MemoryTestUtils.hpp>
#include <QueryManagerTestingInfrastructure.hpp>
#include <SingleNodeWorkerRPCService.pb.h>
#include <TestSource.hpp>

namespace NES::Testing
{
class QueryManagerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("NonBlockingMonotonicSeqQueueTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup NonBlockingMonotonicSeqQueueTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(QueryManagerTest, simpleTests)
{
    TestingHarness test;
    test.start();
    test.stop();
}

/// The Query consists of just a source without any successor pipelines
TEST_F(QueryManagerTest, singleQueryWithShutdown)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline = builder.addSink({source});
    auto query = test.addNewQuery(std::move(builder));
    auto ctrl = test.sourceControls[source];

    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running});

    test.start();
    {
        test.startQuery(QueryId(1), std::move(query));

        EXPECT_TRUE(ctrl->waitUntilOpened());

        ctrl->injectData(identifiableData(1), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);
        test.sinkControls[pipeline]->waitForNumberOfReceivedBuffers(4);
    }
    test.stop();

    EXPECT_TRUE(ctrl->wasOpened());
    EXPECT_TRUE(ctrl->wasClosed());
    EXPECT_TRUE(ctrl->wasDestroyed());
}

/// The Query is stopped via shutdown of the system
TEST_F(QueryManagerTest, singleQueryWithSystemShutdown)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto sink = builder.addSink({builder.addPipeline({source})});
    auto query = test.addNewQuery(std::move(builder));

    auto ctrl = test.sourceControls[source];
    auto sinkCtrl = test.sinkControls[sink];
    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running});

    test.start();
    {
        test.startQuery(QueryId(1), std::move(query));

        EXPECT_TRUE(ctrl->waitUntilOpened());
        EXPECT_FALSE(ctrl->wasClosed());

        ctrl->injectData(identifiableData(1), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);

        EXPECT_TRUE(sinkCtrl->waitForNumberOfReceivedBuffers(4));
    }

    auto buffers = sinkCtrl->takeBuffers();
    EXPECT_TRUE(verifyIdentifier(buffers[0], 23));
    test.stop();

    EXPECT_TRUE(ctrl->waitUntilDestroyed());
    EXPECT_TRUE(ctrl->wasOpened());
    EXPECT_TRUE(ctrl->wasClosed());
}

/// External stop: The Query was stopped the source
TEST_F(QueryManagerTest, singleQueryWithExternalStop)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto sink = builder.addSink({builder.addPipeline({source})});
    auto query = test.addNewQuery(std::move(builder));

    auto ctrl = test.sourceControls[source];
    auto sinkCtrl = test.sinkControls[sink];
    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});
    test.expectSourceTermination(QueryId(1), source, Runtime::QueryTerminationType::Graceful);

    test.start();
    {
        test.startQuery(QueryId(1), std::move(query));

        EXPECT_TRUE(ctrl->waitUntilOpened());

        ctrl->injectData(identifiableData(1), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);
        ctrl->injectEoS();

        sinkCtrl->waitForNumberOfReceivedBuffers(4);
    }
    EXPECT_TRUE(sinkCtrl->waitForShutdown());
    EXPECT_TRUE(ctrl->wasOpened());
    EXPECT_TRUE(ctrl->wasClosed());
    EXPECT_TRUE(ctrl->wasDestroyed());
    EXPECT_TRUE(test.waitForQepTermination(QueryId(1)));
    test.stop();

    auto buffers = sinkCtrl->takeBuffers();
    EXPECT_EQ(buffers.size(), 4);
    EXPECT_TRUE(verifyIdentifier(buffers[0], 23));
}

/// Internal Stop: Meaning the Query was stopped internally from the query manager via the stop query
TEST_F(QueryManagerTest, singleQueryWithInternalStop)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto sink = builder.addSink({builder.addPipeline({source})});
    auto query = test.addNewQuery(std::move(builder));

    auto ctrl = test.sourceControls[source];
    auto sinkCtrl = test.sinkControls[sink];
    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});

    test.start();
    {
        test.startQuery(QueryId(1), std::move(query));

        EXPECT_TRUE(ctrl->waitUntilOpened());
        EXPECT_FALSE(ctrl->wasClosed());

        ctrl->injectData(identifiableData(1), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);

        sinkCtrl->waitForNumberOfReceivedBuffers(1);
        test.stopQuery(QueryId(1));
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);
        EXPECT_TRUE(test.waitForQepTermination(QueryId(1)));
    }
    test.stop();

    EXPECT_TRUE(sinkCtrl->waitForShutdown());
    EXPECT_TRUE(ctrl->wasOpened());
    EXPECT_TRUE(ctrl->wasClosed());
    EXPECT_TRUE(ctrl->wasDestroyed());

    auto buffers = sinkCtrl->takeBuffers();
    EXPECT_GE(buffers.size(), 1) << "Expected at least one buffer";
    EXPECT_TRUE(verifyIdentifier(buffers[0], 23));
}

TEST_F(QueryManagerTest, singleQueryWithSourceFailure)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto sink = builder.addSink({builder.addPipeline({source})});
    auto query = test.addNewQuery(std::move(builder));

    auto ctrl = test.sourceControls[source];
    auto sinkCtrl = test.sinkControls[sink];
    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running});
    EXPECT_CALL(*test.status, logQueryFailure(QueryId(1), ::testing::_));
    test.expectSourceTermination(QueryId(1), source, Runtime::QueryTerminationType::Failure);

    test.start();
    {
        test.startQuery(QueryId(1), std::move(query));

        EXPECT_TRUE(ctrl->waitUntilOpened());
        EXPECT_FALSE(ctrl->waitUntilClosed());

        ctrl->injectData(identifiableData(1), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);
        ctrl->injectData(std::vector(8192, std::byte(0)), 23);

        sinkCtrl->waitForNumberOfReceivedBuffers(1);
        ctrl->injectError("Source Failed");
        test.waitForQepTermination(QueryId(1));
    }
    test.stop();

    EXPECT_TRUE(ctrl->wasOpened());
    EXPECT_TRUE(ctrl->wasClosed());
    EXPECT_TRUE(ctrl->wasDestroyed());

    auto buffers = sinkCtrl->takeBuffers();
    EXPECT_GE(buffers.size(), 1) << "Expected at least one buffer";
    EXPECT_TRUE(verifyIdentifier(buffers[0], 23));
}

/// Shutdown of the Query Engine will `HardStop` all query plans.
TEST_F(QueryManagerTest, singleQueryWithTwoSourcesShutdown)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source1 = builder.addSource();
    auto source2 = builder.addSource();
    auto sink = builder.addSink({builder.addPipeline({source1, source2})});
    auto query = test.addNewQuery(std::move(builder));

    auto ctrl1 = test.sourceControls[source1];
    auto ctrl2 = test.sourceControls[source2];
    auto sinkCtrl = test.sinkControls[sink];
    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running});

    test.start();
    {
        test.startQuery(QueryId(1), std::move(query));

        EXPECT_TRUE(ctrl1->waitUntilOpened());
        EXPECT_FALSE(ctrl1->wasClosed());

        EXPECT_TRUE(ctrl2->waitUntilOpened());
        EXPECT_FALSE(ctrl2->wasClosed());

        ctrl1->injectData(identifiableData(1), 23);
        ctrl1->injectData(identifiableData(2), 23);
        ctrl2->injectData(identifiableData(3), 23);
        ctrl2->injectData(identifiableData(4), 23);
        sinkCtrl->waitForNumberOfReceivedBuffers(4);
    }


    auto buffers = sinkCtrl->takeBuffers();
    test.stop();

    EXPECT_TRUE(ctrl1->waitUntilDestroyed());
    EXPECT_TRUE(ctrl2->waitUntilDestroyed());
}

TEST_F(QueryManagerTest, singleQueryWithTwoSourcesWaitingForTwoStops)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source1 = builder.addSource();
    auto source2 = builder.addSource();
    auto sink = builder.addSink({builder.addPipeline({source1, source2})});
    auto query = test.addNewQuery(std::move(builder));

    auto ctrl1 = test.sourceControls[source1];
    auto ctrl2 = test.sourceControls[source2];
    auto sinkCtrl = test.sinkControls[sink];
    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});
    test.expectSourceTermination(QueryId(1), source1, Runtime::QueryTerminationType::Graceful);
    test.expectSourceTermination(QueryId(1), source2, Runtime::QueryTerminationType::Graceful);

    test.start();
    {
        test.startQuery(QueryId(1), std::move(query));

        EXPECT_TRUE(ctrl1->waitUntilOpened());
        EXPECT_FALSE(ctrl1->wasClosed());

        EXPECT_TRUE(ctrl2->waitUntilOpened());
        EXPECT_FALSE(ctrl2->wasClosed());

        ctrl1->injectData(identifiableData(1), 23);
        ctrl1->injectData(identifiableData(2), 23);
        ctrl2->injectData(identifiableData(3), 23);
        ctrl2->injectData(identifiableData(4), 23);
        ctrl1->injectEoS();

        sinkCtrl->waitForNumberOfReceivedBuffers(4);

        ctrl2->injectData(identifiableData(5), 23);
        sinkCtrl->waitForNumberOfReceivedBuffers(5);
        ctrl2->injectData(identifiableData(6), 23);
        sinkCtrl->waitForNumberOfReceivedBuffers(6);
        ctrl2->injectEoS();

        test.waitForQepTermination(QueryId(1));
    }
    test.stop();

    auto buffers = sinkCtrl->takeBuffers();
    EXPECT_TRUE(ctrl1->wasDestroyed());
    EXPECT_TRUE(ctrl2->wasDestroyed());
}

TEST_F(QueryManagerTest, singleQueryWithManySources)
{
    TestingHarness test(8, 20000);
    auto builder = test.buildNewQuery();
    std::vector<QueryPlanBuilder::identifier_t> sources;
    for (size_t i = 0; i < 100; i++)
    {
        sources.push_back(builder.addSource());
    }

    auto sink = builder.addSink({builder.addPipeline(sources)});
    auto query = test.addNewQuery(std::move(builder));

    std::vector<std::shared_ptr<Sources::TestSourceControl>> sourcesCtrls;
    std::transform(
        sources.begin(),
        sources.end(),
        std::back_inserter(sourcesCtrls),
        [&](auto identifier)
        {
            test.expectSourceTermination(QueryId(1), identifier, Runtime::QueryTerminationType::Graceful);
            return test.sourceControls[identifier];
        });

    auto sinkCtrl = test.sinkControls[sink];
    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});

    test.start();
    {
        test.startQuery(QueryId(1), std::move(query));
        DataGenerator dg;
        dg.start(std::move(sourcesCtrls));
        sinkCtrl->waitForNumberOfReceivedBuffers(1000);
        dg.stop();

        test.waitForQepTermination(QueryId(1));
    }
    test.stop();
}

/// Apparently Sources are not notified if on of the sources fails
/// The Mocked QueryStatusListener does not receive source termination for all sources except the failed source
TEST_F(QueryManagerTest, singleQueryWithManySourcesOneOfThemFails)
{
    TestingHarness test(8, 20000);
    auto builder = test.buildNewQuery();
    std::vector<QueryPlanBuilder::identifier_t> sources;
    for (size_t i = 0; i < 10; i++)
    {
        sources.push_back(builder.addSource());
    }

    auto sink = builder.addSink({builder.addPipeline(sources)});
    auto query = test.addNewQuery(std::move(builder));

    std::vector<std::shared_ptr<Sources::TestSourceControl>> sourcesCtrls;
    std::transform(
        sources.begin(), sources.end(), std::back_inserter(sourcesCtrls), [&](auto identifier) { return test.sourceControls[identifier]; });

    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Failed});
    /// Overwrite Source 0 to expect source failure.
    test.expectSourceTermination(QueryId(1), sources[0], Runtime::QueryTerminationType::Failure);

    auto sinkCtrl = test.sinkControls[sink];

    test.start();
    {
        test.startQuery(QueryId(1), std::move(query));

        DataGenerator<FailAfter<5, 0>> dg;
        dg.start(std::move(sourcesCtrls));
        EXPECT_TRUE(test.waitForQepTermination(QueryId(1)));
        dg.stop();
    }

    for (const auto& ctrl : sourcesCtrls)
    {
        ASSERT_TRUE(ctrl->waitUntilDestroyed());
        ASSERT_TRUE(ctrl->wasOpened());
    }
    test.stop();
}

TEST_F(QueryManagerTest, ManyQueriesWithTwoSources)
{
    TestingHarness test(8, 200000);

    std::vector<QueryPlanBuilder::identifier_t> sources;
    std::vector<QueryPlanBuilder::identifier_t> sinks;
    std::vector<std::unique_ptr<Runtime::InstantiatedQueryPlan>> queryPlans;
    for (size_t i = 0; i < 10; i++)
    {
        auto builder = test.buildNewQuery();
        auto s1 = builder.addSource();
        auto s2 = builder.addSource();
        sources.push_back(s1);
        sources.push_back(s2);
        sinks.push_back(builder.addSink({builder.addPipeline({s1, s2})}));
        queryPlans.push_back(test.addNewQuery(std::move(builder)));
    }

    std::vector<std::shared_ptr<Sources::TestSourceControl>> sourcesCtrls;
    std::vector<std::shared_ptr<TestSinkController>> sinkCtrls;

    for (size_t index = 0; const auto& query : queryPlans)
    {
        sourcesCtrls.push_back(test.sourceControls[sources[index * 2]]);
        sourcesCtrls.push_back(test.sourceControls[sources[index * 2 + 1]]);
        sinkCtrls.push_back(test.sinkControls[sinks[index]]);
        test.expectSourceTermination(QueryId(1 + index), sources[index * 2], Runtime::QueryTerminationType::Graceful);
        test.expectSourceTermination(QueryId(1 + index), sources[index * 2 + 1], Runtime::QueryTerminationType::Graceful);
        test.expectQueryStatusEvents(
            QueryId(1 + index), {Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});
        index++;
    }

    test.start();
    {
        DataGenerator dg;
        dg.start(sourcesCtrls);
        for (QueryId::Underlying qidGenerator = QueryId::INITIAL; auto& query : queryPlans)
        {
            auto q = QueryId(qidGenerator++);
            test.startQuery(q, std::move(query));
            EXPECT_TRUE(test.waitForQepRunning(q));
        }
        sinkCtrls[0]->waitForNumberOfReceivedBuffers(2);
        dg.stop();

        for (size_t queryId = 1; auto& query : queryPlans)
        {
            EXPECT_TRUE(test.waitForQepTermination(QueryId(queryId)));
            queryId++;
        }
    }

    for (const auto& test_source_control : sourcesCtrls)
    {
        EXPECT_TRUE(test_source_control->waitUntilDestroyed());
    }
    test.stop();
}

/// This test creates 10 QueryPlans (each with two sources one intermediate pipeline and a sink)
/// The TestDataGenerator will emit a failure on the 0th source (for QueryId 1)
/// We expect QueryId 1 to terminate without internal intervention and the query failed status to be emitted (likewise for the source)
/// All other queries should be uneffected, which is verified by them not terminating and still receiving data
/// We expect QueryId 2 to be terminated by an internal stop (qm->stop(2)).
/// Lastly all other queries are terminated via EoS
TEST_F(QueryManagerTest, ManyQueriesWithTwoSourcesOneSourceFails)
{
    TestingHarness test(8, 2000);

    std::vector<QueryPlanBuilder::identifier_t> sources;
    std::vector<QueryPlanBuilder::identifier_t> sinks;
    std::vector<std::unique_ptr<Runtime::InstantiatedQueryPlan>> queryPlans;
    for (size_t i = 0; i < 100; i++)
    {
        auto builder = test.buildNewQuery();
        auto s1 = builder.addSource();
        auto s2 = builder.addSource();
        sources.push_back(s1);
        sources.push_back(s2);
        sinks.push_back(builder.addSink({builder.addPipeline({s1, s2})}));
        queryPlans.push_back(test.addNewQuery(std::move(builder)));
    }

    std::vector<std::shared_ptr<Sources::TestSourceControl>> sourcesCtrls;
    std::vector<std::shared_ptr<TestSinkController>> sinkCtrls;

    /// Query 1 is terminated by an Failure of source 0
    sourcesCtrls.push_back(test.sourceControls[sources[0]]);
    sourcesCtrls.push_back(test.sourceControls[sources[1]]);
    sinkCtrls.push_back(test.sinkControls[sinks[0]]);
    test.expectSourceTermination(QueryId(1), sources[0], Runtime::QueryTerminationType::Failure);
    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Failed});

    /// Query 2 is terminated by an internal stop
    sourcesCtrls.push_back(test.sourceControls[sources[2]]);
    sourcesCtrls.push_back(test.sourceControls[sources[3]]);
    sinkCtrls.push_back(test.sinkControls[sinks[1]]);
    test.expectQueryStatusEvents(QueryId(2), {Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});

    /// Rest of the queries are terminated by external stop via eos
    for (size_t index = 2; const auto& query : queryPlans | std::ranges::views::drop(2))
    {
        sourcesCtrls.push_back(test.sourceControls[sources[index * 2]]);
        sourcesCtrls.push_back(test.sourceControls[sources[index * 2 + 1]]);
        sinkCtrls.push_back(test.sinkControls[sinks[index]]);
        test.expectSourceTermination(QueryId(1 + index), sources[index * 2], Runtime::QueryTerminationType::Graceful);
        test.expectSourceTermination(QueryId(1 + index), sources[index * 2 + 1], Runtime::QueryTerminationType::Graceful);
        test.expectQueryStatusEvents(
            QueryId(1 + index), {Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});
        index++;
    }

    test.start();
    {
        /// instruct DataGenerator to emit failure for source 0 after 3 tuple buffer
        DataGenerator<FailAfter<3, 0>> dg;
        dg.start(sourcesCtrls);

        /// start all queries
        for (QueryId::Underlying qidGenerator = QueryId::INITIAL; auto& query : queryPlans)
        {
            auto q = QueryId(qidGenerator++);
            test.startQuery(q, std::move(query));
            EXPECT_TRUE(test.waitForQepRunning(q));
        }

        /// Expect Query 1 to be terminated by the failure.
        EXPECT_TRUE(test.waitForQepTermination(QueryId(1)));
        /// Expect all other queries to be still running
        for (size_t queryId = 2; auto& query : queryPlans | std::ranges::views::drop(1))
        {
            EXPECT_FALSE(test.waitForQepTermination(QueryId(queryId), std::chrono::milliseconds(10)));
            queryId++;
        }

        /// Internally stop Query 2 and wait for termination
        test.stopQuery(QueryId(2));
        EXPECT_TRUE(test.waitForQepTermination(QueryId(2), std::chrono::milliseconds(1000)));

        /// Externally stop all other queries via EoS from the datagenerator
        dg.stop();
        for (size_t queryId = 3; auto& query : queryPlans | std::ranges::views::drop(2))
        {
            EXPECT_TRUE(test.waitForQepTermination(QueryId(queryId)));
            queryId++;
        }
    }

    for (const auto& test_source_control : sourcesCtrls)
    {
        EXPECT_TRUE(test_source_control->waitUntilDestroyed());
    }
    test.stop();
}

TEST_F(QueryManagerTest, singleQueryWithTwoSourceExternalStop)
{
    TestingHarness test(8, 20000);
    auto builder = test.buildNewQuery();
    auto s1 = builder.addSource();
    auto s2 = builder.addSource();
    auto p = builder.addPipeline({s1, s2});
    auto sink = builder.addSink({p});
    auto query = test.addNewQuery(std::move(builder));

    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});

    test.start();
    {
        test.startQuery(QueryId(1), std::move(query));
        EXPECT_TRUE(test.sourceControls[s1]->waitUntilOpened());
        EXPECT_TRUE(test.sourceControls[s2]->waitUntilOpened());

        test.sourceControls[s1]->injectData(std::vector(8192, std::byte(0)), 23);
        test.sourceControls[s1]->injectData(std::vector(8192, std::byte(0)), 23);
        test.sourceControls[s2]->injectData(std::vector(8192, std::byte(0)), 23);
        EXPECT_TRUE(test.sinkControls[sink]->waitForNumberOfReceivedBuffers(3));
        test.stopQuery(QueryId(1));
        EXPECT_TRUE(test.waitForQepTermination(QueryId(1)));
        EXPECT_TRUE(test.sourceControls[s1]->waitUntilDestroyed());
        EXPECT_TRUE(test.sourceControls[s2]->waitUntilDestroyed());
    }
    test.stop();
}

TEST_F(QueryManagerTest, singleQueryWithSlowlyFailingSourceDuringEngineTermination)
{
    TestingHarness test(8, 20000);
    auto builder = test.buildNewQuery();
    auto s = builder.addSource();
    auto p = builder.addPipeline({s});
    auto sink = builder.addSink({p});
    auto query = test.addNewQuery(std::move(builder));

    test.sourceControls[s]->failDuringOpen(std::chrono::milliseconds(100));
    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running});

    test.start();
    {
        test.startQuery(QueryId(1), std::move(query));
        EXPECT_TRUE(test.sinkControls[sink]->waitForInitialization());
        EXPECT_TRUE(test.pipelineControls[p]->waitForInitialization());
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    test.stop();
}

TEST_F(QueryManagerTest, singleQueryWithSlowlyFailingSourceDuringQueryPlanTermination)
{
    TestingHarness test(8, 20000);
    auto builder = test.buildNewQuery();
    auto s = builder.addSource();
    auto p = builder.addPipeline({s});
    auto sink = builder.addSink({p});
    auto query = test.addNewQuery(std::move(builder));

    test.sourceControls[s]->failDuringOpen(std::chrono::milliseconds(100));
    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});

    test.start();
    {
        test.startQuery(QueryId(1), std::move(query));
        EXPECT_TRUE(test.sinkControls[sink]->waitForInitialization());
        EXPECT_TRUE(test.pipelineControls[p]->waitForInitialization());
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        test.stopQuery(QueryId(1));
        test.waitForQepTermination(QueryId(1));
    }
    test.stop();
}
}
