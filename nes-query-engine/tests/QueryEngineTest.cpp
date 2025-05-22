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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <iterator>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Ranges.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <QueryEngineTestingInfrastructure.hpp>
#include <TestSource.hpp>

namespace NES::Testing
{
class QueryEngineTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        NES::Logger::setupLogging("QueryEngineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup QueryEngineTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(QueryEngineTest, simpleTest)
{
    TestingHarness test;
    test.start();
    test.stop();
}

/// The Query consists of just a source without any successor pipelines
TEST_F(QueryEngineTest, singleQueryWithShutdown)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline = builder.addSink({source});
    auto query = test.addNewQuery(std::move(builder));
    auto ctrl = test.sourceControls[source];

    /// Statistics. Note: No Pipeline Terminate and no QueryStop because engine shutdown does not gracefully terminate any query.
    test.stats.expect(
        ExpectStats::QueryStart(1),
        ExpectStats::PipelineStart(1),
        ExpectStats::TaskExecutionStart(4),
        ExpectStats::TaskExecutionComplete(4));

    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running});

    test.start();
    {
        test.startQuery(std::move(query));

        ASSERT_TRUE(ctrl->waitUntilOpened());

        ctrl->injectData(identifiableData(1), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sinkControls[pipeline]->waitForNumberOfReceivedBuffersOrMore(4);
    }
    test.stop();

    ASSERT_TRUE(ctrl->waitUntilDestroyed());
    EXPECT_TRUE(ctrl->wasOpened());
    EXPECT_TRUE(ctrl->wasClosed());
}

/// The Query is stopped via shutdown of the system
TEST_F(QueryEngineTest, singleQueryWithSystemShutdown)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto sink = builder.addSink({builder.addPipeline({source})});
    auto query = test.addNewQuery(std::move(builder));
    auto id = query->queryId;

    auto ctrl = test.sourceControls[source];
    auto sinkCtrl = test.sinkControls[sink];

    /// Statistics. Note: No Pipeline Terminate and no QueryStop because engine shutdown does not gracefully terminate any query.
    test.stats.expect(
        ExpectStats::QueryStart(1),
        ExpectStats::PipelineStart(2),
        ExpectStats::TaskExecutionStart(8),
        ExpectStats::TaskExecutionComplete(8),
        ExpectStats::TaskEmit(4));

    test.expectQueryStatusEvents(id, {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running});

    test.start();
    {
        test.startQuery(std::move(query));

        ASSERT_TRUE(ctrl->waitUntilOpened());
        ASSERT_TRUE(test.waitForQepRunning(id, DEFAULT_LONG_AWAIT_TIMEOUT));
        EXPECT_FALSE(ctrl->wasClosed());

        ctrl->injectData(identifiableData(1), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);

        ASSERT_TRUE(sinkCtrl->waitForNumberOfReceivedBuffersOrMore(4));
    }

    auto buffers = sinkCtrl->takeBuffers();
    EXPECT_TRUE(verifyIdentifier(buffers[0], NUMBER_OF_TUPLES_PER_BUFFER));
    test.stop();

    ASSERT_TRUE(ctrl->waitUntilDestroyed());
    EXPECT_TRUE(ctrl->wasOpened());
    EXPECT_TRUE(ctrl->wasClosed());
}

/// Source stop: The Query was stopped by the source
TEST_F(QueryEngineTest, singleQueryWithExternalStop)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto sink = builder.addSink({builder.addPipeline({source})});
    auto query = test.addNewQuery(std::move(builder));

    auto ctrl = test.sourceControls[source];
    auto sinkCtrl = test.sinkControls[sink];

    /// Statistics. Note: Pipelines are terminated because the query is gracefully stopped. The QueryTermination event is only emitted when
    /// query termination is requested via a system event, not via a source event.
    test.stats.expect(
        ExpectStats::QueryStart(1),
        ExpectStats::QueryStop(0),
        ExpectStats::PipelineStart(2),
        ExpectStats::PipelineStop(2),
        ExpectStats::TaskExecutionStart(8),
        ExpectStats::TaskExecutionComplete(8),
        ExpectStats::TaskEmit(4));

    test.expectQueryStatusEvents(
        QueryId(1),
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});
    test.expectSourceTermination(QueryId(1), source, Runtime::QueryTerminationType::Graceful);

    test.start();
    {
        test.startQuery(std::move(query));

        ASSERT_TRUE(ctrl->waitUntilOpened());

        ctrl->injectData(identifiableData(1), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectEoS();

        ASSERT_TRUE(sinkCtrl->waitForNumberOfReceivedBuffersOrMore(4));
    }
    ASSERT_TRUE(sinkCtrl->waitForShutdown(DEFAULT_LONG_AWAIT_TIMEOUT));
    ASSERT_TRUE(ctrl->waitUntilDestroyed());
    EXPECT_TRUE(ctrl->wasOpened());
    EXPECT_TRUE(ctrl->wasClosed());
    ASSERT_TRUE(test.waitForQepTermination(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
    test.stop();

    auto buffers = sinkCtrl->takeBuffers();
    EXPECT_EQ(buffers.size(), 4);
    EXPECT_TRUE(verifyIdentifier(buffers[0], NUMBER_OF_TUPLES_PER_BUFFER));
}

/// System Stop: Meaning the Query was stopped internally from the query manager via the stop query
TEST_F(QueryEngineTest, singleQueryWithSystemStop)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto sink = builder.addSink({builder.addPipeline({source})});
    auto query = test.addNewQuery(std::move(builder));

    auto ctrl = test.sourceControls[source];
    auto sinkCtrl = test.sinkControls[sink];
    test.expectQueryStatusEvents(
        QueryId(1),
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});

    /// Statistics.
    ///     Note: Pipelines are terminated because the query is gracefully stopped.
    ///           We expect a range of executed tasks between 8-10 because the query stop races with the 2nd-5th emit.
    test.stats.expect(
        ExpectStats::QueryStart(1),
        ExpectStats::QueryStop(1),
        ExpectStats::PipelineStart(2),
        ExpectStats::PipelineStop(2),
        ExpectStats::TaskExecutionStart(2, 10),
        ExpectStats::TaskExecutionComplete(2, 10),
        ExpectStats::TaskEmit(1, 5));

    test.start();
    {
        test.startQuery(std::move(query));

        ASSERT_TRUE(ctrl->waitUntilOpened());
        EXPECT_FALSE(ctrl->wasClosed());

        ctrl->injectData(identifiableData(1), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);

        /// Race between Source Data and System Stop
        ASSERT_TRUE(sinkCtrl->waitForNumberOfReceivedBuffersOrMore(1));
        test.stopQuery(QueryId(1));
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        ASSERT_TRUE(test.waitForQepTermination(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
    }
    test.stop();

    ASSERT_TRUE(sinkCtrl->waitForShutdown(DEFAULT_LONG_AWAIT_TIMEOUT));
    ASSERT_TRUE(ctrl->waitUntilDestroyed());
    EXPECT_TRUE(ctrl->wasOpened());
    EXPECT_TRUE(ctrl->wasClosed());

    auto buffers = sinkCtrl->takeBuffers();
    EXPECT_GE(buffers.size(), 1) << "Expected at least one buffer";
    EXPECT_TRUE(verifyIdentifier(buffers[0], NUMBER_OF_TUPLES_PER_BUFFER));
}

TEST_F(QueryEngineTest, singleQueryWithSourceFailure)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto sink = builder.addSink({builder.addPipeline({source})});
    auto query = test.addNewQuery(std::move(builder));

    auto ctrl = test.sourceControls[source];
    auto sinkCtrl = test.sinkControls[sink];
    test.expectQueryStatusEvents(
        QueryId(1),
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Failed});
    test.expectSourceTermination(QueryId(1), source, Runtime::QueryTerminationType::Failure);

    test.start();
    {
        test.startQuery(std::move(query));

        ASSERT_TRUE(ctrl->waitUntilOpened());
        EXPECT_FALSE(ctrl->waitUntilClosed());

        ctrl->injectData(identifiableData(1), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);

        ASSERT_TRUE(sinkCtrl->waitForNumberOfReceivedBuffersOrMore(1));
        ctrl->injectError("Source Failed");
        ASSERT_TRUE(test.waitForQepTermination(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
    }
    test.stop();

    ASSERT_TRUE(ctrl->waitUntilDestroyed());
    EXPECT_TRUE(ctrl->wasOpened());
    EXPECT_TRUE(ctrl->wasClosed());

    auto buffers = sinkCtrl->takeBuffers();
    EXPECT_GE(buffers.size(), 1) << "Expected at least one buffer";
    EXPECT_TRUE(verifyIdentifier(buffers[0], NUMBER_OF_TUPLES_PER_BUFFER));
}

/// Shutdown of the Query Engine will `HardStop` all query plans.
TEST_F(QueryEngineTest, singleQueryWithTwoSourcesShutdown)
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
    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running});

    /// Statistics.
    ///     Note: Pipelines are not terminated, due to system shutdown

    test.stats.expect(
        ExpectStats::QueryStart(1),
        ExpectStats::QueryStop(0),
        ExpectStats::PipelineStart(2),
        ExpectStats::PipelineStop(0),
        ExpectStats::TaskExecutionStart(8),
        ExpectStats::TaskExecutionComplete(8),
        ExpectStats::TaskEmit(4));

    test.start();
    {
        test.startQuery(std::move(query));

        ASSERT_TRUE(ctrl1->waitUntilOpened());
        EXPECT_FALSE(ctrl1->wasClosed());

        ASSERT_TRUE(ctrl2->waitUntilOpened());
        EXPECT_FALSE(ctrl2->wasClosed());

        ctrl1->injectData(identifiableData(1), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl1->injectData(identifiableData(2), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl2->injectData(identifiableData(3), NUMBER_OF_TUPLES_PER_BUFFER);
        ctrl2->injectData(identifiableData(4), NUMBER_OF_TUPLES_PER_BUFFER);
        ASSERT_TRUE(sinkCtrl->waitForNumberOfReceivedBuffersOrMore(4));
    }


    auto buffers = sinkCtrl->takeBuffers();
    test.stop();

    ASSERT_TRUE(ctrl1->waitUntilDestroyed());
    ASSERT_TRUE(ctrl2->waitUntilDestroyed());
}

TEST_F(QueryEngineTest, failureDuringPipelineStop)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto failingPipeline = builder.addPipeline({source});
    auto pipeline = builder.addPipeline({failingPipeline});
    builder.addSink({pipeline});
    auto query = test.addNewQuery(std::move(builder));
    auto id = query->queryId;
    test.pipelineControls[failingPipeline]->failOnStop = true;

    test.expectQueryStatusEvents(
        id, {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Failed});
    test.expectSourceTermination(id, source, Runtime::QueryTerminationType::Graceful);

    test.start();
    {
        test.startQuery(std::move(query));
        ASSERT_TRUE(test.waitForQepRunning(id, DEFAULT_LONG_AWAIT_TIMEOUT));
        test.sourceControls[source]->injectEoS();
        ASSERT_TRUE(test.waitForQepTermination(id, DEFAULT_LONG_AWAIT_TIMEOUT));

        ASSERT_TRUE(test.pipelineControls[failingPipeline]->waitForStop()) << "Pipeline should be stopped";
        EXPECT_TRUE(test.pipelineControls[pipeline]->keepRunning()) << "Successors of failing pipelines should not be stopped";
    }
    test.stop();
}

TEST_F(QueryEngineTest, failureDuringPipelineStopMultipleSources)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source1 = builder.addSource();
    auto failingPipeline = builder.addPipeline({source1});
    auto failingPipelineSuccessor = builder.addPipeline({failingPipeline});
    auto source2 = builder.addSource();
    auto pipeline = builder.addPipeline({source2});
    auto sink = builder.addSink({failingPipelineSuccessor, pipeline});

    auto query = test.addNewQuery(std::move(builder));
    auto id = query->queryId;
    test.pipelineControls[failingPipeline]->failOnStop = true;

    test.expectQueryStatusEvents(
        id, {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Failed});
    test.expectSourceTermination(id, source1, Runtime::QueryTerminationType::Graceful);
    test.expectSourceTermination(id, source2, Runtime::QueryTerminationType::Graceful);

    test.start();
    {
        test.startQuery(std::move(query));
        ASSERT_TRUE(test.waitForQepRunning(id, DEFAULT_LONG_AWAIT_TIMEOUT));
        test.sourceControls[source2]->injectEoS();
        ASSERT_TRUE(test.pipelineControls[pipeline]->waitForStop())
            << "Pipeline should be stopped after its predecessor source has been stopped";
        EXPECT_FALSE(test.sinkControls[sink]->waitForShutdown(DEFAULT_AWAIT_TIMEOUT))
            << "Sink should not have been stopped as it is kept alive by the other predecessor";

        test.sourceControls[source1]->injectEoS();
        ASSERT_TRUE(test.waitForQepTermination(id, DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.pipelineControls[failingPipeline]->waitForStop()) << "Pipeline should be stopped";
        EXPECT_TRUE(test.pipelineControls[failingPipelineSuccessor]->keepRunning())
            << "Successors of failing pipelines should not be stopped";
        EXPECT_FALSE(test.sinkControls[sink]->waitForShutdown(DEFAULT_AWAIT_TIMEOUT))
            << "Successors of failing pipelines should not be stopped";
    }
    test.stop();
}

TEST_F(QueryEngineTest, failureDuringPipelineStartWithMultiplePipelines)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source1 = builder.addSource();
    auto failingPipeline = builder.addPipeline({source1});
    builder.addSink({failingPipeline});
    for (size_t i = 0; i < 100; i++)
    {
        auto okayPipeline = builder.addPipeline({source1});
        builder.addSink({okayPipeline});
    }

    auto query = test.addNewQuery(std::move(builder));
    auto id = query->queryId;
    test.pipelineControls[failingPipeline]->failOnStart = true;

    test.expectQueryStatusEvents(id, {Runtime::Execution::QueryStatus::Failed});

    test.start();
    {
        test.startQuery(std::move(query));
        ASSERT_TRUE(test.waitForQepTermination(id, DEFAULT_LONG_AWAIT_TIMEOUT));
    }
    test.stop();
}

TEST_F(QueryEngineTest, failureDuringPipelineStartWithMultipleSources)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source1 = builder.addSource();
    auto failingPipeline = builder.addPipeline({source1});
    auto failingPipelineSuccessor = builder.addPipeline({failingPipeline});
    auto source2 = builder.addSource();
    auto pipeline = builder.addPipeline({source2});
    builder.addSink({failingPipelineSuccessor, pipeline});

    auto query = test.addNewQuery(std::move(builder));
    auto id = query->queryId;
    test.pipelineControls[failingPipeline]->failOnStart = true;

    test.expectQueryStatusEvents(id, {Runtime::Execution::QueryStatus::Failed});

    test.start();
    {
        test.startQuery(std::move(query));
        ASSERT_TRUE(test.waitForQepTermination(id, DEFAULT_LONG_AWAIT_TIMEOUT));
    }
    test.stop();
}

TEST_F(QueryEngineTest, singleQueryWithTwoSourcesWaitingForTwoStops)
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
    test.expectQueryStatusEvents(
        QueryId(1),
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});
    test.expectSourceTermination(QueryId(1), source1, Runtime::QueryTerminationType::Graceful);
    test.expectSourceTermination(QueryId(1), source2, Runtime::QueryTerminationType::Graceful);

    test.start();
    {
        test.startQuery(std::move(query));

        ASSERT_TRUE(ctrl1->waitUntilOpened());
        EXPECT_FALSE(ctrl1->wasClosed());

        ASSERT_TRUE(ctrl2->waitUntilOpened());
        EXPECT_FALSE(ctrl2->wasClosed());

        ASSERT_TRUE(ctrl1->injectData(identifiableData(1), NUMBER_OF_TUPLES_PER_BUFFER + 0));
        ASSERT_TRUE(ctrl1->injectData(identifiableData(2), NUMBER_OF_TUPLES_PER_BUFFER + 1));
        ASSERT_TRUE(ctrl2->injectData(identifiableData(3), NUMBER_OF_TUPLES_PER_BUFFER + 2));
        ASSERT_TRUE(ctrl2->injectData(identifiableData(4), NUMBER_OF_TUPLES_PER_BUFFER + 3));
        ASSERT_TRUE(ctrl1->injectEoS());

        ASSERT_TRUE(sinkCtrl->waitForNumberOfReceivedBuffersOrMore(4));

        ASSERT_TRUE(ctrl2->injectData(identifiableData(5), NUMBER_OF_TUPLES_PER_BUFFER + 4));
        ASSERT_TRUE(sinkCtrl->waitForNumberOfReceivedBuffersOrMore(5));
        ASSERT_TRUE(ctrl2->injectData(identifiableData(6), NUMBER_OF_TUPLES_PER_BUFFER + 5));
        ASSERT_TRUE(sinkCtrl->waitForNumberOfReceivedBuffersOrMore(6));
        ASSERT_TRUE(ctrl2->injectEoS());

        ASSERT_TRUE(test.waitForQepTermination(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
    }
    test.stop();

    auto buffers = sinkCtrl->takeBuffers();
    ASSERT_TRUE(ctrl1->waitUntilDestroyed());
    ASSERT_TRUE(ctrl2->waitUntilDestroyed());
}

TEST_F(QueryEngineTest, singleQueryWithManySources)
{
    constexpr size_t numberOfSources = 100;
    constexpr size_t numberOfBuffersBeforeTermination = 1000;

    TestingHarness test(LARGE_NUMBER_OF_THREADS, NUMBER_OF_BUFFERS_PER_SOURCE * numberOfSources);
    auto builder = test.buildNewQuery();
    std::vector<QueryPlanBuilder::identifier_t> sources;
    sources.reserve(numberOfSources);
    for (size_t i = 0; i < numberOfSources; i++)
    {
        sources.push_back(builder.addSource());
    }

    auto sink = builder.addSink({builder.addPipeline(sources)});
    auto query = test.addNewQuery(std::move(builder));

    std::vector<std::shared_ptr<Sources::TestSourceControl>> sourcesCtrls;
    std::ranges::transform(
        sources,
        std::back_inserter(sourcesCtrls),
        [&](auto identifier)
        {
            test.expectSourceTermination(QueryId(1), identifier, Runtime::QueryTerminationType::Graceful);
            return test.sourceControls[identifier];
        });

    auto sinkCtrl = test.sinkControls[sink];
    test.expectQueryStatusEvents(
        QueryId(1),
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});

    test.start();
    {
        test.startQuery(std::move(query));
        DataGenerator dataGenerator;
        dataGenerator.start(std::move(sourcesCtrls));
        sinkCtrl->waitForNumberOfReceivedBuffersOrMore(numberOfBuffersBeforeTermination);
        dataGenerator.stop();

        ASSERT_TRUE(test.waitForQepTermination(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
    }
    test.stop();
}

/// Apparently Sources are not notified if on of the sources fails
/// The Mocked QueryStatusListener does not receive source termination for all sources except the failed source
TEST_F(QueryEngineTest, singleQueryWithManySourcesOneOfThemFails)
{
    constexpr size_t numberOfSources = 10;
    constexpr size_t numberOfBuffersBeforeFailure = 5;

    TestingHarness test(LARGE_NUMBER_OF_THREADS, NUMBER_OF_BUFFERS_PER_SOURCE * numberOfSources);
    auto builder = test.buildNewQuery();
    std::vector<QueryPlanBuilder::identifier_t> sources;
    sources.reserve(numberOfSources);
    for (size_t i = 0; i < numberOfSources; i++)
    {
        sources.push_back(builder.addSource());
    }

    auto sink = builder.addSink({builder.addPipeline(sources)});
    auto query = test.addNewQuery(std::move(builder));

    std::vector<std::shared_ptr<Sources::TestSourceControl>> sourcesCtrls;
    std::ranges::transform(sources, std::back_inserter(sourcesCtrls), [&](auto identifier) { return test.sourceControls[identifier]; });

    test.expectQueryStatusEvents(
        QueryId(1),
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Failed});
    /// Overwrite Source 0 to expect source failure.
    test.expectSourceTermination(QueryId(1), sources[0], Runtime::QueryTerminationType::Failure);

    auto sinkCtrl = test.sinkControls[sink];

    test.start();
    {
        test.startQuery(std::move(query));

        DataGenerator<FailAfter<numberOfBuffersBeforeFailure, 0>> dataGenerator;
        dataGenerator.start(sourcesCtrls);
        ASSERT_TRUE(test.waitForQepTermination(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
        dataGenerator.stop();
    }

    for (const auto& ctrl : sourcesCtrls)
    {
        ASSERT_TRUE(ctrl->waitUntilDestroyed());
        ASSERT_TRUE(ctrl->wasOpened());
    }
    test.stop();
}

TEST_F(QueryEngineTest, ManyQueriesWithTwoSources)
{
    constexpr size_t numberOfSources = 2;
    constexpr size_t numberOfQueries = 10;

    TestingHarness test(LARGE_NUMBER_OF_THREADS, NUMBER_OF_BUFFERS_PER_SOURCE * numberOfSources * numberOfQueries);

    std::vector<QueryPlanBuilder::identifier_t> sources;
    std::vector<QueryPlanBuilder::identifier_t> sinks;
    std::vector<std::unique_ptr<Runtime::ExecutableQueryPlan>> queryPlans;
    for (size_t i = 0; i < numberOfQueries; i++)
    {
        auto builder = test.buildNewQuery();
        auto source1 = builder.addSource();
        auto source2 = builder.addSource();
        sources.push_back(source1);
        sources.push_back(source2);
        sinks.push_back(builder.addSink({builder.addPipeline({source1, source2})}));
        queryPlans.push_back(test.addNewQuery(std::move(builder)));
    }

    std::vector<std::shared_ptr<Sources::TestSourceControl>> sourcesCtrls;
    std::vector<std::shared_ptr<TestSinkController>> sinkCtrls;

    for (const auto& [index, _] : queryPlans | views::enumerate)
    {
        sourcesCtrls.push_back(test.sourceControls[sources[index * 2]]);
        sourcesCtrls.push_back(test.sourceControls[sources[(index * 2) + 1]]);
        sinkCtrls.push_back(test.sinkControls[sinks[index]]);
        test.expectSourceTermination(QueryId(1 + index), sources[index * 2], Runtime::QueryTerminationType::Graceful);
        test.expectSourceTermination(QueryId(1 + index), sources[(index * 2) + 1], Runtime::QueryTerminationType::Graceful);
        test.expectQueryStatusEvents(
            QueryId(1 + index),
            {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});
    }

    test.start();
    {
        DataGenerator dataGenerator;
        dataGenerator.start(sourcesCtrls);
        auto queryIds = queryPlans
            | std::views::transform(
                            [&test](std::unique_ptr<Runtime::ExecutableQueryPlan>& query) -> QueryId
                            {
                                auto queryId = query->queryId;
                                test.startQuery(std::move(query));
                                return queryId;
                            })
            | std::ranges::to<std::vector<QueryId>>();

        for (auto queryId : queryIds)
        {
            ASSERT_TRUE(test.waitForQepRunning(queryId, DEFAULT_LONG_AWAIT_TIMEOUT));
        }

        sinkCtrls[0]->waitForNumberOfReceivedBuffersOrMore(2);
        dataGenerator.stop();

        for (auto queryId : queryIds)
        {
            ASSERT_TRUE(test.waitForQepTermination(queryId, DEFAULT_LONG_AWAIT_TIMEOUT));
        }
    }

    for (const auto& testSourceControl : sourcesCtrls)
    {
        ASSERT_TRUE(testSourceControl->waitUntilDestroyed());
    }
    test.stop();
}

/// This test creates 10 QueryPlans (each with two sources one intermediate pipeline and a sink)
/// The TestDataGenerator will emit a failure on the 0th source (for QueryId 1)
/// We expect QueryId 1 to terminate without internal intervention and the query failed status to be emitted (likewise for the source)
/// All other queries should be uneffected, which is verified by them not terminating and still receiving data
/// We expect QueryId 2 to be terminated by an internal stop (qm->stop(2)).
/// Lastly all other queries are terminated via EoS
TEST_F(QueryEngineTest, ManyQueriesWithTwoSourcesOneSourceFails)
{
    constexpr size_t numberOfSources = 2;
    constexpr size_t numberOfQueries = 10;

    TestingHarness test(LARGE_NUMBER_OF_THREADS, NUMBER_OF_BUFFERS_PER_SOURCE * numberOfQueries * numberOfSources);

    std::vector<QueryPlanBuilder::identifier_t> sources;
    std::vector<QueryPlanBuilder::identifier_t> sinks;
    std::vector<std::unique_ptr<Runtime::ExecutableQueryPlan>> queryPlans;
    for (size_t i = 0; i < numberOfQueries; i++)
    {
        auto builder = test.buildNewQuery();
        auto source1 = builder.addSource();
        auto source2 = builder.addSource();
        sources.push_back(source1);
        sources.push_back(source2);
        sinks.push_back(builder.addSink({builder.addPipeline({source1, source2})}));
        queryPlans.push_back(test.addNewQuery(std::move(builder)));
    }

    std::vector<std::shared_ptr<Sources::TestSourceControl>> sourcesCtrls;
    std::vector<std::shared_ptr<TestSinkController>> sinkCtrls;

    /// Query 1 is terminated by an Failure of source 0
    sourcesCtrls.push_back(test.sourceControls[sources[0]]);
    sourcesCtrls.push_back(test.sourceControls[sources[1]]);
    sinkCtrls.push_back(test.sinkControls[sinks[0]]);
    test.expectSourceTermination(QueryId(1), sources[0], Runtime::QueryTerminationType::Failure);
    test.expectQueryStatusEvents(
        QueryId(1),
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Failed});

    /// Query 2 is terminated by an internal stop
    sourcesCtrls.push_back(test.sourceControls[sources[2]]);
    sourcesCtrls.push_back(test.sourceControls[sources[3]]);
    sinkCtrls.push_back(test.sinkControls[sinks[1]]);
    test.expectQueryStatusEvents(
        QueryId(2),
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});

    /// Rest of the queries are terminated by external stop via eos
    for (size_t index = 2; const auto& query : queryPlans | std::ranges::views::drop(2))
    {
        sourcesCtrls.push_back(test.sourceControls[sources[index * 2]]);
        sourcesCtrls.push_back(test.sourceControls[sources[(index * 2) + 1]]);
        sinkCtrls.push_back(test.sinkControls[sinks[index]]);
        test.expectSourceTermination(QueryId(1 + index), sources[index * 2], Runtime::QueryTerminationType::Graceful);
        test.expectSourceTermination(QueryId(1 + index), sources[(index * 2) + 1], Runtime::QueryTerminationType::Graceful);
        test.expectQueryStatusEvents(
            QueryId(1 + index),
            {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});
        index++;
    }

    test.start();
    {
        /// instruct DataGenerator to emit failure for source 0 after 3 tuple buffer
        DataGenerator<FailAfter<3, 0>> dataGenerator;
        dataGenerator.start(sourcesCtrls);

        /// start all queries
        for (const QueryId::Underlying qidGenerator = QueryId::INITIAL; auto& query : queryPlans)
        {
            auto queryId = query->queryId;
            test.startQuery(std::move(query));
            ASSERT_TRUE(test.waitForQepRunning(queryId, DEFAULT_LONG_AWAIT_TIMEOUT));
        }

        /// Expect Query 1 to be terminated by the failure.
        ASSERT_TRUE(test.waitForQepTermination(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
        /// Expect all other queries to be still running
        for (size_t queryId = 2; auto& query : queryPlans | std::ranges::views::drop(1))
        {
            EXPECT_FALSE(test.waitForQepTermination(QueryId(queryId), std::chrono::milliseconds(10)));
            queryId++;
        }

        /// Internally stop Query 2 and wait for termination
        test.stopQuery(QueryId(2));
        ASSERT_TRUE(test.waitForQepTermination(QueryId(2), DEFAULT_LONG_AWAIT_TIMEOUT));

        /// Externally stop all other queries via EoS from the datagenerator
        dataGenerator.stop();
        for (size_t queryId = 3; auto& query : queryPlans | std::ranges::views::drop(2))
        {
            ASSERT_TRUE(test.waitForQepTermination(QueryId(queryId), DEFAULT_LONG_AWAIT_TIMEOUT));
            queryId++;
        }
    }

    for (const auto& testSourceControl : sourcesCtrls)
    {
        ASSERT_TRUE(testSourceControl->waitUntilDestroyed());
    }
    test.stop();
}

TEST_F(QueryEngineTest, singleQueryWithTwoSourceExternalStop)
{
    constexpr size_t numberOfSources = 2;

    TestingHarness test(LARGE_NUMBER_OF_THREADS, numberOfSources * NUMBER_OF_BUFFERS_PER_SOURCE);
    auto builder = test.buildNewQuery();
    auto source1 = builder.addSource();
    auto source2 = builder.addSource();
    auto pipeline = builder.addPipeline({source1, source2});
    auto sink = builder.addSink({pipeline});
    auto query = test.addNewQuery(std::move(builder));

    test.expectQueryStatusEvents(
        QueryId(1),
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});

    test.start();
    {
        test.startQuery(std::move(query));
        ASSERT_TRUE(test.sourceControls[source1]->waitUntilOpened());
        ASSERT_TRUE(test.sourceControls[source2]->waitUntilOpened());

        test.sourceControls[source1]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sourceControls[source1]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sourceControls[source2]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        ASSERT_TRUE(test.sinkControls[sink]->waitForNumberOfReceivedBuffersOrMore(3));
        test.stopQuery(QueryId(1));
        ASSERT_TRUE(test.waitForQepTermination(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.sourceControls[source1]->waitUntilDestroyed());
        ASSERT_TRUE(test.sourceControls[source2]->waitUntilDestroyed());
    }
    test.stop();
}

TEST_F(QueryEngineTest, singleQueryWithSlowlyFailingSourceDuringEngineTermination)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline = builder.addPipeline({source});
    auto sink = builder.addSink({pipeline});
    auto query = test.addNewQuery(std::move(builder));

    /// Statistics. 1 Query Start/Stop with 2 pipelines and 0 data emitted
    ///   Query Stop and Pipelines are not terminated due to engine shutdown

    test.stats.expect(
        ExpectStats::QueryStart(1),
        ExpectStats::QueryStop(0),
        ExpectStats::PipelineStart(2),
        ExpectStats::PipelineStop(0),
        ExpectStats::TaskExecutionStart(0),
        ExpectStats::TaskExecutionComplete(0),
        ExpectStats::TaskEmit(0));

    test.sourceControls[source]->failDuringOpen(DEFAULT_AWAIT_TIMEOUT);
    test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running});

    test.start();
    {
        test.startQuery(std::move(query));
        ASSERT_TRUE(test.sinkControls[sink]->waitForInitialization(DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.pipelineControls[pipeline]->waitForStart());
        ASSERT_TRUE(test.waitForQepRunning(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
    }
    test.stop();
}

TEST_F(QueryEngineTest, singleQueryWithSlowlyFailingSourceDuringQueryPlanTermination)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline = builder.addPipeline({source});
    auto sink = builder.addSink({pipeline});
    auto query = test.addNewQuery(std::move(builder));

    /// Statistics. 1 Query Start/Stop with 2 pipelines and 0 data emitted
    test.stats.expect(
        ExpectStats::QueryStart(1),
        ExpectStats::QueryStop(1),
        ExpectStats::PipelineStart(2),
        ExpectStats::PipelineStop(2),
        ExpectStats::TaskExecutionStart(0),
        ExpectStats::TaskExecutionComplete(0),
        ExpectStats::TaskEmit(0));

    test.sourceControls[source]->failDuringOpen(DEFAULT_LONG_AWAIT_TIMEOUT);
    test.expectQueryStatusEvents(
        query->queryId,
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});

    test.start();
    {
        test.startQuery(std::move(query));
        ASSERT_TRUE(test.sinkControls[sink]->waitForInitialization(DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.pipelineControls[pipeline]->waitForStart());
        ASSERT_TRUE(test.waitForQepRunning(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
        test.stopQuery(QueryId(1));
        /// Termination only happens after the source has failed so we have to wait at least as long
        ASSERT_TRUE(test.waitForQepTermination(QueryId(1), 2 * DEFAULT_LONG_AWAIT_TIMEOUT));
    }
    test.stop();
}

template <typename T>
auto IsInInclusiveRange(T lo, T hi)
{
    return ::testing::AllOf(::testing::Ge((lo)), ::testing::Le((hi)));
}

TEST_F(QueryEngineTest, singleQueryWithPipelineFailure)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline = builder.addPipeline({source});
    auto sink = builder.addSink({pipeline});
    auto query = test.addNewQuery(std::move(builder));

    test.expectQueryStatusEvents(
        QueryId(1),
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Failed});
    test.pipelineControls[pipeline]->throwOnNthInvocation = 2;

    test.start();
    {
        test.startQuery(std::move(query));
        ASSERT_TRUE(test.sinkControls[sink]->waitForInitialization(DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.pipelineControls[pipeline]->waitForStart());
        ASSERT_TRUE(test.waitForQepRunning(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
        test.sourceControls[source]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sourceControls[source]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sourceControls[source]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sourceControls[source]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);

        ASSERT_TRUE(test.waitForQepTermination(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.sourceControls[source]->waitUntilDestroyed());
        EXPECT_THAT(test.pipelineControls[pipeline]->invocations.load(), IsInInclusiveRange(2, 4));

        /// There is a race between tasks emitted from pipeline 1 to be processed by the sink pipeline and the query termination
        /// caused by the pipeline failure. Any result between 0 and 3 invocation of the sink pipeline is valid.
        EXPECT_THAT(test.sinkControls[sink]->invocations.load(), IsInInclusiveRange(0, 3));
    }
    test.stop();
}

TEST_F(QueryEngineTest, singleSourceWithMultipleSuccessors)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline1 = builder.addPipeline({source});
    auto pipeline2 = builder.addPipeline({source});
    auto pipeline3 = builder.addPipeline({source});
    auto sink = builder.addSink({pipeline1, pipeline2, pipeline3});

    auto query = test.addNewQuery(std::move(builder));
    test.expectQueryStatusEvents(
        QueryId(1),
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});
    test.expectSourceTermination(QueryId(1), source, Runtime::QueryTerminationType::Graceful);

    test.start();
    {
        test.startQuery(std::move(query));
        ASSERT_TRUE(test.sinkControls[sink]->waitForInitialization(DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.waitForQepRunning(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));

        test.sourceControls[source]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sourceControls[source]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sourceControls[source]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sourceControls[source]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sourceControls[source]->injectEoS();

        ASSERT_TRUE(test.sinkControls[sink]->waitForNumberOfReceivedBuffersOrMore(4 * 3));
        ASSERT_TRUE(test.waitForQepTermination(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.sourceControls[source]->waitUntilDestroyed());
        EXPECT_TRUE(test.pipelineControls[pipeline1]->wasStopped());
        EXPECT_TRUE(test.pipelineControls[pipeline2]->wasStopped());
        EXPECT_TRUE(test.pipelineControls[pipeline3]->wasStopped());
    }
    test.stop();
}

TEST_F(QueryEngineTest, singleSourceWithMultipleSuccessorsSourceFailure)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline1 = builder.addPipeline({source});
    auto pipeline2 = builder.addPipeline({source});
    auto pipeline3 = builder.addPipeline({source});
    auto sink = builder.addSink({pipeline1, pipeline2, pipeline3});

    auto query = test.addNewQuery(std::move(builder));
    test.expectQueryStatusEvents(
        QueryId(1),
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Failed});
    test.expectSourceTermination(QueryId(1), source, Runtime::QueryTerminationType::Failure);


    /// There is a race between the source failure and the query termination (Which is intended).
    /// Both results are fine, as long as the pipeline was executed or has expired as the number of buffers multiplied by source successors.
    std::atomic<size_t> pipelinesCompletedOrExpired = 0;

    /// Count number of completed non-sink tasks
    EXPECT_CALL(*test.stats.listener, onEvent(::testing::VariantWith<Runtime::TaskExecutionComplete>(::testing::_)))
        .WillRepeatedly(::testing::Invoke(
            [&](Runtime::Event event)
            {
                const auto& completion = std::get<Runtime::TaskExecutionComplete>(event);
                if (completion.pipelineId != test.pipelineIds.at(sink))
                {
                    ++pipelinesCompletedOrExpired;
                }
            }));

    /// Count number of expired Non-Sink tasks
    EXPECT_CALL(*test.stats.listener, onEvent(::testing::VariantWith<Runtime::TaskExpired>(::testing::_)))
        .WillRepeatedly(::testing::Invoke(
            [&](Runtime::Event event)
            {
                const auto& expired = std::get<Runtime::TaskExpired>(event);
                if (expired.pipelineId != test.pipelineIds.at(sink))
                {
                    ++pipelinesCompletedOrExpired;
                }
            }));

    test.start();
    {
        test.startQuery(std::move(query));
        ASSERT_TRUE(test.sinkControls[sink]->waitForInitialization(DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.waitForQepRunning(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));

        test.sourceControls[source]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sourceControls[source]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sourceControls[source]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sourceControls[source]->injectData(std::vector(DEFAULT_BUFFER_SIZE, std::byte(0)), NUMBER_OF_TUPLES_PER_BUFFER);
        test.sourceControls[source]->injectError("I should fail here!");

        ASSERT_TRUE(test.waitForQepTermination(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.sourceControls[source]->waitUntilDestroyed());
        EXPECT_FALSE(test.pipelineControls[pipeline1]->wasStopped());
        EXPECT_FALSE(test.pipelineControls[pipeline2]->wasStopped());
        EXPECT_FALSE(test.pipelineControls[pipeline3]->wasStopped());
    }

    /// Delay engine shutdown to ensure all tasks have either completed or expired
    std::this_thread::sleep_for(DEFAULT_AWAIT_TIMEOUT);

    test.stop();

    EXPECT_EQ(pipelinesCompletedOrExpired.load(), 3 * 4) << "Expected 4 Buffers for each of 3 pipeline to be either processed or expire";
}

TEST_F(QueryEngineTest, ManyQueriesWithTwoSourcesAndPipelineFailures)
{
    constexpr size_t numberOfSources = 2;
    constexpr size_t numberOfQueries = 10;
    constexpr size_t failAfterNInvocations = 2;

    TestingHarness test(LARGE_NUMBER_OF_THREADS, NUMBER_OF_BUFFERS_PER_SOURCE * numberOfQueries * numberOfSources);

    std::vector<QueryPlanBuilder::identifier_t> sources;
    std::vector<QueryPlanBuilder::identifier_t> pipelines;
    std::vector<QueryPlanBuilder::identifier_t> sinks;
    std::vector<std::unique_ptr<Runtime::ExecutableQueryPlan>> queryPlans;
    for (size_t i = 0; i < numberOfQueries; i++)
    {
        auto builder = test.buildNewQuery();
        auto source1 = builder.addSource();
        auto source2 = builder.addSource();
        auto pipeline1 = builder.addPipeline({source1, source2});
        auto pipeline2 = builder.addPipeline({source1, source2});
        sources.push_back(source1);
        sources.push_back(source2);
        pipelines.push_back(pipeline1);
        sinks.push_back(builder.addSink({pipeline1, pipeline2}));
        queryPlans.push_back(test.addNewQuery(std::move(builder)));
    }

    std::vector<std::shared_ptr<Sources::TestSourceControl>> sourcesCtrls;
    std::vector<std::shared_ptr<TestSinkController>> sinkCtrls;

    /// Query 1 is terminated by an end of stream
    sourcesCtrls.push_back(test.sourceControls[sources[0]]);
    sourcesCtrls.push_back(test.sourceControls[sources[1]]);
    sinkCtrls.push_back(test.sinkControls[sinks[0]]);
    test.expectSourceTermination(QueryId(1), sources[0], Runtime::QueryTerminationType::Graceful);
    test.expectSourceTermination(QueryId(1), sources[1], Runtime::QueryTerminationType::Graceful);
    test.expectQueryStatusEvents(
        QueryId(1),
        {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Stopped});

    /// Rest of the queries are failing due to pipeline errors on pipeline 1
    for (size_t index = 1; const auto& query : queryPlans | std::ranges::views::drop(1))
    {
        test.pipelineControls[pipelines[index]]->throwOnNthInvocation = failAfterNInvocations;
        sourcesCtrls.push_back(test.sourceControls[sources[index * 2]]);
        sourcesCtrls.push_back(test.sourceControls[sources[(index * 2) + 1]]);
        sinkCtrls.push_back(test.sinkControls[sinks[index]]);
        test.expectQueryStatusEvents(
            QueryId(1 + index),
            {Runtime::Execution::QueryStatus::Started, Runtime::Execution::QueryStatus::Running, Runtime::Execution::QueryStatus::Failed});
        index++;
    }

    test.start();
    {
        /// instruct DataGenerator to emit failure for source 0 after 3 tuple buffer
        DataGenerator dataGenerator;
        dataGenerator.start(sourcesCtrls);

        /// start all queries
        for (const QueryId::Underlying qidGenerator = QueryId::INITIAL; auto& query : queryPlans)
        {
            auto queryId = query->queryId;
            test.startQuery(std::move(query));
            ASSERT_TRUE(test.waitForQepRunning(queryId, DEFAULT_LONG_AWAIT_TIMEOUT));
        }

        /// Expect all other queries to have failed
        for (size_t queryId = 2; auto& query : queryPlans | std::ranges::views::drop(1))
        {
            ASSERT_TRUE(test.waitForQepTermination(QueryId(queryId), DEFAULT_LONG_AWAIT_TIMEOUT));
            queryId++;
        }

        /// Query 1 should be alive
        ASSERT_FALSE(test.waitForQepTermination(QueryId(1), DEFAULT_AWAIT_TIMEOUT));

        /// Externally stop all other queries via EoS from the datagenerator
        dataGenerator.stop();
        /// Expect Query 1 to be terminated by end of stream.
        ASSERT_TRUE(test.waitForQepTermination(QueryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
    }

    for (const auto& testSourceControl : sourcesCtrls)
    {
        ASSERT_TRUE(testSourceControl->waitUntilDestroyed());
    }
    test.stop();
}
}
