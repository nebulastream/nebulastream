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

#include <array>
#include <chrono>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryEngineTestingInfrastructure.hpp>
#include <QueryStatus.hpp>

namespace NES::Testing
{
class BufferExhaustionTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("BufferExhaustionTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup BufferExhaustionTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

/// With a deliberately tiny global buffer pool, two queries whose pipelines allocate and hold buffers through the
/// PipelineExecutionContext exhaust the pool. Instead of deadlocking, the buffer-exhaustion arbiter terminates a
/// victim query (TERMINATE_LARGEST, the default) so the engine recovers; the victim fails with QueryBufferExhausted.
/// Once a query is terminated its held buffers are released, so the pool recovers and the remaining holder eventually
/// self-terminates when it is the only candidate left. This exercises the arbiter's allocate() loop, the per-query
/// buffer providers, and QueryCatalog::selectVictim/selectLargest/gatherVictimCandidates/failQuery.
TEST_F(BufferExhaustionTest, ExhaustionTerminatesVictimAndRecovers)
{
    constexpr size_t numberOfThreads = 2;
    constexpr size_t numberOfBuffers = 64;
    constexpr size_t numberOfQueries = 2;
    TestingHarness test(numberOfThreads, numberOfBuffers);

    std::array<QueryPlanBuilder::identifier_t, numberOfQueries> sourceIds{};
    std::array<QueryPlanBuilder::identifier_t, numberOfQueries> pipelineIds{};
    std::vector<std::unique_ptr<ExecutableQueryPlan>> queries;
    for (size_t i = 0; i < numberOfQueries; ++i)
    {
        auto builder = test.buildNewQuery();
        const auto source = builder.addSource();
        const auto pipeline = builder.addPipeline({source});
        builder.addSink({pipeline});
        sourceIds.at(i) = source;
        pipelineIds.at(i) = pipeline;
        queries.emplace_back(test.addNewQuery(std::move(builder)));
    }

    /// Each pipeline invocation grabs the whole pool, guaranteeing exhaustion as soon as data flows.
    for (size_t i = 0; i < numberOfQueries; ++i)
    {
        test.pipelineControls[pipelineIds.at(i)]->allocateAndHoldPerInvocation = numberOfBuffers;
        test.expectQueryStatusEvents(test.queryId(i), {QueryStatus::Started, QueryStatus::Running, QueryStatus::Failed});
    }

    test.start();
    for (auto& query : queries)
    {
        test.startQuery(std::move(query));
    }

    /// Reach Running before any data flows, so no exhaustion happens during startup.
    for (size_t i = 0; i < numberOfQueries; ++i)
    {
        ASSERT_TRUE(test.waitForQepRunning(test.queryId(i), DEFAULT_LONG_AWAIT_TIMEOUT));
    }

    /// Continuously feed both sources so the pipelines keep allocating until each query is terminated.
    DataGenerator<> dataGenerator;
    dataGenerator.start({test.sourceControls[sourceIds.at(0)], test.sourceControls[sourceIds.at(1)]});

    for (size_t i = 0; i < numberOfQueries; ++i)
    {
        EXPECT_TRUE(test.waitForQepTermination(test.queryId(i), DEFAULT_LONG_AWAIT_TIMEOUT));
    }

    dataGenerator.stop();
    test.stop();
}

/// Asymmetric case: one hoarding query and one well-behaved query share a tiny pool. The arbiter must rank victims on
/// the pooled buffers they hold and terminate the hog, while the well-behaved query keeps processing through the
/// exhaustion episode and stops gracefully on end-of-stream. An arbiter that kills everything fails this test.
TEST_F(BufferExhaustionTest, HoardingQueryIsTerminatedWhileWellBehavedQuerySurvives)
{
    constexpr size_t numberOfThreads = 2;
    /// Generous pool: the well-behaved query's sink retains a deep copy of every received buffer (counted against
    /// that query), so leave enough headroom that only the hog can exhaust the pool.
    constexpr size_t numberOfBuffers = 256;
    TestingHarness test(numberOfThreads, numberOfBuffers);

    auto hogBuilder = test.buildNewQuery();
    const auto hogSource = hogBuilder.addSource();
    const auto hogPipeline = hogBuilder.addPipeline({hogSource});
    hogBuilder.addSink({hogPipeline});
    auto hogQuery = test.addNewQuery(std::move(hogBuilder));

    auto niceBuilder = test.buildNewQuery();
    const auto niceSource = niceBuilder.addSource();
    const auto nicePipeline = niceBuilder.addPipeline({niceSource});
    const auto niceSink = niceBuilder.addSink({nicePipeline});
    auto niceQuery = test.addNewQuery(std::move(niceBuilder));

    /// Only the hog allocates-and-holds; the well-behaved query releases its buffers after every task.
    test.pipelineControls[hogPipeline]->allocateAndHoldPerInvocation = numberOfBuffers;
    test.expectQueryStatusEvents(test.queryId(0), {QueryStatus::Started, QueryStatus::Running, QueryStatus::Failed});
    test.expectQueryStatusEvents(test.queryId(1), {QueryStatus::Started, QueryStatus::Running, QueryStatus::Stopped});

    test.start();
    test.startQuery(std::move(hogQuery));
    test.startQuery(std::move(niceQuery));

    ASSERT_TRUE(test.waitForQepRunning(test.queryId(0), DEFAULT_LONG_AWAIT_TIMEOUT));
    ASSERT_TRUE(test.waitForQepRunning(test.queryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));

    DataGenerator<> dataGenerator;
    dataGenerator.start({test.sourceControls[hogSource], test.sourceControls[niceSource]});

    /// The hog exhausts the pool and must be the victim.
    EXPECT_TRUE(test.waitForQepTermination(test.queryId(0), DEFAULT_LONG_AWAIT_TIMEOUT));

    /// The well-behaved query keeps processing after the hog was shed...
    EXPECT_TRUE(test.sinkControls[niceSink]->waitForNumberOfReceivedBuffersOrMore(1));
    /// Release the copies the sink retained, so the backlog processed after the kill cannot exhaust the pool itself.
    test.sinkControls[niceSink]->takeBuffers();

    /// ...and stops gracefully once its source signals end-of-stream (DataGenerator::stop injects EoS).
    dataGenerator.stop();
    EXPECT_TRUE(test.waitForQepTermination(test.queryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
    test.stop();
}

/// Ranking case: two holders of different size. The arbiter must select the LARGE holder as the victim (not an
/// arbitrary catalog entry), and the small holder must survive and stop gracefully.
TEST_F(BufferExhaustionTest, LargestBufferHolderIsSelectedAsVictim)
{
    constexpr size_t numberOfThreads = 2;
    constexpr size_t numberOfBuffers = 64;
    constexpr size_t smallHold = 16;
    /// smallHold + largeHold exceeds the pool, so exhaustion is guaranteed once both pipelines ran once.
    constexpr size_t largeHold = 56;

    TestingHarness test(numberOfThreads, numberOfBuffers);

    auto smallBuilder = test.buildNewQuery();
    const auto smallSource = smallBuilder.addSource();
    const auto smallPipeline = smallBuilder.addPipeline({smallSource});
    smallBuilder.addSink({smallPipeline});
    auto smallQuery = test.addNewQuery(std::move(smallBuilder));

    auto largeBuilder = test.buildNewQuery();
    const auto largeSource = largeBuilder.addSource();
    const auto largePipeline = largeBuilder.addPipeline({largeSource});
    largeBuilder.addSink({largePipeline});
    auto largeQuery = test.addNewQuery(std::move(largeBuilder));

    test.pipelineControls[smallPipeline]->allocateAndHoldPerInvocation = smallHold;
    test.pipelineControls[largePipeline]->allocateAndHoldPerInvocation = largeHold;
    test.expectQueryStatusEvents(test.queryId(0), {QueryStatus::Started, QueryStatus::Running, QueryStatus::Stopped});
    test.expectQueryStatusEvents(test.queryId(1), {QueryStatus::Started, QueryStatus::Running, QueryStatus::Failed});

    test.start();
    test.startQuery(std::move(smallQuery));
    test.startQuery(std::move(largeQuery));

    ASSERT_TRUE(test.waitForQepRunning(test.queryId(0), DEFAULT_LONG_AWAIT_TIMEOUT));
    ASSERT_TRUE(test.waitForQepRunning(test.queryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));

    /// One buffer per source: each pipeline allocates-and-holds its quota exactly once, so the held counts stay at
    /// smallHold vs. largeHold and the ranking decision is deterministic.
    ASSERT_TRUE(test.sourceControls[smallSource]->injectData(identifiableData(0), NUMBER_OF_TUPLES_PER_BUFFER));
    ASSERT_TRUE(test.sourceControls[largeSource]->injectData(identifiableData(1), NUMBER_OF_TUPLES_PER_BUFFER));

    /// The large holder must be terminated...
    EXPECT_TRUE(test.waitForQepTermination(test.queryId(1), DEFAULT_LONG_AWAIT_TIMEOUT));
    /// ...while the small holder is still alive...
    EXPECT_FALSE(test.waitForQepTermination(test.queryId(0), std::chrono::milliseconds(0)));
    /// ...and stops gracefully on end-of-stream.
    test.sourceControls[smallSource]->injectEoS();
    EXPECT_TRUE(test.waitForQepTermination(test.queryId(0), DEFAULT_LONG_AWAIT_TIMEOUT));
    test.stop();
}

}
