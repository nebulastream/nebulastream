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
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
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
/// self-terminates when it is the only candidate left. This exercises the arbiter's allocate() loop,
/// QueryCatalog::selectVictim/selectLargest/gatherVictimCandidates/failQuery, and RunningQueryPlan::sumPendingTasks.
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
        sourceIds[i] = builder.addSource();
        pipelineIds[i] = builder.addPipeline({sourceIds[i]});
        builder.addSink({pipelineIds[i]});
        queries.emplace_back(test.addNewQuery(std::move(builder)));
    }

    /// Each pipeline invocation grabs the whole pool, guaranteeing exhaustion as soon as data flows.
    for (size_t i = 0; i < numberOfQueries; ++i)
    {
        test.pipelineControls[pipelineIds[i]]->allocateAndHoldPerInvocation = numberOfBuffers;
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
    dataGenerator.start({test.sourceControls[sourceIds[0]], test.sourceControls[sourceIds[1]]});

    for (size_t i = 0; i < numberOfQueries; ++i)
    {
        EXPECT_TRUE(test.waitForQepTermination(test.queryId(i), DEFAULT_LONG_AWAIT_TIMEOUT));
    }

    dataGenerator.stop();
    test.stop();
}

}
