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

#include <filesystem>
#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <yaml-cpp/node/parse.h>
#include <yaml-cpp/yaml.h>

#include <QueryManager/QueryManager.hpp>
#include <BaseUnitTest.hpp>
#include <QueryPlanning.hpp>

namespace NES
{
class QueryManagerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("QueryManagerTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryManagerTest class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

    static void TearDownTestSuite() { NES_INFO("Tear down QueryManagerTest class."); }
};

struct MockSubmissionBackend : QuerySubmissionBackend
{
    [[nodiscard]] std::expected<LocalQueryId, Exception> registerQuery(LogicalPlan) override
    {
        randomTick();
        auto state = stateMtx.wlock();
        auto id = LocalQueryId(generateUUID());
        state->registered.insert(id);
        state->metrics[id] = QueryMetrics{};
        return id;
    }

    std::expected<void, Exception> start(LocalQueryId id) override
    {
        randomTick();
        auto state = stateMtx.wlock();
        if (state->registered.erase(id) == 1)
        {
            state->starting.insert(id);
            state->metrics[id].start = std::chrono::system_clock::now();
            return {};
        }
        return std::unexpected{QueryNotFound("Query {} was not registered", id)};
    }

    std::expected<void, Exception> stop(LocalQueryId id) override
    {
        randomTick();
        auto state = stateMtx.wlock();
        if (state->stopping.erase(id) == 1)
        {
            state->stopped.insert(id);
            state->metrics[id].stop = std::chrono::system_clock::now();
            return {};
        }
        if (state->starting.erase(id) == 1)
        {
            state->stopping.insert(id);
            return {};
        }
        if (state->running.erase(id) == 1)
        {
            state->stopping.insert(id);
            return {};
        }
        return std::unexpected{QueryNotFound("Query {} was not started", id)};
    }

    std::expected<void, Exception> unregister(LocalQueryId id) override
    {
        randomTick();
        auto state = stateMtx.wlock();
        if (state->registered.erase(id) == 1)
        {
            state->metrics.erase(id);
            return {};
        }
        if (state->stopping.erase(id) == 1)
        {
            state->metrics.erase(id);
            return {};
        }
        if (state->starting.erase(id) == 1)
        {
            state->metrics.erase(id);
            return {};
        }
        if (state->running.erase(id) == 1)
        {
            state->metrics.erase(id);
            return {};
        }

        return std::unexpected{QueryNotFound("Query {} was not registered", id)};
    }

    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(LocalQueryId id) const override
    {
        randomTick();

        auto state = stateMtx.wlock();
        if (state->failed.contains(id))
        {
            return LocalQueryStatus{.queryId = id, .state = QueryState::Failed, .metrics = state->metrics.at(id)};
        }
        if (state->registered.contains(id))
        {
            return LocalQueryStatus{.queryId = id, .state = QueryState::Registered, .metrics = state->metrics.at(id)};
        }
        if (state->stopped.contains(id))
        {
            return LocalQueryStatus{.queryId = id, .state = QueryState::Stopped, .metrics = state->metrics.at(id)};
        }
        if (state->stopping.contains(id))
        {
            return LocalQueryStatus{.queryId = id, .state = QueryState::Running, .metrics = state->metrics.at(id)};
        }
        if (state->starting.contains(id))
        {
            return LocalQueryStatus{.queryId = id, .state = QueryState::Started, .metrics = state->metrics.at(id)};
        }
        if (state->running.contains(id))
        {
            return LocalQueryStatus{.queryId = id, .state = QueryState::Running, .metrics = state->metrics.at(id)};
        }

        return std::unexpected{QueryNotFound("Query {} was not started", id)};
    }

    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const override
    {
        WorkerStatus status;
        status.after = after;
        status.until = std::chrono::system_clock::now();

        auto state = stateMtx.wlock();
        for (auto starting : state->starting)
        {
            status.activeQueries.emplace_back(starting, state->metrics.at(starting).start.value());
        }

        for (auto running : state->running)
        {
            status.activeQueries.emplace_back(running, state->metrics.at(running).start.value());
        }

        for (auto stopping : state->stopping)
        {
            status.activeQueries.emplace_back(stopping, state->metrics.at(stopping).start.value());
        }
        for (auto stopped : state->stopped)
        {
            auto& m = state->metrics.at(stopped);
            status.terminatedQueries.emplace_back(stopped, m.start.value(), m.stop.value(), std::nullopt);
        }
        for (auto failed : state->failed)
        {
            auto& m = state->metrics.at(failed);
            status.terminatedQueries.emplace_back(failed, m.start.value(), m.stop.value(), m.error);
        }
        return status;
    }

    void randomTick() const
    {
        auto state = stateMtx.wlock();
        if (!state->starting.empty())
        {
            auto id = state->starting.extract(state->starting.begin()).value();
            state->metrics[id].running = std::chrono::system_clock::now();
            state->running.insert(id);
        }
        if (!state->stopping.empty())
        {
            auto id = state->stopping.extract(state->stopping.begin()).value();
            state->metrics[id].stop = std::chrono::system_clock::now();
            state->stopped.insert(id);
        }
    }

    struct State
    {
        std::unordered_set<LocalQueryId> registered;
        std::unordered_set<LocalQueryId> starting;
        std::unordered_set<LocalQueryId> running;
        std::unordered_set<LocalQueryId> stopping;
        std::unordered_set<LocalQueryId> stopped;
        std::unordered_set<LocalQueryId> failed;
        std::unordered_map<LocalQueryId, QueryMetrics> metrics;
    };

    void failQuery(LocalQueryId id)
    {
        auto state = stateMtx.wlock();
        if (auto it = state->starting.find(id); it != state->starting.end())
        {
            state->metrics[id].error = TestException("An Exception");
            state->metrics[id].stop = std::chrono::system_clock::now();
            state->failed.insert(state->starting.extract(it).value());
        }
        if (auto it = state->stopping.find(id); it != state->stopping.end())
        {
            state->metrics[id].error = TestException("An Exception");
            state->metrics[id].stop = std::chrono::system_clock::now();
            state->failed.insert(state->stopping.extract(it).value());
        }
        if (auto it = state->running.find(id); it != state->running.end())
        {
            state->metrics[id].error = TestException("An Exception");
            state->metrics[id].stop = std::chrono::system_clock::now();
            state->failed.insert(state->running.extract(it).value());
        }
    }

    void stopQuery(LocalQueryId id)
    {
        auto state = stateMtx.wlock();
        if (auto it = state->starting.find(id); it != state->starting.end())
        {
            state->metrics[id].stop = std::chrono::system_clock::now();
            state->stopped.insert(state->starting.extract(it).value());
        }
        if (auto it = state->stopping.find(id); it != state->stopping.end())
        {
            state->metrics[id].stop = std::chrono::system_clock::now();
            state->stopped.insert(state->stopping.extract(it).value());
        }
        if (auto it = state->running.find(id); it != state->running.end())
        {
            state->metrics[id].stop = std::chrono::system_clock::now();
            state->stopped.insert(state->running.extract(it).value());
        }
    }

    mutable folly::Synchronized<State> stateMtx;
};

PlanStage::DistributedLogicalPlan dummyPlan()
{
    return PlanStage::DistributedLogicalPlan{
        PlanStage::DecomposedLogicalPlan<GrpcAddr>{{{GrpcAddr("grpc:8080"), {LogicalPlan{}}}}},
        PlanStage::OptimizedLogicalPlan{LogicalPlan{}}};
}

TEST_F(QueryManagerTest, Test)
{
    MockSubmissionBackend* backend = nullptr;
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    workerCatalog->addWorker(HostAddr("host:9090"), GrpcAddr("grpc:8080"), 100000, {});
    QueryManager qm(
        workerCatalog,
        [&backend](const WorkerConfig&) -> UniquePtr<QuerySubmissionBackend>
        {
            auto mockBackend = std::make_unique<MockSubmissionBackend>();
            backend = mockBackend.get();
            return mockBackend;
        });

    auto id = qm.registerQuery(dummyPlan()).value();
    qm.start(id);
    auto status = qm.workerStatus(std::chrono::system_clock::time_point{std::chrono::milliseconds(0)}).value();
    EXPECT_THAT(status.workerStatus.at(GrpcAddr("grpc:8080")).value().activeQueries, ::testing::SizeIs(1));

    ASSERT_EQ(backend->stateMtx->running.size(), 1);
    auto localQueryId = *backend->stateMtx->running.begin();
    backend->stopQuery(localQueryId);

    auto statusAfter = qm.workerStatus(std::chrono::system_clock::time_point{std::chrono::milliseconds(0)}).value();
    EXPECT_THAT(statusAfter.workerStatus.at(GrpcAddr("grpc:8080")).value().terminatedQueries, ::testing::SizeIs(1));
}
}
