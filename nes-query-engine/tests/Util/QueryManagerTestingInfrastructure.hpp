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

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <ranges>
#include <thread>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/AbstractQueryStatusListener.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <folly/Synchronized.h>
#include <gmock/gmock.h>
#include <gtest/gtest-assertion-result.h>
#include <ErrorHandling.hpp>
#include <Executable.hpp>
#include <ExecutableQueryPlan.hpp>
#include <Interfaces.hpp>
#include <MemoryTestUtils.hpp>
#include <QueryEngine.hpp>
#include <RunningQueryPlan.hpp>
#include <TestSource.hpp>

namespace NES::Testing
{
std::vector<std::byte> identifiableData(size_t identifier);
bool verifyIdentifier(const Memory::TupleBuffer& buffer, size_t identifier);

class QueryStatusListener final : public AbstractQueryStatusListener
{
public:
    MOCK_METHOD(bool, logSourceTermination, (QueryId queryId, OriginId sourceId, Runtime::QueryTerminationType), (override));
    MOCK_METHOD(bool, logQueryFailure, (QueryId queryId, Exception exception), (override));
    MOCK_METHOD(bool, logQueryStatusChange, (QueryId queryId, Runtime::Execution::QueryStatus Status), (override));
};

struct TestWorkEmitter : Runtime::WorkEmitter
{
    MOCK_METHOD(
        void, emitWork, (QueryId, std::weak_ptr<Runtime::RunningQueryPlanNode>, Memory::TupleBuffer, onComplete, onFailure), (override));
    MOCK_METHOD(void, emitSetup, (QueryId, std::weak_ptr<Runtime::RunningQueryPlanNode>, onComplete, onFailure), (override));
    MOCK_METHOD(void, emitStop, (QueryId, std::unique_ptr<Runtime::RunningQueryPlanNode>, onComplete, onFailure), (override));
};

struct TestQueryLifetimeController : Runtime::QueryLifetimeController
{
    MOCK_METHOD(void, initializeSourceFailure, (QueryId, OriginId, std::weak_ptr<Runtime::RunningSource>, Exception), (override));
    MOCK_METHOD(void, initializeSourceStop, (QueryId, OriginId, std::weak_ptr<Runtime::RunningSource>), (override));
};

class TestPipelineOperatorHandlerController
{
public:
    std::atomic_bool started;
    std::atomic_bool stopped;
    std::atomic<Runtime::QueryTerminationType> stopType;
};


template <template <typename> class FutType, typename T>
testing::AssertionResult waitForFuture(const FutType<T>& future, std::chrono::milliseconds timeout)
{
    switch (future.wait_for(timeout))
    {
        case std::future_status::deferred:
        case std::future_status::timeout:
            return testing::AssertionFailure() << "Timeout waiting for future";
        case std::future_status::ready:
            return testing::AssertionSuccess();
    }
    std::unreachable();
}

class TestPipelineController
{
public:
    std::size_t invocations;
    std::atomic_bool stopped;
    std::promise<void> setup;
    std::promise<void> stop;
    std::shared_future<void> setup_future = setup.get_future().share();
    std::shared_future<void> stop_future = stop.get_future().share();
    testing::AssertionResult waitForInitialization() const { return waitForFuture(setup_future, std::chrono::milliseconds(100)); }
    testing::AssertionResult waitForTermination() const { return waitForFuture(stop_future, std::chrono::milliseconds(100)); }
};

class TestPipeline final : public Runtime::Execution::ExecutablePipelineStage
{
public:
    explicit TestPipeline(const std::shared_ptr<TestPipelineController>& controller) : controller(controller) { }
    uint32_t setup(Runtime::Execution::PipelineExecutionContext&) override
    {
        controller->setup.set_value();
        return 0;
    }
    uint32_t stop(Runtime::Execution::PipelineExecutionContext&) override
    {
        controller->stop.set_value();
        return 0;
    }

    void
    execute(const Memory::TupleBuffer& inputTupleBuffer, Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override
    {
        controller->invocations++;
        pipelineExecutionContext.emitBuffer(inputTupleBuffer, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }

private:
    std::shared_ptr<TestPipelineController> controller;
};


struct TestSinkController
{
    testing::AssertionResult waitForNumberOfReceivedBuffers(size_t numberOfExpectedBuffers);

    void insertBuffer(Memory::TupleBuffer&& buffer);

    std::vector<Memory::TupleBuffer> takeBuffers();

    testing::AssertionResult waitForInitialization(std::chrono::milliseconds timeout = std::chrono::milliseconds(100)) const
    {
        return waitForFuture(setup_future, timeout);
    }
    testing::AssertionResult waitForDestruction(std::chrono::milliseconds timeout = std::chrono::milliseconds(100)) const
    {
        return waitForFuture(destroyed_future, timeout);
    }
    testing::AssertionResult waitForShutdown(std::chrono::milliseconds timeout = std::chrono::milliseconds(100)) const
    {
        return waitForFuture(shutdown_future, timeout);
    }

private:
    folly::Synchronized<std::vector<Memory::TupleBuffer>, std::mutex> receivedBuffers;
    std::condition_variable receivedBufferTrigger;
    std::promise<void> setup;
    std::promise<void> shutdown;
    std::promise<void> destroyed;
    std::shared_future<void> setup_future = setup.get_future().share();
    std::shared_future<void> shutdown_future = shutdown.get_future().share();
    std::shared_future<void> destroyed_future = destroyed.get_future().share();
    friend class TestSink;
};

class TestSink final : public Runtime::Execution::ExecutablePipelineStage
{
public:
    uint32_t setup(Runtime::Execution::PipelineExecutionContext&) override
    {
        controller->setup.set_value();
        return 0;
    }
    void execute(const Memory::TupleBuffer& inputBuffer, Runtime::Execution::PipelineExecutionContext&) override
    {
        controller->insertBuffer(copyBuffer(inputBuffer, *bufferProvider));
    }

    uint32_t stop(Runtime::Execution::PipelineExecutionContext&) override
    {
        controller->shutdown.set_value();
        return 0;
    }

    TestSink(std::shared_ptr<Memory::AbstractBufferProvider> buffer_provider, std::shared_ptr<TestSinkController> controller)
        : bufferProvider(std::move(buffer_provider)), controller(std::move(controller))
    {
    }

    ~TestSink() override { controller->destroyed.set_value(); }
    TestSink(const TestSink& other) = delete;
    TestSink(TestSink&& other) noexcept = delete;
    TestSink& operator=(const TestSink& other) = delete;
    TestSink& operator=(TestSink&& other) noexcept = delete;

private:
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    std::shared_ptr<TestSinkController> controller;
};

std::tuple<std::shared_ptr<Runtime::Execution::ExecutablePipeline>, std::shared_ptr<TestSinkController>>
createSinkPipeline(std::shared_ptr<Memory::AbstractBufferProvider> bm);

std::tuple<std::shared_ptr<Runtime::Execution::ExecutablePipeline>, std::shared_ptr<TestPipelineController>>
createPipeline(const std::vector<std::shared_ptr<Runtime::Execution::ExecutablePipeline>>& successors);


template <typename... Ts>
struct overloaded : Ts...
{
    using Ts::operator()...;
};
struct QueryPlanBuilder
{
    using identifier_t = size_t;
    struct SourceDescriptor
    {
        OriginId sourceId = INVALID<OriginId>;
    };
    struct PipelineDescriptor
    {
        PipelineId pipelineId = INVALID<PipelineId>;
    };
    struct SinkDescriptor
    {
    };
    using ObjectDescriptor = std::variant<SourceDescriptor, SinkDescriptor, PipelineDescriptor>;

    identifier_t addPipeline(std::vector<identifier_t> predecssors);

    identifier_t addSource();

    identifier_t addSink(std::vector<identifier_t> predecessors);

    struct TestPlanCtrl
    {
        std::unique_ptr<Runtime::InstantiatedQueryPlan> query;
        std::unordered_map<identifier_t, OriginId> sourceIds;
        std::unordered_map<identifier_t, PipelineId> pipelineIds;

        std::unordered_map<identifier_t, std::shared_ptr<Sources::TestSourceControl>> sourceCtrls;
        std::unordered_map<identifier_t, std::shared_ptr<TestSinkController>> sinkCtrls;
        std::unordered_map<identifier_t, std::shared_ptr<TestPipelineController>> pipelineCtrls;
        std::unordered_map<identifier_t, std::shared_ptr<TestPipelineOperatorHandlerController>> pipelineOperatorHandlerCtrls;
        std::unordered_map<identifier_t, Runtime::Execution::ExecutablePipelineStage*> stages;
    };

    TestPlanCtrl build(std::shared_ptr<Memory::BufferManager> bm) &&;

    QueryPlanBuilder(identifier_t next_identifier, PipelineId::Underlying pipeline_id_counter, OriginId::Underlying origin_id_counter)
        : nextIdentifier(next_identifier), pipelineIdCounter(pipeline_id_counter), originIdCounter(origin_id_counter)
    {
    }

    identifier_t nextIdentifier;
    PipelineId::Underlying pipelineIdCounter = PipelineId::INITIAL;
    OriginId::Underlying originIdCounter = OriginId::INITIAL;

private:
    std::unordered_map<identifier_t, std::vector<identifier_t>> forwardRelations{};
    std::unordered_map<identifier_t, std::vector<identifier_t>> backwardRelations{};
    std::unordered_map<identifier_t, ObjectDescriptor> objects{};
};

struct TestingHarness
{
    TestingHarness(size_t numberOfThreads = 2, size_t numberOfBuffers = 1000)
        : bm(Memory::BufferManager::create(8192, numberOfBuffers)), numberOfThreads(numberOfThreads) {};

    std::shared_ptr<Memory::BufferManager> bm = Memory::BufferManager::create();
    std::shared_ptr<QueryStatusListener> status = std::make_shared<QueryStatusListener>();
    std::unique_ptr<Runtime::QueryEngine> qm;
    size_t numberOfThreads;

    QueryId::Underlying queryIdCounter = INITIAL<QueryId>.getRawValue();
    QueryId::Underlying lastOriginIdCounter = INITIAL<OriginId>.getRawValue();
    QueryId::Underlying lastPipelineIdCounter = INITIAL<PipelineId>.getRawValue();

    QueryPlanBuilder::identifier_t lastIdentifier = 0;
    std::unordered_map<QueryPlanBuilder::identifier_t, Runtime::Execution::ExecutablePipelineStage*> stages;
    std::unordered_map<QueryPlanBuilder::identifier_t, std::shared_ptr<Sources::TestSourceControl>> sourceControls;
    std::unordered_map<QueryPlanBuilder::identifier_t, std::shared_ptr<TestSinkController>> sinkControls;
    std::unordered_map<QueryPlanBuilder::identifier_t, std::shared_ptr<TestPipelineController>> pipelineControls;
    std::unordered_map<QueryPlanBuilder::identifier_t, std::shared_ptr<TestPipelineOperatorHandlerController>> operatorHandlerControls;
    std::unordered_map<QueryId, std::unique_ptr<std::promise<void>>> queryTermination;
    std::unordered_map<QueryId, std::shared_future<void>> queryTerminationFutures;
    std::unordered_map<QueryId, std::unique_ptr<std::promise<void>>> queryRunning;
    std::unordered_map<QueryId, std::shared_future<void>> queryRunningFutures;
    std::unordered_map<std::shared_ptr<Sources::SourceDescriptor>, std::unique_ptr<Sources::SourceHandle>> unusedSources;

    std::unordered_map<QueryPlanBuilder::identifier_t, OriginId> sourceIds;
    std::unordered_map<QueryPlanBuilder::identifier_t, PipelineId> pipelineIds;

    QueryPlanBuilder buildNewQuery();
    std::unique_ptr<Runtime::InstantiatedQueryPlan> addNewQuery(QueryPlanBuilder&& builder);

    void expectQueryStatusEvents(QueryId id, std::initializer_list<Runtime::Execution::QueryStatus> states);

    void expectSourceTermination(QueryId id, QueryPlanBuilder::identifier_t source, Runtime::QueryTerminationType type);


    void start();

    void startQuery(QueryId id, std::unique_ptr<Runtime::InstantiatedQueryPlan> query) { qm->start(id, std::move(query)); }
    void stopQuery(QueryId id) { qm->stop(id); }

    void stop() { qm.reset(); }

    testing::AssertionResult waitForQepTermination(QueryId id, std::chrono::milliseconds timeout = std::chrono::milliseconds(1000))
    {
        return waitForFuture(queryTerminationFutures.at(id), timeout);
    }
    testing::AssertionResult waitForQepRunning(QueryId id, std::chrono::milliseconds timeout = std::chrono::milliseconds(1000))
    {
        return waitForFuture(queryRunningFutures.at(id), timeout);
    }
};

struct NeverFailPolicy
{
    std::optional<size_t> operator()() { return {}; }
};

template <size_t FailAfterNElements, size_t SourceToFail>
struct FailAfter
{
    size_t next = 0;
    std::optional<size_t> operator()()
    {
        if (next++ == FailAfterNElements)
        {
            return {SourceToFail};
        }
        return {};
    }
};

template <typename FailPolicy = NeverFailPolicy>
struct DataThread
{
    void operator()(const std::stop_token& stopToken)
    {
        size_t identifier = 0;
        std::mt19937 rng(seed);
        std::uniform_int_distribution<int> gen(0, sources.size() - 1); /// uniform, unbiased
        std::unordered_set<size_t> failedSources;
        std::unordered_set<size_t> stoppedSources;
        FailPolicy failPolicy{};

        while (!stopToken.stop_requested())
        {
            if (auto source = failPolicy())
            {
                ASSERT_TRUE(sources[*source]->injectError("Error"));
                failedSources.insert(*source);
            }
            else
            {
                auto sourceId = gen(rng);
                if (!failedSources.contains(sourceId) && !stoppedSources.contains(sourceId))
                {
                    if (!sources[sourceId]->injectData(identifiableData(identifier++), 23))
                    {
                        stoppedSources.insert(sourceId);
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }

        for (size_t sourceId = 0; sourceId < sources.size(); ++sourceId)
        {
            if (!failedSources.contains(sourceId) && !stoppedSources.contains(sourceId))
            {
                sources[sourceId]->injectEoS();
            }
        }
    }

    explicit DataThread(std::vector<std::shared_ptr<Sources::TestSourceControl>> sources) : sources(std::move(sources)) { }

private:
    std::size_t seed = 77382;
    std::vector<std::shared_ptr<Sources::TestSourceControl>> sources;
    size_t failAfterBuffers = 0;
};

template <typename FailurePolicy = NeverFailPolicy>
class DataGenerator
{
    std::jthread thread;

public:
    void start(std::vector<std::shared_ptr<Sources::TestSourceControl>> sources)
    {
        thread = std::jthread(DataThread<FailurePolicy>{std::move(sources)});
    }

    void stop() { thread = std::jthread(); }
};

}
