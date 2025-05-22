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
#include <initializer_list>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <random>
#include <stop_token>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <unistd.h>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Listeners/AbstractQueryStatusListener.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Util/Overloaded.hpp>
#include <folly/Synchronized.h>
#include <gmock/gmock.h>
#include <gtest/gtest-assertion-result.h>
#include <gtest/gtest.h>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <ExecutableQueryPlan.hpp>
#include <Interfaces.hpp>
#include <MemoryTestUtils.hpp>
#include <QueryEngine.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <RunningQueryPlan.hpp>
#include <Task.hpp>
#include <TestSource.hpp>

namespace NES::Testing
{
static constexpr size_t DEFAULT_BUFFER_SIZE = 8192;
static constexpr size_t NUMBER_OF_TUPLES_PER_BUFFER = 23;
static constexpr size_t NUMBER_OF_BUFFERS_PER_SOURCE = 300;
static constexpr size_t NUMBER_OF_THREADS = 2;
static constexpr size_t LARGE_NUMBER_OF_THREADS = 8;
constexpr std::chrono::milliseconds DEFAULT_AWAIT_TIMEOUT = std::chrono::milliseconds(1000);
constexpr std::chrono::milliseconds DEFAULT_LONG_AWAIT_TIMEOUT = std::chrono::milliseconds(10000);

/// Creates raw TupleBuffer data based on a recognizable pattern which can later be identified using `verifyIdentifier`.
std::vector<std::byte> identifiableData(size_t identifier);
bool verifyIdentifier(const Memory::TupleBuffer& buffer, size_t identifier);

/// Mock Implementation of the QueryEngineStatisticListener. This can be used to verify that certain
/// statistic events have been emitted during test execution.
class TestQueryStatisticListener : public NES::Runtime::QueryEngineStatisticListener
{
public:
    MOCK_METHOD(void, onEvent, (Runtime::Event), (override));
};

/// Mock implementation for the QueryStatusListener. This allows to verify query status events, e.g. `Running`, `Stopped`.
/// Note that the `Register` event will never be emitted by the `QueryEngine` as this is not handled by the `QueryEngine`
struct ExpectStats
{
    std::shared_ptr<TestQueryStatisticListener> listener;

#define STAT_TYPE(Name) \
    struct Name \
    { \
        size_t lower; \
        size_t upper; \
        Name(size_t lower, size_t upper) : lower(lower), upper(upper) \
        { \
        } \
        Name(size_t exact) : lower(exact), upper(exact) \
        { \
        } \
    }; \
\
    void apply(Name v) \
    { \
        EXPECT_CALL(*listener, onEvent(::testing::VariantWith<Runtime::Name>(::testing::_))).Times(::testing::Between(v.lower, v.upper)); \
    }
    STAT_TYPE(QueryStart);
    STAT_TYPE(QueryStop);
    STAT_TYPE(PipelineStart);
    STAT_TYPE(PipelineStop);
    STAT_TYPE(TaskExecutionStart);
    STAT_TYPE(TaskExecutionComplete);
    STAT_TYPE(TaskExpired);
    STAT_TYPE(TaskEmit);

    explicit ExpectStats(std::shared_ptr<TestQueryStatisticListener> listener) : listener(std::move(listener))
    {
        EXPECT_CALL(*this->listener, onEvent(::testing::VariantWith<Runtime::QueryStart>(::testing::_)))
            .WillRepeatedly(::testing::Invoke([](auto) { }));
        EXPECT_CALL(*this->listener, onEvent(::testing::VariantWith<Runtime::QueryStop>(::testing::_)))
            .WillRepeatedly(::testing::Invoke([](auto) { }));
        EXPECT_CALL(*this->listener, onEvent(::testing::VariantWith<Runtime::PipelineStart>(::testing::_)))
            .WillRepeatedly(::testing::Invoke([](auto) { }));
        EXPECT_CALL(*this->listener, onEvent(::testing::VariantWith<Runtime::PipelineStop>(::testing::_)))
            .WillRepeatedly(::testing::Invoke([](auto) { }));
        EXPECT_CALL(*this->listener, onEvent(::testing::VariantWith<Runtime::TaskExecutionStart>(::testing::_)))
            .WillRepeatedly(::testing::Invoke([](auto) { }));
        EXPECT_CALL(*this->listener, onEvent(::testing::VariantWith<Runtime::TaskExecutionComplete>(::testing::_)))
            .WillRepeatedly(::testing::Invoke([](auto) { }));
        EXPECT_CALL(*this->listener, onEvent(::testing::VariantWith<Runtime::TaskExpired>(::testing::_)))
            .WillRepeatedly(::testing::Invoke([](auto) { }));
        EXPECT_CALL(*this->listener, onEvent(::testing::VariantWith<Runtime::TaskEmit>(::testing::_)))
            .WillRepeatedly(::testing::Invoke([](auto) { }));
    }

    template <typename... Args>
    void expect(Args... args)
    {
        (apply(args), ...);
    }
};

/// Mock implementation for internal interfaces of the QueryEngine. These are used when verifying the behavior of internal
/// components for the QueryEngine like the RunningQueryPlan.
class QueryStatusListener final : public AbstractQueryStatusListener
{
public:
    MOCK_METHOD(
        bool, logSourceTermination, (QueryId, OriginId, Runtime::QueryTerminationType, std::chrono::system_clock::time_point), (override));
    MOCK_METHOD(bool, logQueryFailure, (QueryId, Exception, std::chrono::system_clock::time_point), (override));
    MOCK_METHOD(bool, logQueryStatusChange, (QueryId, Runtime::Execution::QueryStatus, std::chrono::system_clock::time_point), (override));
};

/// Mock implementation for internal interfaces of the QueryEngine. These are used when verifying the behavior of internal
/// components for the QueryEngine like the RunningQueryPlan.
struct TestWorkEmitter : Runtime::WorkEmitter
{
    MOCK_METHOD(
        bool,
        emitWork,
        (QueryId,
         const std::shared_ptr<Runtime::RunningQueryPlanNode>&,
         Memory::TupleBuffer,
         Runtime::BaseTask::onComplete,
         Runtime::BaseTask::onFailure,
         Runtime::Execution::PipelineExecutionContext::ContinuationPolicy),
        (override));
    MOCK_METHOD(
        void,
        emitPipelineStart,
        (QueryId, const std::shared_ptr<Runtime::RunningQueryPlanNode>&, Runtime::BaseTask::onComplete, Runtime::BaseTask::onFailure),
        (override));
    MOCK_METHOD(
        void,
        emitPendingPipelineStop,
        (QueryId, std::shared_ptr<Runtime::RunningQueryPlanNode>, Runtime::BaseTask::onComplete, Runtime::BaseTask::onFailure),
        (override));
    MOCK_METHOD(
        void,
        emitPipelineStop,
        (QueryId, std::unique_ptr<Runtime::RunningQueryPlanNode>, Runtime::BaseTask::onComplete, Runtime::BaseTask::onFailure),
        (override));
};

struct TestQueryLifetimeController : Runtime::QueryLifetimeController
{
    MOCK_METHOD(void, initializeSourceFailure, (QueryId, OriginId, std::weak_ptr<Runtime::RunningSource>, Exception), (override));
    MOCK_METHOD(void, initializeSourceStop, (QueryId, OriginId, std::weak_ptr<Runtime::RunningSource>), (override));
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

/// Controller for the TestPipelines used in the QueryEngine Tests.
/// The controller can verify that the pipeline was started and stopped as well as individual invocations.
/// The controller can be constructed to inject errors.
class TestPipelineController
{
public:
    std::atomic_size_t invocations;
    std::atomic<std::chrono::milliseconds> startDuration = std::chrono::milliseconds(0);
    std::atomic<std::chrono::milliseconds> stopDuration = std::chrono::milliseconds(0);
    std::atomic_bool failOnStart = false;
    std::atomic_bool failOnStop = false;
    std::atomic<size_t> throwOnNthInvocation = -1;

    std::promise<void> start;
    std::promise<void> stop;
    std::shared_future<void> startFuture = start.get_future().share();
    std::shared_future<void> stopFuture = stop.get_future().share();

    /// Back reference this is set during construction of a TestPipeline
    Runtime::Execution::ExecutablePipelineStage* stage = nullptr;

    [[nodiscard]] testing::AssertionResult waitForStart() const { return waitForFuture(startFuture, DEFAULT_LONG_AWAIT_TIMEOUT); }

    [[nodiscard]] testing::AssertionResult waitForStop() const { return waitForFuture(stopFuture, DEFAULT_LONG_AWAIT_TIMEOUT); }

    [[nodiscard]] testing::AssertionResult keepRunning() const
    {
        return waitForFuture(stopFuture, DEFAULT_AWAIT_TIMEOUT) ? testing::AssertionFailure() : testing::AssertionSuccess();
    }

    [[nodiscard]] testing::AssertionResult wasStarted() const { return waitForFuture(startFuture, std::chrono::milliseconds(0)); }

    [[nodiscard]] testing::AssertionResult wasStopped() const { return waitForFuture(stopFuture, std::chrono::milliseconds(0)); }
};

struct TestPipeline final : Runtime::Execution::ExecutablePipelineStage
{
    explicit TestPipeline(std::shared_ptr<TestPipelineController> controller) : controller(std::move(controller))
    {
        this->controller->stage = this;
    }

    ~TestPipeline() override { controller->stage = nullptr; }

    void start(Runtime::Execution::PipelineExecutionContext&) override
    {
        std::this_thread::sleep_for(controller->startDuration.load());
        controller->start.set_value();
        if (controller->failOnStart)
        {
            throw Exception("I should throw here.", 9999);
        }
    }

    void stop(Runtime::Execution::PipelineExecutionContext&) override
    {
        std::this_thread::sleep_for(controller->stopDuration.load());
        controller->stop.set_value();
        if (controller->failOnStop)
        {
            throw Exception("I should throw here.", 9999);
        }
    }

    void
    execute(const Memory::TupleBuffer& inputTupleBuffer, Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override
    {
        if (controller->invocations.fetch_add(1) + 1 == controller->throwOnNthInvocation)
        {
            throw Exception("I should throw here.", 9999);
        }
        pipelineExecutionContext.emitBuffer(inputTupleBuffer, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }

    std::shared_ptr<TestPipelineController> controller;

protected:
    std::ostream& toString(std::ostream& os) const override;
};

struct TestSinkController
{
    /// Waits for *at least* `numberOfExpectedBuffers`
    testing::AssertionResult waitForNumberOfReceivedBuffersOrMore(size_t numberOfExpectedBuffers);

    void insertBuffer(Memory::TupleBuffer&& buffer);

    std::vector<Memory::TupleBuffer> takeBuffers();

    testing::AssertionResult waitForInitialization(std::chrono::milliseconds timeout) const { return waitForFuture(setup_future, timeout); }

    testing::AssertionResult waitForDestruction(std::chrono::milliseconds timeout) const
    {
        return waitForFuture(destroyed_future, timeout);
    }

    testing::AssertionResult waitForShutdown(std::chrono::milliseconds timeout) const { return waitForFuture(shutdown_future, timeout); }

    std::atomic<size_t> invocations = 0;

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
    void start(Runtime::Execution::PipelineExecutionContext&) override { controller->setup.set_value(); }

    void execute(const Memory::TupleBuffer& inputBuffer, Runtime::Execution::PipelineExecutionContext&) override
    {
        controller->insertBuffer(copyBuffer(inputBuffer, *bufferProvider));
    }

    void stop(Runtime::Execution::PipelineExecutionContext&) override { controller->shutdown.set_value(); }

    TestSink(std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider, std::shared_ptr<TestSinkController> controller)
        : bufferProvider(std::move(bufferProvider)), controller(std::move(controller))
    {
    }

    ~TestSink() override { controller->destroyed.set_value(); }

    TestSink(const TestSink& other) = delete;
    TestSink(TestSink&& other) noexcept = delete;
    TestSink& operator=(const TestSink& other) = delete;
    TestSink& operator=(TestSink&& other) noexcept = delete;

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    std::shared_ptr<TestSinkController> controller;
};

std::tuple<std::shared_ptr<Runtime::Execution::ExecutablePipeline>, std::shared_ptr<TestSinkController>>
createSinkPipeline(PipelineId id, std::shared_ptr<Memory::AbstractBufferProvider> bm);

std::tuple<std::shared_ptr<Runtime::Execution::ExecutablePipeline>, std::shared_ptr<TestPipelineController>>
createPipeline(PipelineId id, const std::vector<std::shared_ptr<Runtime::Execution::ExecutablePipeline>>& successors);

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
        PipelineId pipelineId = INVALID<PipelineId>;
    };

    using QueryComponentDescriptor = std::variant<SourceDescriptor, SinkDescriptor, PipelineDescriptor>;

    identifier_t addPipeline(const std::vector<identifier_t>& predecssors);

    identifier_t addSource();

    identifier_t addSink(const std::vector<identifier_t>& predecessors);

    struct TestPlanCtrl
    {
        std::unique_ptr<Runtime::ExecutableQueryPlan> query;
        std::unordered_map<identifier_t, OriginId> sourceIds;
        std::unordered_map<identifier_t, PipelineId> pipelineIds;

        std::unordered_map<identifier_t, std::shared_ptr<Sources::TestSourceControl>> sourceCtrls;
        std::unordered_map<identifier_t, std::shared_ptr<TestSinkController>> sinkCtrls;
        std::unordered_map<identifier_t, std::shared_ptr<TestPipelineController>> pipelineCtrls;
        std::unordered_map<identifier_t, Runtime::Execution::ExecutablePipelineStage*> stages;
    };

    TestPlanCtrl build(QueryId queryId, std::shared_ptr<Memory::BufferManager> bm) &&;

    QueryPlanBuilder(identifier_t nextIdentifier, PipelineId::Underlying pipelineIdCounter, OriginId::Underlying originIdCounter);

    identifier_t nextIdentifier;
    PipelineId::Underlying pipelineIdCounter = PipelineId::INITIAL;
    OriginId::Underlying originIdCounter = OriginId::INITIAL;

private:
    std::unordered_map<identifier_t, std::vector<identifier_t>> forwardRelations;
    std::unordered_map<identifier_t, std::vector<identifier_t>> backwardRelations;
    std::unordered_map<identifier_t, QueryComponentDescriptor> objects;
};

struct TestingHarness
{
    explicit TestingHarness(size_t numberOfThreads, size_t numberOfBuffers);
    explicit TestingHarness();

    std::shared_ptr<Memory::BufferManager> bm = Memory::BufferManager::create();
    std::shared_ptr<TestQueryStatisticListener> statListener = std::make_shared<TestQueryStatisticListener>();
    ExpectStats stats{statListener};
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
    std::unordered_map<QueryId, std::unique_ptr<std::promise<void>>> queryTermination;
    std::unordered_map<QueryId, std::shared_future<void>> queryTerminationFutures;
    std::unordered_map<QueryId, std::unique_ptr<std::promise<void>>> queryRunning;
    std::unordered_map<QueryId, std::shared_future<void>> queryRunningFutures;
    std::unordered_map<std::shared_ptr<Sources::SourceDescriptor>, std::unique_ptr<Sources::SourceHandle>> unusedSources;

    std::unordered_map<QueryPlanBuilder::identifier_t, OriginId> sourceIds;
    std::unordered_map<QueryPlanBuilder::identifier_t, PipelineId> pipelineIds;

    /// Constructs a new builder to create a new query. Once building the query plan is done it is submitted to the `addNewQuery` method.
    /// The Identifier returned by add{Source,Sink,Pipeline} are used to index into the corresponding controller maps.
    /// ```c++
    ///   TestingHarness test;
    ///   auto builder = test.buildNewQuery();
    ///   auto source = builder.addSource();
    ///   auto sourceCtrl = test.sourceControls[source]; /// get controller for source
    ///   auto sink = builder.addSink({source});
    ///   auto query = test.addNewQuery(std::move(builder));
    /// ```
    QueryPlanBuilder buildNewQuery() const;
    std::unique_ptr<Runtime::ExecutableQueryPlan> addNewQuery(QueryPlanBuilder&& builder);

    /// List of status events to be emitted by a query with QueryId `id`
    void expectQueryStatusEvents(QueryId id, std::initializer_list<Runtime::Execution::QueryStatus> states);

    /// Expects a source for a given query to be terminated (gracefully or due to a failure)
    void expectSourceTermination(QueryId id, QueryPlanBuilder::identifier_t source, Runtime::QueryTerminationType type);


    /// Starts the query engine and initializes internal futures used to track query termination events.
    /// All expected query runtime events should be declared beforehand
    /// ```c++
    ///  test.expectQueryStatusEvents(QueryId(1), {Runtime::Execution::QueryStatus::Running});
    ///  test.start();
    /// ```
    void start();

    /// Inserts a new Query into the Query Engine. Requires `start` to be called beforehand.
    void startQuery(std::unique_ptr<Runtime::ExecutableQueryPlan> query) const;
    /// Stops a Query. Requires `start` to be called beforehand.
    void stopQuery(QueryId id) const;

    /// Shuts the query engine down by calling its destructor
    void stop();

    testing::AssertionResult waitForQepTermination(QueryId id, std::chrono::milliseconds timeout) const;
    testing::AssertionResult waitForQepRunning(QueryId id, std::chrono::milliseconds timeout);
};

/// Data Generator used within the QueryEngineTest
/// Data Generator inserts data into the test queues which would be cumbersome to do manually. The DataGenerator runs on a different
/// thread and injects EndOfStream into all sources once stopped.
/// Different Policies can be used to inject failures into specific sources.
struct NeverFailPolicy
{
    std::optional<size_t> operator()() const { return std::nullopt; }
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
    constexpr static auto DEFAULT_DATA_GENERATOR_INTERVAL = std::chrono::milliseconds(10);
    constexpr static size_t SEED = 0xDEADBEEF;

    void operator()(const std::stop_token& stopToken)
    {
        size_t identifier = 0;
        std::mt19937 rng(SEED);
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
                    if (!sources[sourceId]->injectData(identifiableData(identifier++), NUMBER_OF_TUPLES_PER_BUFFER))
                    {
                        stoppedSources.insert(sourceId);
                    }
                }
                std::this_thread::sleep_for(DEFAULT_DATA_GENERATOR_INTERVAL);
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
