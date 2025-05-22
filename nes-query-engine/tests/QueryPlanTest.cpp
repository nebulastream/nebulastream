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

#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <ranges>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <Interfaces.hpp>
#include <PipelineExecutionContext.hpp>
#include <QueryEngineTestingInfrastructure.hpp>
#include <RunningQueryPlan.hpp>
#include <RunningSource.hpp>
#include <Task.hpp>

using namespace std::chrono_literals;
namespace stdv = std::ranges::views;

namespace NES::Testing
{
using namespace NES::Runtime;

class QueryPlanTest : public BaseUnitTest
{
public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestSuite()
    {
        NES::Logger::setupLogging("QueryPlanTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup QueryPlanTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

class UniquePtrStageMatcher
{
public:
    using is_gtest_matcher = void;

    explicit UniquePtrStageMatcher(Execution::ExecutablePipelineStage* stage) : stage(stage) { }

    bool MatchAndExplain(const std::unique_ptr<RunningQueryPlanNode>& foo, std::ostream* /* listener */) const
    {
        return foo->stage.get() == stage;
    }

    void DescribeTo(std::ostream* os) const { *os << "RunningQueryPlanNode Stage equals " << stage; }

    void DescribeNegationTo(std::ostream* os) const { *os << "RunningQueryPlanNode does not equal " << stage; }

private:
    Execution::ExecutablePipelineStage* stage;
};

class StageMatcher
{
public:
    using is_gtest_matcher = void;

    explicit StageMatcher(Execution::ExecutablePipelineStage* stage) : stage(stage) { }

    bool MatchAndExplain(const std::shared_ptr<RunningQueryPlanNode>& foo, std::ostream* /* listener */) const
    {
        return foo->stage.get() == stage;
    }

    bool MatchAndExplain(const std::weak_ptr<RunningQueryPlanNode>& foo, std::ostream* /* listener */) const
    {
        return !foo.expired() && foo.lock()->stage.get() == stage;
    }

    void DescribeTo(std::ostream* os) const { *os << "RunningQueryPlanNode Stage equals " << stage; }

    void DescribeNegationTo(std::ostream* os) const { *os << "RunningQueryPlanNode does not equal " << stage; }

private:
    Execution::ExecutablePipelineStage* stage;
};

class DataSourceMatcher
{
public:
    using is_gtest_matcher = void;

    explicit DataSourceMatcher(OriginId id) : source(id) { }

    bool matchAndExplain(const std::weak_ptr<RunningSource>& foo, std::ostream* /* listener */) const
    {
        if (auto realSource = foo.lock())
        {
            return realSource->getOriginId() == source;
        }
        return false;
    }

    void describeTo(std::ostream* os) const { *os << "RunningSource equals " << source; }

    void describeNegationTo(std::ostream* os) const { *os << "RunningSource does not equal " << source; }

private:
    OriginId source;
};

template <typename R, typename T>
concept RangeOf = std::ranges::range<R> && std::same_as<std::ranges::range_value_t<R>, T>;

struct TestPipelineExecutionContext : Execution::PipelineExecutionContext
{
    MOCK_METHOD(WorkerThreadId, getId, (), (const, override));
    MOCK_METHOD(Memory::TupleBuffer, allocateTupleBuffer, (), (override));
    MOCK_METHOD(uint64_t, getNumberOfWorkerThreads, (), (const, override));
    MOCK_METHOD(std::shared_ptr<Memory::AbstractBufferProvider>, getBufferManager, (), (const, override));
    MOCK_METHOD(PipelineId, getPipelineId, (), (const, override));
    MOCK_METHOD(std::vector<std::shared_ptr<Execution::OperatorHandler>>&, getOperatorHandlers, (), (override));
    MOCK_METHOD(void, setOperatorHandlers, (std::vector<std::shared_ptr<Execution::OperatorHandler>>&), (override));
    MOCK_METHOD(bool, emitBuffer, (const Memory::TupleBuffer&, ContinuationPolicy), (override));
};

struct TerminatePipelineArgs
{
    std::unique_ptr<RunningQueryPlanNode> target;
    BaseTask::onComplete onComplete;
    BaseTask::onFailure onFailure;
};

template <typename Args, typename KeyT>
struct EmittedTask
{
    mutable std::mutex mutex;
    std::unordered_map<KeyT, Args> arguments;
    std::condition_variable addCondition;

    template <typename... TArgs>
    static std::unique_ptr<EmittedTask> setup(RangeOf<KeyT> auto stages, TArgs&&...);

    ::testing::AssertionResult executeEmittedTask(Args&&);

    bool empty() const
    {
        const std::unique_lock lock(mutex);
        return arguments.empty();
    }

    size_t size() const
    {
        const std::unique_lock lock(mutex);
        return arguments.size();
    }

    ::testing::AssertionResult handleAll()
    {
        std::unordered_map<KeyT, Args> copy;
        {
            const std::unique_lock lock(mutex);
            copy = std::move(arguments);
        }

        for (auto it = copy.begin(); it != copy.end() /* not hoisted */; /* no increment */)
        {
            auto result = executeEmittedTask(std::move(*it).second);
            if (!result)
            {
                return result;
            }
            it = copy.erase(it);
        }
        return ::testing::AssertionSuccess();
    }

    ::testing::AssertionResult waitForTasks(size_t numberOfTasks)
    {
        std::unique_lock lock(mutex);
        if (arguments.size() < numberOfTasks)
        {
            auto result
                = addCondition.wait_for(lock, DEFAULT_AWAIT_TIMEOUT, [this, numberOfTasks]() { return arguments.size() >= numberOfTasks; });
            if (!result)
            {
                return testing::AssertionFailure() << "Timeout waiting for " << numberOfTasks << " terminations.";
            }
        }

        return ::testing::AssertionSuccess();
    }

    ::testing::AssertionResult handle(KeyT stage)
    {
        Args task;
        {
            std::unique_lock lock(mutex);
            if (!arguments.contains(stage))
            {
                auto result = addCondition.wait_for(lock, DEFAULT_AWAIT_TIMEOUT, [this, stage]() { return arguments.contains(stage); });
                if (!result)
                {
                    return testing::AssertionFailure() << "Timeout waiting for " << stage << " terminations.";
                }
            }
            task = std::move(arguments[stage]);
            arguments.erase(stage);
        }

        return executeEmittedTask(std::move(task));
    }

private:
    template <typename... Arguments>
    requires(std::constructible_from<Args, Arguments...>)
    void add(KeyT key, Arguments&&... args)
    {
        {
            const std::scoped_lock lock(mutex);
            arguments.try_emplace(key, std::forward<Arguments>(args)...);
        }
        addCondition.notify_all();
    }
};

using Terminations = EmittedTask<TerminatePipelineArgs, Execution::ExecutablePipelineStage*>;

template <>
template <typename... TArgs>
std::unique_ptr<Terminations> Terminations::setup(RangeOf<Execution::ExecutablePipelineStage*> auto stages, TArgs&&... args)
{
    auto& emitter = std::get<0>(std::forward_as_tuple<TArgs>(args)...);
    auto terminations = std::make_unique<Terminations>();
    for (auto* stage : stages)
    {
        EXPECT_CALL(emitter, emitPipelineStop(::testing::_, UniquePtrStageMatcher(stage), ::testing::_, ::testing::_))
            .WillOnce(::testing::Invoke([&terminations, stage](auto, auto termination, auto complete, auto fail)
                                        { terminations->add(stage, std::move(termination), std::move(complete), std::move(fail)); }));
    }

    return terminations;
}

template <>
::testing::AssertionResult Terminations::executeEmittedTask(TerminatePipelineArgs&& args)
{
    TestPipelineExecutionContext pec{};
    EXPECT_CALL(pec, emitBuffer(::testing::_, ::testing::_)).Times(0);
    try
    {
        args.target->stage->stop(pec);
        args.onComplete();
    }
    catch (const Exception& e)
    {
        args.onFailure(e);
    }

    return ::testing::AssertionSuccess();
}

struct SetupPipelineArgs
{
    std::weak_ptr<RunningQueryPlanNode> target;
    BaseTask::onComplete onComplete;
    BaseTask::onFailure onFailure;
};

using Setups = EmittedTask<SetupPipelineArgs, Execution::ExecutablePipelineStage*>;

template <>
template <typename... TArgs>
std::unique_ptr<Setups> Setups::setup(RangeOf<Execution::ExecutablePipelineStage*> auto stages, TArgs&&... args)
{
    auto setups = std::make_unique<Setups>();
    auto& emitter = std::get<0>(std::forward_as_tuple<TArgs>(args)...);
    for (auto* stage : stages)
    {
        EXPECT_CALL(emitter, emitPipelineStart(::testing::_, StageMatcher(stage), ::testing::_, ::testing::_))
            .WillOnce(::testing::Invoke([&setups, stage](auto, auto setup, auto complete, auto fail)
                                        { setups->add(stage, std::move(setup), std::move(complete), std::move(fail)); }));
    }

    return setups;
}

template <>
::testing::AssertionResult Setups::executeEmittedTask(SetupPipelineArgs&& args)
{
    TestPipelineExecutionContext pec{};
    EXPECT_CALL(pec, emitBuffer(::testing::_, ::testing::_)).Times(0);
    try
    {
        if (auto strongRef = args.target.lock())
        {
            strongRef->stage->start(pec);
            args.onComplete();
        }
    }
    catch (const Exception& e)
    {
        args.onFailure(e);
    }

    return ::testing::AssertionSuccess();
}

struct SourceStopArgs
{
    std::weak_ptr<RunningSource> target;
};

using SourceStops = EmittedTask<SourceStopArgs, OriginId>;

template <>
template <typename... TArgs>
std::unique_ptr<SourceStops> SourceStops::setup(RangeOf<OriginId> auto originIds, TArgs&&... args)
{
    auto setups = std::make_unique<SourceStops>();
    auto& controller = std::get<0>(std::forward_as_tuple<TArgs>(args)...);
    for (auto originId : originIds)
    {
        EXPECT_CALL(controller, initializeSourceStop(::testing::_, ::testing::_, ::testing::_))
            .WillOnce(
                ::testing::Invoke([&setups, originId](auto, auto, auto sourceStop) { setups->add(originId, std::move(sourceStop)); }));
    }

    return setups;
}

template <>
::testing::AssertionResult SourceStops::executeEmittedTask(SourceStopArgs&& stops)
{
    if (auto target = stops.target.lock())
    {
        while (!target->attemptUnregister())
        {
        };
    }
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult destroyPipeline(std::shared_ptr<RunningQueryPlanNode>&& node)
{
    auto toBeDestroyed = std::move(node);
    if (toBeDestroyed.use_count() != 1)
    {
        return ::testing::AssertionFailure() << "Node cannot be destroyed it is referenced somewhere else";
    }
    return ::testing::AssertionSuccess();
}

struct TestQueryLifetimeListener : QueryLifetimeListener
{
    MOCK_METHOD(void, onRunning, (), (override));
    MOCK_METHOD(void, onFailure, (Exception), (override));
    MOCK_METHOD(void, onDestruction, (), (override));
};

template <typename T>
auto dropRef(std::pair<T, CallbackRef> pair)
{
    return std::move(pair.first);
}

void leak(std::unique_ptr<RunningQueryPlan> runningQueryPlan, std::shared_ptr<TestQueryLifetimeListener>&& listener)
{
    ::testing::Mock::VerifyAndClearExpectations(listener.get());
    ::testing::Mock::AllowLeak(listener.get());
    runningQueryPlan.release();
}

TEST_F(QueryPlanTest, RunningQueryNodeSetup)
{
    /// Setup Callbacks that trigger once all pipelines have been initilaized and once all pipelines have been destroyed
    ::testing::MockFunction<void()> onRunningCallback;
    ::testing::MockFunction<void()> onExpirationCallback;
    EXPECT_CALL(onRunningCallback, Call()).Times(1);
    EXPECT_CALL(onExpirationCallback, Call()).Times(1);
    auto [expirationOwner, expirationRef] = Callback::create("Expiration");
    auto [setupOwner, setupRef] = Callback::create("Setup");
    setupOwner.addCallback(onRunningCallback.AsStdFunction());
    expirationOwner.addCallback(onExpirationCallback.AsStdFunction());

    auto pipeline1Ctrl = std::make_shared<TestPipelineController>();
    auto pipeline2Ctrl = std::make_shared<TestPipelineController>();
    auto stage1 = std::make_unique<TestPipeline>(pipeline1Ctrl);
    auto stage2 = std::make_unique<TestPipeline>(pipeline2Ctrl);

    /// Verify that a setup and stop tasks have been emitted
    TestWorkEmitter emitter;
    auto terminations = Terminations::setup(std::vector<Execution::ExecutablePipelineStage*>{stage1.get(), stage2.get()}, emitter);
    auto setups = Setups::setup(std::vector<Execution::ExecutablePipelineStage*>{stage1.get(), stage2.get()}, emitter);

    /// Build chain of two pipelines. Verify that on construction of a RunningQueryPlan node a setup task has been submitted
    auto sink
        = RunningQueryPlanNode::create(QueryId(1), PipelineId(1), emitter, {}, std::move(stage2), [](auto) { }, expirationRef, setupRef);
    EXPECT_THAT(*setups, testing::SizeIs(1));
    auto pipeline = RunningQueryPlanNode::create(
        QueryId(1),
        PipelineId(2),
        emitter,
        {std::move(sink)},
        std::move(stage1),
        [](auto) { },
        std::move(expirationRef),
        std::move(setupRef));
    EXPECT_THAT(*setups, testing::SizeIs(2));

    /// Run all setup tasks.
    EXPECT_TRUE(setups->handleAll());
    EXPECT_THAT(*terminations, testing::IsEmpty());

    /// Destroy root pipeline
    EXPECT_TRUE(destroyPipeline(std::move(pipeline)));
    EXPECT_TRUE(terminations->handle(pipeline1Ctrl->stage));
    EXPECT_TRUE(terminations->handle(pipeline2Ctrl->stage));
}

/// If a running query plan is dropped, it should perform a hard stop: e.g. no pipelines should be terminated.
/// The destruction listener should be notified
TEST_F(QueryPlanTest, RunningQueryPlanDefaultDestructor)
{
    Testing::TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline = builder.addPipeline({source});
    builder.addSink({pipeline});
    auto queryPlan = test.addNewQuery(std::move(builder));
    auto srcCtrl = test.sourceControls[source];

    TestQueryLifetimeController controller;
    TestWorkEmitter emitter;

    /// The RunningQueryPlan Requested Setup for all Pipelines in the QueryPlan.
    auto setups = Setups::setup(stdv::values(test.stages), emitter);
    auto listener = std::make_shared<TestQueryLifetimeListener>();
    EXPECT_CALL(*listener, onDestruction()).Times(1);
    EXPECT_CALL(*listener, onRunning()).Times(0);
    {
        auto runningQueryPlan = RunningQueryPlan::start(QueryId(0), std::move(queryPlan), controller, emitter, listener);
    }
    EXPECT_TRUE(srcCtrl->waitUntilDestroyed());
    EXPECT_FALSE(srcCtrl->wasOpened());
}

/// Disposing a RunningQueryPlan does not trigger the destruction listener and performs the hard stop.
TEST_F(QueryPlanTest, RunningQueryPlanDispose)
{
    Testing::TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline = builder.addPipeline({source});
    builder.addSink({pipeline});
    auto queryPlan = test.addNewQuery(std::move(builder));
    auto srcCtrl = test.sourceControls[source];

    TestQueryLifetimeController controller;
    TestWorkEmitter emitter;

    /// The RunningQueryPlan Requested Setup for all Pipelines in the QueryPlan.
    auto setups = Setups::setup(stdv::values(test.stages), emitter);
    auto listener = std::make_shared<TestQueryLifetimeListener>();
    EXPECT_CALL(*listener, onDestruction()).Times(0);
    EXPECT_CALL(*listener, onRunning()).Times(0);

    {
        auto runningQueryPlan = dropRef(RunningQueryPlan::start(QueryId(0), std::move(queryPlan), controller, emitter, listener));
        RunningQueryPlan::dispose(std::move(runningQueryPlan));
    }

    EXPECT_TRUE(srcCtrl->waitUntilDestroyed());
    EXPECT_FALSE(srcCtrl->wasOpened());
}

TEST_F(QueryPlanTest, RunningQueryPlanTestInitialPipelineSetup)
{
    Testing::TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline = builder.addPipeline({source});
    builder.addSink({pipeline});
    auto queryPlan = test.addNewQuery(std::move(builder));
    auto srcCtrl = test.sourceControls[source];

    TestQueryLifetimeController controller;
    TestWorkEmitter emitter;

    /// The RunningQueryPlan Requested Setup for all Pipelines in the QueryPlan.
    auto setups = Setups::setup(stdv::values(test.stages), emitter);

    auto listener = std::make_shared<TestQueryLifetimeListener>();
    EXPECT_CALL(*listener, onDestruction()).Times(1);
    EXPECT_CALL(*listener, onRunning()).Times(0);

    {
        auto runningQueryPlan = RunningQueryPlan::start(QueryId(0), std::move(queryPlan), controller, emitter, listener);
        setups->waitForTasks(2);
        EXPECT_FALSE(srcCtrl->waitUntilOpened());
    }

    EXPECT_TRUE(srcCtrl->waitUntilDestroyed());
    EXPECT_FALSE(srcCtrl->wasClosed());
}

/// Happy Path:
/// 1. Start the Query which should emit pipeline SetupTasks.
/// 2. AFTER we handle all SetupTasks. We expect the SourceThread to start
/// ------
/// Trigger Termination:
/// 1. We take the TerminationCallback of the RunningQueryPlan.
/// 2. The RunningQueryPlan is destroyed. Which destroys all sources.
///    Which drops the references to their successor pipelines.
/// 3. If the reference count for a RunningQueryPlanNode reaches 0 a custom deleter is called, which rescues the Node into a unique_ptr and
///    submits a PipelineTermination task. The Node still holds a reference to the Termination Callback, so it will not be triggered before
///    the termination task is done. The Node also still retains references to its successor, thus termination of successors will only be
///    submitted after the termination of the node has completed.
/// 4. Once all pipelines are destroyed the TerminationCallback will be invoked.
TEST_F(QueryPlanTest, RunningQueryPlanTestSourceSetup)
{
    Testing::TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline = builder.addPipeline({source});
    auto sink = builder.addSink({pipeline});
    auto queryPlan = test.addNewQuery(std::move(builder));
    auto srcCtrl = test.sourceControls[source];

    TestQueryLifetimeController controller;
    TestWorkEmitter emitter;

    /// The RunningQueryPlan Requested Setup for all Pipelines in the QueryPlan.
    auto setups = Setups::setup(stdv::values(test.stages), emitter);
    auto terminations = Terminations::setup(stdv::values(test.stages), emitter);

    auto listener = std::make_shared<TestQueryLifetimeListener>();
    EXPECT_CALL(*listener, onDestruction()).Times(1);
    EXPECT_CALL(*listener, onRunning()).Times(1);

    std::unique_ptr<StoppingQueryPlan> stopping;
    {
        auto runningQueryPlan = dropRef(RunningQueryPlan::start(QueryId(0), std::move(queryPlan), controller, emitter, listener));
        EXPECT_FALSE(srcCtrl->wasOpened());
        EXPECT_FALSE(srcCtrl->wasClosed());

        EXPECT_TRUE(setups->waitForTasks(2));
        EXPECT_TRUE(setups->handleAll());

        EXPECT_TRUE(srcCtrl->waitUntilOpened());
        EXPECT_FALSE(srcCtrl->waitUntilClosed());
        stopping = dropRef(RunningQueryPlan::stop(std::move(runningQueryPlan)));
    }

    EXPECT_TRUE(terminations->handle(test.stages.at(pipeline)));
    EXPECT_THAT(*terminations, ::testing::SizeIs(1));
    EXPECT_TRUE(terminations->handle(test.stages.at(sink)));

    EXPECT_TRUE(srcCtrl->waitUntilDestroyed());
    EXPECT_TRUE(srcCtrl->wasClosed());
}

TEST_F(QueryPlanTest, RunningQueryPlanTestPartialConstruction)
{
    Testing::TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline = builder.addPipeline({source});
    auto pipeline1 = builder.addPipeline({pipeline});
    builder.addSink({pipeline1});
    auto queryPlan = test.addNewQuery(std::move(builder));
    auto srcCtrl = test.sourceControls[source];

    TestQueryLifetimeController controller;
    TestWorkEmitter emitter;

    /// The RunningQueryPlan Requested Setup for all Pipelines in the QueryPlan.
    auto setups = Setups::setup(stdv::values(test.stages), emitter);
    auto terminations = Terminations::setup(std::vector{test.stages[pipeline1]}, emitter);

    auto listener = std::make_shared<TestQueryLifetimeListener>();
    EXPECT_CALL(*listener, onDestruction()).Times(1);
    /// Setup Never Completes
    EXPECT_CALL(*listener, onRunning()).Times(0);

    std::unique_ptr<StoppingQueryPlan> stopping;
    {
        auto runningQueryPlan = dropRef(RunningQueryPlan::start(QueryId(0), std::move(queryPlan), controller, emitter, listener));
        EXPECT_FALSE(srcCtrl->waitUntilOpened());

        EXPECT_TRUE(setups->waitForTasks(3));
        /// Only setup the pipeline1 pipeline
        EXPECT_TRUE(setups->handle(test.stages[pipeline1]));
        stopping = dropRef(RunningQueryPlan::stop(std::move(runningQueryPlan)));
    }

    EXPECT_TRUE(terminations->handle(test.stages[pipeline1]));
    EXPECT_THAT(*terminations, ::testing::IsEmpty());

    /// Late setup tasks will not initialize pipelines as they have been expired
    /// Calling the completion callback is safe
    EXPECT_TRUE(setups->handleAll());

    EXPECT_TRUE(srcCtrl->waitUntilDestroyed());
    EXPECT_FALSE(srcCtrl->wasOpened());
    EXPECT_FALSE(srcCtrl->wasClosed());
}

TEST_F(QueryPlanTest, RefCountTestSourceEoS)
{
    Testing::TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline = builder.addPipeline({source});
    auto sink = builder.addSink({pipeline});
    auto queryPlan = test.addNewQuery(std::move(builder));

    /// Verify running query plan terminates
    auto listener = std::make_shared<TestQueryLifetimeListener>();
    EXPECT_CALL(*listener, onDestruction()).Times(1);
    EXPECT_CALL(*listener, onRunning()).Times(1);

    TestQueryLifetimeController controller;
    TestWorkEmitter emitter;

    auto sourceStops = SourceStops::setup(std::vector{test.sourceIds.at(source)}, controller);

    /// The RunningQueryPlan Requested Setup for all Pipelines in the QueryPlan.
    auto setups = Setups::setup(stdv::values(test.stages), emitter);
    auto terminations = Terminations::setup(stdv::values(test.stages), emitter);

    {
        auto runningQueryPlan = dropRef(RunningQueryPlan::start(QueryId(0), std::move(queryPlan), controller, emitter, listener));

        EXPECT_TRUE(setups->handleAll());
        EXPECT_TRUE(test.sourceControls[source]->waitUntilOpened());
        EXPECT_FALSE(test.sourceControls[source]->waitUntilClosed());

        test.sourceControls[source]->injectEoS();
        EXPECT_TRUE(sourceStops->handle(test.sourceIds.at(source)));
        EXPECT_TRUE(test.sourceControls[source]->waitUntilDestroyed());
        EXPECT_TRUE(test.sourceControls[source]->wasOpened());
        EXPECT_TRUE(test.sourceControls[source]->wasClosed());

        EXPECT_THAT(*terminations, ::testing::SizeIs(1));
        EXPECT_TRUE(terminations->handle(test.stages[pipeline]));
        EXPECT_THAT(*terminations, ::testing::SizeIs(1));
        EXPECT_TRUE(terminations->handle(test.stages[sink]));
        /// NOLINTNEXTLINE
        runningQueryPlan.release();
    }
}

TEST_F(QueryPlanTest, RefCountTestMultipleSourceOneOfThemEoS)
{
    Testing::TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto source1 = builder.addSource();
    auto pipeline = builder.addPipeline({source, source1});
    builder.addSink({pipeline});
    auto queryPlan = test.addNewQuery(std::move(builder));

    /// Verify running query plan does not terminates
    auto listener = std::make_shared<TestQueryLifetimeListener>();
    EXPECT_CALL(*listener, onDestruction()).Times(0);
    EXPECT_CALL(*listener, onRunning()).Times(1);

    TestQueryLifetimeController controller;
    TestWorkEmitter emitter;

    /// The RunningQueryPlan Requested Setup for all Pipelines in the QueryPlan.
    /// No pipeline is terminated, because p is kept alive by source1
    auto sourceStops = SourceStops::setup(std::vector{test.sourceIds.at(source)}, controller);
    auto setups = Setups::setup(stdv::values(test.stages), emitter);
    {
        auto runningQueryPlan = dropRef(RunningQueryPlan::start(QueryId(0), std::move(queryPlan), controller, emitter, listener));
        EXPECT_TRUE(setups->handleAll());
        EXPECT_TRUE(test.sourceControls[source]->waitUntilOpened());
        EXPECT_TRUE(test.sourceControls[source1]->waitUntilOpened());
        test.sourceControls[source]->injectEoS();
        EXPECT_TRUE(test.sourceControls[source]->waitUntilClosed());
        EXPECT_TRUE(sourceStops->handle(test.sourceIds.at(source)));

        EXPECT_TRUE(test.sourceControls[source]->waitUntilDestroyed());
        EXPECT_FALSE(test.sourceControls[source1]->waitUntilClosed());
        leak(std::move(runningQueryPlan), std::move(listener));
    }
}

TEST_F(QueryPlanTest, DisposingQueryPlanWhileSourceIsAboutToBeTerminated)
{
    TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline = builder.addPipeline({source});
    builder.addSink({pipeline});
    auto queryPlan = test.addNewQuery(std::move(builder));
    auto srcCtrl = test.sourceControls[source];

    TestQueryLifetimeController controller;
    TestWorkEmitter emitter;

    /// The RunningQueryPlan Requested Setup for all Pipelines in the QueryPlan.
    auto setups = Setups::setup(stdv::values(test.stages), emitter);
    auto sourceStops = SourceStops::setup(std::vector{test.sourceIds.at(source)}, controller);

    auto listener = std::make_shared<TestQueryLifetimeListener>();

    EXPECT_CALL(*listener, onRunning()).Times(1);

    {
        auto runningQueryPlan = dropRef(RunningQueryPlan::start(QueryId(0), std::move(queryPlan), controller, emitter, listener));
        EXPECT_TRUE(setups->waitForTasks(2));
        EXPECT_TRUE(setups->handleAll());
        srcCtrl->injectEoS();
        EXPECT_TRUE(sourceStops->waitForTasks(1));
        RunningQueryPlan::dispose(std::move(runningQueryPlan));
    }

    EXPECT_TRUE(srcCtrl->waitUntilDestroyed());
    EXPECT_TRUE(srcCtrl->wasClosed());
}

TEST_F(QueryPlanTest, DestroyingQueryPlanWhileSourceIsAboutToBeTerminated)
{
    Testing::TestingHarness test;
    auto builder = test.buildNewQuery();
    auto source = builder.addSource();
    auto pipeline = builder.addPipeline({source});
    builder.addSink({pipeline});
    auto queryPlan = test.addNewQuery(std::move(builder));
    auto srcCtrl = test.sourceControls[source];

    TestQueryLifetimeController controller;
    TestWorkEmitter emitter;

    /// The RunningQueryPlan Requested Setup for all Pipelines in the QueryPlan.
    auto setups = Setups::setup(stdv::values(test.stages), emitter);
    auto sourceStops = SourceStops::setup(std::vector{test.sourceIds.at(source)}, controller);

    auto listener = std::make_shared<TestQueryLifetimeListener>();
    EXPECT_CALL(*listener, onRunning()).Times(1);

    EXPECT_CALL(*listener, onDestruction()).Times(1);

    {
        auto runningQueryPlan = dropRef(RunningQueryPlan::start(QueryId(0), std::move(queryPlan), controller, emitter, listener));
        EXPECT_TRUE(setups->waitForTasks(2));
        EXPECT_TRUE(setups->handleAll());
        srcCtrl->injectEoS();
        EXPECT_TRUE(sourceStops->waitForTasks(1));
    }

    EXPECT_TRUE(srcCtrl->waitUntilDestroyed()) << "Expected Source to be destroyed when destroying the RQP";
    EXPECT_TRUE(srcCtrl->wasClosed());
    EXPECT_TRUE(sourceStops->handleAll()) << "Source stop should not fail";
}

}
