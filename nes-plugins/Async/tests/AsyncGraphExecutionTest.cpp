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
#include <chrono>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncFunction.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/system/system_error.hpp>
#include <gtest/gtest.h>
#include <AsyncGraphExecution.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SharedAsyncResult.hpp>

namespace NES
{
namespace
{
namespace asio = boost::asio;
using namespace std::chrono_literals;
using asio::experimental::awaitable_operators::operator&&;
const DataType INTEGER{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};

struct Outcome
{
    std::optional<AsyncValues> values;
    std::exception_ptr error;
};

asio::awaitable<Outcome> observe(asio::awaitable<AsyncValues> operation)
{
    try
    {
        co_return Outcome{co_await std::move(operation), {}};
    }
    catch (...)
    {
        co_return Outcome{{}, std::current_exception()};
    } /// NOLINT(no-raw-catch-all)
}

boost::system::error_code errorCode(const Outcome& outcome)
{
    if (!outcome.error)
    {
        return {};
    }
    try
    {
        std::rethrow_exception(outcome.error);
    }
    catch (const boost::system::system_error& error)
    {
        return error.code();
    }
    catch (...)
    {
        return asio::error::fault;
    } /// NOLINT(no-raw-catch-all)
}

asio::awaitable<void> pause(const std::chrono::milliseconds duration)
{
    asio::steady_timer timer{co_await asio::this_coro::executor, duration};
    co_await timer.async_wait(asio::use_awaitable);
}

asio::awaitable<int> after(const std::chrono::milliseconds delay, std::function<void()> action)
{
    co_await pause(delay);
    action();
    co_return 0;
}

template <typename Body>
void run(Body body)
{
    asio::io_context context;
    auto executor = asio::make_strand(context);
    /// body remains alive while a coroutine lambda uses its captures.
    auto result = asio::co_spawn(executor, body(executor), asio::use_future);
    context.run();
    result.get();
}

AsyncGraphExecution::ReadInput noInputs()
{
    return [](const InputFieldReference&) -> RuntimeValue { throw InvalidQuerySyntax("unexpected input"); };
}
}

TEST(SharedAsyncResultTest, LazyProducerRunsOnceAndAllSubscribersIncludingLateOnesKeepTheValue)
{
    run(
        [](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            int calls = 0;
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                [&]() -> asio::awaitable<AsyncValues>
                {
                    ++calls;
                    co_await pause(5ms);
                    co_return AsyncValues::make(std::string{"a\0b", 3});
                });
            EXPECT_EQ(calls, 0);
            auto [first, second] = co_await (value->get() && value->get());
            EXPECT_EQ(calls, 1);
            EXPECT_EQ(first[0].get<std::string_view>(), std::string_view("a\0b", 3));
            EXPECT_EQ(second[0].get<std::string_view>(), std::string_view("a\0b", 3));
            const auto late = co_await value->get();
            EXPECT_EQ(late[0].get<std::string_view>(), std::string_view("a\0b", 3));
        });
}

TEST(SharedAsyncResultTest, ImmediateCompletionCannotLoseTheWakeup)
{
    run(
        [](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            for (int repeat = 0; repeat < 50; ++repeat)
            {
                auto value = std::make_shared<SharedAsyncResult>(
                    executor, []() -> asio::awaitable<AsyncValues> { co_return AsyncValues::make(int64_t{42}); });
                auto [first, second] = co_await (value->get() && value->get());
                EXPECT_EQ(first[0].get<int64_t>(), 42);
                EXPECT_EQ(second[0].get<int64_t>(), 42);
            }
        });
}

TEST(SharedAsyncResultTest, ConsumerTimeoutDoesNotCancelProducerOrAnotherSubscriber)
{
    run(
        [](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            int calls = 0;
            bool completed = false;
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                [&]() -> asio::awaitable<AsyncValues>
                {
                    ++calls;
                    co_await pause(30ms);
                    completed = true;
                    co_return AsyncValues::make(int64_t{42});
                });
            auto [shortWait, other] = co_await (observe(value->getUntil(std::chrono::steady_clock::now() + 2ms)) && value->get());
            EXPECT_EQ(errorCode(shortWait), asio::error::timed_out);
            EXPECT_EQ(other[0].get<int64_t>(), 42);
            EXPECT_TRUE(completed);
            EXPECT_EQ(calls, 1);
        });
}

TEST(SharedAsyncResultTest, CancellingOneSubscriberDoesNotCancelTheSharedProducer)
{
    run(
        [](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            asio::cancellation_signal cancelWaiter;
            int calls = 0;
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                [&]() -> asio::awaitable<AsyncValues>
                {
                    ++calls;
                    co_await pause(30ms);
                    co_return AsyncValues::make(int64_t{42});
                });
            auto firstWait
                = asio::co_spawn(executor, observe(value->get()), asio::bind_cancellation_slot(cancelWaiter.slot(), asio::use_awaitable));
            auto [first, second, cancelled] = co_await (
                std::move(firstWait) && value->get() && after(2ms, [&] { cancelWaiter.emit(asio::cancellation_type::terminal); }));
            EXPECT_EQ(errorCode(first), asio::error::operation_aborted);
            EXPECT_EQ(second[0].get<int64_t>(), 42);
            EXPECT_EQ(calls, 1);
        });
}

TEST(SharedAsyncResultTest, LateProducerCompletionCannotTurnAnExpiredWaitIntoSuccess)
{
    run(
        [](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                []() -> asio::awaitable<AsyncValues>
                {
                    /// Deliberately stall this test's executor to exercise completion/deadline notification ordering.
                    std::this_thread::sleep_for(30ms);
                    co_return AsyncValues::make(int64_t{42});
                });
            const auto timedOut = co_await observe(value->getUntil(std::chrono::steady_clock::now() + 10ms));
            EXPECT_EQ(errorCode(timedOut), asio::error::timed_out);
            const auto late = co_await value->get();
            EXPECT_EQ(late[0].get<int64_t>(), 42);
        });
}

TEST(SharedAsyncResultTest, ProducerTimeoutIsSharedByEverySubscriber)
{
    run(
        [](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            int calls = 0;
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                [&]() -> asio::awaitable<AsyncValues>
                {
                    ++calls;
                    co_await pause(2ms);
                    throw boost::system::system_error{asio::error::timed_out};
                    co_return AsyncValues::make();
                });
            auto [first, second] = co_await (observe(value->get()) && observe(value->get()));
            EXPECT_EQ(errorCode(first), asio::error::timed_out);
            EXPECT_EQ(errorCode(second), asio::error::timed_out);
            EXPECT_EQ(calls, 1);
            const auto late = co_await observe(value->get());
            EXPECT_EQ(errorCode(late), asio::error::timed_out);
        });
}

TEST(SharedAsyncResultTest, OwnerCancellationStopsProducerAndWakesAllSubscribers)
{
    run(
        [](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                []() -> asio::awaitable<AsyncValues>
                {
                    co_await pause(10min);
                    co_return AsyncValues::make(int64_t{42});
                });
            auto [first, second, cancelled]
                = co_await (observe(value->get()) && observe(value->get()) && after(2ms, [value] { value->cancel(); }));
            EXPECT_EQ(errorCode(first), asio::error::operation_aborted);
            EXPECT_EQ(errorCode(second), asio::error::operation_aborted);
            EXPECT_TRUE(value->finished());
            co_await value->join();
        });
}

TEST(SharedAsyncResultTest, AlreadyCancelledWaiterDoesNotStartProducerEvenWhenAutomaticThrowsAreDisabled)
{
    run(
        [](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            int calls = 0;
            asio::cancellation_signal cancelWaiter;
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                [&]() -> asio::awaitable<AsyncValues>
                {
                    ++calls;
                    co_return AsyncValues::make(int64_t{42});
                });
            const auto waiter = [&]() -> asio::awaitable<Outcome>
            {
                co_await asio::this_coro::throw_if_cancelled(false);
                cancelWaiter.emit(asio::cancellation_type::terminal);
                co_return co_await observe(value->get());
            };
            const auto outcome
                = co_await asio::co_spawn(executor, waiter(), asio::bind_cancellation_slot(cancelWaiter.slot(), asio::use_awaitable));
            EXPECT_EQ(errorCode(outcome), asio::error::operation_aborted);
            EXPECT_EQ(calls, 0);
            const auto other = co_await value->get();
            EXPECT_EQ(other[0].get<int64_t>(), 42);
            EXPECT_EQ(calls, 1);
        });
}

TEST(SharedAsyncResultTest, CancelBeforeStartAndExpiredDeadlineDoNotRunProducer)
{
    run(
        [](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            int calls = 0;
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                [&]() -> asio::awaitable<AsyncValues>
                {
                    ++calls;
                    co_return AsyncValues::make(int64_t{42});
                });
            const auto expired = co_await observe(value->getUntil(std::chrono::steady_clock::now() - 1ms));
            EXPECT_EQ(errorCode(expired), asio::error::timed_out);
            EXPECT_EQ(calls, 0);
            value->cancel();
            const auto cancelled = co_await observe(value->get());
            EXPECT_EQ(errorCode(cancelled), asio::error::operation_aborted);
            EXPECT_EQ(calls, 0);
        });
}

TEST(AsyncGraphExecutionTest, DiamondUsesOneInvocationAndSelectsTypedOutputs)
{
    AsyncCallGraphBuilder builder{{}};
    const auto producer = builder.call("Producer", {RuntimeValue{int64_t{42}}}, {INTEGER, INTEGER});
    const auto left = builder.call("Left", {producer->output(0)}, {INTEGER});
    const auto right = builder.call("Right", {producer->output(1)}, {INTEGER});
    const auto root = builder.call("Root", {left->output(0), right->output(0)}, {INTEGER, INTEGER});
    builder.exportOutput(root->output(0), QualifiedIdentifier::parse("LEFT"));
    builder.exportOutput(root->output(1), QualifiedIdentifier::parse("RIGHT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            std::unordered_map<std::string, int> counts;
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr node, AsyncValues arguments) -> asio::awaitable<AsyncValues>
                {
                    ++counts[node->getFunctionType()];
                    if (node->getFunctionType() == "Producer")
                    {
                        co_return AsyncValues::make(int64_t{42}, int64_t{7});
                    }
                    if (node->getFunctionType() == "Left")
                    {
                        co_return AsyncValues::make(arguments[0].get<int64_t>() + 1);
                    }
                    if (node->getFunctionType() == "Right")
                    {
                        co_return AsyncValues::make(arguments[0].get<int64_t>() + 2);
                    }
                    co_return arguments;
                });
            const auto output = co_await execution->run();
            EXPECT_EQ(output[0].get<int64_t>(), 43);
            EXPECT_EQ(output[1].get<int64_t>(), 9);
            EXPECT_EQ(counts["Producer"], 1);
            EXPECT_EQ(counts["Left"], 1);
            EXPECT_EQ(counts["Right"], 1);
            EXPECT_EQ(counts["Root"], 1);
        });
}

#ifndef NO_ASSERT
class AsyncGraphExecutionDeathTest : public testing::Test
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("AsyncGraphExecutionDeathTest.log", LogLevel::LOG_DEBUG, false); }
};

TEST_F(AsyncGraphExecutionDeathTest, InputReaderMustProduceTheDeclaredNullability)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const auto name = QualifiedIdentifier::parse("INPUT.VALUE");
    AsyncCallGraphBuilder builder{Schema<QualifiedUnboundField, Ordered>{QualifiedUnboundField{name, nullable}}};
    const auto node = builder.call("Identity", {builder.input(name)}, {nullable});
    builder.exportOutput(node->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    EXPECT_DEATH(
        run(
            [graph](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
            {
                auto execution = std::make_shared<AsyncGraphExecution>(
                    executor,
                    graph,
                    [](const InputFieldReference&) { return RuntimeValue{int64_t{42}}; },
                    [](AsyncCall::Ptr, AsyncValues arguments) -> asio::awaitable<AsyncValues> { co_return arguments; });
                static_cast<void>(co_await execution->run());
            }),
        "Invariant violated.*async input");
}
#endif

TEST(AsyncGraphExecutionTest, ChainedValuesRetainTheProducersDeclaredNullability)
{
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    AsyncCallGraphBuilder builder{{}};
    const auto producer = builder.call("Producer", {}, {nullable});
    const auto consumer = builder.call("Consumer", {producer->output(0)}, {INTEGER});
    builder.exportOutput(consumer->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph, nullable](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [nullable](AsyncCall::Ptr node, AsyncValues arguments) -> asio::awaitable<AsyncValues>
                {
                    if (node->getFunctionType() == "Producer")
                    {
                        co_return AsyncValues::make(RuntimeValue{nullable, int64_t{42}});
                    }
                    EXPECT_EQ(arguments[0].getDataType(), nullable);
                    EXPECT_FALSE(arguments[0].isNull());
                    co_return AsyncValues::make(arguments[0].get<int64_t>());
                });
            const auto result = co_await execution->run();
            EXPECT_EQ(result[0].getDataType(), INTEGER);
            EXPECT_EQ(result[0].get<int64_t>(), 42);
        });
}

TEST(AsyncGraphExecutionTest, ConsumerDoesNotWaitForAnUnrelatedRoot)
{
    AsyncCallGraphBuilder builder{{}};
    const auto fast = builder.call("Fast", {}, {INTEGER});
    const auto dependent = builder.call("Dependent", {fast->output(0)}, {INTEGER});
    const auto unrelated = builder.call("Unrelated", {}, {INTEGER});
    builder.exportOutput(dependent->output(0), QualifiedIdentifier::parse("A"));
    builder.exportOutput(unrelated->output(0), QualifiedIdentifier::parse("B"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            asio::steady_timer gate{executor, std::chrono::steady_clock::time_point::max()};
            bool dependentRan = false;
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr node, AsyncValues arguments) -> asio::awaitable<AsyncValues>
                {
                    if (node->getFunctionType() == "Fast")
                    {
                        co_await pause(1ms);
                        co_return AsyncValues::make(int64_t{1});
                    }
                    if (node->getFunctionType() == "Dependent")
                    {
                        dependentRan = true;
                        gate.expires_at(std::chrono::steady_clock::now());
                        co_return arguments;
                    }
                    boost::system::error_code ignored;
                    co_await gate.async_wait(asio::redirect_error(asio::use_awaitable, ignored));
                    EXPECT_TRUE(dependentRan);
                    co_return AsyncValues::make(int64_t{2});
                });
            const auto result = co_await execution->run();
            EXPECT_EQ(result.size(), 2);
        });
}

TEST(AsyncGraphExecutionTest, IndependentArgumentsAreStartedTogether)
{
    AsyncCallGraphBuilder builder{{}};
    const auto first = builder.call("First", {}, {INTEGER});
    const auto second = builder.call("Second", {}, {INTEGER});
    const auto root = builder.call("Root", {first->output(0), second->output(0)}, {INTEGER});
    builder.exportOutput(root->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            asio::steady_timer gate{executor, std::chrono::steady_clock::time_point::max()};
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr node, AsyncValues arguments) -> asio::awaitable<AsyncValues>
                {
                    if (node->getFunctionType() == "Second")
                    {
                        gate.expires_at(std::chrono::steady_clock::now());
                        co_return AsyncValues::make(int64_t{2});
                    }
                    if (node->getFunctionType() == "First")
                    {
                        boost::system::error_code ignored;
                        co_await gate.async_wait(asio::redirect_error(asio::use_awaitable, ignored));
                        co_return AsyncValues::make(int64_t{1});
                    }
                    co_return AsyncValues::make(arguments[0].get<int64_t>() + arguments[1].get<int64_t>());
                });
            const auto result = co_await execution->run();
            EXPECT_EQ(result[0].get<int64_t>(), 3);
        });
}

TEST(AsyncGraphExecutionTest, ConsumerDeadlineDoesNotBecomeTheSharedNodesDeadline)
{
    AsyncCallGraphBuilder builder{{}};
    const auto producer = builder.call("Producer", {}, {INTEGER});
    builder.exportOutput(producer->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph, producer](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            int invocations = 0;
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr, AsyncValues) -> asio::awaitable<AsyncValues>
                {
                    ++invocations;
                    co_await pause(30ms);
                    co_return AsyncValues::make(int64_t{42});
                });
            const auto subscription = execution->evaluate(producer);
            auto [shortWait, result]
                = co_await (observe(subscription->getUntil(std::chrono::steady_clock::now() + 2ms)) && execution->run());
            EXPECT_EQ(errorCode(shortWait), asio::error::timed_out);
            EXPECT_EQ(result[0].get<int64_t>(), 42);
            EXPECT_EQ(invocations, 1);
        });
}

TEST(AsyncGraphExecutionTest, EmptyGraphCompletesSuccessfully)
{
    AsyncCallGraphBuilder builder{{}};
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [](AsyncCall::Ptr, AsyncValues) -> asio::awaitable<AsyncValues>
                {
                    ADD_FAILURE() << "An empty graph must not invoke any function";
                    co_return AsyncValues::make();
                });
            const auto result = co_await execution->run();
            EXPECT_TRUE(result.empty());
        });
}

TEST(AsyncGraphExecutionTest, CancellationWithoutAutomaticThrowsDoesNotReturnSuccess)
{
    AsyncCallGraphBuilder builder{{}};
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            asio::cancellation_signal stop;
            const auto body = [&]() -> asio::awaitable<void>
            {
                co_await asio::this_coro::throw_if_cancelled(false);
                stop.emit(asio::cancellation_type::terminal);
                auto execution = std::make_shared<AsyncGraphExecution>(
                    executor,
                    graph,
                    noInputs(),
                    [](AsyncCall::Ptr, AsyncValues) -> asio::awaitable<AsyncValues> { co_return AsyncValues::make(); });
                const auto outcome = co_await observe(execution->run());
                EXPECT_EQ(errorCode(outcome), asio::error::operation_aborted);
                EXPECT_FALSE(co_await asio::this_coro::throw_if_cancelled());
            };
            co_await asio::co_spawn(executor, body(), asio::bind_cancellation_slot(stop.slot(), asio::use_awaitable));
        });
}

TEST(AsyncGraphExecutionTest, FailureCancelsAndDrainsUnrelatedWorkAndPreservesTheInitiatingError)
{
    AsyncCallGraphBuilder builder{{}};
    const auto bad = builder.call("Bad", {}, {INTEGER});
    const auto slow = builder.call("Slow", {}, {INTEGER});
    builder.exportOutput(bad->output(0), QualifiedIdentifier::parse("BAD"));
    builder.exportOutput(slow->output(0), QualifiedIdentifier::parse("SLOW"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            asio::steady_timer started{executor, std::chrono::steady_clock::time_point::max()};
            bool slowRunning = false;
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr node, AsyncValues) -> asio::awaitable<AsyncValues>
                {
                    if (node->getFunctionType() == "Bad")
                    {
                        boost::system::error_code ignored;
                        co_await started.async_wait(asio::redirect_error(asio::use_awaitable, ignored));
                        throw CannotReadFromSource("initiating failure");
                    }
                    slowRunning = true;
                    started.expires_at(std::chrono::steady_clock::now());
                    try
                    {
                        co_await pause(10min);
                    }
                    catch (...)
                    {
                        slowRunning = false;
                        throw;
                    } /// NOLINT(no-raw-catch-all)
                    slowRunning = false;
                    co_return AsyncValues::make(int64_t{1});
                });
            const auto outcome = co_await observe(execution->run());
            EXPECT_TRUE(outcome.error);
            if (!outcome.error)
            {
                co_return;
            }
            EXPECT_FALSE(slowRunning);
            try
            {
                std::rethrow_exception(outcome.error);
            }
            catch (const Exception& error)
            {
                EXPECT_NE(std::string{error.what()}.find("initiating failure"), std::string::npos);
            }
        });
}

TEST(AsyncGraphExecutionTest, ExecutionStateIsPerRowNotPerPlanAndEqualNodesAreNotImplicitlyMerged)
{
    AsyncCallGraphBuilder builder{{}};
    const auto first = builder.call("Same", {}, {INTEGER});
    const auto second = builder.call("Same", {}, {INTEGER});
    builder.exportOutput(first->output(0), QualifiedIdentifier::parse("A"));
    builder.exportOutput(second->output(0), QualifiedIdentifier::parse("B"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            int invocations = 0;
            auto invoke = [&](AsyncCall::Ptr, AsyncValues) -> asio::awaitable<AsyncValues>
            {
                ++invocations;
                co_return AsyncValues::make(int64_t{invocations});
            };
            auto firstRow = std::make_shared<AsyncGraphExecution>(executor, graph, noInputs(), invoke);
            auto secondRow = std::make_shared<AsyncGraphExecution>(executor, graph, noInputs(), invoke);
            auto [a, b] = co_await (firstRow->run() && secondRow->run());
            EXPECT_EQ(invocations, 4);
            EXPECT_NE(a[0].get<int64_t>(), a[1].get<int64_t>());
            EXPECT_NE(a[0].get<int64_t>(), b[0].get<int64_t>());
        });
}

TEST(AsyncGraphExecutionTest, CancellingTheRootWaitAlsoCancelsAndDrainsItsOwnedGraph)
{
    AsyncCallGraphBuilder builder{{}};
    const auto producer = builder.call("Producer", {}, {INTEGER});
    builder.exportOutput(producer->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            asio::cancellation_signal stopRow;
            bool active = false;
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr, AsyncValues) -> asio::awaitable<AsyncValues>
                {
                    active = true;
                    try
                    {
                        co_await pause(10min);
                    }
                    catch (...)
                    {
                        active = false;
                        throw;
                    } /// NOLINT(no-raw-catch-all)
                    active = false;
                    co_return AsyncValues::make(int64_t{42});
                });
            auto root
                = asio::co_spawn(executor, observe(execution->run()), asio::bind_cancellation_slot(stopRow.slot(), asio::use_awaitable));
            const auto result = co_await (std::move(root) && after(2ms, [&] { stopRow.emit(asio::cancellation_type::terminal); }));
            EXPECT_EQ(errorCode(std::get<0>(result)), asio::error::operation_aborted);
            EXPECT_FALSE(active);
        });
}

TEST(AsyncGraphExecutionTest, SuccessfulExecutionDoesNotDisableTheCallersCancellationState)
{
    AsyncCallGraphBuilder builder{{}};
    const auto producer = builder.call("Producer", {}, {INTEGER});
    builder.exportOutput(producer->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            asio::cancellation_signal stop;
            const auto body = [&]() -> asio::awaitable<void>
            {
                auto execution = std::make_shared<AsyncGraphExecution>(
                    executor,
                    graph,
                    noInputs(),
                    [](AsyncCall::Ptr, AsyncValues) -> asio::awaitable<AsyncValues> { co_return AsyncValues::make(int64_t{42}); });
                static_cast<void>(co_await execution->run());
                stop.emit(asio::cancellation_type::terminal);
                const auto state = co_await asio::this_coro::cancellation_state;
                EXPECT_NE(state.cancelled(), asio::cancellation_type::none);
            };
            co_await asio::co_spawn(executor, body(), asio::bind_cancellation_slot(stop.slot(), asio::use_awaitable));
        });
}

TEST(AsyncGraphExecutionTest, RowCancellationDrainsAllProducersBeforeReturning)
{
    AsyncCallGraphBuilder builder{{}};
    const auto producer = builder.call("Producer", {}, {INTEGER});
    builder.exportOutput(producer->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Executor executor) -> asio::awaitable<void>
        {
            bool active = false;
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr, AsyncValues) -> asio::awaitable<AsyncValues>
                {
                    active = true;
                    try
                    {
                        co_await pause(10min);
                    }
                    catch (...)
                    {
                        active = false;
                        throw;
                    } /// NOLINT(no-raw-catch-all)
                    active = false;
                    co_return AsyncValues::make(int64_t{42});
                });
            const auto result = co_await (observe(execution->run()) && after(2ms, [execution] { execution->cancel(); }));
            EXPECT_EQ(errorCode(std::get<0>(result)), asio::error::operation_aborted);
            EXPECT_FALSE(active);
        });
}

}
