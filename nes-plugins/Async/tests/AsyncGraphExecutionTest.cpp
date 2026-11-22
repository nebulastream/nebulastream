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
/// NOLINTBEGIN(readability-magic-numbers) Numeric values are fixture data in these tests.
#include <chrono>
#include <cstdint>
#include <exception>
#include <expected>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncError.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Util/AsyncTestOperator.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp> /// NOLINT(misc-include-cleaner) Public Logger facade; checker misattributes setupLogging.
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/system/error_code.hpp> /// NOLINT(misc-include-cleaner) Public Boost.System provider; checker misses direct type use
#include <gtest/gtest.h>
#include <AsyncGraphExecution.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SharedAsyncResult.hpp>

namespace NES
{
/// Test-only access to producer subscriptions; production callers can only run/cancel a row.
struct AsyncGraphExecutionTestAccess
{
    static std::shared_ptr<SharedAsyncResult> evaluate(AsyncGraphExecution& execution, const AsyncCall::Ptr& node)
    {
        return execution.evaluate(node);
    }
};

namespace
{
namespace asio = boost::asio;
using namespace std::chrono_literals;
/// NOLINTNEXTLINE(misc-unused-using-decls) required to find Asio's coroutine operator overloads
using asio::experimental::awaitable_operators::operator&&;
const DataType INTEGER{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};

using Outcome = AsyncResult<SharedRuntimeValues>;

asio::awaitable<Outcome> observe(asio::awaitable<AsyncResult<SharedRuntimeValues>> operation)
{
    co_return co_await std::move(operation);
}

const SharedRuntimeValues& successValues(const Outcome& outcome)
{
    EXPECT_TRUE(outcome);
    static const SharedRuntimeValues EmptyValues;
    return outcome ? *outcome : EmptyValues;
}

/// NOLINTNEXTLINE(misc-include-cleaner) error_code is declared by public boost/system/error_code.hpp.
boost::system::error_code errorCode(const Outcome& outcome)
{
    if (outcome)
    {
        return {};
    }
    switch (outcome.error().kind)
    {
        case AsyncError::Type::Cancelled:
            return asio::error::operation_aborted;
        case AsyncError::Type::TimedOut:
            return asio::error::timed_out;
        case AsyncError::Type::Permanent:
        case AsyncError::Type::Retryable:
            return asio::error::fault;
    }
    return asio::error::fault;
}

asio::awaitable<boost::system::error_code> pause(const std::chrono::milliseconds duration)
{
    asio::steady_timer timer{co_await asio::this_coro::executor, duration};
    boost::system::error_code error;
    co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, error));
    co_return error;
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
    return [](const InputFieldReference&) -> RuntimeValue
    {
        ADD_FAILURE() << "Unexpected input requested from a graph with no inputs";
        return RuntimeValue{int64_t{0}};
    };
}
}

TEST(SharedAsyncResultTest, LazyProducerRunsOnceAndAllSubscribersIncludingLateOnesKeepTheValue)
{
    run(
        [](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            int calls = 0;
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                [&]() -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    ++calls;
                    /// NOLINTNEXTLINE(misc-include-cleaner) chrono literals are declared by the public <chrono> header.
                    co_await pause(5ms);
                    co_return SharedRuntimeValues::make(std::string{"a\0b", 3});
                });
            EXPECT_EQ(calls, 0);
            auto [first, second] = co_await (value->get() && value->get());
            EXPECT_EQ(calls, 1);
            EXPECT_EQ(successValues(first)[0].get<std::string_view>(), std::string_view("a\0b", 3));
            EXPECT_EQ(successValues(second)[0].get<std::string_view>(), std::string_view("a\0b", 3));
            const auto late = co_await value->get();
            EXPECT_EQ(successValues(late)[0].get<std::string_view>(), std::string_view("a\0b", 3));
        });
}

TEST(SharedAsyncResultTest, DefaultEmptyResultIsSuccessfulAndCached)
{
    run(
        [](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            int calls = 0;
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                [&]() -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    ++calls;
                    co_return SharedRuntimeValues{};
                });
            EXPECT_FALSE(value->finished());
            auto [first, second] = co_await (value->get() && value->get());
            EXPECT_TRUE(successValues(first).empty());
            EXPECT_TRUE(successValues(second).empty());
            EXPECT_TRUE(value->finished());
            const auto late = co_await value->get();
            EXPECT_TRUE(successValues(late).empty());
            EXPECT_EQ(calls, 1);
        });
}

/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(SharedAsyncResultTest, SpawnDefaultValueOnProducerFailureIsNeverPublishedAsEmptySuccess)
{
    for (const bool synchronousFailure : {false, true})
    {
        asio::io_context context;
        const auto executor = asio::make_strand(context);
        int calls = 0;
        int completions = 0;
        auto value = std::make_shared<SharedAsyncResult>(
            executor,
            [&]() -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
            {
                ++calls;
                if (synchronousFailure)
                {
                    throw CannotReadFromSource("synchronous producer failure");
                }
                return []() -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    /// NOLINTNEXTLINE(misc-include-cleaner) chrono literals are declared by the public <chrono> header.
                    co_await pause(1ms);
                    co_return std::unexpected(
                        AsyncError{.exception = IOError("asynchronous producer failure"), .kind = AsyncError::Type::Permanent});
                }();
            });
        const auto completed = [&](std::exception_ptr exception, AsyncResult<SharedRuntimeValues> result)
        {
            ++completions;
            EXPECT_TRUE(exception || !result);
            /// An operational failure is an expected error, never a default-empty success.
            EXPECT_FALSE(result);
            if (result)
            {
                EXPECT_TRUE(result->empty());
            }
        };
        asio::co_spawn(
            executor, value->get(), completed);
        asio::co_spawn(executor, value->get(), completed);
        context.run();
        EXPECT_EQ(completions, 2);
        EXPECT_EQ(calls, 1);
        context.restart();
        asio::co_spawn(executor, value->get(), completed);
        context.run();
        EXPECT_EQ(completions, 3);
        EXPECT_EQ(calls, 1);
    }
}
/// NOLINTEND(misc-include-cleaner)

TEST(SharedAsyncResultTest, ImmediateCompletionCannotLoseTheWakeup)
{
    run(
        [](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            for (int repeat = 0; repeat < 50; ++repeat)
            {
                auto value = std::make_shared<SharedAsyncResult>(
                    executor,
                    []() -> asio::awaitable<AsyncResult<SharedRuntimeValues>> { co_return SharedRuntimeValues::make(int64_t{42}); });
                auto [first, second] = co_await (value->get() && value->get());
                EXPECT_EQ(successValues(first)[0].get<int64_t>(), 42);
                EXPECT_EQ(successValues(second)[0].get<int64_t>(), 42);
            }
        });
}

TEST(SharedAsyncResultTest, ConsumerTimeoutDoesNotCancelProducerOrAnotherSubscriber)
{
    run(
        [](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            int calls = 0;
            bool completed = false;
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                [&]() -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    ++calls;
                    /// NOLINTNEXTLINE(misc-include-cleaner) chrono literals are declared by the public <chrono> header.
                    co_await pause(30ms);
                    completed = true;
                    co_return SharedRuntimeValues::make(int64_t{42});
                });
            /// NOLINTNEXTLINE(misc-include-cleaner) chrono literals are declared by the public <chrono> header.
            auto [shortWait, other] = co_await (observe(value->getUntil(std::chrono::steady_clock::now() + 2ms)) && value->get());
            EXPECT_EQ(errorCode(shortWait), asio::error::timed_out);
            EXPECT_EQ(successValues(other)[0].get<int64_t>(), 42);
            EXPECT_TRUE(completed);
            EXPECT_EQ(calls, 1);
        });
}

/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(SharedAsyncResultTest, CancellingOneSubscriberDoesNotCancelTheSharedProducer)
{
    run(
        [](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            asio::cancellation_signal cancelWaiter;
            int calls = 0;
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                [&]() -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    ++calls;
                    co_await pause(30ms);
                    co_return SharedRuntimeValues::make(int64_t{42});
                });
            auto firstWait = asio::co_spawn(
                executor,
                observe(value->get()),
                asio::bind_cancellation_slot(
                    cancelWaiter.slot(),
                    asio::use_awaitable));
            auto [first, second, cancelled] = co_await (
                std::move(firstWait) && value->get() && after(2ms, [&] { cancelWaiter.emit(asio::cancellation_type::terminal); }));
            EXPECT_EQ(errorCode(first), asio::error::operation_aborted);
            EXPECT_EQ(successValues(second)[0].get<int64_t>(), 42);
            EXPECT_EQ(calls, 1);
        });
}
/// NOLINTEND(misc-include-cleaner)

TEST(SharedAsyncResultTest, LateProducerCompletionCannotTurnAnExpiredWaitIntoSuccess)
{
    run(
        [](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                []() -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    /// Deliberately stall this test's executor to exercise completion/deadline notification ordering.
                    std::this_thread::sleep_for(30ms);
                    co_return SharedRuntimeValues::make(int64_t{42});
                });
            /// NOLINTNEXTLINE(misc-include-cleaner) chrono literals are declared by the public <chrono> header.
            const auto timedOut = co_await observe(value->getUntil(std::chrono::steady_clock::now() + 10ms));
            EXPECT_EQ(errorCode(timedOut), asio::error::timed_out);
            const auto late = co_await value->get();
            EXPECT_EQ(successValues(late)[0].get<int64_t>(), 42);
        });
}

TEST(SharedAsyncResultTest, ProducerTimeoutIsSharedByEverySubscriber)
{
    run(
        [](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            int calls = 0;
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                [&]() -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    ++calls;
                    co_await pause(2ms);
                    co_return std::unexpected(AsyncError::timedOut());
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
        [](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                []() -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    /// NOLINTNEXTLINE(misc-include-cleaner) chrono literals are declared by the public <chrono> header.
                    const auto error = co_await pause(10min);
                    if (error == asio::error::operation_aborted)
                    {
                        co_return std::unexpected(AsyncError::cancelled());
                    }
                    co_return SharedRuntimeValues::make(int64_t{42});
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
        [](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            int calls = 0;
            asio::cancellation_signal cancelWaiter;
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                [&]() -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    ++calls;
                    co_return SharedRuntimeValues::make(int64_t{42});
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
            EXPECT_EQ(successValues(other)[0].get<int64_t>(), 42);
            EXPECT_EQ(calls, 1);
        });
}

TEST(SharedAsyncResultTest, CancelBeforeStartAndExpiredDeadlineDoNotRunProducer)
{
    run(
        [](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            int calls = 0;
            auto value = std::make_shared<SharedAsyncResult>(
                executor,
                [&]() -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    ++calls;
                    co_return SharedRuntimeValues::make(int64_t{42});
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

/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(AsyncGraphExecutionTest, DiamondUsesOneInvocationAndSelectsTypedOutputs)
{
    AsyncCallGraphBuilder builder{{}};
    const auto producer = builder.call(makeAsyncTestOperator("Producer", {INTEGER, INTEGER}), {RuntimeValue{int64_t{42}}});
    const auto left = builder.call(makeAsyncTestOperator("Left", {INTEGER}), {producer->output(0)});
    const auto right = builder.call(makeAsyncTestOperator("Right", {INTEGER}), {producer->output(1)});
    const auto root = builder.call(makeAsyncTestOperator("Root", {INTEGER, INTEGER}), {left->output(0), right->output(0)});
    builder.exportOutput(root->output(0), QualifiedIdentifier::parse("LEFT"));
    builder.exportOutput(root->output(1), QualifiedIdentifier::parse("RIGHT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            std::unordered_map<std::string, int> counts;
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr node, SharedRuntimeValues arguments) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    ++counts[node->getFunctionType()];
                    if (node->getFunctionType() == "Producer")
                    {
                        co_return SharedRuntimeValues::make(int64_t{42}, int64_t{7});
                    }
                    if (node->getFunctionType() == "Left")
                    {
                        co_return SharedRuntimeValues::make(arguments[0].get<int64_t>() + 1);
                    }
                    if (node->getFunctionType() == "Right")
                    {
                        co_return SharedRuntimeValues::make(arguments[0].get<int64_t>() + 2);
                    }
                    co_return arguments;
                });
            const auto output = co_await execution->run();
            EXPECT_EQ(successValues(output)[0].get<int64_t>(), 43);
            EXPECT_EQ(successValues(output)[1].get<int64_t>(), 9);
            EXPECT_EQ(counts["Producer"], 1);
            EXPECT_EQ(counts["Left"], 1);
            EXPECT_EQ(counts["Right"], 1);
            EXPECT_EQ(counts["Root"], 1);
        });
}
/// NOLINTEND(misc-include-cleaner)

#ifndef NO_ASSERT
namespace
{
class AsyncGraphExecutionDeathTest : public testing::Test
{
public:
    static void SetUpTestSuite()
    {
        /// NOLINTBEGIN(misc-include-cleaner) setupLogging is declared by the included public Logger facade.
        Logger::setupLogging("AsyncGraphExecutionDeathTest.log", LogLevel::LOG_DEBUG, false);
        /// NOLINTEND(misc-include-cleaner)
    }
};

TEST_F(AsyncGraphExecutionDeathTest, EvaluateRejectsNodeOutsideGraph)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    AsyncCallGraphBuilder builder{{}};
    const auto included = builder.call(makeAsyncTestOperator("Included", {INTEGER}), {});
    builder.exportOutput(included->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    AsyncCallGraphBuilder otherBuilder{{}};
    const auto foreign = otherBuilder.call(makeAsyncTestOperator("Foreign", {INTEGER}), {});
    EXPECT_DEATH(
        run(
            [graph, foreign](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
            {
                auto execution = std::make_shared<AsyncGraphExecution>(
                    executor,
                    graph,
                    noInputs(),
                    [](AsyncCall::Ptr, SharedRuntimeValues) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                    { co_return SharedRuntimeValues::make(int64_t{1}); });
                static_cast<void>(AsyncGraphExecutionTestAccess::evaluate(*execution, foreign));
                co_return;
            }),
        "Precondition violated.*not part of this async graph");
}

TEST_F(AsyncGraphExecutionDeathTest, ARowExecutionCanOnlyRunOnce)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    AsyncCallGraphBuilder builder{{}};
    const auto node = builder.call(makeAsyncTestOperator("Producer", {INTEGER}), {});
    builder.exportOutput(node->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    EXPECT_DEATH(
        run(
            [graph](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
            {
                auto execution = std::make_shared<AsyncGraphExecution>(
                    executor,
                    graph,
                    noInputs(),
                    [](AsyncCall::Ptr, SharedRuntimeValues) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                    { co_return SharedRuntimeValues::make(int64_t{1}); });
                static_cast<void>(co_await execution->run());
                static_cast<void>(co_await execution->run());
            }),
        "Precondition violated.*can only be run once");
}

TEST_F(AsyncGraphExecutionDeathTest, InputReaderMustProduceTheDeclaredNullability)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const auto name = QualifiedIdentifier::parse("INPUT.VALUE");
    AsyncCallGraphBuilder builder{Schema<QualifiedUnboundField, Ordered>{QualifiedUnboundField{name, nullable}}};
    const auto node = builder.call(makeAsyncTestOperator("Identity", {nullable}), {builder.input(name)});
    builder.exportOutput(node->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    EXPECT_DEATH(
        run(
            [graph](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
            {
                auto execution = std::make_shared<AsyncGraphExecution>(
                    executor,
                    graph,
                    [](const InputFieldReference&) { return RuntimeValue{int64_t{42}}; },
                    [](AsyncCall::Ptr, SharedRuntimeValues arguments) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                    { co_return arguments; });
                static_cast<void>(co_await execution->run());
            }),
        "Invariant violated.*async input");
}
}
#endif

TEST(AsyncGraphExecutionTest, ChainedValuesRetainTheProducersDeclaredNullability)
{
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    AsyncCallGraphBuilder builder{{}};
    const auto producer = builder.call(makeAsyncTestOperator("Producer", {nullable}), {});
    const auto consumer = builder.call(makeAsyncTestOperator("Consumer", {INTEGER}), {producer->output(0)});
    builder.exportOutput(consumer->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph, nullable](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [nullable](AsyncCall::Ptr node, SharedRuntimeValues arguments) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    if (node->getFunctionType() == "Producer")
                    {
                        co_return SharedRuntimeValues::make(RuntimeValue{nullable, int64_t{42}});
                    }
                    EXPECT_EQ(arguments[0].getDataType(), nullable);
                    EXPECT_FALSE(arguments[0].isNull());
                    co_return SharedRuntimeValues::make(arguments[0].get<int64_t>());
                });
            const auto result = co_await execution->run();
            EXPECT_EQ(successValues(result)[0].getDataType(), INTEGER);
            EXPECT_EQ(successValues(result)[0].get<int64_t>(), 42);
        });
}

TEST(AsyncGraphExecutionTest, ConsumerDoesNotWaitForAnUnrelatedRoot)
{
    AsyncCallGraphBuilder builder{{}};
    const auto fast = builder.call(makeAsyncTestOperator("Fast", {INTEGER}), {});
    const auto dependent = builder.call(makeAsyncTestOperator("Dependent", {INTEGER}), {fast->output(0)});
    const auto unrelated = builder.call(makeAsyncTestOperator("Unrelated", {INTEGER}), {});
    builder.exportOutput(dependent->output(0), QualifiedIdentifier::parse("A"));
    builder.exportOutput(unrelated->output(0), QualifiedIdentifier::parse("B"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            asio::steady_timer gate{executor, std::chrono::steady_clock::time_point::max()};
            bool dependentRan = false;
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr node, SharedRuntimeValues arguments) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    if (node->getFunctionType() == "Fast")
                    {
                        co_await pause(1ms);
                        co_return SharedRuntimeValues::make(int64_t{1});
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
                    co_return SharedRuntimeValues::make(int64_t{2});
                });
            const auto result = co_await execution->run();
            EXPECT_EQ(successValues(result).size(), 2);
        });
}

TEST(AsyncGraphExecutionTest, IndependentArgumentsAreStartedTogether)
{
    AsyncCallGraphBuilder builder{{}};
    const auto first = builder.call(makeAsyncTestOperator("First", {INTEGER}), {});
    const auto second = builder.call(makeAsyncTestOperator("Second", {INTEGER}), {});
    const auto root = builder.call(makeAsyncTestOperator("Root", {INTEGER}), {first->output(0), second->output(0)});
    builder.exportOutput(root->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            asio::steady_timer gate{executor, std::chrono::steady_clock::time_point::max()};
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr node, SharedRuntimeValues arguments) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    if (node->getFunctionType() == "Second")
                    {
                        gate.expires_at(std::chrono::steady_clock::now());
                        co_return SharedRuntimeValues::make(int64_t{2});
                    }
                    if (node->getFunctionType() == "First")
                    {
                        boost::system::error_code ignored;
                        co_await gate.async_wait(asio::redirect_error(asio::use_awaitable, ignored));
                        co_return SharedRuntimeValues::make(int64_t{1});
                    }
                    co_return SharedRuntimeValues::make(arguments[0].get<int64_t>() + arguments[1].get<int64_t>());
                });
            const auto result = co_await execution->run();
            EXPECT_EQ(successValues(result)[0].get<int64_t>(), 3);
        });
}

TEST(AsyncGraphExecutionTest, ConsumerDeadlineDoesNotBecomeTheSharedNodesDeadline)
{
    AsyncCallGraphBuilder builder{{}};
    const auto producer = builder.call(makeAsyncTestOperator("Producer", {INTEGER}), {});
    builder.exportOutput(producer->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph, producer](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            int invocations = 0;
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr, SharedRuntimeValues) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    ++invocations;
                    co_await pause(30ms);
                    co_return SharedRuntimeValues::make(int64_t{42});
                });
            const auto subscription = AsyncGraphExecutionTestAccess::evaluate(*execution, producer);
            auto [shortWait, result]
                = co_await (observe(subscription->getUntil(std::chrono::steady_clock::now() + 2ms)) && execution->run());
            EXPECT_EQ(errorCode(shortWait), asio::error::timed_out);
            EXPECT_EQ(successValues(result)[0].get<int64_t>(), 42);
            EXPECT_EQ(invocations, 1);
        });
}

TEST(AsyncGraphExecutionTest, EmptyGraphCompletesSuccessfully)
{
    AsyncCallGraphBuilder builder{{}};
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [](AsyncCall::Ptr, SharedRuntimeValues) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    ADD_FAILURE() << "An empty graph must not invoke any function";
                    co_return SharedRuntimeValues::make();
                });
            const auto result = co_await execution->run();
            EXPECT_TRUE(successValues(result).empty());
        });
}

/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(AsyncGraphExecutionTest, CancellationWithoutAutomaticThrowsDoesNotReturnSuccess)
{
    AsyncCallGraphBuilder builder{{}};
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
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
                    [](AsyncCall::Ptr, SharedRuntimeValues) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                    { co_return SharedRuntimeValues::make(); });
                const auto outcome = co_await observe(execution->run());
                EXPECT_EQ(errorCode(outcome), asio::error::operation_aborted);
                EXPECT_FALSE(co_await asio::this_coro::throw_if_cancelled());
            };
            co_await asio::co_spawn(
                executor,
                body(),
                asio::bind_cancellation_slot(
                    stop.slot(),
                    asio::use_awaitable));
        });
}
/// NOLINTEND(misc-include-cleaner)

TEST(AsyncGraphExecutionTest, FailureCancelsAndDrainsUnrelatedWorkAndPreservesTheInitiatingError)
{
    AsyncCallGraphBuilder builder{{}};
    const auto bad = builder.call(makeAsyncTestOperator("Bad", {INTEGER}), {});
    const auto slow = builder.call(makeAsyncTestOperator("Slow", {INTEGER}), {});
    builder.exportOutput(bad->output(0), QualifiedIdentifier::parse("BAD"));
    builder.exportOutput(slow->output(0), QualifiedIdentifier::parse("SLOW"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            asio::steady_timer started{executor, std::chrono::steady_clock::time_point::max()};
            bool slowRunning = false;
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr node, SharedRuntimeValues) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    if (node->getFunctionType() == "Bad")
                    {
                        boost::system::error_code ignored;
                        co_await started.async_wait(asio::redirect_error(asio::use_awaitable, ignored));
                        co_return std::unexpected(
                            AsyncError{.exception = CannotReadFromSource("initiating failure"), .kind = AsyncError::Type::Permanent});
                    }
                    slowRunning = true;
                    started.expires_at(std::chrono::steady_clock::now());
                    const auto error = co_await pause(10min);
                    slowRunning = false;
                    if (error == asio::error::operation_aborted)
                    {
                        co_return std::unexpected(AsyncError::cancelled());
                    }
                    co_return SharedRuntimeValues::make(int64_t{1});
                });
            const auto outcome = co_await observe(execution->run());
            EXPECT_FALSE(outcome);
            if (outcome)
            {
                co_return;
            }
            EXPECT_EQ(outcome.error().kind, AsyncError::Type::Permanent);
            EXPECT_NE(std::string{outcome.error().exception.what()}.find("initiating failure"), std::string::npos);
            EXPECT_FALSE(slowRunning);
        });
}

TEST(AsyncGraphExecutionTest, ExecutionStateIsPerRowNotPerPlanAndEqualNodesAreNotImplicitlyMerged)
{
    AsyncCallGraphBuilder builder{{}};
    const auto first = builder.call(makeAsyncTestOperator("Same", {INTEGER}), {});
    const auto second = builder.call(makeAsyncTestOperator("Same", {INTEGER}), {});
    builder.exportOutput(first->output(0), QualifiedIdentifier::parse("A"));
    builder.exportOutput(second->output(0), QualifiedIdentifier::parse("B"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            int invocations = 0;
            auto invoke = [&](AsyncCall::Ptr, SharedRuntimeValues) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
            {
                ++invocations;
                co_return SharedRuntimeValues::make(int64_t{invocations});
            };
            auto firstRow = std::make_shared<AsyncGraphExecution>(executor, graph, noInputs(), invoke);
            auto secondRow = std::make_shared<AsyncGraphExecution>(executor, graph, noInputs(), invoke);
            auto [firstResult, secondResult] = co_await (firstRow->run() && secondRow->run());
            EXPECT_EQ(invocations, 4);
            EXPECT_NE(successValues(firstResult)[0].get<int64_t>(), successValues(firstResult)[1].get<int64_t>());
            EXPECT_NE(successValues(firstResult)[0].get<int64_t>(), successValues(secondResult)[0].get<int64_t>());
        });
}

TEST(AsyncGraphExecutionTest, CancellingTheRootWaitAlsoCancelsAndDrainsItsOwnedGraph)
{
    AsyncCallGraphBuilder builder{{}};
    const auto producer = builder.call(makeAsyncTestOperator("Producer", {INTEGER}), {});
    builder.exportOutput(producer->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            asio::cancellation_signal stopRow;
            bool active = false;
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr, SharedRuntimeValues) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    active = true;
                    const auto error = co_await pause(10min);
                    active = false;
                    if (error == asio::error::operation_aborted)
                    {
                        co_return std::unexpected(AsyncError::cancelled());
                    }
                    co_return SharedRuntimeValues::make(int64_t{42});
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
    const auto producer = builder.call(makeAsyncTestOperator("Producer", {INTEGER}), {});
    builder.exportOutput(producer->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            asio::cancellation_signal stop;
            const auto body = [&]() -> asio::awaitable<void>
            {
                auto execution = std::make_shared<AsyncGraphExecution>(
                    executor,
                    graph,
                    noInputs(),
                    [](AsyncCall::Ptr, SharedRuntimeValues) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                    { co_return SharedRuntimeValues::make(int64_t{42}); });
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
    const auto producer = builder.call(makeAsyncTestOperator("Producer", {INTEGER}), {});
    builder.exportOutput(producer->output(0), QualifiedIdentifier::parse("OUT"));
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    run(
        [graph](SharedAsyncResult::Strand executor) -> asio::awaitable<void>
        {
            bool active = false;
            auto execution = std::make_shared<AsyncGraphExecution>(
                executor,
                graph,
                noInputs(),
                [&](AsyncCall::Ptr, SharedRuntimeValues) -> asio::awaitable<AsyncResult<SharedRuntimeValues>>
                {
                    active = true;
                    const auto error = co_await pause(10min);
                    active = false;
                    if (error == asio::error::operation_aborted)
                    {
                        co_return std::unexpected(AsyncError::cancelled());
                    }
                    co_return SharedRuntimeValues::make(int64_t{42});
                });
            const auto result = co_await (observe(execution->run()) && after(2ms, [execution] { execution->cancel(); }));
            EXPECT_EQ(errorCode(std::get<0>(result)), asio::error::operation_aborted);
            EXPECT_FALSE(active);
        });
}

}

/// NOLINTEND(readability-magic-numbers)
