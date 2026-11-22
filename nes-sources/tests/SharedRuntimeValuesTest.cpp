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
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncError.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncOperatorReflection.hpp>
#include <Sources/PreparedAsyncCall.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp> /// NOLINT(misc-include-cleaner) Public Logger facade; cleaner misattributes setupLogging.
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp> /// NOLINT(misc-include-cleaner) Public Asio provider; cleaner misattributes co_spawn.
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <AsyncTestOperator.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

/// NOLINTBEGIN(bugprone-unchecked-optional-access) GoogleTest fatal assertions guard optional reads
namespace NES
{
namespace
{
boost::asio::awaitable<SharedRuntimeValues> echoAfterSuspension(SharedRuntimeValues values)
{
    const auto borrowed = values[0].get<std::string_view>();
    boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, std::chrono::milliseconds{1}};
    co_await timer.async_wait(boost::asio::use_awaitable);
    co_return SharedRuntimeValues::make(borrowed, values[1].toOwned());
}

struct EchoStats
{
    size_t attempts = 0;
    size_t preparations = 0;
    size_t livePrepared = 0;
};

struct EchoOperator
{
    struct Prepared final : PreparedAsyncCall
    {
        Prepared(std::shared_ptr<EchoStats> stats, SharedRuntimeValues arguments, std::optional<size_t> retries)
            : PreparedAsyncCall(retries), stats(std::move(stats)), arguments(std::move(arguments))
        {
            ++this->stats->livePrepared;
        }

        Prepared(const Prepared&) = delete;
        Prepared& operator=(const Prepared&) = delete;
        Prepared(Prepared&&) = delete;
        Prepared& operator=(Prepared&&) = delete;

        ~Prepared() override { --stats->livePrepared; }

        boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> executeAttempt() override
        {
            ++stats->attempts;
            if (failAttempt)
            {
                co_return std::unexpected(AsyncError{.exception = IOError("attempt failed"), .kind = AsyncError::Type::Retryable});
            }
            co_return co_await echoAfterSuspension(arguments);
        }

        std::shared_ptr<EchoStats> stats;
        SharedRuntimeValues arguments;
        bool failAttempt = false;
    };

    std::shared_ptr<EchoStats> stats = std::make_shared<EchoStats>();
    std::optional<size_t> retryPolicy = size_t{4};
    bool failPreparation = false;
    bool failAttempt = false;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) implements the operator interface
    [[nodiscard]] std::string_view getType() const { return "TEST_ECHO"; }

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) implements the operator interface
    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getOutputSchema() const { return {}; }

    [[nodiscard]] EchoOperator withInferredArguments(const AsyncArgumentsWithConstants&) const { return *this; }

    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>>
    prepare(AsyncExecutionContext& context, SharedRuntimeValues arguments) const
    {
        ++context.getSlotState<size_t>();
        ++stats->preparations;
        if (failPreparation)
        {
            return std::unexpected(AsyncError{.exception = ArithmeticalError("preparation failed")});
        }
        auto prepared = std::make_unique<Prepared>(stats, std::move(arguments), retryPolicy);
        prepared->failAttempt = failAttempt;
        return prepared;
    }
};
}

TEST(SharedRuntimeValuesTest, CopyAndMoveRetainSmallLargeAndBinaryPayloads)
{
    for (const auto& text : {std::string{"short"}, std::string(8192, 'x'), std::string{"a\0b", 3}})
    {
        auto original = SharedRuntimeValues::make(text, uint64_t{42});
        auto retained = original;
        original = SharedRuntimeValues::make();
        auto moved = std::move(retained);
        EXPECT_EQ(moved[0].get<std::string_view>(), text);
        EXPECT_EQ(moved[1].get<uint64_t>(), 42);
    }
}

/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(SharedRuntimeValuesTest, ArgumentsAndResultsOwnTheirStorageAcrossSuspension)
{
    boost::asio::io_context context;
    auto input = SharedRuntimeValues::make(std::string{"small string"}, std::numeric_limits<uint64_t>::max());
    auto result = boost::asio::co_spawn(
        context,
        echoAfterSuspension(input),
        boost::asio::use_future);
    input = SharedRuntimeValues::make();
    context.run();
    const auto output = result.get();
    EXPECT_EQ(output[0].get<std::string_view>(), "small string");
    EXPECT_EQ(output[1].get<uint64_t>(), std::numeric_limits<uint64_t>::max());
}
/// NOLINTEND(misc-include-cleaner)

/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(SharedRuntimeValuesTest, PreparationOwnsArgumentsAndFixesRetryMetadata)
{
    EchoOperator concrete;
    const AsyncOperator op{concrete};
    AsyncExecutionContext executionContext;
    auto arguments = SharedRuntimeValues::make(std::string{"retained"}, uint64_t{42});
    const auto prepared = op.prepare(executionContext, arguments);
    arguments = SharedRuntimeValues::make();
    EXPECT_EQ(concrete.stats->preparations, 1);
    ASSERT_TRUE(prepared);
    EXPECT_EQ((*prepared)->getRetryOverride(), size_t{4});
    concrete.retryPolicy = 0;
    EXPECT_EQ((*prepared)->getRetryOverride(), size_t{4});
    boost::asio::io_context context;
    const auto run = [&]() -> boost::asio::awaitable<void>
    {
        for (size_t attempt = 0; attempt < 2; ++attempt)
        {
            const auto result = co_await (*prepared)->executeAttempt();
            EXPECT_TRUE(result);
            if (result)
            {
                EXPECT_EQ((*result)[0].get<std::string_view>(), "retained");
                EXPECT_EQ((*result)[1].get<uint64_t>(), 42);
            }
        }
    };
    auto result = boost::asio::co_spawn(
        context, run(), boost::asio::use_future);
    context.run();
    result.get();
    EXPECT_EQ(concrete.stats->preparations, 1);
    EXPECT_EQ(concrete.stats->attempts, 2);
    const auto zeroRetry = concrete.prepare(executionContext, {});
    ASSERT_TRUE(zeroRetry);
    concrete.retryPolicy = std::nullopt;
    const auto defaultRetry = concrete.prepare(executionContext, {});
    ASSERT_TRUE(defaultRetry);
    EXPECT_EQ((*zeroRetry)->getRetryOverride(), size_t{0});
    EXPECT_EQ((*defaultRetry)->getRetryOverride(), std::nullopt);
}
/// NOLINTEND(misc-include-cleaner)

/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(SharedRuntimeValuesTest, InvokeOnceOwnsPreparedCallAcrossSuspension)
{
    const EchoOperator concrete;
    boost::asio::io_context context;
    auto arguments = SharedRuntimeValues::make(std::string{"retained"}, uint64_t{42});
    auto result = boost::asio::co_spawn(
        context,
        invokeOnce(AsyncOperator{concrete}, arguments),
        boost::asio::use_future);
    arguments = {};
    context.run();
    const auto values = result.get();
    ASSERT_TRUE(values);
    EXPECT_EQ((*values)[0].get<std::string_view>(), "retained");
    EXPECT_EQ((*values)[1].get<uint64_t>(), 42);
    EXPECT_EQ(concrete.stats->preparations, 1);
    EXPECT_EQ(concrete.stats->attempts, 1);
    EXPECT_EQ(concrete.stats->livePrepared, 0);
}
/// NOLINTEND(misc-include-cleaner)

TEST(SharedRuntimeValuesTest, InvokeOncePropagatesPreparationAndAttemptFailuresWithoutRetries)
{
    for (const bool failPreparation : {false, true})
    {
        EchoOperator concrete;
        concrete.failPreparation = failPreparation;
        concrete.failAttempt = true;
        boost::asio::io_context context;
        auto result = boost::asio::co_spawn(context, invokeOnce(AsyncOperator{concrete}, {}), boost::asio::use_future);
        context.run();
        const auto failure = result.get();
        ASSERT_FALSE(failure);
        EXPECT_EQ(failure.error().exception.code(), failPreparation ? ErrorCode::ArithmeticalError : ErrorCode::IOError);
        EXPECT_EQ(failure.error().kind, failPreparation ? AsyncError::Type::Permanent : AsyncError::Type::Retryable);
        EXPECT_EQ(concrete.stats->preparations, 1);
        EXPECT_EQ(concrete.stats->attempts, failPreparation ? 0 : 1);
        EXPECT_EQ(concrete.stats->livePrepared, 0);
    }
}

TEST(SharedRuntimeValuesTest, InvokeOnceRestoresCallerCancellationPolicyOnReturnedErrors)
{
    for (const bool policy : {false, true})
    {
        for (const bool failPreparation : {false, true})
        {
            EchoOperator concrete;
            concrete.failPreparation = failPreparation;
            concrete.failAttempt = true;
            boost::asio::io_context context;
            const auto run = [&]() -> boost::asio::awaitable<void>
            {
                co_await boost::asio::this_coro::throw_if_cancelled(policy);
                const auto result = co_await invokeOnce(AsyncOperator{concrete}, {});
                EXPECT_FALSE(result);
                const bool restoredPolicy = co_await boost::asio::this_coro::throw_if_cancelled();
                EXPECT_EQ(restoredPolicy, policy);
            };
            auto completion = boost::asio::co_spawn(context, run(), boost::asio::use_future);
            context.run();
            completion.get();
        }
    }
}

TEST(SharedRuntimeValuesTest, PreparedCallDefaultsToExecutorRetryPolicyAndMayReturnEmptySuccess)
{
    struct EmptyOperator
    {
        struct EmptyPrepared final : PreparedAsyncCall
        {
            EmptyPrepared() = default;
            EmptyPrepared(const EmptyPrepared&) = delete;
            EmptyPrepared& operator=(const EmptyPrepared&) = delete;
            EmptyPrepared(EmptyPrepared&&) = delete;
            EmptyPrepared& operator=(EmptyPrepared&&) = delete;
            ~EmptyPrepared() override = default;

            [[nodiscard]] boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> executeAttempt() override
            {
                co_return SharedRuntimeValues{};
            }
        };

        /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) implements the operator interface
        [[nodiscard]] std::string_view getType() const { return "EMPTY"; }

        /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) implements the operator interface
        [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getOutputSchema() const { return {}; }

        [[nodiscard]] EmptyOperator withInferredArguments(const AsyncArgumentsWithConstants&) const { return *this; }

        /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) implements the operator interface
        [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext&, SharedRuntimeValues) const
        {
            return std::make_unique<EmptyPrepared>();
        }
    };

    const EmptyOperator concrete;

    AsyncOperator op{concrete};
    AsyncExecutionContext executionContext;
    const auto prepared = op.prepare(executionContext, {});
    ASSERT_TRUE(prepared);
    EXPECT_EQ((*prepared)->getRetryOverride(), std::nullopt);
    EXPECT_EQ(op.maxConcurrency(), std::nullopt);
    boost::asio::io_context context;
    auto result = boost::asio::co_spawn(context, invokeOnce(std::move(op), {}), boost::asio::use_future);
    context.run();
    const auto values = result.get();
    ASSERT_TRUE(values);
    EXPECT_TRUE(values->empty());
}

TEST(SharedRuntimeValuesTest, EmptyAndTypedNullAreDifferent)
{
    static_assert(std::is_default_constructible_v<SharedRuntimeValues>);
    const SharedRuntimeValues empty;
    EXPECT_TRUE(empty.empty());
    EXPECT_EQ(empty.size(), 0);
    EXPECT_TRUE(SharedRuntimeValues::make().empty());
    const auto& copy = empty;
    auto moved = copy;
    const auto retained = std::move(moved);
    EXPECT_TRUE(retained.empty());
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const auto values = SharedRuntimeValues::make(RuntimeValue::null(nullable));
    EXPECT_EQ(values.size(), 1);
    EXPECT_TRUE(values[0].isNull());
    EXPECT_EQ(values[0].getDataType(), nullable);
}

TEST(SharedRuntimeValuesTest, BindingPreservesConstantPositionsAndTypedNulls)
{
    const DataType integer{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    const DataType nullable{DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE};
    const AsyncArgumentsWithConstants binding{
        {integer, std::nullopt}, {integer, RuntimeValue{uint64_t{4}}}, {nullable, RuntimeValue::null(nullable)}};
    /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) the fixture initializes three constant positions
    EXPECT_EQ(binding[0].first, integer);
    EXPECT_FALSE(binding[0].second);
    const auto& integerConstant = binding[1].second;
    ASSERT_TRUE(integerConstant.has_value());
    EXPECT_EQ(integerConstant->view().get<uint64_t>(), 4);
    const auto& nullConstant = binding[2].second;
    ASSERT_TRUE(nullConstant.has_value());
    EXPECT_EQ(binding[2].first, nullable);
    EXPECT_TRUE(nullConstant->isNull());
    /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
}

TEST(SharedRuntimeValuesTest, InvokeOnceRetainsOwningConfigAndExplicitContextAcrossPreparedExecution)
{
    const EchoOperator concrete;
    AsyncExecutionContext context;
    boost::asio::io_context executor;
    auto completion = boost::asio::co_spawn(
        executor,
        invokeOnce(AsyncOperator{concrete}, context, SharedRuntimeValues::make(std::string{"s"}, uint64_t{7})),
        boost::asio::use_future);
    executor.run();
    const auto result = completion.get();
    ASSERT_TRUE(result);
    EXPECT_EQ(concrete.stats->preparations, 1);
    EXPECT_EQ(context.getSlotState<size_t>(), 1);
}

TEST(AsyncOperatorReflectionTest, PreservesTypedNativeConfigThroughUnreflection)
{
    const DataType integer{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    auto erased = makeAsyncTestOperator("TypedConfigTest", {integer}, 501);
    auto typed = erased.getAs<AsyncTestOperatorConfig>();
    const ReflectionContext reflection;
    const auto restored = reflection.unreflect<TypedAsyncOperator<AsyncTestOperatorConfig>>(reflection.reflect(typed));
    EXPECT_EQ(restored->label, "TypedConfigTest");
    EXPECT_EQ(restored->marker, 501);
    EXPECT_EQ(restored->resultTypes, std::vector<DataType>{integer});
}

TEST(AsyncExecutionContextTest, LazilyOwnsNoncopyableStateAndMovesIt)
{
    struct State
    {
        State() = default;
        State(const State&) = delete;
        State& operator=(const State&) = delete;
        State(State&&) = delete;
        State& operator=(State&&) = delete;
        ~State() = default;

        int value = 7;
    };

    AsyncExecutionContext context;
    auto& state = context.getSlotState<State>();
    EXPECT_EQ(state.value, 7);
    ++state.value;
    AsyncExecutionContext moved{std::move(context)};
    EXPECT_EQ(moved.getSlotState<State>().value, 8);
}

TEST(AsyncExecutionContextTest, ConstructsMoveOnlyNonDefaultStateOnceAndMovesItsIdentity)
{
    struct State
    {
        explicit State(std::unique_ptr<int> value) : value(std::move(value)) { }

        State(const State&) = delete;
        State& operator=(const State&) = delete;
        State(State&&) = delete;
        State& operator=(State&&) = delete;
        ~State() = default;

        std::unique_ptr<int> value;
    };

    AsyncExecutionContext context;
    auto& first = context.getSlotState<State>(std::make_unique<int>(17));
    const State* const identity = &first;
    auto& reused = context.getSlotState<State>(std::make_unique<int>(29));
    EXPECT_EQ(&reused, identity);
    EXPECT_EQ(*reused.value, 17);
    AsyncExecutionContext moved{std::move(context)};
    auto& retained = moved.getSlotState<State>(std::make_unique<int>(31));
    EXPECT_EQ(&retained, identity);
    EXPECT_EQ(*retained.value, 17);
}

TEST(AsyncExecutionContextTest, SlotAndOperatorScopesHaveIndependentStableIdentity)
{
    struct State
    {
        explicit State(int value) : value(value) { }

        int value;
    };

    AsyncExecutionContext original;
    auto sibling = original.createSlotContext();
    auto& slot = original.getSlotState<State>(1);
    auto& siblingSlot = sibling.getSlotState<State>(2);
    auto& op = original.getOperatorState<State>(3);
    auto& siblingOp = sibling.getOperatorState<State>(4);
    EXPECT_NE(&slot, &siblingSlot);
    EXPECT_NE(&slot, &op);
    EXPECT_EQ(&op, &siblingOp);
    EXPECT_EQ(siblingOp.value, 3);
    EXPECT_EQ(sibling.getSlotState<State>(5).value, 2);

    const State* const sharedIdentity = &siblingOp;
    original = AsyncExecutionContext{};
    EXPECT_EQ(&sibling.getOperatorState<State>(6), sharedIdentity);
    EXPECT_EQ(siblingOp.value, 3);
}

TEST(AsyncExecutionContextTest, DefaultContextsHaveIndependentOperatorsAndSlots)
{
    struct State
    {
        int value = 0;
    };

    AsyncExecutionContext first;
    AsyncExecutionContext second;
    auto firstSlot = first.createSlotContext();
    auto secondSlot = second.createSlotContext();
    auto& firstOperator = first.getOperatorState<State>();
    auto& secondOperator = second.getOperatorState<State>();
    EXPECT_NE(&firstOperator, &secondOperator);
    ++firstOperator.value;
    EXPECT_EQ(secondOperator.value, 0);
    EXPECT_NE(&firstSlot.getSlotState<State>(), &secondSlot.getSlotState<State>());
}

TEST(AsyncExecutionContextTest, ConstructsSharedMoveOnlyStateOnceAcrossConcurrentSiblingAccess)
{
    struct State
    {
        State(std::atomic<int>& constructions, std::unique_ptr<int> initial) : value(std::move(initial)) { ++constructions; }

        std::unique_ptr<int> value;
        std::atomic<int> updates{0};
    };

    std::atomic<int> constructions{0};
    AsyncExecutionContext owner;
    std::vector<AsyncExecutionContext> siblings;
    siblings.reserve(12);
    for (int i = 0; i < 12; ++i)
    {
        siblings.push_back(owner.createSlotContext());
    }
    std::vector<std::thread> threads;
    threads.reserve(siblings.size());
    for (auto& sibling : siblings)
    {
        threads.emplace_back(
            [siblingContext = &sibling, &constructions]
            {
                auto& state = siblingContext->getOperatorState<State>(constructions, std::make_unique<int>(71));
                ++state.updates;
            });
    }
    for (auto& thread : threads)
    {
        thread.join();
    }
    auto& state = owner.getOperatorState<State>(constructions, std::make_unique<int>(99));
    EXPECT_EQ(constructions.load(), 1);
    EXPECT_EQ(*state.value, 71);
    EXPECT_EQ(state.updates.load(), 12);
}

TEST(AsyncExecutionContextTest, PreparedCallRetainsBothScopesAcrossSuspension)
{
    struct State
    {
        int value = 0;
    };

    struct Prepared final : PreparedAsyncCall
    {
        explicit Prepared(AsyncExecutionContext context) : context(std::move(context)) { }

        boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> executeAttempt() override
        {
            auto& slot = context.getSlotState<State>();
            auto& op = context.getOperatorState<State>();
            boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, std::chrono::milliseconds{1}};
            co_await timer.async_wait(boost::asio::use_awaitable);
            ++slot.value;
            ++op.value;
            co_return SharedRuntimeValues::make(static_cast<uint64_t>(slot.value), static_cast<uint64_t>(op.value));
        }

        AsyncExecutionContext context;
    };

    AsyncExecutionContext parent;
    auto slotContext = parent.createSlotContext();
    auto prepared = std::make_unique<Prepared>(std::move(slotContext));
    parent = AsyncExecutionContext{};
    boost::asio::io_context executor;
    auto completion = boost::asio::co_spawn(executor, prepared->executeAttempt(), boost::asio::use_future);
    executor.run();
    const auto result = completion.get();
    ASSERT_TRUE(result);
    EXPECT_EQ((*result)[0].get<uint64_t>(), 1);
    EXPECT_EQ((*result)[1].get<uint64_t>(), 1);
}

TEST(AsyncExecutionContextTest, OperatorStateLivesUntilEverySlotAndPreparedContextIsReleased)
{
    struct State
    {
        explicit State(int& destructions) : destructions(destructions) { }

        State(const State&) = delete;
        State& operator=(const State&) = delete;
        State(State&&) = delete;
        State& operator=(State&&) = delete;

        ~State() { ++destructions; }

        int& destructions;
    };

    int destructions = 0;
    AsyncExecutionContext owner;
    owner.getOperatorState<State>(destructions);
    auto slot = owner.createSlotContext();
    auto prepared = owner.createSlotContext();
    owner = AsyncExecutionContext{};
    EXPECT_EQ(destructions, 0);
    slot = AsyncExecutionContext{};
    EXPECT_EQ(destructions, 0);
    prepared = AsyncExecutionContext{};
    EXPECT_EQ(destructions, 1);
}

TEST(AsyncExecutionContextTest, MoveAssignmentDestroysOldSlotBeforeItsOperatorOwner)
{
    std::vector<int> destructionOrder;

    struct OperatorState
    {
        explicit OperatorState(std::vector<int>& order) : order(order) { }

        OperatorState(const OperatorState&) = delete;
        OperatorState& operator=(const OperatorState&) = delete;
        OperatorState(OperatorState&&) = delete;
        OperatorState& operator=(OperatorState&&) = delete;

        ~OperatorState() { order.push_back(2); }

        std::vector<int>& order;
        int value = 41;
    };

    struct SlotState
    {
        SlotState(AsyncExecutionContext& owner, std::vector<int>& order) : owner(owner), order(order) { }

        SlotState(const SlotState&) = delete;
        SlotState& operator=(const SlotState&) = delete;
        SlotState(SlotState&&) = delete;
        SlotState& operator=(SlotState&&) = delete;

        ~SlotState()
        {
            EXPECT_EQ(owner.getOperatorState<OperatorState>(order).value, 41);
            order.push_back(1);
        }

        AsyncExecutionContext& owner;
        std::vector<int>& order;
    };

    AsyncExecutionContext destination;
    destination.getOperatorState<OperatorState>(destructionOrder);
    destination.getSlotState<SlotState>(destination, destructionOrder);
    destination = AsyncExecutionContext{};
    ASSERT_EQ(destructionOrder.size(), 2);
    EXPECT_EQ(destructionOrder[0], 1);
    EXPECT_EQ(destructionOrder[1], 2);
}

TEST(AsyncExecutionContextTest, SharedOperatorStateOutlivesItsContext)
{
    struct State
    {
        explicit State(int& destructions) : destructions(destructions) { }

        State(const State&) = delete;
        State& operator=(const State&) = delete;
        State(State&&) = delete;
        State& operator=(State&&) = delete;

        ~State() { ++destructions; }

        int& destructions;
    };

    int destructions = 0;
    std::shared_ptr<State> retained;
    {
        AsyncExecutionContext context;
        retained = context.getSharedOperatorState<State>(destructions);
    }
    EXPECT_EQ(destructions, 0);
    retained.reset();
    EXPECT_EQ(destructions, 1);
}

TEST(AsyncExecutionContextTest, SharedSlotStateRetainsOperatorDependencyAfterContextReplacement)
{
    struct OperatorState
    {
        explicit OperatorState(std::vector<int>& order) : order(order) { }

        OperatorState(const OperatorState&) = delete;
        OperatorState& operator=(const OperatorState&) = delete;
        OperatorState(OperatorState&&) = delete;
        OperatorState& operator=(OperatorState&&) = delete;

        ~OperatorState() { order.push_back(2); }

        std::vector<int>& order;
        int value = 41;
    };

    struct SlotState
    {
        SlotState(OperatorState& owner, std::vector<int>& order) : owner(owner), order(order) { }

        SlotState(const SlotState&) = delete;
        SlotState& operator=(const SlotState&) = delete;
        SlotState(SlotState&&) = delete;
        SlotState& operator=(SlotState&&) = delete;

        ~SlotState()
        {
            EXPECT_EQ(owner.value, 41);
            order.push_back(1);
        }

        OperatorState& owner;
        std::vector<int>& order;
    };

    std::vector<int> order;
    std::shared_ptr<SlotState> retained;
    {
        AsyncExecutionContext context;
        auto& owner = context.getOperatorState<OperatorState>(order);
        retained = context.getSharedSlotState<SlotState>(owner, order);
        context = AsyncExecutionContext{};
        EXPECT_TRUE(order.empty());
    }
    EXPECT_TRUE(order.empty());
    retained.reset();
    EXPECT_EQ(order, (std::vector<int>{1, 2}));
}

/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(AsyncExecutionContextTest, RejectsInconsistentStateType)
{
#ifndef NO_ASSERT
    SKIP_IF_TSAN();
    Logger::setupLogging(
        "AsyncExecutionContextDeathTest.log",
        LogLevel::LOG_DEBUG,
        false);
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    AsyncExecutionContext context;
    static_cast<void>(context.getSlotState<int>());
    EXPECT_DEATH(static_cast<void>(context.getSlotState<std::string>()), "Precondition violated");
#endif
}
/// NOLINTEND(misc-include-cleaner)
}

/// NOLINTEND(bugprone-unchecked-optional-access)
/// NOLINTEND(readability-magic-numbers)
