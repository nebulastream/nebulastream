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
#include <any>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <Async/HandoffChannel.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncError.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncSourceConfig.hpp>
#include <Sources/PreparedAsyncCall.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Any.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp> /// NOLINT(misc-include-cleaner) Public Logger facade; cleaner misattributes setupLogging.
#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <gtest/gtest.h>
#include <AsyncSource.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES::Test
{
struct AsyncSourceTestOperator;
}

namespace NES
{
template <>
struct Reflector<Test::AsyncSourceTestOperator>
{
    Reflected operator()(const Test::AsyncSourceTestOperator&, const ReflectionContext&) const;
};

template <>
struct Unreflector<Test::AsyncSourceTestOperator>
{
    Test::AsyncSourceTestOperator operator()(const Reflected&, const ReflectionContext&) const;
};
}

namespace NES::Test
{
namespace
{
struct AsyncSourceTestStats
{
    std::atomic<size_t> preparations{0};
    std::atomic<size_t> attempts{0};
    std::atomic<size_t> operatorStateConstructions{0};
    std::atomic<size_t> operatorStateDestructions{0};
    std::atomic<size_t> livePrepared{0};
    std::atomic<size_t> livePreparedAtOperatorStateDestruction{0};
    std::atomic<size_t> slotStateConstructions{0};
    std::atomic<size_t> slotPreparations{0};
    std::atomic<size_t> maxPreparationsPerSlot{0};
};

struct AsyncSourceTestOperatorState
{
    explicit AsyncSourceTestOperatorState(std::shared_ptr<AsyncSourceTestStats> stats) : stats(std::move(stats))
    {
        ++this->stats->operatorStateConstructions;
    }

    AsyncSourceTestOperatorState(const AsyncSourceTestOperatorState&) = delete;
    AsyncSourceTestOperatorState& operator=(const AsyncSourceTestOperatorState&) = delete;
    AsyncSourceTestOperatorState(AsyncSourceTestOperatorState&&) = delete;
    AsyncSourceTestOperatorState& operator=(AsyncSourceTestOperatorState&&) = delete;

    ~AsyncSourceTestOperatorState()
    {
        stats->livePreparedAtOperatorStateDestruction.store(stats->livePrepared.load());
        ++stats->operatorStateDestructions;
    }

    std::shared_ptr<AsyncSourceTestStats> stats;
};

struct AsyncSourceTestSlotState
{
    explicit AsyncSourceTestSlotState(std::shared_ptr<AsyncSourceTestStats> stats) : stats(std::move(stats))
    {
        ++this->stats->slotStateConstructions;
    }

    std::shared_ptr<AsyncSourceTestStats> stats;
    size_t preparations = 0;
};

class AsyncSourceTestPreparedCall final : public PreparedAsyncCall
{
public:
    AsyncSourceTestPreparedCall(SharedRuntimeValues result,
                                std::shared_ptr<AsyncSourceTestStats> stats,
                                size_t retryLimit,
                                size_t failures,
                                bool retryable)
        : PreparedAsyncCall(retryLimit), result(std::move(result)), stats(std::move(stats)), failures(failures), retryable(retryable)
    {
        ++this->stats->livePrepared;
    }

    AsyncSourceTestPreparedCall(const AsyncSourceTestPreparedCall&) = delete;
    AsyncSourceTestPreparedCall& operator=(const AsyncSourceTestPreparedCall&) = delete;
    AsyncSourceTestPreparedCall(AsyncSourceTestPreparedCall&&) = delete;
    AsyncSourceTestPreparedCall& operator=(AsyncSourceTestPreparedCall&&) = delete;

    ~AsyncSourceTestPreparedCall() override { --stats->livePrepared; }

    static void setExecutionDelay(std::chrono::milliseconds value) { delay = value; }

    boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> executeAttempt() override
    {
        const auto executor = co_await boost::asio::this_coro::executor;
        if (delay.count() > 0)
        {
            boost::asio::steady_timer timer{executor};
            timer.expires_after(delay);
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
        if (++stats->attempts <= failures)
        {
            co_return std::unexpected(AsyncError{.exception = IOError(retryable ? "test invocation failed" : "permanent test failure"),
                                                 .kind = retryable ? AsyncError::Type::Retryable : AsyncError::Type::Permanent});
        }
        co_return result;
    }

private:
    SharedRuntimeValues result;
    std::shared_ptr<AsyncSourceTestStats> stats;
    size_t failures;
    bool retryable;
    inline static std::chrono::milliseconds delay{0};
};
}

struct AsyncSourceTestOperator
{
    std::string label;
    DataType outputType;
    SharedRuntimeValues result;
    std::shared_ptr<AsyncSourceTestStats> stats;
    size_t retryLimit = 0;
    size_t failures = 0;
    bool retryable = true;
    bool preparationFailure = false;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) implements the operator interface
    [[nodiscard]] std::string_view getType() const { return label; }

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) implements the operator interface
    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getWrittenFields() const
    {
        return Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("OUT"), outputType}};
    }

    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext& context, const AsyncRowHandle&) const
    {
        auto& operatorState = context.getOperatorState<AsyncSourceTestOperatorState>(stats);
        (void)operatorState;
        auto& slotState = context.getSlotState<AsyncSourceTestSlotState>(stats);
        const auto slotPreparations = ++slotState.preparations;
        ++stats->slotPreparations;
        auto previousMaximum = stats->maxPreparationsPerSlot.load();
        while (previousMaximum < slotPreparations
               && !stats->maxPreparationsPerSlot.compare_exchange_weak(previousMaximum, slotPreparations))
        {
        }
        ++stats->preparations;
        if (preparationFailure)
        {
            return std::unexpected(
                AsyncError{.exception = IOError("Preparation failures must never be retried"), .kind = AsyncError::Type::Retryable});
        }
        return std::make_unique<AsyncSourceTestPreparedCall>(result, stats, retryLimit, failures, retryable);
    }

    static AsyncSourceTestOperator create()
    {
        return {.label = "FixedResult",
                .outputType = DataType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE},
                .result = {},
                .stats = std::make_shared<AsyncSourceTestStats>(),
                .retryLimit = 0,
                .failures = 0,
                .retryable = true,
                .preparationFailure = false};
    }
};
}

namespace NES
{
Reflected Reflector<Test::AsyncSourceTestOperator>::operator()(const Test::AsyncSourceTestOperator&, const ReflectionContext&) const
{
    throw CannotSerialize("In-process async source test fixture is not serializable");
}

Test::AsyncSourceTestOperator Unreflector<Test::AsyncSourceTestOperator>::operator()(const Reflected&, const ReflectionContext&) const
{
    throw CannotDeserialize("In-process async source test fixture is not serializable");
}
}

namespace NES
{
namespace
{
class AsyncSourceValueTest : public Testing::BaseUnitTest
{
public:
    /// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
    static void SetUpTestSuite() { Logger::setupLogging("AsyncSourceValueTest.log", LogLevel::LOG_DEBUG, false); }

    /// NOLINTEND(misc-include-cleaner)

    /// Exercise the actual source's validation path, not just a scalar conversion helper.
    static std::shared_ptr<NES::Test::AsyncSourceTestStats> run(const SharedRuntimeValues& result, const DataType declaredType)
    {
        return run(result, declaredType, 0, 0, 1, 1, 1, true, false, {}, std::chrono::milliseconds{0}, false);
    }

    static std::shared_ptr<NES::Test::AsyncSourceTestStats>
    run(const SharedRuntimeValues& result, const DataType declaredType, size_t retryLimit)
    {
        return run(result, declaredType, retryLimit, 0, 1, 1, 1, true, false, {}, std::chrono::milliseconds{0}, false);
    }

    static std::shared_ptr<NES::Test::AsyncSourceTestStats>
    run(const SharedRuntimeValues& result, const DataType declaredType, size_t retryLimit, size_t failures)
    {
        return run(result, declaredType, retryLimit, failures, 1, 1, 1, true, false, {}, std::chrono::milliseconds{0}, false);
    }

    static std::shared_ptr<NES::Test::AsyncSourceTestStats> run(const SharedRuntimeValues& result,
                                                                const DataType declaredType,
                                                                size_t retryLimit,
                                                                size_t failures,
                                                                size_t rowsPerBuffer,
                                                                size_t bufferCount,
                                                                size_t concurrency)
    {
        return run(result,
                   declaredType,
                   retryLimit,
                   failures,
                   rowsPerBuffer,
                   bufferCount,
                   concurrency,
                   true,
                   false,
                   {},
                   std::chrono::milliseconds{0},
                   false);
    }

    static std::shared_ptr<NES::Test::AsyncSourceTestStats> run(const SharedRuntimeValues& result,
                                                                const DataType declaredType,
                                                                size_t retryLimit,
                                                                size_t failures,
                                                                size_t rowsPerBuffer,
                                                                size_t bufferCount,
                                                                size_t concurrency,
                                                                bool retryable,
                                                                bool preparationFailure,
                                                                std::shared_ptr<NES::Test::AsyncSourceTestStats> stats)
    {
        return run(result,
                   declaredType,
                   retryLimit,
                   failures,
                   rowsPerBuffer,
                   bufferCount,
                   concurrency,
                   retryable,
                   preparationFailure,
                   std::move(stats),
                   std::chrono::milliseconds{0},
                   false);
    }

    static std::shared_ptr<NES::Test::AsyncSourceTestStats> run(const SharedRuntimeValues& result,
                                                                const DataType declaredType,
                                                                size_t retryLimit,
                                                                size_t failures,
                                                                size_t rowsPerBuffer,
                                                                size_t bufferCount,
                                                                size_t concurrency,
                                                                bool retryable,
                                                                bool preparationFailure,
                                                                std::shared_ptr<NES::Test::AsyncSourceTestStats> stats,
                                                                std::chrono::milliseconds executionDelay)
    {
        return run(result,
                   declaredType,
                   retryLimit,
                   failures,
                   rowsPerBuffer,
                   bufferCount,
                   concurrency,
                   retryable,
                   preparationFailure,
                   std::move(stats),
                   executionDelay,
                   false);
    }

    static std::shared_ptr<NES::Test::AsyncSourceTestStats> run(const SharedRuntimeValues& result,
                                                                const DataType declaredType,
                                                                size_t retryLimit,
                                                                size_t failures,
                                                                size_t rowsPerBuffer,
                                                                size_t bufferCount,
                                                                size_t concurrency,
                                                                bool retryable,
                                                                bool preparationFailure,
                                                                std::shared_ptr<NES::Test::AsyncSourceTestStats> stats,
                                                                std::chrono::milliseconds executionDelay,
                                                                bool twoGraphNodes)
    {
        if (!stats)
        {
            stats = std::make_shared<NES::Test::AsyncSourceTestStats>();
        }
        const auto channel = std::make_shared<HandoffChannel>(2);
        const std::string type{"FixedResult"};
        const SourceCatalog catalog;
        const Schema<UnqualifiedUnboundField, Ordered> schema{UnqualifiedUnboundField{Identifier::parse("RESULT"), declaredType}};
        AsyncCallGraphBuilder builder{schema};
        auto nativeConfig = NES::Test::AsyncSourceTestOperator::create();
        nativeConfig.label = type;
        nativeConfig.outputType = declaredType;
        nativeConfig.result = result;
        nativeConfig.stats = stats;
        nativeConfig.retryLimit = retryLimit;
        nativeConfig.failures = failures;
        nativeConfig.retryable = retryable;
        nativeConfig.preparationFailure = preparationFailure;
        NES::Test::AsyncSourceTestPreparedCall::setExecutionDelay(executionDelay);
        const auto producer = builder.call(AsyncOperator{nativeConfig}, {});
        if (twoGraphNodes)
        {
            const auto consumer = builder.call(AsyncOperator{nativeConfig}, {producer});
            builder.exportOutput(consumer->output(0), QualifiedIdentifier::parse("RESULT"));
        }
        else
        {
            builder.exportOutput(producer->output(0), QualifiedIdentifier::parse("RESULT"));
        }
        const AsyncSourceConfig config{.channel = channel, .calls = builder.build(), .maxConcurrency = concurrency};
        const auto descriptor = catalog.getAnonymousSource(
            PhysicalSourceBuilder{GeneralSourceConfig{.host = Host{"localhost:8080"}, .maxInflightBuffers = std::nullopt},
                                  PluginSourceConfiguration{Identifier::parse("ASYNC"), ExplicitAny{std::any{config}}},
                                  InputFormatterDescriptor{Identifier::parse("NATIVE"), ExplicitAny{std::any{std::monostate{}}}}},
            schema);
        EXPECT_TRUE(descriptor.has_value());
        if (!descriptor)
        {
            return stats;
        }
        const auto& installedConfig = descriptor->getPluginData().getAs<AsyncSourceConfig>();
        EXPECT_EQ(installedConfig.calls.getCalls().front(), producer);
        const auto provider = BufferManager::create(
            size_t{1024} * size_t{1024}, 0.1, BufferAlignment{64}, 4096, std::make_shared<NesDefaultMemoryAllocator>());
        {
            auto sink = channel->takeSender();
            AsyncSource source{installedConfig, *descriptor};
            channel->validate();
            source.open(provider);
            for (size_t buffer = 0; buffer < bufferCount; ++buffer)
            {
                auto input = provider->getBufferBlocking();
                input.setNumberOfTuples(rowsPerBuffer);
                EXPECT_EQ(sink.tryPush(input), HandoffChannel::PushResult::PUSHED);
            }
            sink.close();
            for (size_t buffer = 0; buffer < bufferCount; ++buffer)
            {
                auto output = provider->getBufferBlocking();
                const auto filled = source.fillTupleBuffer(output, std::stop_token{});
                EXPECT_FALSE(filled.isEoS());
            }
            source.close();
        }
        provider->destroy();
        return stats;
    }
};
}

#ifndef NO_ASSERT
namespace
{
class AsyncSourceValueDeathTest : public AsyncSourceValueTest
{
public:
    void SetUp() override
    {
        AsyncSourceValueTest::SetUp();
        GTEST_FLAG_SET(death_test_style, "threadsafe");
    }
};

TEST_F(AsyncSourceValueDeathTest, WrongResultArityViolatesInvariant)
{
    SKIP_IF_TSAN();
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_DEATH(run(SharedRuntimeValues{}, type), "Invariant violated.*produced.*values");
}

TEST_F(AsyncSourceValueDeathTest, WrongResultTypeViolatesInvariant)
{
    SKIP_IF_TSAN();
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_DEATH(run(SharedRuntimeValues::make(uint64_t{42}), type), "Invariant violated.*output.*has type");
}

TEST_F(AsyncSourceValueDeathTest, NullForNonNullableOutputViolatesInvariant)
{
    SKIP_IF_TSAN();
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    EXPECT_DEATH(run(SharedRuntimeValues::make(RuntimeValue::null(nullable)), type), "Invariant violated.*output.*has type");
}

TEST_F(AsyncSourceValueDeathTest, NonNullableValueForNullableOutputViolatesInvariant)
{
    SKIP_IF_TSAN();
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    EXPECT_DEATH(run(SharedRuntimeValues::make(int64_t{42}), type), "Invariant violated.*output.*has type");
}

TEST_F(AsyncSourceValueDeathTest, NullableValueForNonNullableOutputViolatesInvariant)
{
    SKIP_IF_TSAN();
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    EXPECT_DEATH(run(SharedRuntimeValues::make(RuntimeValue{nullable, int64_t{42}}), type), "Invariant violated.*output.*has type");
}
}
#endif

TEST_F(AsyncSourceValueTest, InvalidArgumentsFailPreparationOnceWithoutExecutingOrRetrying)
{
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    auto stats = std::make_shared<NES::Test::AsyncSourceTestStats>();
    EXPECT_THROW(run(SharedRuntimeValues::make(int64_t{42}), type, 8, 0, 1, 1, 1, true, true, stats), Exception);
    EXPECT_EQ(stats->preparations.load(), 1);
    EXPECT_EQ(stats->attempts.load(), 0);
}

TEST_F(AsyncSourceValueTest, PermanentFailureIsNotRetriedEvenWithExplicitRetries)
{
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    auto stats = std::make_shared<NES::Test::AsyncSourceTestStats>();
    EXPECT_THROW(run(SharedRuntimeValues::make(int64_t{42}), type, 3, 1, 1, 1, 1, false, false, stats), Exception);
    EXPECT_EQ(stats->attempts.load(), 1);
}

TEST_F(AsyncSourceValueTest, NativeConfigurationSurvivesSourceConstructionAndExecution)
{
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto stats = run(SharedRuntimeValues::make(int64_t{42}), type, 0);
    EXPECT_EQ(stats->preparations.load(), 1);
    EXPECT_EQ(stats->attempts.load(), 1);
}

TEST_F(AsyncSourceValueTest, ValueOwnedBuffersSupportConcurrentRowsAndOrderedRemoval)
{
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto stats = run(SharedRuntimeValues::make(int64_t{42}), type, 0, 0, 8, 2, 4);
    EXPECT_EQ(stats->attempts.load(), 16);
}

TEST_F(AsyncSourceValueTest, FailedRowsDrainBeforeBorrowedBuffersAreCleared)
{
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    auto stats = std::make_shared<NES::Test::AsyncSourceTestStats>();
    EXPECT_THROW(run(SharedRuntimeValues::make(int64_t{42}), type, 0, 1, 8, 2, 4, true, false, stats), Exception);
    EXPECT_GE(stats->attempts.load(), 1);
}

TEST_F(AsyncSourceValueTest, DefaultZeroRetriesFailsAfterSingleAttempt)
{
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    auto stats = std::make_shared<NES::Test::AsyncSourceTestStats>();
    EXPECT_THROW(run(SharedRuntimeValues::make(int64_t{42}), type, 0, 2, 1, 1, 1, true, false, stats), Exception);
    EXPECT_EQ(stats->attempts.load(), 1);
}

TEST_F(AsyncSourceValueTest, PerRowRetriesSucceedWithinBudget)
{
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto stats = run(SharedRuntimeValues::make(int64_t{42}), type, 2, 2);
    EXPECT_EQ(stats->attempts.load(), 3);
}

TEST_F(AsyncSourceValueTest, PerRowRetryCountBoundsTheNumberOfAttempts)
{
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    auto stats = std::make_shared<NES::Test::AsyncSourceTestStats>();
    EXPECT_THROW(run(SharedRuntimeValues::make(int64_t{42}), type, 1, 2, 1, 1, 1, true, false, stats), Exception);
    EXPECT_EQ(stats->attempts.load(), 2);
}

TEST_F(AsyncSourceValueTest, ConcurrentRowsShareOperatorStateButUseIndependentSlotState)
{
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    auto stats = std::make_shared<NES::Test::AsyncSourceTestStats>();
    run(SharedRuntimeValues::make(int64_t{42}), type, 0, 0, 8, 1, 4, true, false, stats, std::chrono::milliseconds{25});
    EXPECT_EQ(stats->operatorStateConstructions.load(), 1);
    EXPECT_EQ(stats->operatorStateDestructions.load(), 1);
    EXPECT_GE(stats->slotStateConstructions.load(), 2);
    EXPECT_GE(stats->maxPreparationsPerSlot.load(), 2);
    EXPECT_EQ(stats->preparations.load(), 8);
    EXPECT_EQ(stats->livePreparedAtOperatorStateDestruction.load(), 0);
}

TEST_F(AsyncSourceValueTest, SeparateSourceExecutionsHaveIndependentOperatorState)
{
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    auto stats = std::make_shared<NES::Test::AsyncSourceTestStats>();
    run(SharedRuntimeValues::make(int64_t{42}), type, 0, 0, 1, 1, 2, true, false, stats);
    run(SharedRuntimeValues::make(int64_t{42}), type, 0, 0, 1, 1, 2, true, false, stats);
    EXPECT_EQ(stats->operatorStateConstructions.load(), 2);
    EXPECT_EQ(stats->operatorStateDestructions.load(), 2);
    EXPECT_EQ(stats->livePreparedAtOperatorStateDestruction.load(), 0);
}

TEST_F(AsyncSourceValueTest, GraphCallsUsingTheSameConfigHaveIndependentOperatorState)
{
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    auto stats = std::make_shared<NES::Test::AsyncSourceTestStats>();
    run(SharedRuntimeValues::make(int64_t{42}), type, 0, 0, 2, 1, 2, true, false, stats, std::chrono::milliseconds{10}, true);
    EXPECT_EQ(stats->operatorStateConstructions.load(), 2);
    EXPECT_EQ(stats->operatorStateDestructions.load(), 2);
    EXPECT_EQ(stats->preparations.load(), 4);
    EXPECT_EQ(stats->livePreparedAtOperatorStateDestruction.load(), 0);
}

TEST_F(AsyncSourceValueTest, NullableOutputAcceptsACorrectlyTypedNonNullValue)
{
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const auto stats = run(SharedRuntimeValues::make(RuntimeValue{type, int64_t{42}}), type);
    EXPECT_EQ(stats->attempts.load(), 1);
}
}

/// NOLINTEND(readability-magic-numbers)
