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

#include <any>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncFunction.hpp>
#include <Sources/AsyncSourceConfig.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Any.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <AsyncFunctionRegistry.hpp>
#include <AsyncSource.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <HandoffChannel.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES
{
namespace
{
class FixedResultFunction final : public AsyncFunction
{
public:
    FixedResultFunction(
        SharedRuntimeValues result, std::shared_ptr<std::atomic<size_t>> calls, std::optional<size_t> retryLimit, size_t failures)
        : result(std::move(result)), calls(std::move(calls)), retryLimit(retryLimit), failures(failures)
    {
    }

    boost::asio::awaitable<SharedRuntimeValues> call(SharedRuntimeValues) override
    {
        if (++*calls <= failures)
        {
            throw CannotReadFromSource("test invocation failed");
        }
        co_return result;
    }

    std::optional<size_t> maxRetries(const SharedRuntimeValues&) const override { return retryLimit; }

private:
    SharedRuntimeValues result;
    std::shared_ptr<std::atomic<size_t>> calls;
    std::optional<size_t> retryLimit;
    size_t failures;
};

class PreparationFailureFunction final : public AsyncFunction
{
public:
    PreparationFailureFunction(std::shared_ptr<std::atomic<size_t>> preparations, std::shared_ptr<std::atomic<size_t>> attempts)
        : preparations(std::move(preparations)), attempts(std::move(attempts))
    {
    }

    std::unique_ptr<PreparedAsyncCall> prepare(SharedRuntimeValues) override
    {
        ++*preparations;
        throw CannotReadFromSource("Invalid per-row arguments");
    }

    boost::asio::awaitable<SharedRuntimeValues> call(SharedRuntimeValues arguments) override
    {
        ++*attempts;
        co_return arguments;
    }

private:
    std::shared_ptr<std::atomic<size_t>> preparations;
    std::shared_ptr<std::atomic<size_t>> attempts;
};

class AsyncSourceValueTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("AsyncSourceValueTest.log", LogLevel::LOG_DEBUG, false); }

    /// Exercise the actual source's validation path, not just a scalar conversion helper.
    static void
    run(const SharedRuntimeValues& result,
        const DataType declaredType,
        const std::shared_ptr<std::atomic<size_t>>& calls,
        std::optional<size_t> retryLimit = std::nullopt,
        size_t failures = 0,
        size_t sourceRetries = 3,
        size_t rowsPerBuffer = 1,
        size_t bufferCount = 1,
        size_t concurrency = 1,
        std::function<std::unique_ptr<AsyncFunction>()> create = {})
    {
        const AsyncChannelId id{generateUUID()};
        const std::string functionType{"FixedResult_" + id.getRawValue()};
        const bool added = AsyncFunctionRegistry::instance().addEntry(
            functionType,
            [result, calls, retryLimit, failures, create = std::move(create)](
                AsyncFunctionRegistryArguments) -> std::unique_ptr<AsyncFunction>
            {
                if (create)
                {
                    return create();
                }
                return std::make_unique<FixedResultFunction>(result, calls, retryLimit, failures);
            });
        ASSERT_TRUE(added);
        SourceCatalog catalog;
        const Schema<UnqualifiedUnboundField, Ordered> schema{UnqualifiedUnboundField{Identifier::parse("RESULT"), declaredType}};
        AsyncCallGraphBuilder builder{schema};
        const auto producer = builder.call(functionType, {}, {declaredType});
        builder.exportOutput(producer->output(0), QualifiedIdentifier::parse("RESULT"));
        const AsyncSourceConfig config{
            .channel = id, .calls = builder.build(), .maxConcurrency = concurrency, .channelCapacity = 2, .maxRetries = sourceRetries};
        const auto descriptor = catalog.getAnonymousSource(
            PhysicalSourceBuilder{
                GeneralSourceConfig{.host = Host{"localhost:8080"}, .maxInflightBuffers = std::nullopt},
                PluginSourceConfiguration{Identifier::parse("ASYNC"), ExplicitAny{std::any{config}}},
                InputFormatterDescriptor{Identifier::parse("NATIVE"), ExplicitAny{std::any{std::monostate{}}}}},
            schema);
        ASSERT_TRUE(descriptor.has_value());
        const auto& installedConfig = descriptor->getPluginData().getAs<AsyncSourceConfig>();
        EXPECT_EQ(installedConfig.calls.getCalls().front(), producer);
        const auto provider
            = BufferManager::create(1024 * 1024, 0.1, BufferAlignment{64}, 4096, std::make_shared<NesDefaultMemoryAllocator>());
        {
            HandoffChannel::Attachment sink{id, 2, HandoffChannel::Role::SINK};
            AsyncSource source{installedConfig, *descriptor};
            source.open(provider);
            for (size_t buffer = 0; buffer < bufferCount; ++buffer)
            {
                auto input = provider->getBufferBlocking();
                input.setNumberOfTuples(rowsPerBuffer);
                ASSERT_EQ(sink->tryPush(input), HandoffChannel::PushResult::PUSHED);
            }
            sink->close();
            try
            {
                for (size_t buffer = 0; buffer < bufferCount; ++buffer)
                {
                    auto output = provider->getBufferBlocking();
                    const auto filled = source.fillTupleBuffer(output, std::stop_token{});
                    EXPECT_FALSE(filled.isEoS());
                }
            }
            catch (...)
            {
                source.close();
                throw;
            }
            source.close();
        }
        provider->destroy();
    }
};
}

#ifndef NO_ASSERT
class AsyncSourceValueDeathTest : public AsyncSourceValueTest
{
protected:
    void SetUp() override
    {
        AsyncSourceValueTest::SetUp();
        GTEST_FLAG_SET(death_test_style, "threadsafe");
    }
};

TEST_F(AsyncSourceValueDeathTest, WrongResultArityViolatesInvariant)
{
    SKIP_IF_TSAN();
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_DEATH(run(SharedRuntimeValues::make(), type, calls), "Invariant violated.*produced.*values");
}

TEST_F(AsyncSourceValueDeathTest, WrongResultTypeViolatesInvariant)
{
    SKIP_IF_TSAN();
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_DEATH(run(SharedRuntimeValues::make(uint64_t{42}), type, calls), "Invariant violated.*output.*has type");
}

TEST_F(AsyncSourceValueDeathTest, NullForNonNullableOutputViolatesInvariant)
{
    SKIP_IF_TSAN();
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    EXPECT_DEATH(run(SharedRuntimeValues::make(RuntimeValue::null(nullable)), type, calls), "Invariant violated.*output.*has type");
}

TEST_F(AsyncSourceValueDeathTest, NonNullableValueForNullableOutputViolatesInvariant)
{
    SKIP_IF_TSAN();
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    EXPECT_DEATH(run(SharedRuntimeValues::make(int64_t{42}), type, calls), "Invariant violated.*output.*has type");
}

TEST_F(AsyncSourceValueDeathTest, NullableValueForNonNullableOutputViolatesInvariant)
{
    SKIP_IF_TSAN();
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    EXPECT_DEATH(run(SharedRuntimeValues::make(RuntimeValue{nullable, int64_t{42}}), type, calls), "Invariant violated.*output.*has type");
}
#endif

TEST_F(AsyncSourceValueTest, InvalidArgumentsFailPreparationOnceWithoutExecutingOrRetrying)
{
    const auto preparations = std::make_shared<std::atomic<size_t>>(0);
    const auto attempts = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_THROW(
        run(SharedRuntimeValues::make(int64_t{42}),
            type,
            attempts,
            std::nullopt,
            0,
            8,
            1,
            1,
            1,
            [=] { return std::make_unique<PreparationFailureFunction>(preparations, attempts); }),
        Exception);
    EXPECT_EQ(preparations->load(), 1);
    EXPECT_EQ(attempts->load(), 0);
}

TEST_F(AsyncSourceValueTest, AllImplementationsAreResolvedBeforeAnyInstancesAreCreated)
{
    const AsyncChannelId channel{generateUUID()};
    const auto known = "Bound_" + channel.getRawValue();
    const auto missing = "Missing_" + channel.getRawValue();
    const auto creations = std::make_shared<std::atomic<size_t>>(0);
    ASSERT_TRUE(
        AsyncFunctionRegistry::instance().addEntry(
            known,
            [creations](AsyncFunctionRegistryArguments)
            {
                ++*creations;
                return std::make_unique<FixedResultFunction>(SharedRuntimeValues::make(int64_t{42}), creations, std::nullopt, 0);
            }));
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const Schema<UnqualifiedUnboundField, Ordered> schema{
        UnqualifiedUnboundField{Identifier::parse("LEFT"), type}, UnqualifiedUnboundField{Identifier::parse("RIGHT"), type}};
    AsyncCallGraphBuilder builder{schema};
    builder.exportOutput(builder.call(known, {}, {type})->output(0), QualifiedIdentifier::parse("LEFT"));
    builder.exportOutput(builder.call(missing, {}, {type})->output(0), QualifiedIdentifier::parse("RIGHT"));
    const AsyncSourceConfig config{.channel = channel, .calls = builder.build()};
    SourceCatalog catalog;
    const auto descriptor = catalog.getAnonymousSource(
        PhysicalSourceBuilder{
            GeneralSourceConfig{.host = Host{"localhost:8080"}, .maxInflightBuffers = std::nullopt},
            PluginSourceConfiguration{Identifier::parse("ASYNC"), ExplicitAny{std::any{config}}},
            InputFormatterDescriptor{Identifier::parse("NATIVE"), ExplicitAny{std::any{std::monostate{}}}}},
        schema);
    ASSERT_TRUE(descriptor.has_value());
    ASSERT_EXCEPTION_ERRORCODE((AsyncSource{config, *descriptor}), ErrorCode::UnknownFunctionType);
    EXPECT_EQ(creations->load(), 0);
}

TEST_F(AsyncSourceValueTest, ValueOwnedBuffersSupportConcurrentRowsAndOrderedRemoval)
{
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_NO_THROW(run(SharedRuntimeValues::make(int64_t{42}), type, calls, size_t{0}, 0, 0, 8, 2, 4));
    EXPECT_EQ(calls->load(), 16);
}

TEST_F(AsyncSourceValueTest, FailedRowsDrainBeforeBorrowedBuffersAreCleared)
{
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_THROW(run(SharedRuntimeValues::make(int64_t{42}), type, calls, size_t{0}, 1, 0, 8, 2, 4), Exception);
    EXPECT_GE(calls->load(), 1);
}

TEST_F(AsyncSourceValueTest, ExplicitZeroRetriesOverridesTheSourceDefault)
{
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_THROW(run(SharedRuntimeValues::make(int64_t{42}), type, calls, size_t{0}, 2), Exception);
    EXPECT_EQ(calls->load(), 1);
}

TEST_F(AsyncSourceValueTest, ExplicitRetriesOverrideADisabledSourceDefault)
{
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_NO_THROW(run(SharedRuntimeValues::make(int64_t{42}), type, calls, size_t{2}, 2, 0));
    EXPECT_EQ(calls->load(), 3);
}

TEST_F(AsyncSourceValueTest, ExplicitRetryCountBoundsTheNumberOfAttempts)
{
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_THROW(run(SharedRuntimeValues::make(int64_t{42}), type, calls, size_t{1}, 2), Exception);
    EXPECT_EQ(calls->load(), 2);
}

TEST_F(AsyncSourceValueTest, UnspecifiedRetryPolicyUsesTheSourceDefault)
{
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_NO_THROW(run(SharedRuntimeValues::make(int64_t{42}), type, calls, std::nullopt, 2));
    EXPECT_EQ(calls->load(), 3);
}

TEST_F(AsyncSourceValueTest, NullableOutputAcceptsACorrectlyTypedNonNullValue)
{
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    EXPECT_NO_THROW(run(SharedRuntimeValues::make(RuntimeValue{type, int64_t{42}}), type, calls));
    EXPECT_EQ(calls->load(), 1);
}

}
