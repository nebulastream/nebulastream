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

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <utility>
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
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Reflection.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <rfl/json/write.hpp>
#include <AsyncFunctionRegistry.hpp>
#include <AsyncSource.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <HandoffChannel.hpp>

namespace NES
{
namespace
{
class FixedResultFunction final : public AsyncFunction
{
public:
    FixedResultFunction(AsyncValues result, std::shared_ptr<std::atomic<size_t>> calls) : result(std::move(result)), calls(std::move(calls))
    {
    }

    boost::asio::awaitable<AsyncValues> call(AsyncValues) override
    {
        ++*calls;
        co_return result;
    }

private:
    AsyncValues result;
    std::shared_ptr<std::atomic<size_t>> calls;
};

class AsyncSourceValueTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("AsyncSourceValueTest.log", LogLevel::LOG_DEBUG); }

    /// Exercise the actual source's validation path, not just a scalar conversion helper.
    static void run(const AsyncValues& result, const DataType declaredType, const std::shared_ptr<std::atomic<size_t>>& calls)
    {
        const auto id = UUIDToString(generateUUID());
        const AsyncFunctionType functionType{"FixedResult_" + id};
        const bool added = AsyncFunctionRegistry::instance().addEntry(
            functionType, [result, calls](AsyncFunctionRegistryArguments) { return std::make_unique<FixedResultFunction>(result, calls); });
        ASSERT_TRUE(added);
        SourceCatalog catalog;
        const Schema<UnqualifiedUnboundField, Ordered> schema{UnqualifiedUnboundField{Identifier::parse("RESULT"), declaredType}};
        AsyncCallGraphBuilder builder{schema};
        const auto producer = builder.call(functionType, {}, {declaredType});
        builder.exportOutput(producer->output(0), QualifiedIdentifier::parse("RESULT"));
        const auto serialized = rfl::json::write(*ReflectionContext{}.reflect(builder.build()));
        const auto descriptor = catalog.getAnonymousSource(
            Identifier::parse("ASYNC"),
            schema,
            Host{"localhost:8080"},
            {{Identifier::parse("TYPE"), "NATIVE"}},
            {{Identifier::parse("CHANNEL"), id}, {Identifier::parse("CALLS"), serialized}});
        ASSERT_TRUE(descriptor.has_value());
        const auto provider
            = BufferManager::create(1024 * 1024, 0.1, BufferAlignment{64}, 4096, std::make_shared<NesDefaultMemoryAllocator>());
        {
            HandoffChannel::Lease sink{id, 2, HandoffChannel::Role::SINK};
            const AsyncSourceConfig config{.channel = id, .calls = serialized, .maxConcurrency = 1, .channelCapacity = 2, .maxRetries = 3};
            AsyncSource source{config, *descriptor};
            source.open(provider);
            auto input = provider->getBufferBlocking();
            input.setNumberOfTuples(1);
            ASSERT_EQ(sink->tryPush(input), HandoffChannel::PushResult::PUSHED);
            sink->close();
            auto output = provider->getBufferBlocking();
            try
            {
                const auto filled = source.fillTupleBuffer(output, std::stop_token{});
                EXPECT_FALSE(filled.isEoS());
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

TEST_F(AsyncSourceValueTest, WrongResultArityFailsWithoutRetry)
{
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_THROW(run(AsyncValues{}, type, calls), Exception);
    EXPECT_EQ(calls->load(), 1);
}

TEST_F(AsyncSourceValueTest, WrongResultTypeFailsWithoutRetry)
{
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_THROW(run(AsyncValues::make(uint64_t{42}), type, calls), Exception);
    EXPECT_EQ(calls->load(), 1);
}

TEST_F(AsyncSourceValueTest, NullForNonNullableOutputFailsWithoutRetry)
{
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    EXPECT_THROW(run(AsyncValues::make(RuntimeValue::null(nullable)), type, calls), Exception);
    EXPECT_EQ(calls->load(), 1);
}

TEST_F(AsyncSourceValueTest, NullableOutputAcceptsANonNullNativeScalar)
{
    const auto calls = std::make_shared<std::atomic<size_t>>(0);
    const DataType type{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    EXPECT_NO_THROW(run(AsyncValues::make(int64_t{42}), type, calls));
    EXPECT_EQ(calls->load(), 1);
}

}
