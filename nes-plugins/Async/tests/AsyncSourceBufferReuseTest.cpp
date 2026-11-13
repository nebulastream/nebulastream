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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/RuntimeValueNautilus.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Schema/Schema.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncFunction.hpp>
#include <Sources/AsyncSourceConfig.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Any.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
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
using RowSchema = Schema<UnqualifiedUnboundField, Ordered>;
using Evaluate = std::function<AsyncValues(int64_t)>;
const DataType INT64{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
const DataType BYTES{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE};

RowSchema schema(const DataType result, bool text = false)
{
    std::vector<UnqualifiedUnboundField> fields{
        UnqualifiedUnboundField{Identifier::parse("ID"), INT64},
        UnqualifiedUnboundField{Identifier::parse("KEEP"), INT64},
        UnqualifiedUnboundField{Identifier::parse("RESULT"), result}};
    if (text)
    {
        fields.emplace_back(Identifier::parse("TEXT"), BYTES);
    }
    return RowSchema{std::move(fields)};
}

class RowFunction final : public AsyncFunction
{
public:
    RowFunction(Evaluate evaluate, bool suspend) : evaluate(std::move(evaluate)), suspend(suspend) { }

    boost::asio::awaitable<AsyncValues> call(AsyncValues arguments) override
    {
        const auto id = arguments[0].get<int64_t>();
        if (suspend)
        {
            boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor};
            timer.expires_after(std::chrono::milliseconds{4 - id % 5});
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
        co_return evaluate(id);
    }

private:
    Evaluate evaluate;
    bool suspend;
};

class RejectingOutputProvider final : public AbstractBufferProvider
{
public:
    explicit RejectingOutputProvider(std::shared_ptr<AbstractBufferProvider> provider) : provider(std::move(provider)) { }

    BufferManagerType getBufferManagerType() const override { return provider->getBufferManagerType(); }

    size_t getBufferSize() const override { return provider->getBufferSize(); }

    size_t getNumOfPooledBuffers() const override { return provider->getNumOfPooledBuffers(); }

    size_t getNumOfUnpooledBuffers() const override { return provider->getNumOfUnpooledBuffers(); }

    TupleBuffer getBufferBlocking() override { throw CannotAllocateBuffer("Unexpected blocking allocation"); }

    std::optional<TupleBuffer> getBufferWithTimeout(std::chrono::milliseconds) override
    {
        throw CannotAllocateBuffer("Unexpected timed allocation");
    }

    std::optional<TupleBuffer> getBufferNoBlocking() override
    {
        ++pooledAttempts;
        return std::nullopt;
    }

    std::optional<TupleBuffer> getUnpooledBuffer(size_t) override
    {
        ++unpooledAttempts;
        return std::nullopt;
    }

    std::atomic<size_t> pooledAttempts{0};
    std::atomic<size_t> unpooledAttempts{0};

private:
    std::shared_ptr<AbstractBufferProvider> provider;
};

class AsyncSourceBufferReuseTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("AsyncSourceBufferReuseTest.log", LogLevel::LOG_DEBUG, false); }

    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        provider = BufferManager::create(1024 * 1024, 0.1, BufferAlignment{64}, 4096, std::make_shared<NesDefaultMemoryAllocator>());
    }

    void TearDown() override
    {
        if (source)
        {
            source->close();
            source.reset();
        }
        sink.reset();
        provider->destroy();
        Testing::BaseUnitTest::TearDown();
    }

    void start(
        RowSchema input,
        RowSchema output,
        DataType resultType,
        Evaluate evaluate,
        bool suspend = false,
        std::shared_ptr<AbstractBufferProvider> outputProvider = {})
    {
        inputSchema = std::move(input);
        outputSchema = std::move(output);
        const AsyncChannelId channel{generateUUID()};
        const auto functionType = "BufferReuse_" + channel.getRawValue();
        ASSERT_TRUE(
            AsyncFunctionRegistry::instance().addEntry(
                functionType,
                [evaluate = std::move(evaluate), suspend](AsyncFunctionRegistryArguments)
                { return std::make_unique<RowFunction>(evaluate, suspend); }));
        AsyncCallGraphBuilder builder{inputSchema};
        const auto call = builder.call(functionType, {builder.input(QualifiedIdentifier::parse("ID"))}, {resultType});
        builder.exportOutput(call->output(0), QualifiedIdentifier::parse("RESULT"));
        const AsyncSourceConfig config{
            .channel = channel, .calls = builder.build(), .maxConcurrency = 8, .channelCapacity = 4, .maxRetries = 0};
        const auto descriptor = catalog.getAnonymousSource(
            PhysicalSourceBuilder{
                GeneralSourceConfig{.host = Host{"localhost:8080"}, .maxInflightBuffers = std::nullopt},
                PluginSourceConfiguration{Identifier::parse("ASYNC"), ExplicitAny{std::any{config}}},
                InputFormatterDescriptor{Identifier::parse("NATIVE"), ExplicitAny{std::any{std::monostate{}}}}},
            outputSchema);
        ASSERT_TRUE(descriptor.has_value());
        sink = std::make_unique<HandoffChannel::Lease>(channel, 4, HandoffChannel::Role::SINK);
        source = std::make_unique<AsyncSource>(config, *descriptor);
        EXPECT_FALSE(source->requiresPreallocatedBuffer());
        source->open(outputProvider ? std::move(outputProvider) : provider);
    }

    void write(TupleBuffer& buffer, size_t row, const std::vector<std::pair<std::string, RuntimeValue>>& values)
    {
        const auto ref = LowerSchemaProvider::lowerSchema(provider->getBufferSize(), inputSchema, MemoryLayoutType::ROW_LAYOUT);
        Record record;
        for (const auto& [name, value] : values)
        {
            record.write(QualifiedIdentifier::parse(name), toVarVal(value.view()));
        }
        const RecordBuffer target{BorrowedNautilusBuffer::from(nautilus::val<TupleBuffer*>{&buffer})};
        auto index = nautilus::val<uint64_t>{row};
        const auto result = ref->writeRecord(index, target, record, nautilus::val<AbstractBufferProvider*>{provider.get()});
        ASSERT_TRUE(static_cast<bool>(result.successful));
    }

    RuntimeValue read(const TupleBuffer& buffer, size_t row, const std::string& name, DataType type, bool original = false)
    {
        const auto ref = LowerSchemaProvider::lowerSchema(
            provider->getBufferSize(), original ? inputSchema : outputSchema, MemoryLayoutType::ROW_LAYOUT);
        const RecordBuffer target{BorrowedNautilusBuffer::from(nautilus::val<const TupleBuffer*>{&buffer})};
        auto index = nautilus::val<uint64_t>{row};
        const auto field = QualifiedIdentifier::parse(name);
        return fromVarVal(ref->readRecord({field}, target, index).read(field), type);
    }

    TupleBuffer input(size_t rows, bool text = false)
    {
        auto buffer = provider->getBufferBlocking();
        for (size_t row = 0; row < rows; ++row)
        {
            std::vector<std::pair<std::string, RuntimeValue>> values{
                {"ID", RuntimeValue{static_cast<int64_t>(row)}}, {"KEEP", RuntimeValue{int64_t{12345}}}};
            if (text)
            {
                values.emplace_back("TEXT", RuntimeValue{std::string{"original\0bytes", 14}});
                values.emplace_back("RESULT", RuntimeValue{std::string{}});
            }
            else
            {
                values.emplace_back("RESULT", RuntimeValue{int64_t{-99}});
            }
            write(buffer, row, values);
        }
        buffer.setNumberOfTuples(rows);
        return buffer;
    }

    void push(TupleBuffer buffer) { ASSERT_EQ(sink->operator->()->tryPush(buffer), HandoffChannel::PushResult::PUSHED); }

    TupleBuffer next()
    {
        TupleBuffer output;
        const auto result = source->fillTupleBuffer(output, {});
        EXPECT_FALSE(result.isEoS());
        if (!result.isEoS())
        {
            if (output.getNumberOfTuples() != result.getNumberOfBytes())
            {
                output.setNumberOfTuples(result.getNumberOfBytes());
            }
        }
        return output;
    }

    SourceCatalog catalog;
    RowSchema inputSchema;
    RowSchema outputSchema;
    std::shared_ptr<BufferManager> provider;
    std::unique_ptr<HandoffChannel::Lease> sink;
    std::unique_ptr<AsyncSource> source;
};

TEST_F(AsyncSourceBufferReuseTest, ReusesExclusiveParentWithoutAllocatingAndPreservesMetadata)
{
    start(schema(INT64), schema(INT64), INT64, [](int64_t id) { return AsyncValues::make(id * 3); }, true);
    auto buffer = input(32);
    const auto* address = buffer.getAvailableMemoryArea().data();
    buffer.setSequenceNumber(SequenceNumber{17});
    buffer.setChunkNumber(ChunkNumber{3});
    buffer.setLastChunk(true);
    buffer.setOriginId(OriginId{9});
    buffer.setWatermark(Timestamp{123});
    buffer.setCreationTimestampInMS(Timestamp{456});
    push(std::move(buffer));
    const auto available = provider->getNumberOfAvailableBuffers();
    const auto output = next();
    EXPECT_EQ(output.getAvailableMemoryArea().data(), address);
    EXPECT_EQ(provider->getNumberOfAvailableBuffers(), available);
    EXPECT_EQ(output.getNumberOfTuples(), 32);
    EXPECT_EQ(output.getSequenceNumber(), SequenceNumber{17});
    EXPECT_EQ(output.getChunkNumber(), ChunkNumber{3});
    EXPECT_TRUE(output.isLastChunk());
    EXPECT_EQ(output.getOriginId(), OriginId{9});
    EXPECT_EQ(output.getWatermark(), Timestamp{123});
    EXPECT_EQ(output.getCreationTimestampInMS(), Timestamp{456});
    for (size_t row = 0; row < 32; ++row)
    {
        const auto id = read(output, row, "ID", INT64);
        const auto keep = read(output, row, "KEEP", INT64);
        const auto result = read(output, row, "RESULT", INT64);
        EXPECT_EQ(id.view().get<int64_t>(), row);
        EXPECT_EQ(keep.view().get<int64_t>(), 12345);
        EXPECT_EQ(result.view().get<int64_t>(), row * 3);
    }
}

TEST_F(AsyncSourceBufferReuseTest, SharedParentIsCopiedWithoutChangingTheOtherConsumer)
{
    start(schema(INT64), schema(INT64), INT64, [](int64_t id) { return AsyncValues::make(id + 10); });
    auto original = input(8);
    push(original);
    const auto output = next();
    EXPECT_NE(output.getAvailableMemoryArea().data(), original.getAvailableMemoryArea().data());
    for (size_t row = 0; row < 8; ++row)
    {
        const auto before = read(original, row, "RESULT", INT64, true);
        const auto after = read(output, row, "RESULT", INT64);
        EXPECT_EQ(before.view().get<int64_t>(), -99);
        EXPECT_EQ(after.view().get<int64_t>(), row + 10);
    }
}

TEST_F(AsyncSourceBufferReuseTest, DifferentFieldOrderUsesTheCopyPath)
{
    const RowSchema reordered{
        UnqualifiedUnboundField{Identifier::parse("RESULT"), INT64},
        UnqualifiedUnboundField{Identifier::parse("ID"), INT64},
        UnqualifiedUnboundField{Identifier::parse("KEEP"), INT64}};
    start(schema(INT64), reordered, INT64, [](int64_t id) { return AsyncValues::make(id + 10); });
    auto buffer = input(8);
    const auto* address = buffer.getAvailableMemoryArea().data();
    push(std::move(buffer));
    const auto output = next();
    EXPECT_NE(output.getAvailableMemoryArea().data(), address);
    for (size_t row = 0; row < 8; ++row)
    {
        const auto id = read(output, row, "ID", INT64);
        const auto keep = read(output, row, "KEEP", INT64);
        const auto result = read(output, row, "RESULT", INT64);
        EXPECT_EQ(id.view().get<int64_t>(), row);
        EXPECT_EQ(keep.view().get<int64_t>(), 12345);
        EXPECT_EQ(result.view().get<int64_t>(), row + 10);
    }
}

TEST_F(AsyncSourceBufferReuseTest, ReusesParentAndPreservesExistingVariablePayloadsForLargeBinaryResults)
{
    start(
        schema(BYTES, true),
        schema(BYTES, true),
        BYTES,
        [](int64_t id) { return AsyncValues::make(id == 0 ? std::string{} : std::string(8192, static_cast<char>(id))); });
    auto buffer = input(8, true);
    const auto* address = buffer.getAvailableMemoryArea().data();
    const auto originalChildren = buffer.getNumberOfChildBuffers();
    push(std::move(buffer));
    const auto output = next();
    EXPECT_EQ(output.getAvailableMemoryArea().data(), address);
    EXPECT_GE(output.getNumberOfChildBuffers(), originalChildren);
    for (size_t row = 0; row < 8; ++row)
    {
        const auto text = read(output, row, "TEXT", BYTES);
        EXPECT_EQ(text.view().get<std::string_view>(), std::string_view("original\0bytes", 14));
        const auto result = read(output, row, "RESULT", BYTES);
        EXPECT_EQ(result.view().get<std::string_view>(), row == 0 ? std::string{} : std::string(8192, static_cast<char>(row)));
    }
}

TEST_F(AsyncSourceBufferReuseTest, SharedChildRemainsUnchangedAndNewResultsShareAPrivateChild)
{
    start(schema(BYTES, true), schema(BYTES, true), BYTES, [](int64_t) { return AsyncValues::make(std::string{"result"}); });
    auto buffer = input(8, true);
    const auto* address = buffer.getAvailableMemoryArea().data();
    ASSERT_EQ(buffer.getNumberOfChildBuffers(), 1);
    auto retainedChild = buffer.loadChildBuffer(ChildBufferIndex{0});
    const auto used = retainedChild.getNumberOfTuples();
    push(std::move(buffer));
    const auto output = next();
    EXPECT_EQ(output.getAvailableMemoryArea().data(), address);
    EXPECT_EQ(retainedChild.getNumberOfTuples(), used);
    EXPECT_EQ(output.getNumberOfChildBuffers(), 2);
    for (size_t row = 0; row < 8; ++row)
    {
        const auto result = read(output, row, "RESULT", BYTES);
        const auto text = read(output, row, "TEXT", BYTES);
        EXPECT_EQ(result.view().get<std::string_view>(), "result");
        EXPECT_EQ(text.view().get<std::string_view>(), std::string_view("original\0bytes", 14));
    }
}

TEST_F(AsyncSourceBufferReuseTest, CopyPathDoesNotAppendToTheOriginalParentsChildren)
{
    start(schema(BYTES, true), schema(BYTES, true), BYTES, [](int64_t) { return AsyncValues::make(std::string{"result"}); });
    auto original = input(8, true);
    auto child = original.loadChildBuffer(ChildBufferIndex{0});
    const auto used = child.getNumberOfTuples();
    push(original);
    const auto output = next();
    EXPECT_NE(output.getAvailableMemoryArea().data(), original.getAvailableMemoryArea().data());
    EXPECT_EQ(child.getNumberOfTuples(), used);
    EXPECT_EQ(original.getNumberOfChildBuffers(), 1);
    EXPECT_EQ(output.getNumberOfChildBuffers(), 2);
    const auto before = read(original, 0, "RESULT", BYTES, true);
    const auto after = read(output, 0, "RESULT", BYTES);
    EXPECT_TRUE(before.view().bytes().empty());
    EXPECT_EQ(after.view().get<std::string_view>(), "result");
}

TEST_F(AsyncSourceBufferReuseTest, NullableVariableResultsSupportNullEmptyAndBinaryValues)
{
    const DataType nullable{DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE};
    start(
        schema(nullable),
        schema(nullable),
        nullable,
        [nullable](int64_t id)
        {
            if (id % 3 == 0)
            {
                return AsyncValues::make(RuntimeValue::null(nullable));
            }
            return AsyncValues::make(RuntimeValue{nullable, id % 3 == 1 ? std::string{} : std::string{"\0\xff", 2}});
        });
    auto buffer = provider->getBufferBlocking();
    for (size_t row = 0; row < 9; ++row)
    {
        write(buffer, row, {{"ID", RuntimeValue{static_cast<int64_t>(row)}}, {"RESULT", RuntimeValue::null(nullable)}});
    }
    buffer.setNumberOfTuples(9);
    const auto* address = buffer.getAvailableMemoryArea().data();
    push(std::move(buffer));
    const auto output = next();
    EXPECT_EQ(output.getAvailableMemoryArea().data(), address);
    for (size_t row = 0; row < 9; ++row)
    {
        const auto value = read(output, row, "RESULT", nullable);
        EXPECT_EQ(value.isNull(), row % 3 == 0);
        if (!value.isNull())
        {
            const auto expected = row % 3 == 1 ? std::string{} : std::string{"\0\xff", 2};
            EXPECT_EQ(value.view().get<std::string_view>(), expected);
        }
    }
}

TEST_F(AsyncSourceBufferReuseTest, EmptyBufferIsReusedAndEndOfStreamNeedsNoOutputBuffer)
{
    start(
        schema(INT64),
        schema(INT64),
        INT64,
        [](int64_t) -> AsyncValues
        {
            ADD_FAILURE() << "An empty buffer must not invoke the function";
            return AsyncValues::make(int64_t{0});
        });
    auto buffer = input(0);
    const auto* address = buffer.getAvailableMemoryArea().data();
    push(std::move(buffer));
    sink->operator->()->close();
    const auto output = next();
    EXPECT_EQ(output.getAvailableMemoryArea().data(), address);
    EXPECT_EQ(output.getNumberOfTuples(), 0);
    TupleBuffer unused;
    EXPECT_TRUE(source->fillTupleBuffer(unused, {}).isEoS());
    EXPECT_TRUE(!unused);
}

TEST_F(AsyncSourceBufferReuseTest, ReuseNeedsNoFreePooledBuffer)
{
    start(schema(INT64), schema(INT64), INT64, [](int64_t id) { return AsyncValues::make(id + 1); });
    auto buffer = input(8);
    const auto* address = buffer.getAvailableMemoryArea().data();
    push(std::move(buffer));
    std::vector<TupleBuffer> occupied;
    while (auto spare = provider->getBufferNoBlocking())
    {
        occupied.push_back(std::move(*spare));
    }
    EXPECT_EQ(provider->getNumberOfAvailableBuffers(), 0);
    const auto output = next();
    EXPECT_EQ(output.getAvailableMemoryArea().data(), address);
    EXPECT_EQ(provider->getNumberOfAvailableBuffers(), 0);
}

TEST_F(AsyncSourceBufferReuseTest, SharedParentCopyUsesUnpooledMemoryInsteadOfWaitingForItsOwnPool)
{
    start(schema(INT64), schema(INT64), INT64, [](int64_t id) { return AsyncValues::make(id + 1); });
    auto original = input(8);
    push(original);
    std::vector<TupleBuffer> occupied;
    while (auto spare = provider->getBufferNoBlocking())
    {
        occupied.push_back(std::move(*spare));
    }
    const auto unpooled = provider->getNumOfUnpooledBuffers();
    const auto output = next();
    EXPECT_NE(output.getAvailableMemoryArea().data(), original.getAvailableMemoryArea().data());
    EXPECT_EQ(provider->getNumOfUnpooledBuffers(), unpooled + 1);
    const auto before = read(original, 0, "RESULT", INT64, true);
    const auto after = read(output, 0, "RESULT", INT64);
    EXPECT_EQ(before.view().get<int64_t>(), -99);
    EXPECT_EQ(after.view().get<int64_t>(), 1);
}

TEST_F(AsyncSourceBufferReuseTest, RejectedCopyAllocationFailsAndCloseDrainsOtherBorrowedBuffers)
{
    const auto rejecting = std::make_shared<RejectingOutputProvider>(provider);
    start(schema(INT64), schema(INT64), INT64, [](int64_t id) { return AsyncValues::make(id + 1); }, true, rejecting);
    auto original = input(1);
    push(original);
    push(input(32));
    TupleBuffer output;
    try
    {
        source->fillTupleBuffer(output, {});
        FAIL() << "Expected async output allocation to fail";
    }
    catch (const Exception& error)
    {
        EXPECT_NE(std::string_view{error.what()}.find("Cannot allocate async output buffer"), std::string_view::npos);
    }
    EXPECT_TRUE(!output);
    EXPECT_EQ(rejecting->pooledAttempts.load(), 1);
    EXPECT_EQ(rejecting->unpooledAttempts.load(), 1);
    source->close();
    const auto value = read(original, 0, "RESULT", INT64, true);
    EXPECT_EQ(value.view().get<int64_t>(), -99);
    EXPECT_EQ(provider->getNumberOfAvailableBuffers(), provider->getNumOfPooledBuffers() - 1);
}

TEST_F(AsyncSourceBufferReuseTest, ConcurrentRowsAndBuffersAreReusedInHandoffOrder)
{
    start(schema(INT64), schema(INT64), INT64, [](int64_t id) { return AsyncValues::make(id * 7); }, true);
    std::vector<const std::byte*> addresses;
    for (size_t index = 0; index < 4; ++index)
    {
        auto buffer = input(3);
        buffer.setSequenceNumber(SequenceNumber{index + 1});
        addresses.push_back(buffer.getAvailableMemoryArea().data());
        push(std::move(buffer));
    }
    sink->operator->()->close();
    for (size_t index = 0; index < 4; ++index)
    {
        const auto output = next();
        EXPECT_EQ(output.getAvailableMemoryArea().data(), addresses[index]);
        EXPECT_EQ(output.getSequenceNumber(), SequenceNumber{index + 1});
        for (size_t row = 0; row < 3; ++row)
        {
            const auto value = read(output, row, "RESULT", INT64);
            EXPECT_EQ(value.view().get<int64_t>(), row * 7);
        }
    }
}

TEST_F(AsyncSourceBufferReuseTest, FixedSizeNullableResultsOverwriteOnlyTheirOwnSlots)
{
    const DataType nullable{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    start(
        schema(nullable),
        schema(nullable),
        nullable,
        [nullable](int64_t id) { return AsyncValues::make(id % 2 == 0 ? RuntimeValue::null(nullable) : RuntimeValue{nullable, id * 11}); },
        true);
    auto buffer = input(32);
    const auto* address = buffer.getAvailableMemoryArea().data();
    push(std::move(buffer));
    const auto output = next();
    EXPECT_EQ(output.getAvailableMemoryArea().data(), address);
    for (size_t row = 0; row < 32; ++row)
    {
        const auto value = read(output, row, "RESULT", nullable);
        const auto keep = read(output, row, "KEEP", INT64);
        EXPECT_EQ(keep.view().get<int64_t>(), 12345);
        EXPECT_EQ(value.isNull(), row % 2 == 0);
        if (!value.isNull())
        {
            EXPECT_EQ(value.view().get<int64_t>(), row * 11);
        }
    }
}

TEST_F(AsyncSourceBufferReuseTest, FailureDrainsRowsWithoutPublishingOrMutatingTheInput)
{
    start(
        schema(INT64),
        schema(INT64),
        INT64,
        [](int64_t id)
        {
            if (id == 3)
            {
                throw CannotReadFromSource("test failure");
            }
            return AsyncValues::make(id + 100);
        },
        true);
    auto original = input(32);
    push(original);
    TupleBuffer output;
    EXPECT_THROW(source->fillTupleBuffer(output, {}), Exception);
    EXPECT_TRUE(!output);
    for (size_t row = 0; row < 32; ++row)
    {
        const auto value = read(original, row, "RESULT", INT64, true);
        EXPECT_EQ(value.view().get<int64_t>(), -99);
    }
}

TEST_F(AsyncSourceBufferReuseTest, ConcurrentParentsNeverAppendToTheirSharedChild)
{
    auto shared = provider->getBufferBlocking();
    shared.setNumberOfTuples(17);
    std::vector<TupleBuffer> parents;
    for (size_t index = 0; index < 8; ++index)
    {
        auto parent = provider->getBufferBlocking();
        auto alias = shared;
        EXPECT_EQ(parent.storeChildBuffer(alias), ChildBufferIndex{0});
        parents.push_back(std::move(parent));
    }
    {
        std::vector<std::jthread> writers;
        for (auto& parent : parents)
        {
            writers.emplace_back(
                [&, target = &parent]
                {
                    const std::string payload(16, 'x');
                    for (size_t row = 0; row < 128; ++row)
                    {
                        const auto access = TupleBufferRef::writeVarSized(*target, *provider, std::as_bytes(std::span{payload}));
                        EXPECT_EQ(access.getIndex(), ChildBufferIndex{1});
                        EXPECT_EQ(access.getOffset().getRawOffset(), row * payload.size());
                    }
                });
        }
    }
    EXPECT_EQ(shared.getNumberOfTuples(), 17);
    for (const auto& parent : parents)
    {
        EXPECT_EQ(parent.getNumberOfChildBuffers(), 2);
        const auto child = parent.loadChildBuffer(ChildBufferIndex{1});
        EXPECT_EQ(child.getNumberOfTuples(), 128 * 16);
    }
}
}
}
