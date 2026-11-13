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


#include <algorithm>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <ranges>
#include <source_location>
#include <stop_token>
#include <utility>
#include <variant>
#include <vector>
#include <unistd.h>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/chrono.h>
#include <folly/Synchronized.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BackpressureChannel.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SourceThread.hpp>
#include <TestSource.hpp>
#include <nameof.hpp>

namespace NES
{
namespace
{
constexpr std::chrono::milliseconds DEFAULT_TIMEOUT = std::chrono::milliseconds(100);
constexpr std::chrono::milliseconds DEFAULT_LONG_TIMEOUT = std::chrono::milliseconds(1000);
constexpr size_t DEFAULT_BUFFER_SIZE = 8192;
constexpr size_t DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER = 23;
constexpr size_t DEFAULT_NUMBER_OF_LOCAL_BUFFERS = 100;
constexpr uint32_t POOLED_BUFFER_SIZE = 8192;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 1024;
constexpr NES::BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.9;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;

class MetadataSource final : public Source
{
public:
    explicit MetadataSource(Timestamp watermark) : watermark(watermark) { }

    void open(std::shared_ptr<AbstractBufferProvider>) override { }

    void close() override { }

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& buffer, const std::stop_token&) override
    {
        if (emitted)
        {
            return FillTupleBufferResult::eos();
        }
        emitted = true;
        buffer.setSequenceNumber(SequenceNumber{42});
        buffer.setWatermark(watermark);
        return FillTupleBufferResult::withBytes(0);
    }

    [[nodiscard]] bool addsMetadata() const override { return true; }

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override { return str << "MetadataSource"; }

private:
    Timestamp watermark;
    bool emitted = false;
};

}

using namespace std::literals;

class SourceThreadTest : public Testing::BaseUnitTest
{
public:
    size_t pageSize = getpagesize();

    static void SetUpTestSuite()
    {
        Logger::setupLogging("SourceThreadTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup SourceThreadTest test class.");
    }

    void SetUp() override { Testing::BaseUnitTest::SetUp(); }
};

struct RecordingEmitFunction
{
    void operator()(const OriginId, SourceReturnType::SourceReturnType emittedData)
    {
        auto value = std::visit(
            [&]<typename T>(T emitted) -> SourceReturnType::SourceReturnType
            {
                if constexpr (std::same_as<T, SourceReturnType::Data>)
                {
                    /// Release old Buffer
                    return SourceReturnType::Data{deepCopyBuffer(emitted.buffer, bm)};
                }
                else
                {
                    return emitted;
                }
            },
            emittedData);
        recordedEmits.lock()->emplace_back(std::move(value));
        block.notify_one();
    }

    AbstractBufferProvider& bm;
    folly::Synchronized<std::vector<SourceReturnType::SourceReturnType>, std::mutex> recordedEmits;
    std::condition_variable block;
};

void wait_for_emits(RecordingEmitFunction& recorder, size_t numberOfEmits, std::source_location location = std::source_location::current())
{
    const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "wait_for_emits");
    auto records = recorder.recordedEmits.lock();
    if (records->size() >= numberOfEmits)
    {
        return;
    }
    EXPECT_TRUE(recorder.block.wait_for(records.as_lock(), DEFAULT_LONG_TIMEOUT, [&]() { return records->size() >= numberOfEmits; }))
        << "Timeout waiting for " << numberOfEmits << " emits";
}

void verify_non_blocking_stop(SourceThread& sourceThread, std::source_location location = std::source_location::current())
{
    const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_non_block_stop");
    auto calledStop = std::chrono::high_resolution_clock::now();
    sourceThread.stop();
    auto stopDone = std::chrono::high_resolution_clock::now();
    EXPECT_LT(stopDone - calledStop, DEFAULT_LONG_TIMEOUT)
        << fmt::format("Stopping a SourceThread exceeded its {} deadline", DEFAULT_LONG_TIMEOUT);
}

void verify_non_blocking_start(
    SourceThread& sourceThread, SourceReturnType::EmitFunction emitFn, std::source_location location = std::source_location::current())
{
    const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_non_block_start");
    auto calledStart = std::chrono::high_resolution_clock::now();
    EXPECT_TRUE(sourceThread.start(std::move(emitFn)));
    auto startDone = std::chrono::high_resolution_clock::now();
    EXPECT_LT(startDone - calledStart, DEFAULT_TIMEOUT)
        << fmt::format("Starting a SourceThread should be non blocking. We estimate a block with around {}", DEFAULT_TIMEOUT);
}

void verify_no_events(RecordingEmitFunction& recorder, std::source_location location = std::source_location::current())
{
    const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_no_events");
    EXPECT_THAT(*recorder.recordedEmits.lock(), ::testing::SizeIs(0)) << "Expected no source events to be emitted";
}

template <typename T>
void verify_last_event(RecordingEmitFunction& recorder, std::source_location location = std::source_location::current())
{
    const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_last_event");
    EXPECT_THAT(*recorder.recordedEmits.lock(), ::testing::SizeIs(::testing::Gt(0))) << "Expected source events to be emitted";
    auto& lastEvent = recorder.recordedEmits.lock()->back();
    EXPECT_TRUE(std::holds_alternative<T>(lastEvent)) << "Last event was not a `" << NAMEOF_TYPE(T) << "` event";
}

void verify_number_of_emits(
    RecordingEmitFunction& recorder, size_t numberOfEmits, std::source_location location = std::source_location::current())
{
    const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_number_of_emits");
    EXPECT_THAT(*recorder.recordedEmits.lock(), ::testing::SizeIs(numberOfEmits))
        << "Expected " << numberOfEmits << " source events to be emitted";

    std::vector<SequenceNumber> sequenceNumbers;
    for (size_t i = 0; i < numberOfEmits; ++i)
    {
        auto& emitted = recorder.recordedEmits.lock()->at(i);
        if (auto* data = std::get_if<SourceReturnType::Data>(&emitted))
        {
            sequenceNumbers.push_back(data->buffer.getSequenceNumber());
        }
    }
    auto expectedView = std::views::iota(static_cast<size_t>(0), sequenceNumbers.size())
        | std::views::transform([](auto seq) { return SequenceNumber(seq + 1); });
    std::vector<SequenceNumber> expected;
    std::ranges::copy(expectedView, std::back_inserter(expected));
    EXPECT_THAT(sequenceNumbers, ::testing::ContainerEq(expected));
}

/// The final watermark is the last data buffer, directly before the EoS.
void verify_final_watermark(RecordingEmitFunction& recorder)
{
    const auto events = recorder.recordedEmits.lock();
    ASSERT_GE(events->size(), 2U);
    const auto& event = events->at(events->size() - 2);
    const auto* data = std::get_if<SourceReturnType::Data>(&event);
    ASSERT_NE(data, nullptr);
    EXPECT_EQ(data->buffer.getNumberOfTuples(), 0);
    EXPECT_EQ(data->buffer.getWatermark().getRawValue(), Timestamp::INFINITE_VALUE);
}

TEST_F(SourceThreadTest, CompletesAlreadyInfiniteMetadataSource)
{
    auto bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    RecordingEmitFunction recorder(*bm);
    SourceThread sourceThread(
        std::move(backpressureListener), INITIAL<OriginId>, bm, std::make_unique<MetadataSource>(Timestamp{Timestamp::INFINITE_VALUE}));
    verify_non_blocking_start(
        sourceThread,
        [&](const OriginId originId, SourceReturnType::SourceReturnType ret, const std::stop_token&)
        {
            recorder(originId, std::move(ret));
            return SourceReturnType::EmitResult::SUCCESS;
        });
    wait_for_emits(recorder, 3);
    verify_non_blocking_stop(sourceThread);

    const auto events = recorder.recordedEmits.lock();
    ASSERT_EQ(events->size(), 3);
    const auto* data = std::get_if<SourceReturnType::Data>(&events->at(0));
    const auto* finalWatermark = std::get_if<SourceReturnType::Data>(&events->at(1));
    ASSERT_NE(data, nullptr);
    ASSERT_NE(finalWatermark, nullptr);
    EXPECT_EQ(data->buffer.getSequenceNumber(), SequenceNumber{42});
    EXPECT_EQ(data->buffer.getWatermark(), Timestamp{Timestamp::INFINITE_VALUE});
    EXPECT_EQ(finalWatermark->buffer.getSequenceNumber(), SequenceNumber{43});
    EXPECT_EQ(finalWatermark->buffer.getWatermark(), Timestamp{Timestamp::INFINITE_VALUE});
    EXPECT_TRUE(std::holds_alternative<SourceReturnType::EoS>(events->at(2)));
}

TEST_F(SourceThreadTest, CompletesFiniteMetadataSourceWithNextSequenceNumber)
{
    auto bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    RecordingEmitFunction recorder(*bm);
    SourceThread sourceThread(std::move(backpressureListener), INITIAL<OriginId>, bm, std::make_unique<MetadataSource>(Timestamp{5000}));
    verify_non_blocking_start(
        sourceThread,
        [&](const OriginId originId, SourceReturnType::SourceReturnType ret, const std::stop_token&)
        {
            recorder(originId, std::move(ret));
            return SourceReturnType::EmitResult::SUCCESS;
        });
    wait_for_emits(recorder, 3);
    verify_non_blocking_stop(sourceThread);

    const auto events = recorder.recordedEmits.lock();
    ASSERT_EQ(events->size(), 3);
    const auto* data = std::get_if<SourceReturnType::Data>(&events->at(0));
    const auto* finalWatermark = std::get_if<SourceReturnType::Data>(&events->at(1));
    ASSERT_NE(data, nullptr);
    ASSERT_NE(finalWatermark, nullptr);
    EXPECT_EQ(data->buffer.getSequenceNumber(), SequenceNumber{42});
    EXPECT_EQ(finalWatermark->buffer.getSequenceNumber(), SequenceNumber{43});
    EXPECT_EQ(finalWatermark->buffer.getWatermark(), Timestamp{Timestamp::INFINITE_VALUE});
    EXPECT_TRUE(std::holds_alternative<SourceReturnType::EoS>(events->at(2)));
}

namespace
{
class MockBufferProvider : public AbstractBufferProvider
{
public:
    MOCK_METHOD(BufferManagerType, getBufferManagerType, (), (const, override));
    MOCK_METHOD(size_t, getBufferSize, (), (const, override));
    MOCK_METHOD(size_t, getNumOfPooledBuffers, (), (const, override));
    MOCK_METHOD(size_t, getNumOfUnpooledBuffers, (), (const, override));
    MOCK_METHOD(TupleBuffer, getBufferBlocking, (), (override));
    MOCK_METHOD(std::optional<TupleBuffer>, getBufferNoBlocking, (), (override));
    MOCK_METHOD(std::optional<TupleBuffer>, getBufferWithTimeout, (std::chrono::milliseconds), (override));
    MOCK_METHOD(std::optional<TupleBuffer>, getUnpooledBuffer, (size_t), (override));
};

/// Deliberately inherits the default preallocation capability.
class MockSource : public Source
{
public:
    MOCK_METHOD(FillTupleBufferResult, fillTupleBuffer, (TupleBuffer&, const std::stop_token&), (override));
    MOCK_METHOD(void, open, (std::shared_ptr<AbstractBufferProvider>), (override));
    MOCK_METHOD(void, close, (), (override));
    MOCK_METHOD(bool, addsMetadata, (), (const, override));

protected:
    std::ostream& toString(std::ostream& out) const override { return out << "MockSource"; }
};

class ReturningBufferSource : public MockSource
{
public:
    bool requiresPreallocatedBuffer() const override { return false; }
};
}

class SourceBufferCapabilityTest : public SourceThreadTest
{
protected:
    std::shared_ptr<BufferManager> bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    /// Strict mocks reject every unexpected allocation, including on EOS and failure paths.
    std::shared_ptr<testing::StrictMock<MockBufferProvider>> provider = std::make_shared<testing::StrictMock<MockBufferProvider>>();
    RecordingEmitFunction recorder{*bm, {}, {}};
    std::pair<BackpressureController, BackpressureListener> backpressure = createBackpressureChannel();

    std::unique_ptr<SourceThread> start(std::unique_ptr<MockSource> source, bool addsMetadata = false)
    {
        EXPECT_CALL(*source, open(testing::Eq(provider)));
        EXPECT_CALL(*source, addsMetadata()).WillOnce(testing::Return(addsMetadata));
        auto thread = std::make_unique<SourceThread>(backpressure.second, INITIAL<OriginId>, provider, std::move(source));
        verify_non_blocking_start(
            *thread,
            [this](OriginId originId, SourceReturnType::SourceReturnType ret, const std::stop_token&)
            {
                recorder(originId, std::move(ret));
                return SourceReturnType::EmitResult::SUCCESS;
            });
        return thread;
    }
};

TEST_F(SourceBufferCapabilityTest, ReturningBuffersNeedNoSpareAndPreserveSourceMetadata)
{
    auto buffer = bm->getBufferBlocking();
    buffer.setNumberOfTuples(DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    buffer.setOriginId(OriginId(42));
    buffer.setSequenceNumber(SequenceNumber(17));
    buffer.setChunkNumber(ChunkNumber(3));
    buffer.setLastChunk(false);
    /// Keep an alias, as consumers of reused buffers do.
    const auto alias = buffer;
    auto source = std::make_unique<testing::StrictMock<ReturningBufferSource>>();
    EXPECT_FALSE(source->requiresPreallocatedBuffer());
    EXPECT_CALL(*source, close()).Times(1);
    EXPECT_CALL(*source, fillTupleBuffer(testing::_, testing::_))
        .Times(3)
        .WillOnce(
            [&](TupleBuffer& output, const std::stop_token&)
            {
                EXPECT_FALSE(!!output);
                output = buffer;
                return Source::FillTupleBufferResult::withBytes(DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
            })
        .WillOnce(
            [&](TupleBuffer& output, const std::stop_token&)
            {
                EXPECT_FALSE(!!output);
                output = buffer;
                return Source::FillTupleBufferResult::withBytes(DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
            })
        .WillOnce(
            [](TupleBuffer& output, const std::stop_token&)
            {
                EXPECT_FALSE(!!output);
                return Source::FillTupleBufferResult::eos();
            });
    auto thread = start(std::move(source), true);
    wait_for_emits(recorder, 3);
    verify_non_blocking_stop(*thread);
    auto records = recorder.recordedEmits.lock();
    ASSERT_EQ(records->size(), 3);
    for (size_t i = 0; i < 2; ++i)
    {
        ASSERT_TRUE(std::holds_alternative<SourceReturnType::Data>(records->at(i)));
        const auto& emitted = std::get<SourceReturnType::Data>(records->at(i)).buffer;
        EXPECT_EQ(emitted.getNumberOfTuples(), alias.getNumberOfTuples());
        EXPECT_EQ(emitted.getOriginId(), alias.getOriginId());
        EXPECT_EQ(emitted.getSequenceNumber(), alias.getSequenceNumber());
        EXPECT_EQ(emitted.getChunkNumber(), alias.getChunkNumber());
        EXPECT_EQ(emitted.isLastChunk(), alias.isLastChunk());
    }
    EXPECT_TRUE(std::holds_alternative<SourceReturnType::EoS>(records->back()));
}

TEST_F(SourceBufferCapabilityTest, ReturningBufferGetsByteCountAndDefaultMetadata)
{
    auto buffer = bm->getBufferBlocking();
    buffer.setNumberOfTuples(0);
    auto source = std::make_unique<testing::StrictMock<ReturningBufferSource>>();
    EXPECT_CALL(*source, close()).Times(1);
    EXPECT_CALL(*source, fillTupleBuffer(testing::_, testing::_))
        .WillOnce(
            [&](TupleBuffer& output, const std::stop_token&)
            {
                EXPECT_FALSE(!!output);
                output = buffer;
                return Source::FillTupleBufferResult::withBytes(DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
            })
        .WillOnce(testing::Return(Source::FillTupleBufferResult::eos()));
    auto thread = start(std::move(source));
    wait_for_emits(recorder, 2);
    verify_non_blocking_stop(*thread);
    verify_number_of_emits(recorder, 2);
    verify_last_event<SourceReturnType::EoS>(recorder);
    EXPECT_EQ(buffer.getNumberOfTuples(), DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    EXPECT_EQ(buffer.getOriginId(), INITIAL<OriginId>);
    EXPECT_EQ(buffer.getSequenceNumber(), SequenceNumber(1));
    EXPECT_EQ(buffer.getChunkNumber(), INITIAL_CHUNK_NUMBER);
    EXPECT_TRUE(buffer.isLastChunk());
}

TEST_F(SourceBufferCapabilityTest, ReturningSourceCanEndWithoutAllocating)
{
    auto source = std::make_unique<testing::StrictMock<ReturningBufferSource>>();
    EXPECT_CALL(*source, close()).Times(1);
    EXPECT_CALL(*source, fillTupleBuffer(testing::_, testing::_))
        .WillOnce(
            [](TupleBuffer& output, const std::stop_token&)
            {
                EXPECT_FALSE(!!output);
                return Source::FillTupleBufferResult::eos();
            });
    auto thread = start(std::move(source));
    wait_for_emits(recorder, 1);
    verify_non_blocking_stop(*thread);
    verify_number_of_emits(recorder, 1);
    verify_last_event<SourceReturnType::EoS>(recorder);
}

TEST_F(SourceBufferCapabilityTest, ReturningSourceStopClosesWithoutEmittingEos)
{
    std::promise<void> filling;
    auto enteredFill = filling.get_future();
    auto source = std::make_unique<testing::StrictMock<ReturningBufferSource>>();
    EXPECT_CALL(*source, close()).Times(1);
    EXPECT_CALL(*source, fillTupleBuffer(testing::_, testing::_))
        .WillOnce(
            [&](TupleBuffer& output, const std::stop_token& stopToken)
            {
                EXPECT_FALSE(!!output);
                filling.set_value();
                std::mutex mutex;
                std::condition_variable_any condition;
                std::unique_lock lock(mutex);
                condition.wait(lock, stopToken, [] { return false; });
                return Source::FillTupleBufferResult::eos();
            });
    auto thread = start(std::move(source));
    EXPECT_EQ(enteredFill.wait_for(DEFAULT_LONG_TIMEOUT), std::future_status::ready);
    verify_non_blocking_stop(*thread);
    verify_no_events(recorder);
}

TEST_F(SourceBufferCapabilityTest, ReturningSourceFailureClosesAndPreservesOriginalError)
{
    auto source = std::make_unique<testing::StrictMock<ReturningBufferSource>>();
    EXPECT_CALL(*source, close()).WillOnce(testing::Throw(RunningRoutineFailure("close failure")));
    EXPECT_CALL(*source, fillTupleBuffer(testing::_, testing::_))
        .WillOnce(
            [](TupleBuffer& output, const std::stop_token&) -> Source::FillTupleBufferResult
            {
                EXPECT_FALSE(!!output);
                throw RunningRoutineFailure("fill failure");
            });
    auto thread = start(std::move(source));
    wait_for_emits(recorder, 1);
    verify_non_blocking_stop(*thread);
    verify_number_of_emits(recorder, 1);
    verify_last_event<SourceReturnType::Error>(recorder);
    auto records = recorder.recordedEmits.lock();
    ASSERT_TRUE(std::holds_alternative<SourceReturnType::Error>(records->front()));
    EXPECT_THAT(std::get<SourceReturnType::Error>(records->front()).ex.what(), testing::HasSubstr("fill failure"));
    EXPECT_THAT(std::get<SourceReturnType::Error>(records->front()).ex.what(), testing::Not(testing::HasSubstr("close failure")));
}

TEST_F(SourceBufferCapabilityTest, SuccessfulFillRequiresValidBufferEvenForZeroBytes)
{
#ifndef NO_ASSERT
    SKIP_IF_TSAN();
    Logger::setupLogging("SourceThreadInvariantDeathTest.log", LogLevel::LOG_DEBUG, false);
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    for (const size_t bytes : {size_t{0}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER})
    {
        SCOPED_TRACE(bytes);
        EXPECT_DEATH(
            {
                auto source = std::make_unique<testing::StrictMock<ReturningBufferSource>>();
                EXPECT_CALL(*source, close()).Times(1);
                EXPECT_CALL(*source, fillTupleBuffer(testing::_, testing::_))
                    .WillOnce(testing::Return(Source::FillTupleBufferResult::withBytes(bytes)));
                auto thread = start(std::move(source));
                std::this_thread::sleep_for(DEFAULT_LONG_TIMEOUT);
            },
            "Invariant violated.*Source returned data without a valid TupleBuffer");
    }
#endif
}

TEST_F(SourceBufferCapabilityTest, DefaultSourceStillAcquiresAndRetriesPreallocatedBuffers)
{
    auto buffer = bm->getBufferBlocking();
    buffer.setNumberOfTuples(0);
    auto source = std::make_unique<testing::StrictMock<MockSource>>();
    EXPECT_TRUE(source->requiresPreallocatedBuffer());
    EXPECT_CALL(*provider, getBufferWithTimeout(25ms))
        .WillOnce(testing::Return(std::nullopt))
        .WillOnce(testing::Return(std::optional{buffer}))
        .WillOnce(testing::Return(std::optional{buffer}));
    EXPECT_CALL(*source, close()).Times(1);
    EXPECT_CALL(*source, fillTupleBuffer(testing::_, testing::_))
        .WillOnce(
            [&](TupleBuffer& output, const std::stop_token&)
            {
                EXPECT_TRUE(!!output);
                EXPECT_EQ(output.getAvailableMemoryArea().data(), buffer.getAvailableMemoryArea().data());
                return Source::FillTupleBufferResult::withBytes(DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
            })
        .WillOnce(
            [](TupleBuffer& output, const std::stop_token&)
            {
                EXPECT_TRUE(!!output);
                return Source::FillTupleBufferResult::eos();
            });
    auto thread = start(std::move(source));
    wait_for_emits(recorder, 2);
    verify_non_blocking_stop(*thread);
    verify_number_of_emits(recorder, 2);
    verify_last_event<SourceReturnType::EoS>(recorder);
    EXPECT_EQ(buffer.getNumberOfTuples(), DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    EXPECT_EQ(buffer.getOriginId(), INITIAL<OriginId>);
}

/// Internal Stop by destroying the SourceThread
TEST_F(SourceThreadTest, DestructionOfStartedSourceThread)
{
    auto bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<TestSourceControl>();
    {
        SourceThread sourceThread(
            std::move(backpressureListener),
            INITIAL<OriginId>,
            bm,

            std::make_unique<TestSource>(INITIAL<OriginId>, control));
        verify_non_blocking_start(
            sourceThread,
            [&](const OriginId originId, SourceReturnType::SourceReturnType ret, const std::stop_token&)
            {
                recorder(originId, std::move(ret));
                return SourceReturnType::EmitResult::SUCCESS;
            });
    }

    verify_no_events(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->waitUntilClosed()) << "The SourceThread destructor should stop the implementation";
    EXPECT_TRUE(control->wasDestroyed()) << "The SourceThread destructor should destroy the implementation";
}

TEST_F(SourceThreadTest, NoOpDestruction)
{
    auto bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<TestSourceControl>();
    {
        const SourceThread sourceThread(
            backpressureListener, INITIAL<OriginId>, bm, std::make_unique<TestSource>(INITIAL<OriginId>, control));
    }

    verify_no_events(recorder);
    EXPECT_FALSE(control->wasOpened());
    EXPECT_FALSE(control->wasClosed()) << "The SourceThread should not close the implementation if the source was never started";
    EXPECT_TRUE(control->wasDestroyed()) << "The SourceThread destructor should destroy the implementation regardless";
}

TEST_F(SourceThreadTest, FailureDuringRunning)
{
    auto bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<TestSourceControl>();
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectError("I should fail");
    {
        SourceThread sourceThread(backpressureListener, INITIAL<OriginId>, bm, std::make_unique<TestSource>(INITIAL<OriginId>, control));
        verify_non_blocking_start(
            sourceThread,
            [&](const OriginId originId, SourceReturnType::SourceReturnType ret, const std::stop_token&)
            {
                recorder(originId, std::move(ret));
                return SourceReturnType::EmitResult::SUCCESS;
            });
        wait_for_emits(recorder, 3);
        verify_non_blocking_stop(sourceThread);
    }

    verify_number_of_emits(recorder, 3);
    verify_last_event<SourceReturnType::Error>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed()) << "The SourceThread should attempt to close the implementation if the source was started";
    EXPECT_TRUE(control->wasDestroyed()) << "The SourceThread destructor should destroy the implementation regardless";
}

TEST_F(SourceThreadTest, FailureDuringOpen)
{
    auto bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<TestSourceControl>();
    control->failDuringOpen(std::chrono::milliseconds(0));
    {
        SourceThread sourceThread(backpressureListener, INITIAL<OriginId>, bm, std::make_unique<TestSource>(INITIAL<OriginId>, control));
        verify_non_blocking_start(
            sourceThread,
            [&](const OriginId originId, SourceReturnType::SourceReturnType ret, const std::stop_token&)
            {
                recorder(originId, std::move(ret));
                return SourceReturnType::EmitResult::SUCCESS;
            });
        wait_for_emits(recorder, 1);
        verify_non_blocking_stop(sourceThread);
    }

    verify_number_of_emits(recorder, 1);
    verify_last_event<SourceReturnType::Error>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_FALSE(control->wasClosed()) << "The SourceThread should not close the implementation if the source was never started";
    EXPECT_TRUE(control->wasDestroyed()) << "The SourceThread destructor should destroy the implementation regardless";
}

TEST_F(SourceThreadTest, SimpleCaseWithInternalStop)
{
    auto bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<TestSourceControl>();
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    {
        SourceThread sourceThread(backpressureListener, INITIAL<OriginId>, bm, std::make_unique<TestSource>(INITIAL<OriginId>, control));
        verify_non_blocking_start(
            sourceThread,
            [&](const OriginId originId, SourceReturnType::SourceReturnType ret, const std::stop_token&)
            {
                recorder(originId, std::move(ret));
                return SourceReturnType::EmitResult::SUCCESS;
            });
        wait_for_emits(recorder, 3);
        verify_non_blocking_stop(sourceThread);
    }

    verify_number_of_emits(recorder, 3);
    verify_last_event<SourceReturnType::Data>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed());
    EXPECT_TRUE(control->wasDestroyed());
}

TEST_F(SourceThreadTest, EoSFromSourceWithStop)
{
    auto bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    RecordingEmitFunction recorder(*bm);
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    auto control = std::make_shared<TestSourceControl>();
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    {
        SourceThread sourceThread(backpressureListener, INITIAL<OriginId>, bm, std::make_unique<TestSource>(INITIAL<OriginId>, control));
        verify_non_blocking_start(
            sourceThread,
            [&](const OriginId originId, SourceReturnType::SourceReturnType ret, const std::stop_token&)
            {
                recorder(originId, std::move(ret));
                return SourceReturnType::EmitResult::SUCCESS;
            });
        wait_for_emits(recorder, 3);
        control->injectEoS();
        wait_for_emits(recorder, 5);
        verify_non_blocking_stop(sourceThread);
    }

    verify_number_of_emits(recorder, 5);
    verify_last_event<SourceReturnType::EoS>(recorder);
    verify_final_watermark(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed());
    EXPECT_TRUE(control->wasDestroyed());
}

TEST_F(SourceThreadTest, ApplyBackbressure)
{
    auto bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    RecordingEmitFunction recorder(*bm);
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    backpressureController.applyPressure();
    auto control = std::make_shared<TestSourceControl>();
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectEoS();
    {
        SourceThread sourceThread(backpressureListener, INITIAL<OriginId>, bm, std::make_unique<TestSource>(INITIAL<OriginId>, control));
        verify_non_blocking_start(
            sourceThread,
            [&](const OriginId originId, SourceReturnType::SourceReturnType ret, const auto&)
            {
                recorder(originId, std::move(ret));
                return SourceReturnType::EmitResult::SUCCESS;
            });
        wait_for_emits(recorder, 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        EXPECT_FALSE(control->wasClosed());
        wait_for_emits(recorder, 0);
        backpressureController.releasePressure();
        wait_for_emits(recorder, 5);
        verify_non_blocking_stop(sourceThread);
    }

    verify_number_of_emits(recorder, 5);
    verify_last_event<SourceReturnType::EoS>(recorder);
    verify_final_watermark(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed());
    EXPECT_TRUE(control->wasDestroyed());
}

TEST_F(SourceThreadTest, StopDuringBackpressure)
{
    auto bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    RecordingEmitFunction recorder(*bm);
    auto [backpressureController, ingestion] = createBackpressureChannel();
    backpressureController.applyPressure();
    auto control = std::make_shared<TestSourceControl>();
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectEoS();
    {
        SourceThread sourceThread(ingestion, INITIAL<OriginId>, bm, std::make_unique<TestSource>(INITIAL<OriginId>, control));
        verify_non_blocking_start(
            sourceThread,
            [&](const OriginId originId, SourceReturnType::SourceReturnType ret, const auto&)
            {
                recorder(originId, std::move(ret));
                return SourceReturnType::EmitResult::SUCCESS;
            });
        wait_for_emits(recorder, 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        EXPECT_FALSE(control->wasClosed());
        wait_for_emits(recorder, 0);
        verify_non_blocking_stop(sourceThread);
    }

    verify_no_events(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed());
    EXPECT_TRUE(control->wasDestroyed());
}


}
