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
#include <atomic>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <ranges>
#include <source_location>
#include <stop_token>
#include <thread>
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
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/chrono.h>
#include <folly/Synchronized.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
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

/// Delivers one buffer, then a reconfiguration marker.
class MarkerSource final : public Source
{
public:
    explicit MarkerSource(std::string marker) : marker(std::move(marker)) { }

    void open(std::shared_ptr<AbstractBufferProvider>) override { }

    void close() override { }

    FillTupleBufferResult fillTupleBuffer(TupleBuffer&, const std::stop_token&) override
    {
        if (emitted)
        {
            return FillTupleBufferResult::marker(marker);
        }
        emitted = true;
        return FillTupleBufferResult::withBytes(DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    }

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override { return str << "MarkerSource"; }

private:
    std::string marker;
    bool emitted = false;
};

/// Blocks in fillTupleBuffer until it is interrupted. It cannot drain by itself, so a drain has to interrupt it.
class BlockingSource final : public Source
{
public:
    void open(std::shared_ptr<AbstractBufferProvider>) override { }

    void close() override { }

    FillTupleBufferResult fillTupleBuffer(TupleBuffer&, const std::stop_token& stopToken) override
    {
        entered = true;
        std::mutex mutex;
        std::condition_variable_any interrupted;
        std::unique_lock lock(mutex);
        interrupted.wait(lock, stopToken, [] { return false; });
        wasInterrupted = true;
        return FillTupleBufferResult::eos();
    }

    /// Waits until the source thread blocks in fillTupleBuffer.
    [[nodiscard]] bool waitUntilEntered() const
    {
        const auto deadline = std::chrono::steady_clock::now() + DEFAULT_LONG_TIMEOUT;
        while (!entered)
        {
            if (std::chrono::steady_clock::now() > deadline)
            {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return true;
    }

    std::atomic_bool entered = false;
    std::atomic_bool wasInterrupted = false;

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override { return str << "BlockingSource"; }
};

/// Ends its stream by itself when drained: it delivers its remaining buffers, then EoS. Optionally, it blocks after the remaining
/// buffers until it is interrupted.
class SelfDrainingSource final : public Source
{
public:
    SelfDrainingSource(size_t remainingBuffers, bool blockAfterRemainingBuffers)
        : remainingBuffers(remainingBuffers), blockAfterRemainingBuffers(blockAfterRemainingBuffers)
    {
    }

    void open(std::shared_ptr<AbstractBufferProvider>) override { }

    void close() override { }

    [[nodiscard]] bool drain() override
    {
        drained = true;
        return true;
    }

    FillTupleBufferResult fillTupleBuffer(TupleBuffer&, const std::stop_token& stopToken) override
    {
        while (!drained)
        {
            if (stopToken.stop_requested())
            {
                wasInterrupted = true;
                return FillTupleBufferResult::eos();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (remainingBuffers > 0)
        {
            --remainingBuffers;
            return FillTupleBufferResult::withBytes(DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
        }
        if (blockAfterRemainingBuffers)
        {
            std::mutex mutex;
            std::condition_variable_any interrupted;
            std::unique_lock lock(mutex);
            interrupted.wait(lock, stopToken, [] { return false; });
            wasInterrupted = true;
        }
        return FillTupleBufferResult::eos();
    }

    std::atomic_bool drained = false;
    std::atomic_bool wasInterrupted = false;

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override { return str << "SelfDrainingSource"; }

private:
    size_t remainingBuffers;
    bool blockAfterRemainingBuffers;
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

std::shared_ptr<BufferManager> createDrainTestBufferManager()
{
    return BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
}

SourceReturnType::EmitFunction recordingEmit(RecordingEmitFunction& recorder)
{
    return [&recorder](const OriginId originId, SourceReturnType::SourceReturnType ret, const std::stop_token&)
    {
        recorder(originId, std::move(ret));
        return SourceReturnType::EmitResult::SUCCESS;
    };
}

/// Verifies that the recorded events end with the final watermark and a `Drained` with the given marker.
void verify_drained(
    RecordingEmitFunction& recorder, const std::string& marker, std::source_location location = std::source_location::current())
{
    const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_drained");
    const auto events = recorder.recordedEmits.lock();
    ASSERT_GE(events->size(), 2);
    const auto* finalWatermark = std::get_if<SourceReturnType::Data>(&events->at(events->size() - 2));
    ASSERT_NE(finalWatermark, nullptr);
    EXPECT_EQ(finalWatermark->buffer.getWatermark(), Timestamp{Timestamp::INFINITE_VALUE});
    const auto* drained = std::get_if<SourceReturnType::Drained>(&events->back());
    ASSERT_NE(drained, nullptr) << "Last event was not a `Drained` event";
    EXPECT_EQ(drained->marker, marker);
}

/// A marker from the source ends the stream like an EoS, but is reported as a drain.
TEST_F(SourceThreadTest, MarkerFromSourceIsReportedAsDrained)
{
    auto bm = createDrainTestBufferManager();
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    RecordingEmitFunction recorder(*bm);
    SourceThread sourceThread(std::move(backpressureListener), INITIAL<OriginId>, bm, std::make_unique<MarkerSource>("marker 9"));
    verify_non_blocking_start(sourceThread, recordingEmit(recorder));
    wait_for_emits(recorder, 3);
    verify_non_blocking_stop(sourceThread);

    verify_drained(recorder, "marker 9");
    const auto events = recorder.recordedEmits.lock();
    ASSERT_EQ(events->size(), 3);
    const auto* data = std::get_if<SourceReturnType::Data>(&events->at(0));
    ASSERT_NE(data, nullptr);
    EXPECT_EQ(data->buffer.getSequenceNumber(), INITIAL_SEQ_NUMBER);
    EXPECT_EQ(
        std::get<SourceReturnType::Data>(events->at(1)).buffer.getSequenceNumber(), SequenceNumber(INITIAL_SEQ_NUMBER.getRawValue() + 1));
}

/// A source which cannot drain by itself is interrupted, but unlike a stop, the source thread emits the final watermark and `Drained`.
TEST_F(SourceThreadTest, DrainInterruptsSourceWhichCannotDrainByItself)
{
    auto bm = createDrainTestBufferManager();
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    RecordingEmitFunction recorder(*bm);
    auto source = std::make_unique<BlockingSource>();
    auto* sourcePtr = source.get();
    SourceThread sourceThread(std::move(backpressureListener), INITIAL<OriginId>, bm, std::move(source));
    verify_non_blocking_start(sourceThread, recordingEmit(recorder));
    ASSERT_TRUE(sourcePtr->waitUntilEntered());

    EXPECT_TRUE(sourceThread.drain("marker 3"));
    wait_for_emits(recorder, 2);
    EXPECT_TRUE(sourcePtr->wasInterrupted);
    verify_non_blocking_stop(sourceThread);

    verify_drained(recorder, "marker 3");
    EXPECT_THAT(*recorder.recordedEmits.lock(), ::testing::SizeIs(2));
}

/// A source which drains by itself is not interrupted: it delivers its remaining data before the stream ends.
TEST_F(SourceThreadTest, SourceWhichDrainsByItselfDeliversItsRemainingData)
{
    auto bm = createDrainTestBufferManager();
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    RecordingEmitFunction recorder(*bm);
    auto source = std::make_unique<SelfDrainingSource>(2, false);
    auto* sourcePtr = source.get();
    SourceThread sourceThread(std::move(backpressureListener), INITIAL<OriginId>, bm, std::move(source));
    verify_non_blocking_start(sourceThread, recordingEmit(recorder));

    EXPECT_TRUE(sourceThread.drain("marker 5"));
    wait_for_emits(recorder, 4);
    verify_non_blocking_stop(sourceThread);

    EXPECT_TRUE(sourcePtr->drained);
    EXPECT_FALSE(sourcePtr->wasInterrupted);
    verify_drained(recorder, "marker 5");
    const auto events = recorder.recordedEmits.lock();
    ASSERT_EQ(events->size(), 4);
    EXPECT_EQ(std::get<SourceReturnType::Data>(events->at(0)).buffer.getNumberOfTuples(), DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    EXPECT_EQ(std::get<SourceReturnType::Data>(events->at(1)).buffer.getNumberOfTuples(), DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
}

/// Only the first drain is accepted.
TEST_F(SourceThreadTest, SecondDrainIsRejected)
{
    auto bm = createDrainTestBufferManager();
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    RecordingEmitFunction recorder(*bm);
    SourceThread sourceThread(std::move(backpressureListener), INITIAL<OriginId>, bm, std::make_unique<BlockingSource>());
    verify_non_blocking_start(sourceThread, recordingEmit(recorder));

    EXPECT_TRUE(sourceThread.drain("marker 1"));
    EXPECT_FALSE(sourceThread.drain("marker 2"));
    wait_for_emits(recorder, 2);
    verify_non_blocking_stop(sourceThread);

    verify_drained(recorder, "marker 1");
}

/// A stop while the source is still draining wins: a stopped query needs neither the final watermark nor `Drained`.
TEST_F(SourceThreadTest, StopDuringDrainIsNotReportedAsDrained)
{
    auto bm = createDrainTestBufferManager();
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    RecordingEmitFunction recorder(*bm);
    SourceThread sourceThread(std::move(backpressureListener), INITIAL<OriginId>, bm, std::make_unique<SelfDrainingSource>(1, true));
    verify_non_blocking_start(sourceThread, recordingEmit(recorder));

    EXPECT_TRUE(sourceThread.drain("marker 4"));
    wait_for_emits(recorder, 1);
    verify_non_blocking_stop(sourceThread);

    const auto events = recorder.recordedEmits.lock();
    ASSERT_EQ(events->size(), 1);
    EXPECT_TRUE(std::holds_alternative<SourceReturnType::Data>(events->at(0)));
}

}
