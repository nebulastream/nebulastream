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
#include <cstddef>
#include <iterator>
#include <memory>
#include <mutex>
#include <ranges>
#include <source_location>
#include <thread>
#include <utility>
#include <variant>
#include <vector>
#include <unistd.h>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <folly/Synchronized.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <MemoryTestUtils.hpp>
#include <SourceThread.hpp>
#include <TestSource.hpp>

///NOLINTBEGIN(fuchsia-default-arguments-declarations, bugprone-unchecked-optional-access)
namespace NES
{
namespace
{
constexpr std::chrono::milliseconds DEFAULT_TIMEOUT = std::chrono::milliseconds(100);
constexpr std::chrono::milliseconds DEFAULT_LONG_TIMEOUT = std::chrono::milliseconds(1000);
constexpr size_t DEFAULT_BUFFER_SIZE = 8192;
constexpr size_t DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER = 23;
constexpr size_t DEFAULT_NUMBER_OF_LOCAL_BUFFERS = 64;

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
    void operator()(const OriginId, Sources::SourceReturnType::SourceReturnType emittedData)
    {
        auto value = std::visit(
            [&]<typename T>(T emitted) -> Sources::SourceReturnType::SourceReturnType
            {
                if constexpr (std::same_as<T, Sources::SourceReturnType::Data>)
                {
                    /// Release old Buffer
                    return Sources::SourceReturnType::Data{Testing::copyBuffer(emitted.buffer, bm)};
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

    Memory::AbstractBufferProvider& bm;
    folly::Synchronized<std::vector<Sources::SourceReturnType::SourceReturnType>, std::mutex> recordedEmits;
    std::condition_variable block;
};

void waitForEmits(RecordingEmitFunction& recorder, size_t numberOfEmits, std::source_location location = std::source_location::current())
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

void verifyNonBlockingStop(Sources::SourceThread& sourceThread, std::source_location location = std::source_location::current())
{
    const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_non_block_stop");
    auto calledStop = std::chrono::high_resolution_clock::now();
    sourceThread.stop();
    auto stopDone = std::chrono::high_resolution_clock::now();
    EXPECT_LT(stopDone - calledStop, DEFAULT_TIMEOUT)
        << "Stopping a SourceThread should be non blocking. We estimate a block with around 100 ms";
}

void verifyNonBlockingStart(
    Sources::SourceThread& sourceThread,
    Sources::SourceReturnType::EmitFunction emitFn,
    std::source_location location = std::source_location::current())
{
    const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_non_block_start");
    auto calledStart = std::chrono::high_resolution_clock::now();
    EXPECT_TRUE(sourceThread.start(std::move(emitFn)));
    auto startDone = std::chrono::high_resolution_clock::now();
    EXPECT_LT(startDone - calledStart, DEFAULT_TIMEOUT)
        << "Starting a SourceThread should be non blocking. We estimate a block with around 100 ms";
}

void verifyNoEvents(RecordingEmitFunction& recorder, std::source_location location = std::source_location::current())
{
    const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_no_events");
    EXPECT_THAT(*recorder.recordedEmits.lock(), ::testing::SizeIs(0)) << "Expected no source events to be emitted";
}
template <typename T>
void verifyLastEvent(RecordingEmitFunction& recorder, std::source_location location = std::source_location::current())
{
    const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_last_event");
    EXPECT_THAT(*recorder.recordedEmits.lock(), ::testing::SizeIs(::testing::Gt(0))) << "Expected source events to be emitted";
    auto& lastEvent = recorder.recordedEmits.lock()->back();
    EXPECT_TRUE(std::holds_alternative<T>(lastEvent)) << "Last event was not a `" << typeid(T).name() << "` event";
}
void verifyNumberOfEmits(
    RecordingEmitFunction& recorder, size_t numberOfEmits, std::source_location location = std::source_location::current())
{
    const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_number_of_emits");
    ASSERT_THAT(*recorder.recordedEmits.lock(), ::testing::SizeIs(numberOfEmits))
        << "Expected " << numberOfEmits << " source events to be emitted";

    std::vector<SequenceNumber> sequenceNumbers;
    for (size_t i = 0; i < numberOfEmits; ++i)
    {
        auto& emitted = recorder.recordedEmits.lock()->at(i);
        if (auto* data = std::get_if<Sources::SourceReturnType::Data>(&emitted))
        {
            sequenceNumbers.push_back(data->buffer.getSequenceNumber());
        }
    }
    auto expected_view
        = std::views::iota(size_t(0), sequenceNumbers.size()) | std::views::transform([](auto s) { return SequenceNumber(s + 1); });
    std::vector<SequenceNumber> expected;
    std::ranges::copy(expected_view, std::back_inserter(expected));
    EXPECT_THAT(sequenceNumbers, ::testing::ContainerEq(expected));
}

/// Internal Stop by destroying the SourceThread
TEST_F(SourceThreadTest, DestructionOfStartedSourceThread)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    {
        Sources::SourceThread sourceThread(
            INITIAL<OriginId>, bm, DEFAULT_NUMBER_OF_LOCAL_BUFFERS, std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control));
        verifyNonBlockingStart(
            sourceThread,
            [&](const OriginId originId, Sources::SourceReturnType::SourceReturnType ret) { recorder(originId, std::move(ret)); });
        ASSERT_TRUE(control->waitUntilOpened());
    }

    verifyNoEvents(recorder);
    EXPECT_TRUE(control->waitUntilClosed()) << "The SourceThread destructor should stop the implementation";
    EXPECT_TRUE(control->wasDestroyed()) << "The SourceThread destructor should destroy the implementation";
}

/// Source Thread should not fail if it cannot allocate its desired number of buffers. Instead, it should reattempt until buffers
/// are available. This test verifies that the source reattempts buffer allocation by reserving a large chunk of the available buffers
/// before the source creation. The source should not be able to open its implementation because it attempts to allocate buffers fail.
/// The initial reservation is released which allows the source to proceed.
TEST_F(SourceThreadTest, ReattemptBufferAllocation)
{
    /// Restrict the bufferprovider to one more buffer than needed for the sources fixed size buffer.
    auto bm = Memory::BufferManager::create(DEFAULT_BUFFER_SIZE, DEFAULT_NUMBER_OF_LOCAL_BUFFERS + 1);

    /// create reservation that should prevent the source from starting
    auto fixedSizeBufferProvider = bm->createFixedSizeBufferPool(DEFAULT_NUMBER_OF_LOCAL_BUFFERS).value();
    EXPECT_EQ(bm->getAvailableBuffers(), 1);
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    {
        Sources::SourceThread sourceThread(
            INITIAL<OriginId>, bm, DEFAULT_NUMBER_OF_LOCAL_BUFFERS, std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control));
        verifyNonBlockingStart(
            sourceThread,
            [&](const OriginId originId, Sources::SourceReturnType::SourceReturnType ret) { recorder(originId, std::move(ret)); });

        /// Source should not be able to open
        std::this_thread::sleep_for(DEFAULT_TIMEOUT);
        ASSERT_FALSE(control->wasOpened());
        verifyNoEvents(recorder);

        /// Release reservation
        fixedSizeBufferProvider->destroy();

        /// Source should now be able to open
        ASSERT_TRUE(control->waitUntilOpened());
        waitForEmits(recorder, 1);
        recorder.recordedEmits.lock()->clear();

        verifyNonBlockingStop(sourceThread);
    }

    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->waitUntilClosed()) << "The SourceThread destructor should stop the implementation";
    EXPECT_TRUE(control->wasDestroyed()) << "The SourceThread destructor should destroy the implementation";
    EXPECT_EQ(bm->getAvailableBuffers(), DEFAULT_NUMBER_OF_LOCAL_BUFFERS + 1) << "Not all buffers have been released by the source";
}

TEST_F(SourceThreadTest, NoOpDestruction)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    {
        const Sources::SourceThread sourceThread(
            INITIAL<OriginId>, bm, DEFAULT_NUMBER_OF_LOCAL_BUFFERS, std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control));
    }

    verifyNoEvents(recorder);
    EXPECT_FALSE(control->wasOpened());
    EXPECT_FALSE(control->wasClosed()) << "The SourceThread should not close the implementation if the source was never started";
    EXPECT_TRUE(control->wasDestroyed()) << "The SourceThread destructor should destroy the implementation regardless";
}

TEST_F(SourceThreadTest, FailureDuringRunning)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectError("I should fail");
    {
        Sources::SourceThread sourceThread(
            INITIAL<OriginId>, bm, DEFAULT_NUMBER_OF_LOCAL_BUFFERS, std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control));
        verifyNonBlockingStart(
            sourceThread,
            [&](const OriginId originId, Sources::SourceReturnType::SourceReturnType ret) { recorder(originId, std::move(ret)); });
        waitForEmits(recorder, 3);
        verifyNonBlockingStop(sourceThread);
    }

    verifyNumberOfEmits(recorder, 3);
    verifyLastEvent<Sources::SourceReturnType::Error>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed()) << "The SourceThread should attempt to close the implementation if the source was started";
    EXPECT_TRUE(control->wasDestroyed()) << "The SourceThread destructor should destroy the implementation regardless";
}

TEST_F(SourceThreadTest, FailureDuringOpen)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    control->failDuringOpen(std::chrono::milliseconds(0));
    {
        Sources::SourceThread sourceThread(
            INITIAL<OriginId>, bm, DEFAULT_NUMBER_OF_LOCAL_BUFFERS, std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control));
        verifyNonBlockingStart(
            sourceThread,
            [&](const OriginId originId, Sources::SourceReturnType::SourceReturnType ret) { recorder(originId, std::move(ret)); });
        waitForEmits(recorder, 1);
        verifyNonBlockingStop(sourceThread);
    }

    verifyNumberOfEmits(recorder, 1);
    verifyLastEvent<Sources::SourceReturnType::Error>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_FALSE(control->wasClosed()) << "The SourceThread should not close the implementation if the source was never started";
    EXPECT_TRUE(control->wasDestroyed()) << "The SourceThread destructor should destroy the implementation regardless";
}

TEST_F(SourceThreadTest, SimpleCaseWithInternalStop)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    {
        Sources::SourceThread sourceThread(
            INITIAL<OriginId>, bm, DEFAULT_NUMBER_OF_LOCAL_BUFFERS, std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control));
        verifyNonBlockingStart(
            sourceThread,
            [&](const OriginId originId, Sources::SourceReturnType::SourceReturnType ret) { recorder(originId, std::move(ret)); });
        waitForEmits(recorder, 3);
        verifyNonBlockingStop(sourceThread);
    }

    verifyNumberOfEmits(recorder, 3);
    verifyLastEvent<Sources::SourceReturnType::Data>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed());
    EXPECT_TRUE(control->wasDestroyed());
}

TEST_F(SourceThreadTest, EoSFromSourceWithStop)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, std::byte(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectEoS();
    {
        Sources::SourceThread sourceThread(
            INITIAL<OriginId>, bm, DEFAULT_NUMBER_OF_LOCAL_BUFFERS, std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control));
        verifyNonBlockingStart(
            sourceThread,
            [&](const OriginId originId, Sources::SourceReturnType::SourceReturnType ret) { recorder(originId, std::move(ret)); });
        waitForEmits(recorder, 3);
        control->injectEoS();
        waitForEmits(recorder, 4);
        verifyNonBlockingStop(sourceThread);
    }

    verifyNumberOfEmits(recorder, 4);
    verifyLastEvent<Sources::SourceReturnType::EoS>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed());
    EXPECT_TRUE(control->wasDestroyed());
}

}

///NOLINTEND(fuchsia-default-arguments-declarations, bugprone-unchecked-optional-access)
