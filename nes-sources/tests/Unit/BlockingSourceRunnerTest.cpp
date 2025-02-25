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
#include <numeric>
#include <ranges>
#include <source_location>
#include <utility>
#include <variant>
#include <vector>
#include <unistd.h>
#include <Blocking/BlockingSourceHandle.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <folly/Synchronized.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <MemoryTestUtils.hpp>
#include <TestSource.hpp>

namespace NES
{
namespace
{
constexpr std::chrono::milliseconds DEFAULT_TIMEOUT = std::chrono::milliseconds(100);
constexpr std::chrono::milliseconds DEFAULT_LONG_TIMEOUT = std::chrono::milliseconds(1000);
constexpr size_t DEFAULT_BUFFER_SIZE = 8192;
constexpr size_t DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER = 23;
constexpr size_t DEFAULT_NUMBER_OF_LOCAL_BUFFERS = 100;

}
using namespace std::literals;
class BlockingSourceRunnerTest : public Testing::BaseUnitTest
{
public:
    size_t pageSize = getpagesize();
    static void SetUpTestSuite()
    {
        Logger::setupLogging("BlockingSourceRunner.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup BlockingSourceRunner test class.");
    }
    void SetUp() override { Testing::BaseUnitTest::SetUp(); }
};

struct RecordingEmitFunction
{
    void operator()(const OriginId, Sources::SourceReturnType emittedData)
    {
        auto value = std::visit(
            [&]<typename T>(T emitted) -> Sources::SourceReturnType
            {
                if constexpr (std::same_as<T, Sources::Data>)
                {
                    /// Release old Buffer
                    return Sources::Data{Testing::copyBuffer(emitted.buffer, bm)};
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
    folly::Synchronized<std::vector<Sources::SourceReturnType>, std::mutex> recordedEmits;
    std::condition_variable block;
};

void wait_for_emits(RecordingEmitFunction& recorder, size_t numberOfEmits, std::source_location location = std::source_location::current())
{
    testing::ScopedTrace const scopedTrace(location.file_name(), static_cast<int>(location.line()), "wait_for_emits");
    auto records = recorder.recordedEmits.lock();
    if (records->size() >= numberOfEmits)
    {
        return;
    }
    EXPECT_TRUE(recorder.block.wait_for(records.as_lock(), DEFAULT_LONG_TIMEOUT, [&]() { return records->size() >= numberOfEmits; }))
        << "Timeout waiting for " << numberOfEmits << " emits";
}

void verify_non_blocking_stop(Sources::BlockingSourceHandle& sourceHandle, std::source_location location = std::source_location::current())
{
    testing::ScopedTrace const scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_non_block_stop");
    auto calledStop = std::chrono::high_resolution_clock::now();
    EXPECT_TRUE(sourceHandle.stop());
    auto stopDone = std::chrono::high_resolution_clock::now();
    EXPECT_LT(stopDone - calledStop, DEFAULT_TIMEOUT)
        << "Stopping a BlockingSourceRunner should be non blocking. We estimate a block with around 100 ms";
}

void verify_non_blocking_start(
    Sources::BlockingSourceHandle& sourceHandle,
    Sources::EmitFunction emitFn,
    std::source_location location = std::source_location::current())
{
    testing::ScopedTrace const scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_non_block_start");
    auto calledStart = std::chrono::high_resolution_clock::now();
    EXPECT_TRUE(sourceHandle.start(std::move(emitFn)));
    auto startDone = std::chrono::high_resolution_clock::now();
    EXPECT_LT(startDone - calledStart, DEFAULT_TIMEOUT)
        << "Starting a BlockingSourceRunner should still be non-blocking. We estimate a block with around 100 ms";
}

void verify_no_events(RecordingEmitFunction& recorder, std::source_location location = std::source_location::current())
{
    testing::ScopedTrace const scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_no_events");
    EXPECT_THAT(*recorder.recordedEmits.lock(), ::testing::SizeIs(0)) << "Expected no source events to be emitted";
}
template <typename T>
void verify_last_event(RecordingEmitFunction& recorder, std::source_location location = std::source_location::current())
{
    testing::ScopedTrace const scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_last_event");
    EXPECT_THAT(*recorder.recordedEmits.lock(), ::testing::SizeIs(::testing::Gt(0))) << "Expected source events to be emitted";
    auto& lastEvent = recorder.recordedEmits.lock()->back();
    EXPECT_TRUE(std::holds_alternative<T>(lastEvent)) << "Last event was not a `" << typeid(T).name() << "` event";
}
void verify_number_of_emits(
    RecordingEmitFunction& recorder, size_t numberOfEmits, std::source_location location = std::source_location::current())
{
    testing::ScopedTrace const scopedTrace(location.file_name(), static_cast<int>(location.line()), "verify_number_of_emits");
    EXPECT_THAT(*recorder.recordedEmits.lock(), ::testing::SizeIs(numberOfEmits))
        << "Expected " << numberOfEmits << " source events to be emitted";

    std::vector<SequenceNumber> sequenceNumbers;
    for (size_t i = 0; i < numberOfEmits; ++i)
    {
        auto& emitted = recorder.recordedEmits.lock()->at(i);
        if (auto* data = std::get_if<Sources::Data>(&emitted))
        {
            sequenceNumbers.push_back(data->buffer.getSequenceNumber());
        }
    }
    auto expected_view
        = std::views::iota(size_t{0}, sequenceNumbers.size()) | std::views::transform([](auto s) { return SequenceNumber(s + 1); });
    std::vector<SequenceNumber> expected;
    std::ranges::copy(expected_view, std::back_inserter(expected));
    EXPECT_THAT(sequenceNumbers, ::testing::ContainerEq(expected));
}

/// Internal Stop by destroying the BlockingSourceRunner
TEST_F(BlockingSourceRunnerTest, DestructionOfStartedSourceRunner)
{
    const auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    {
        auto context = Sources::SourceExecutionContext<Sources::BlockingSource>{
            INITIAL<OriginId>,
            std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control),
            *bm->createFixedSizeBufferPool(DEFAULT_NUMBER_OF_LOCAL_BUFFERS),
            std::make_unique<Sources::NoOpInputFormatter>()};

        Sources::BlockingSourceHandle blockingSourceHandle{std::move(context)};
        verify_non_blocking_start(
            blockingSourceHandle, [&](const OriginId originId, Sources::SourceReturnType ret) { recorder(originId, std::move(ret)); });
    }

    verify_no_events(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->waitUntilClosed()) << "The BlockingSourceRunner destructor should stop the implementation";
    EXPECT_TRUE(control->wasDestroyed()) << "The BlockingSourceRunner destructor should destroy the implementation";
}

TEST_F(BlockingSourceRunnerTest, NoOpDestruction)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    {
        auto context = Sources::SourceExecutionContext<Sources::BlockingSource>{
            INITIAL<OriginId>,
            std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control),
            *bm->createFixedSizeBufferPool(DEFAULT_NUMBER_OF_LOCAL_BUFFERS),
            std::make_unique<Sources::NoOpInputFormatter>()};
        Sources::BlockingSourceHandle blockingSourceHandle{std::move(context)};
    }

    verify_no_events(recorder);
    EXPECT_FALSE(control->wasOpened());
    EXPECT_FALSE(control->wasClosed()) << "The BlockingSourceHandle should not close the implementation if the source was never started";
    EXPECT_TRUE(control->wasDestroyed()) << "The BlockingSourceHandle destructor should destroy the implementation regardless";
}

TEST_F(BlockingSourceRunnerTest, FailureDuringRunning)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, static_cast<std::byte>(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, static_cast<std::byte>(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectError("I should fail");
    {
        auto context = Sources::SourceExecutionContext<Sources::BlockingSource>{
            INITIAL<OriginId>,
            std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control),
            *bm->createFixedSizeBufferPool(DEFAULT_NUMBER_OF_LOCAL_BUFFERS),
            std::make_unique<Sources::NoOpInputFormatter>()};
        Sources::BlockingSourceHandle source{std::move(context)};
        verify_non_blocking_start(
            source, [&](const OriginId originId, Sources::SourceReturnType ret) { recorder(originId, std::move(ret)); });
        wait_for_emits(recorder, 3);
        verify_non_blocking_stop(source);
    }

    verify_number_of_emits(recorder, 3);
    verify_last_event<Sources::Error>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed()) << "The BlockingSourceRunner should attempt to close the implementation if the source was started";
    EXPECT_TRUE(control->wasDestroyed()) << "The BlockingSourceRunner destructor should destroy the implementation regardless";
}

TEST_F(BlockingSourceRunnerTest, FailureDuringOpen)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    control->failDuringOpen(std::chrono::milliseconds(0));
    {
        auto context = Sources::SourceExecutionContext<Sources::BlockingSource>{
            .originId = INITIAL<OriginId>,
            .sourceImpl = std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control),
            .bufferProvider = *bm->createFixedSizeBufferPool(DEFAULT_NUMBER_OF_LOCAL_BUFFERS),
            .inputFormatter = std::make_unique<Sources::NoOpInputFormatter>()};
        Sources::BlockingSourceHandle source{std::move(context)};
        verify_non_blocking_start(
            source, [&](const OriginId originId, Sources::SourceReturnType ret) { recorder(originId, std::move(ret)); });
        wait_for_emits(recorder, 1);
        verify_non_blocking_stop(source);
    }

    verify_number_of_emits(recorder, 1);
    verify_last_event<Sources::Error>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_FALSE(control->wasClosed()) << "The BlockingSourceRunner should not close the implementation if the source was never started";
    EXPECT_TRUE(control->wasDestroyed()) << "The BlockingSourceRunner destructor should destroy the implementation regardless";
}

TEST_F(BlockingSourceRunnerTest, SimpleCaseWithInternalStop)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, static_cast<std::byte>(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, static_cast<std::byte>(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, static_cast<std::byte>(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    {
        auto context = Sources::SourceExecutionContext<Sources::BlockingSource>{
            INITIAL<OriginId>,
            std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control),
            *bm->createFixedSizeBufferPool(DEFAULT_NUMBER_OF_LOCAL_BUFFERS),
            std::make_unique<Sources::NoOpInputFormatter>()};
        Sources::BlockingSourceHandle source{std::move(context)};
        verify_non_blocking_start(
            source, [&](const OriginId originId, Sources::SourceReturnType ret) { recorder(originId, std::move(ret)); });
        wait_for_emits(recorder, 3);
        verify_non_blocking_stop(source);
    }

    verify_number_of_emits(recorder, 3);
    verify_last_event<Sources::Data>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed());
    EXPECT_TRUE(control->wasDestroyed());
}

TEST_F(BlockingSourceRunnerTest, EoSFromSourceWithStop)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, static_cast<std::byte>(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, static_cast<std::byte>(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectData(std::vector{DEFAULT_BUFFER_SIZE, static_cast<std::byte>(0)}, DEFAULT_NUMBER_OF_TUPLES_IN_BUFFER);
    control->injectEoS();
    {
        auto context = Sources::SourceExecutionContext<Sources::BlockingSource>{
            INITIAL<OriginId>,
            std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control),
            *bm->createFixedSizeBufferPool(DEFAULT_NUMBER_OF_LOCAL_BUFFERS),
            std::make_unique<Sources::NoOpInputFormatter>()};
        Sources::BlockingSourceHandle source{std::move(context)};
        verify_non_blocking_start(
            source, [&](const OriginId originId, Sources::SourceReturnType ret) { recorder(originId, std::move(ret)); });
        wait_for_emits(recorder, 3);
        control->injectEoS();
        wait_for_emits(recorder, 4);
        verify_non_blocking_stop(source);
    }

    verify_number_of_emits(recorder, 4);
    verify_last_event<Sources::EoS>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed());
    EXPECT_TRUE(control->wasDestroyed());
}

}
