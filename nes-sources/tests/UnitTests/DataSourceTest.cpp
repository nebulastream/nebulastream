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


#include <cstddef>
#include <memory>
#include <vector>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceThread.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <MemoryTestUtils.hpp>
#include "TestSource.hpp"

namespace NES
{
using namespace std::literals;
class SourceThreadTest : public Testing::BaseUnitTest
{
public:
    size_t pageSize = getpagesize();
    static void SetUpTestCase()
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
    folly::Synchronized<std::vector<Sources::SourceReturnType::SourceReturnType>, std::mutex> recordedEmits{};
    std::condition_variable block;
};

void wait_for_emits(RecordingEmitFunction& recorder, size_t numberOfEmits, std::source_location location = std::source_location::current())
{
    testing::ScopedTrace scoped_trace(location.file_name(), location.line(), "wait_for_emits");
    auto records = recorder.recordedEmits.lock();
    if (records->size() >= numberOfEmits)
    {
        return;
    }
    EXPECT_TRUE(
        recorder.block.wait_for(records.as_lock(), std::chrono::milliseconds(1000), [&]() { return records->size() >= numberOfEmits; }))
        << "Timeout waiting for " << numberOfEmits << " emits";
}

void verify_non_blocking_stop(Sources::SourceThread& ds, std::source_location location = std::source_location::current())
{
    testing::ScopedTrace scoped_trace(location.file_name(), location.line(), "verify_non_block_stop");
    auto called_stop = std::chrono::high_resolution_clock::now();
    EXPECT_TRUE(ds.stop());
    auto stop_done = std::chrono::high_resolution_clock::now();
    EXPECT_LT(stop_done - called_stop, std::chrono::milliseconds(200))
        << "Stopping a SourceThread should be non blocking. We estimate a block with around 100 ms";
}

void verify_non_blocking_start(
    Sources::SourceThread& ds,
    Sources::SourceReturnType::EmitFunction emitFn,
    std::source_location location = std::source_location::current())
{
    testing::ScopedTrace scoped_trace(location.file_name(), location.line(), "verify_non_block_start");
    auto called_start = std::chrono::high_resolution_clock::now();
    EXPECT_TRUE(ds.start(std::move(emitFn)));
    auto start_done = std::chrono::high_resolution_clock::now();
    EXPECT_LT(start_done - called_start, std::chrono::milliseconds(100))
        << "Starting a SourceThread should be non blocking. We estimate a block with around 100 ms";
}

void verify_no_events(RecordingEmitFunction& recorder, std::source_location location = std::source_location::current())
{
    testing::ScopedTrace scoped_trace(location.file_name(), location.line(), "verify_no_events");
    EXPECT_THAT(*recorder.recordedEmits.lock(), ::testing::SizeIs(0)) << "Expected no source events to be emitted";
}
template <typename T>
void verify_last_event(RecordingEmitFunction& recorder, std::source_location location = std::source_location::current())
{
    testing::ScopedTrace scoped_trace(location.file_name(), location.line(), "verify_last_event");
    EXPECT_THAT(*recorder.recordedEmits.lock(), ::testing::SizeIs(::testing::Gt(0))) << "Expected source events to be emitted";
    auto& last_event = recorder.recordedEmits.lock()->back();
    EXPECT_TRUE(std::holds_alternative<T>(last_event)) << "Last event was not a `" << typeid(T).name() << "` event";
}
void verify_number_of_emits(
    RecordingEmitFunction& recorder, size_t numberOfEmits, std::source_location location = std::source_location::current())
{
    testing::ScopedTrace scoped_trace(location.file_name(), location.line(), "verify_number_of_emits");
    EXPECT_THAT(*recorder.recordedEmits.lock(), ::testing::SizeIs(numberOfEmits))
        << "Expected " << numberOfEmits << " source events to be emitted";
}

/// Internal Stop by destroying the SourceThread
TEST_F(SourceThreadTest, DestructionOfStartedSourceThread)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    {
        Sources::SourceThread ds(
            INITIAL<OriginId>, Schema::create(), bm, 100, std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control));
        verify_non_blocking_start(
            ds, [&](const OriginId origin_id, Sources::SourceReturnType::SourceReturnType ret) { recorder(origin_id, ret); });
    }

    verify_no_events(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed()) << "The SourceThread destructor should stop the implementation";
    EXPECT_TRUE(control->wasDestroyed()) << "The SourceThread destructor should destroy the implementation";
}

TEST_F(SourceThreadTest, NoOpDestruction)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    {
        Sources::SourceThread ds(
            INITIAL<OriginId>, Schema::create(), bm, 100, std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control));
    }

    verify_no_events(recorder);
    EXPECT_FALSE(control->wasOpened());
    EXPECT_FALSE(control->wasClosed()) << "The SourceThread should not close the implementation if the source was never started";
    EXPECT_TRUE(control->wasDestroyed()) << "The SourceThread destructor should destroy the implementation regardless";
}

TEST_F(SourceThreadTest, FailureDuringRunning)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    control->injectData(std::vector{8192, std::byte(0)}, 23);
    control->injectData(std::vector{8192, std::byte(0)}, 23);
    control->injectError("I should fail");
    {
        Sources::SourceThread ds(
            INITIAL<OriginId>, Schema::create(), bm, 100, std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control));
        verify_non_blocking_start(
            ds, [&](const OriginId origin_id, Sources::SourceReturnType::SourceReturnType ret) { recorder(origin_id, ret); });
        wait_for_emits(recorder, 3);
        verify_non_blocking_stop(ds);
    }

    verify_number_of_emits(recorder, 3);
    verify_last_event<Sources::SourceReturnType::Error>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed()) << "The SourceThread should attempt to close the implementation if the source was started";
    EXPECT_TRUE(control->wasDestroyed()) << "The SourceThread destructor should destroy the implementation regardless";
}

TEST_F(SourceThreadTest, FailureDuringOpen)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    control->failDuringOpen();
    {
        Sources::SourceThread ds(
            INITIAL<OriginId>, Schema::create(), bm, 100, std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control));
        verify_non_blocking_start(
            ds, [&](const OriginId origin_id, Sources::SourceReturnType::SourceReturnType ret) { recorder(origin_id, ret); });
        wait_for_emits(recorder, 1);
        verify_non_blocking_stop(ds);
    }

    verify_number_of_emits(recorder, 1);
    verify_last_event<Sources::SourceReturnType::Error>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_FALSE(control->wasClosed()) << "The SourceThread should not close the implementation if the source was never started";
    EXPECT_TRUE(control->wasDestroyed()) << "The SourceThread destructor should destroy the implementation regardless";
}

TEST_F(SourceThreadTest, SimpleCaseWithInternalStop)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    control->injectData(std::vector{8192, std::byte(0)}, 23);
    control->injectData(std::vector{8192, std::byte(0)}, 23);
    control->injectData(std::vector{8192, std::byte(0)}, 23);
    {
        Sources::SourceThread ds(
            INITIAL<OriginId>, Schema::create(), bm, 100, std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control));
        verify_non_blocking_start(
            ds, [&](const OriginId origin_id, Sources::SourceReturnType::SourceReturnType ret) { recorder(origin_id, ret); });
        wait_for_emits(recorder, 3);
        verify_non_blocking_stop(ds);
    }

    verify_number_of_emits(recorder, 3);
    verify_last_event<Sources::SourceReturnType::Data>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed());
    EXPECT_TRUE(control->wasDestroyed());
}

TEST_F(SourceThreadTest, EoSFromSourceWithStop)
{
    auto bm = Memory::BufferManager::create();
    RecordingEmitFunction recorder(*bm);
    auto control = std::make_shared<Sources::TestSourceControl>();
    control->injectData(std::vector{8192, std::byte(0)}, 23);
    control->injectData(std::vector{8192, std::byte(0)}, 23);
    control->injectData(std::vector{8192, std::byte(0)}, 23);
    control->injectEoS();
    {
        Sources::SourceThread ds(
            INITIAL<OriginId>, Schema::create(), bm, 100, std::make_unique<Sources::TestSource>(INITIAL<OriginId>, control));
        verify_non_blocking_start(
            ds, [&](const OriginId origin_id, Sources::SourceReturnType::SourceReturnType ret) { recorder(origin_id, ret); });
        wait_for_emits(recorder, 3);
        control->injectEoS();
        wait_for_emits(recorder, 4);
        verify_non_blocking_stop(ds);
    }

    verify_number_of_emits(recorder, 4);
    verify_last_event<Sources::SourceReturnType::EoS>(recorder);
    EXPECT_TRUE(control->wasOpened());
    EXPECT_TRUE(control->wasClosed());
    EXPECT_TRUE(control->wasDestroyed());
}

}
