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

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <source_location>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <MemoryTestUtils.hpp>

// CXX-generated headers for Rust FFI
#include <nes-source-bindings/lib.h>  // init_source_runtime
#include <nes-source-lib/src/lib.h>   // spawn_source_by_name, stop_source, SourceHandle

// IoBindings provides EmitContext, ErrorContext, bridge_emit, on_source_error_callback
#include <IoBindings.hpp>

namespace NES
{
namespace
{

constexpr std::chrono::milliseconds WAIT_TIMEOUT = std::chrono::milliseconds(5000);

/// Records emitted buffers for verification. Unlike SourceThreadTest's RecordingEmitFunction
/// which copies buffers via BufferManager, we store raw byte vectors extracted from the
/// TupleBuffer data pointer, since the buffer lifecycle is managed by the Rust bridge.
struct TokioRecorder
{
    struct EmittedBuffer
    {
        OriginId originId;
        std::vector<uint8_t> data;
        uint64_t numberOfTuples;
    };

    folly::Synchronized<std::vector<EmittedBuffer>, std::mutex> emits;
    std::condition_variable cv;
};

void wait_for_emits(TokioRecorder& recorder, size_t count, std::source_location location = std::source_location::current())
{
    const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "wait_for_emits");
    auto records = recorder.emits.lock();
    if (records->size() >= count)
    {
        return;
    }
    EXPECT_TRUE(recorder.cv.wait_for(records.as_lock(), WAIT_TIMEOUT, [&]() { return records->size() >= count; }))
        << "Timeout waiting for " << count << " emits, got " << records->size();
}

/// Helper: build config vectors for spawn_source_by_name.
std::pair<std::vector<std::string>, std::vector<std::string>>
makeGeneratorConfig(uint64_t count, uint64_t intervalMs)
{
    return {
        {"generator_count", "generator_interval_ms"},
        {std::to_string(count), std::to_string(intervalMs)}
    };
}

} // anonymous namespace

class TokioSourceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("TokioSourceTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup TokioSourceTest test class.");
        // Initialize the shared Tokio runtime (OnceLock, idempotent)
        ::init_source_runtime(2);
    }

    void SetUp() override { Testing::BaseUnitTest::SetUp(); }
};

/// Test 1: GeneratorSource emits 5 buffers with incrementing u64 values 0-4 in order.
TEST_F(TokioSourceTest, GeneratorEmitsIncrementingValues)
{
    constexpr uint64_t SOURCE_ID = 30101;
    constexpr uint64_t COUNT = 5;
    constexpr uint64_t INTERVAL_MS = 1;
    constexpr uint32_t INFLIGHT_LIMIT = 10;

    auto bm = BufferManager::create();
    auto recorder = std::make_shared<TokioRecorder>();

    // Create EmitContext with a recording emit function
    auto emitContext = std::unique_ptr<EmitContext>(new EmitContext{
        [recorder](const OriginId originId, SourceReturnType::SourceReturnType ret, const std::stop_token&)
            -> SourceReturnType::EmitResult
        {
            if (auto* data = std::get_if<SourceReturnType::Data>(&ret))
            {
                auto& buf = data->buffer;
                auto dataSpan = buf.getAvailableMemoryArea<uint8_t>();
                TokioRecorder::EmittedBuffer emitted{
                    originId,
                    std::vector<uint8_t>(dataSpan.begin(), dataSpan.end()),
                    buf.getNumberOfTuples()
                };
                recorder->emits.lock()->emplace_back(std::move(emitted));
                recorder->cv.notify_one();
            }
            return SourceReturnType::EmitResult::SUCCESS;
        },
        std::stop_source{}
    });

    auto errorContext = std::make_unique<ErrorContext>(ErrorContext{SOURCE_ID});

    auto [keys, values] = makeGeneratorConfig(COUNT, INTERVAL_MS);
    auto handle = ::spawn_source_by_name(
        std::string("TokioGenerator"),
        SOURCE_ID,
        keys,
        values,
        reinterpret_cast<uintptr_t>(bm.get()),
        INFLIGHT_LIMIT,
        reinterpret_cast<uintptr_t>(bridge_emit),
        reinterpret_cast<uintptr_t>(emitContext.get()),
        reinterpret_cast<uintptr_t>(on_source_error_callback),
        reinterpret_cast<uintptr_t>(errorContext.get())
    );

    wait_for_emits(*recorder, COUNT);

    // Verify 5 emits with correct u64 values in order
    auto emits = recorder->emits.lock();
    ASSERT_GE(emits->size(), COUNT) << "Expected at least " << COUNT << " emits";

    for (uint64_t i = 0; i < COUNT; ++i)
    {
        auto& emitted = (*emits)[i];
        EXPECT_EQ(emitted.originId, OriginId(SOURCE_ID)) << "Origin ID mismatch at emit " << i;
        EXPECT_EQ(emitted.numberOfTuples, 1u) << "Number of tuples mismatch at emit " << i;

        // GeneratorSource writes u64::to_le_bytes() at offset 0
        ASSERT_GE(emitted.data.size(), sizeof(uint64_t)) << "Buffer too small at emit " << i;
        uint64_t value = 0;
        std::memcpy(&value, emitted.data.data(), sizeof(uint64_t));
        EXPECT_EQ(value, i) << "Expected value " << i << " at emit " << i;
    }

    ::stop_source(*handle);
    // Keep handle, emitContext, errorContext alive for cleanup
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

/// Test 2: BackpressureStability -- 50 emits with inflight_limit=2 complete without overflow.
/// A slow emit function simulates downstream processing delay. The semaphore-based
/// inflight limit ensures the Rust side blocks when 2 buffers are in-flight.
TEST_F(TokioSourceTest, BackpressureStability)
{
    constexpr uint64_t SOURCE_ID = 30102;
    constexpr uint64_t COUNT = 50;
    constexpr uint64_t INTERVAL_MS = 0;
    constexpr uint32_t INFLIGHT_LIMIT = 2;

    auto bm = BufferManager::create();
    auto recorder = std::make_shared<TokioRecorder>();

    auto emitContext = std::unique_ptr<EmitContext>(new EmitContext{
        [recorder](const OriginId originId, SourceReturnType::SourceReturnType ret, const std::stop_token&)
            -> SourceReturnType::EmitResult
        {
            // Simulate slow downstream processing
            std::this_thread::sleep_for(std::chrono::milliseconds(5));

            if (auto* data = std::get_if<SourceReturnType::Data>(&ret))
            {
                auto& buf = data->buffer;
                auto dataSpan = buf.getAvailableMemoryArea<uint8_t>();
                TokioRecorder::EmittedBuffer emitted{
                    originId,
                    std::vector<uint8_t>(dataSpan.begin(), dataSpan.end()),
                    buf.getNumberOfTuples()
                };
                recorder->emits.lock()->emplace_back(std::move(emitted));
                recorder->cv.notify_one();
            }
            return SourceReturnType::EmitResult::SUCCESS;
        },
        std::stop_source{}
    });

    auto errorContext = std::make_unique<ErrorContext>(ErrorContext{SOURCE_ID});

    auto [keys, values] = makeGeneratorConfig(COUNT, INTERVAL_MS);
    auto handle = ::spawn_source_by_name(
        std::string("TokioGenerator"),
        SOURCE_ID,
        keys,
        values,
        reinterpret_cast<uintptr_t>(bm.get()),
        INFLIGHT_LIMIT,
        reinterpret_cast<uintptr_t>(bridge_emit),
        reinterpret_cast<uintptr_t>(emitContext.get()),
        reinterpret_cast<uintptr_t>(on_source_error_callback),
        reinterpret_cast<uintptr_t>(errorContext.get())
    );

    // Longer timeout: 50 emits * 5ms each = ~250ms minimum, plus overhead
    wait_for_emits(*recorder, COUNT);

    auto emits = recorder->emits.lock();
    ASSERT_EQ(emits->size(), COUNT) << "Expected exactly " << COUNT << " emits";

    // Verify all values arrived in order (backpressure should not reorder)
    for (uint64_t i = 0; i < COUNT; ++i)
    {
        ASSERT_GE((*emits)[i].data.size(), sizeof(uint64_t)) << "Buffer too small at emit " << i;
        uint64_t value = 0;
        std::memcpy(&value, (*emits)[i].data.data(), sizeof(uint64_t));
        EXPECT_EQ(value, i) << "Expected value " << i << " at emit " << i;
    }

    ::stop_source(*handle);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

/// Test 3: ShutdownMidEmit -- calling stop_source while the generator is actively emitting
/// completes cleanly without hanging or leaking semaphore permits.
TEST_F(TokioSourceTest, ShutdownMidEmit)
{
    constexpr uint64_t SOURCE_ID = 30103;
    constexpr uint64_t COUNT = 1000000;  // Very large -- will be stopped early
    constexpr uint64_t INTERVAL_MS = 1;
    constexpr uint32_t INFLIGHT_LIMIT = 10;

    auto bm = BufferManager::create();
    auto recorder = std::make_shared<TokioRecorder>();

    auto emitContext = std::unique_ptr<EmitContext>(new EmitContext{
        [recorder](const OriginId originId, SourceReturnType::SourceReturnType ret, const std::stop_token&)
            -> SourceReturnType::EmitResult
        {
            if (auto* data = std::get_if<SourceReturnType::Data>(&ret))
            {
                auto& buf = data->buffer;
                auto dataSpan = buf.getAvailableMemoryArea<uint8_t>();
                TokioRecorder::EmittedBuffer emitted{
                    originId,
                    std::vector<uint8_t>(dataSpan.begin(), dataSpan.end()),
                    buf.getNumberOfTuples()
                };
                recorder->emits.lock()->emplace_back(std::move(emitted));
                recorder->cv.notify_one();
            }
            return SourceReturnType::EmitResult::SUCCESS;
        },
        std::stop_source{}
    });

    auto errorContext = std::make_unique<ErrorContext>(ErrorContext{SOURCE_ID});

    auto [keys, values] = makeGeneratorConfig(COUNT, INTERVAL_MS);
    auto handle = ::spawn_source_by_name(
        std::string("TokioGenerator"),
        SOURCE_ID,
        keys,
        values,
        reinterpret_cast<uintptr_t>(bm.get()),
        INFLIGHT_LIMIT,
        reinterpret_cast<uintptr_t>(bridge_emit),
        reinterpret_cast<uintptr_t>(emitContext.get()),
        reinterpret_cast<uintptr_t>(on_source_error_callback),
        reinterpret_cast<uintptr_t>(errorContext.get())
    );

    // Wait for some emissions to prove source is active
    wait_for_emits(*recorder, 5);

    // Stop the source mid-emission
    ::stop_source(*handle);

    // Allow time for the monitoring task to clean up
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Verify source stopped early (did not emit all 1M buffers)
    auto totalEmits = recorder->emits.lock()->size();
    EXPECT_GT(totalEmits, 0u) << "Should have emitted some buffers before stop";
    EXPECT_LT(totalEmits, COUNT) << "Source should have stopped before emitting all " << COUNT << " buffers";

    NES_INFO("ShutdownMidEmit: stopped after {} emits (out of {})", totalEmits, COUNT);
    // Test completing without hanging proves shutdown is clean
}

} // namespace NES
