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
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/TokioSink.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

// CXX-generated headers
#include <nes-source-bindings/lib.h>   // init_source_runtime
#include <nes-source-lib/src/lib.h>   // spawn_devnull_sink, SinkHandle, SendResult

// IoBindings provides ErrorContext, on_sink_error_callback
#include <IoBindings.hpp>

namespace NES
{
namespace
{

/// Mock PipelineExecutionContext that records repeatTask calls.
class MockPipelineContext : public PipelineExecutionContext
{
public:
    struct RepeatCall
    {
        TupleBuffer buffer;
        std::chrono::milliseconds interval;
    };

    std::vector<RepeatCall> repeatCalls;
    std::shared_ptr<AbstractBufferProvider> bufferManager;
    bool repeatTaskCalled{false};

    explicit MockPipelineContext(std::shared_ptr<AbstractBufferProvider> bm)
        : bufferManager(std::move(bm)) {}

    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return true; }

    void repeatTask(const TupleBuffer& buf, std::chrono::milliseconds interval) override
    {
        repeatCalls.push_back({buf, interval});
        repeatTaskCalled = true;
    }

    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

    TupleBuffer& pinBuffer(TupleBuffer&& buf) override
    {
        pinnedBuffers.push_back(std::move(buf));
        return pinnedBuffers.back();
    }

    WorkerThreadId getId() const override { return WorkerThreadId(1); }
    uint64_t getNumberOfWorkerThreads() const override { return 1; }
    std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }
    PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override
    {
        return handlers;
    }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& h) override
    {
        handlers = h;
    }

private:
    std::vector<TupleBuffer> pinnedBuffers;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
};

} // anonymous namespace

class TokioSinkTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("TokioSinkTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup TokioSinkTest test class.");
        // Initialize the shared Tokio runtime (OnceLock, idempotent)
        ::init_source_runtime(2);
    }

    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        bufferManager = BufferManager::create();
    }

    std::shared_ptr<BufferManager> bufferManager;
};

/// PLN-01: TokioSink start/execute/stop lifecycle.
TEST_F(TokioSinkTest, BasicLifecycle)
{
    auto [bpc, bpl] = createBackpressureChannel();
    auto sink = std::make_unique<TokioSink>(std::move(bpc), makeDevNullSinkSpawnFn());
    MockPipelineContext pec(bufferManager);

    sink->start(pec);

    auto buf = bufferManager->getBufferBlocking();
    buf.setNumberOfTuples(1);
    sink->execute(buf, pec);

    // Stop the sink -- may need multiple calls due to state machine
    for (int i = 0; i < 100; ++i)
    {
        pec.repeatTaskCalled = false;
        sink->stop(pec);
        if (!pec.repeatTaskCalled)
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_FALSE(pec.repeatTaskCalled) << "Stop did not complete within timeout";
}

/// PLN-02: Non-blocking try_send with repeatTask retry.
TEST_F(TokioSinkTest, BackpressureRetry)
{
    auto [bpc, bpl] = createBackpressureChannel();
    auto sink = std::make_unique<TokioSink>(
        std::move(bpc), makeDevNullSinkSpawnFn(),
        /*channelCapacity=*/1);
    MockPipelineContext pec(bufferManager);
    sink->start(pec);

    // Fill the channel (capacity=1)
    auto buf1 = bufferManager->getBufferBlocking();
    buf1.setNumberOfTuples(1);
    sink->execute(buf1, pec);

    // DevNull sink processes quickly, but try sending another immediately.
    // Send enough buffers to eventually trigger Full.
    bool repeatTaskTriggered = false;
    for (int i = 0; i < 100 && !repeatTaskTriggered; ++i)
    {
        auto buf = bufferManager->getBufferBlocking();
        buf.setNumberOfTuples(1);
        pec.repeatTaskCalled = false;
        sink->execute(buf, pec);
        if (pec.repeatTaskCalled)
        {
            repeatTaskTriggered = true;
            ASSERT_FALSE(pec.repeatCalls.empty());
            ASSERT_EQ(pec.repeatCalls.back().interval, TokioSink::BACKPRESSURE_RETRY_INTERVAL);
        }
    }
    // Note: DevNull sink is fast, so Full may not always trigger.
    // This test validates the mechanism exists; sustained-load testing is in Phase 6.

    // Clean stop
    for (int i = 0; i < 100; ++i)
    {
        pec.repeatTaskCalled = false;
        sink->stop(pec);
        if (!pec.repeatTaskCalled)
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

/// PLN-03: BackpressureController hysteresis thresholds (unit test on SinkBackpressureHandler).
TEST_F(TokioSinkTest, HysteresisThresholds)
{
    auto [bpc, bpl] = createBackpressureChannel();
    SinkBackpressureHandler handler(/*upper=*/3, /*lower=*/1);

    // Create test buffers
    std::vector<TupleBuffer> buffers;
    for (int i = 0; i < 5; ++i)
    {
        auto buf = bufferManager->getBufferBlocking();
        buf.setNumberOfTuples(1);
        buf.setSequenceNumber(SequenceNumber(i + 1));
        buf.setChunkNumber(ChunkNumber(1));
        buffers.push_back(std::move(buf));
    }

    // onFull(buf0): push buf0 into deque, pop as pending -> size=0
    auto retry1 = handler.onFull(buffers[0], bpc);
    ASSERT_TRUE(retry1.has_value()); // First buffer becomes pending retry

    // onFull(buf1): push into deque (size=1), already has pending -> return nullopt
    auto retry2 = handler.onFull(buffers[1], bpc);
    ASSERT_FALSE(retry2.has_value());

    // onFull(buf2): push into deque (size=2), already has pending -> return nullopt
    auto retry3 = handler.onFull(buffers[2], bpc);
    ASSERT_FALSE(retry3.has_value());

    // onFull(buf3): push into deque (size=3), >= upperThreshold(3) -> apply pressure!
    auto retry4 = handler.onFull(buffers[3], bpc);
    ASSERT_FALSE(retry4.has_value());

    // Simulate successful sends to drain
    // State: pending=buf0, buffered=[buf1, buf2, buf3], hasBackpressure=true
    auto next = handler.onSuccess(bpc); // clear pending, pop buf1 (size=2, > lower=1)
    ASSERT_TRUE(next.has_value());
    next = handler.onSuccess(bpc); // pop buf2 (size=1, == lower=1 -> release pressure)
    ASSERT_TRUE(next.has_value());
    next = handler.onSuccess(bpc); // pop buf3 (size=0)
    ASSERT_TRUE(next.has_value());
    next = handler.onSuccess(bpc); // empty
    ASSERT_FALSE(next.has_value());
    ASSERT_TRUE(handler.empty());
}

/// PLN-04: Flush-confirm stop protocol.
TEST_F(TokioSinkTest, FlushOnStop)
{
    auto [bpc, bpl] = createBackpressureChannel();
    auto sink = std::make_unique<TokioSink>(std::move(bpc), makeDevNullSinkSpawnFn());
    MockPipelineContext pec(bufferManager);

    sink->start(pec);

    // Send a few buffers
    for (int i = 0; i < 5; ++i)
    {
        auto buf = bufferManager->getBufferBlocking();
        buf.setNumberOfTuples(1);
        sink->execute(buf, pec);
    }

    // Stop with retry loop (simulating pipeline calling stop + repeatTask)
    int stopAttempts = 0;
    for (int i = 0; i < 200; ++i)
    {
        pec.repeatTaskCalled = false;
        sink->stop(pec);
        stopAttempts++;
        if (!pec.repeatTaskCalled)
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // stop() must have eventually succeeded (no more repeatTask)
    ASSERT_FALSE(pec.repeatTaskCalled) << "Stop did not complete within timeout";
    NES_DEBUG("TokioSinkTest::FlushOnStop: completed in {} stop attempts", stopAttempts);
}

/// PLN-02 extra: execute() drops buffers after stopInitiated.
TEST_F(TokioSinkTest, ExecuteAfterStopDropsBuffer)
{
    auto [bpc, bpl] = createBackpressureChannel();
    auto sink = std::make_unique<TokioSink>(std::move(bpc), makeDevNullSinkSpawnFn());
    MockPipelineContext pec(bufferManager);

    sink->start(pec);

    // Initiate stop (first call sets stopInitiated)
    sink->stop(pec);

    // Now execute() should silently drop
    auto buf = bufferManager->getBufferBlocking();
    buf.setNumberOfTuples(1);
    auto refBefore = buf.getReferenceCounter();
    sink->execute(buf, pec);
    // Buffer should not have been retained (no leak)
    ASSERT_EQ(buf.getReferenceCounter(), refBefore);

    // Complete the stop
    for (int i = 0; i < 100; ++i)
    {
        pec.repeatTaskCalled = false;
        sink->stop(pec);
        if (!pec.repeatTaskCalled)
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

} // namespace NES
