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
#include <chrono>
#include <cstring>
#include <memory>
#include <stop_token>
#include <thread>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipeService.hpp>

namespace NES
{

class PipeServiceTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        schemaA = std::make_shared<const Schema>(Schema{}.addField("id", DataType::Type::UINT64).addField("value", DataType::Type::UINT64));
        schemaB = std::make_shared<const Schema>(Schema{}.addField("x", DataType::Type::FLOAT64));
        bufferManager = BufferManager::create(1024, 256);
    }

    void TearDown() override
    {
        PipeService::instance().unregisterSink("test_pipe");
        PipeService::instance().unregisterSink("pipe_a");
        PipeService::instance().unregisterSink("pipe_b");
    }

    std::shared_ptr<const Schema> schemaA;
    std::shared_ptr<const Schema> schemaB;
    std::shared_ptr<BufferManager> bufferManager;
};

TEST_F(PipeServiceTest, RegisterSinkThenSource)
{
    auto [bpController, bpListener] = createBackpressureChannel();
    auto sinkHandle = PipeService::instance().registerSink("test_pipe", schemaA, &bpController);
    ASSERT_NE(sinkHandle, nullptr);

    auto queue = PipeService::instance().registerSource("test_pipe", schemaA);
    ASSERT_NE(queue, nullptr);

    /// Source registered after sink goes to pending (awaiting sequence boundary activation)
    sinkHandle->queues.withRLock(
        [](const auto& queueState)
        {
            EXPECT_EQ(queueState.pending.size(), 1);
            EXPECT_EQ(queueState.active.size(), 0);
        });

    PipeService::instance().unregisterSource("test_pipe", queue);
}

TEST_F(PipeServiceTest, RegisterSourceWithoutSinkFails)
{
    EXPECT_THROW(PipeService::instance().registerSource("test_pipe", schemaA), Exception);
}

TEST_F(PipeServiceTest, RegisterSourceAfterSinkUnregisteredFails)
{
    auto [bpController, bpListener] = createBackpressureChannel();
    auto sinkHandle = PipeService::instance().registerSink("test_pipe", schemaA, &bpController);
    PipeService::instance().unregisterSink("test_pipe");
    EXPECT_THROW(PipeService::instance().registerSource("test_pipe", schemaA), Exception);
}

TEST_F(PipeServiceTest, SchemaMismatchOnSourceRegistration)
{
    auto [bpController, bpListener] = createBackpressureChannel();
    PipeService::instance().registerSink("test_pipe", schemaA, &bpController);
    EXPECT_THROW(PipeService::instance().registerSource("test_pipe", schemaB), Exception);
}

TEST_F(PipeServiceTest, DuplicateSinkRegistration)
{
    auto [bpController1, bpListener1] = createBackpressureChannel();
    auto [bpController2, bpListener2] = createBackpressureChannel();
    PipeService::instance().registerSink("test_pipe", schemaA, &bpController1);
    EXPECT_THROW(PipeService::instance().registerSink("test_pipe", schemaA, &bpController2), Exception);
}

TEST_F(PipeServiceTest, MultipleSourcesSamePipe)
{
    auto [bpController, bpListener] = createBackpressureChannel();
    auto sinkHandle = PipeService::instance().registerSink("test_pipe", schemaA, &bpController);

    auto queue1 = PipeService::instance().registerSource("test_pipe", schemaA);
    auto queue2 = PipeService::instance().registerSource("test_pipe", schemaA);
    auto queue3 = PipeService::instance().registerSource("test_pipe", schemaA);

    /// All three go to pending since they registered after the sink
    sinkHandle->queues.withRLock(
        [](const auto& queueState)
        {
            EXPECT_EQ(queueState.pending.size(), 3);
            EXPECT_EQ(queueState.active.size(), 0);
        });

    PipeService::instance().unregisterSource("test_pipe", queue1);
    PipeService::instance().unregisterSource("test_pipe", queue2);
    PipeService::instance().unregisterSource("test_pipe", queue3);
}

TEST_F(PipeServiceTest, UnregisterSourceRemovesFromHandle)
{
    auto [bpController, bpListener] = createBackpressureChannel();
    auto sinkHandle = PipeService::instance().registerSink("test_pipe", schemaA, &bpController);
    auto queue = PipeService::instance().registerSource("test_pipe", schemaA);

    sinkHandle->queues.withRLock([](const auto& queueState) { EXPECT_EQ(queueState.pending.size(), 1); });

    PipeService::instance().unregisterSource("test_pipe", queue);

    sinkHandle->queues.withRLock([](const auto& queueState) { EXPECT_EQ(queueState.pending.size(), 0); });
}

TEST_F(PipeServiceTest, UnregisterSinkCleansUp)
{
    auto [bpController1, bpListener1] = createBackpressureChannel();
    auto sinkHandle = PipeService::instance().registerSink("test_pipe", schemaA, &bpController1);
    auto queue = PipeService::instance().registerSource("test_pipe", schemaA);

    PipeService::instance().unregisterSink("test_pipe");

    /// After unregistering sink, we should be able to register a new one
    auto [bpController2, bpListener2] = createBackpressureChannel();
    auto newHandle = PipeService::instance().registerSink("test_pipe", schemaA, &bpController2);
    ASSERT_NE(newHandle, nullptr);

    PipeService::instance().unregisterSource("test_pipe", queue);
}

TEST_F(PipeServiceTest, MultiplePipesIndependent)
{
    auto [bpControllerA, bpListenerA] = createBackpressureChannel();
    auto [bpControllerB, bpListenerB] = createBackpressureChannel();
    auto handleA = PipeService::instance().registerSink("pipe_a", schemaA, &bpControllerA);
    auto handleB = PipeService::instance().registerSink("pipe_b", schemaB, &bpControllerB);

    auto queueA = PipeService::instance().registerSource("pipe_a", schemaA);
    auto queueB = PipeService::instance().registerSource("pipe_b", schemaB);

    handleA->queues.withRLock([](const auto& queueState) { EXPECT_EQ(queueState.pending.size(), 1); });
    handleB->queues.withRLock([](const auto& queueState) { EXPECT_EQ(queueState.pending.size(), 1); });

    /// Schema mismatch across pipes
    EXPECT_THROW(PipeService::instance().registerSource("pipe_a", schemaB), Exception);

    PipeService::instance().unregisterSource("pipe_a", queueA);
    PipeService::instance().unregisterSource("pipe_b", queueB);
}

TEST_F(PipeServiceTest, BackpressureReleasedOnFirstConsumer)
{
    auto [bpController, bpListener] = createBackpressureChannel();

    /// Start with backpressure applied (as PipeSink::start() would do)
    bpController.applyPressure();

    auto sinkHandle = PipeService::instance().registerSink("test_pipe", schemaA, &bpController);

    /// Producer thread: waits on backpressure, then sets flag.
    /// Uses std::thread (not jthread) to avoid the race where jthread::join()
    /// calls request_stop() before the thread checks the stop token.
    std::atomic<bool> produced{false};
    std::stop_source stopSource;
    auto producer = std::thread(
        [&]
        {
            bpListener.wait(stopSource.get_token());
            produced = true;
        });

    /// Give the producer time to block on backpressure
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(produced.load()) << "Producer should be blocked by backpressure";

    /// Register first consumer → should release backpressure
    auto queue = PipeService::instance().registerSource("test_pipe", schemaA);

    /// Wait for producer to unblock
    producer.join();
    EXPECT_TRUE(produced.load()) << "Producer should have been unblocked after consumer registered";

    PipeService::instance().unregisterSource("test_pipe", queue);
}

TEST_F(PipeServiceTest, BackpressureReappliedOnLastConsumerRemoval)
{
    auto [bpController, bpListener] = createBackpressureChannel();
    bpController.applyPressure();
    auto sinkHandle = PipeService::instance().registerSink("test_pipe", schemaA, &bpController);

    auto queue1 = PipeService::instance().registerSource("test_pipe", schemaA);
    auto queue2 = PipeService::instance().registerSource("test_pipe", schemaA);

    /// Two consumers → backpressure released
    /// Remove one → still have one → backpressure stays released
    PipeService::instance().unregisterSource("test_pipe", queue1);

    std::atomic<bool> stillOpen{false};
    auto checker = std::jthread(
        [&](std::stop_token stopToken)
        {
            bpListener.wait(stopToken);
            if (!stopToken.stop_requested())
            {
                stillOpen = true;
            }
        });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    checker.request_stop();
    checker.join();
    EXPECT_TRUE(stillOpen.load()) << "Backpressure should still be released with one consumer remaining";

    /// Remove last consumer → backpressure should be re-applied
    PipeService::instance().unregisterSource("test_pipe", queue2);

    std::atomic<bool> blocked{true};
    auto checker2 = std::jthread(
        [&](std::stop_token stopToken)
        {
            bpListener.wait(stopToken);
            if (!stopToken.stop_requested())
            {
                blocked = false;
            }
        });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_TRUE(blocked.load()) << "Backpressure should be applied after last consumer removed";
    checker2.request_stop();
    checker2.join();
}

}
