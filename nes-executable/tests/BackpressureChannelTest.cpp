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

#include <BackpressureChannel.hpp>

#include <atomic>
#include <barrier>
#include <chrono>
#include <future>
#include <latch>
#include <memory>
#include <random>
#include <stop_token>
#include <thread>
#include <utility>
#include <vector>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{

class BackpressureChannelTest : public ::testing::Test
{
protected:
    void SetUp() override { Logger::setupLogging("BackpressureChannelTest.log", NES::LogLevel::LOG_DEBUG); }
};

/// Test basic construction and destruction of Backpressure Controller and BackpressureListener
TEST_F(BackpressureChannelTest, BasicConstruction)
{
    /// Test that we can create a backpressure channel
    auto [backpressureController, backpressureListener] = createBackpressureChannel();

    /// Test that the objects are functional by using their public methods
    /// Initially, the channel should be open (no backpressure)
    EXPECT_TRUE(backpressureController.applyPressure()); /// Should return true (was open)
    EXPECT_TRUE(backpressureController.releasePressure()); /// Should return true (was closed)
}

/// Test basic functionality with 1 Backpressure Controller and 1 backpressureListener
TEST_F(BackpressureChannelTest, BasicFunctionality)
{
    auto [backpressureController, backpressureListener] = createBackpressureChannel();

    /// Initially, the channel should be open (no backpressure)
    /// We can't directly test the internal state, but we can test the behavior

    /// Apply pressure - should return true (was open)
    EXPECT_TRUE(backpressureController.applyPressure());

    /// Apply pressure again - should return false (was already closed)
    EXPECT_FALSE(backpressureController.applyPressure());

    /// Release pressure - should return true (was closed)
    EXPECT_TRUE(backpressureController.releasePressure());

    /// Release pressure again - should return false (was already open)
    EXPECT_FALSE(backpressureController.releasePressure());
}

/// Test that backpressureListener proceeds immediately when no pressure is applied
TEST_F(BackpressureChannelTest, BackpressureListenerProceedsWhenNoPressure)
{
    auto [backpressureController, backpressureListener] = createBackpressureChannel();

    /// Join without requesting stop so every wait must complete with the channel open.
    std::jthread backpressureListenerThread(
        [&](const std::stop_token& stopToken)
        {
            for (size_t i = 0; i < 100; ++i)
            {
                backpressureListener.wait(stopToken);
            }
        });
    backpressureListenerThread.join();
}

/// Test that backpressureListener is blocked when pressure is applied
TEST_F(BackpressureChannelTest, BackpressureListenerProceedsWithPressure)
{
    std::barrier syncBarrier{2};
    std::latch readyToWait{1};
    std::promise<void> completed;
    auto completion = completed.get_future();

    auto [backpressureController, backpressureListener] = createBackpressureChannel();

    std::jthread backpressureListenerThread(
        [&](const std::stop_token& stopToken)
        {
            for (size_t i = 0; i < 100; ++i)
            {
                backpressureListener.wait(stopToken);
            }
            syncBarrier.arrive_and_wait();
            syncBarrier.arrive_and_wait();

            readyToWait.count_down();
            backpressureListener.wait(stopToken);
            completed.set_value();
        });

    /// Wait for open-channel ingestion to finish before applying pressure.
    syncBarrier.arrive_and_wait();
    EXPECT_TRUE(backpressureController.applyPressure());
    syncBarrier.arrive_and_wait();
    readyToWait.wait();

    /// Observe that ingestion does not complete while pressure is applied.
    EXPECT_EQ(completion.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    EXPECT_TRUE(backpressureController.releasePressure());
    backpressureListenerThread.join();
    completion.get();
}

/// Test that backpressureListener waits when pressure is applied
TEST_F(BackpressureChannelTest, IngestionWaitsWhenPressureApplied)
{
    constexpr size_t numberOfSources = 5;
    std::latch readyToWait{numberOfSources};
    std::vector<std::promise<void>> completed(numberOfSources);
    std::vector<std::future<void>> completions;
    for (auto& promise : completed)
    {
        completions.emplace_back(promise.get_future());
    }

    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    EXPECT_TRUE(backpressureController.applyPressure());

    std::vector<std::jthread> backpressureListenerThreads;
    backpressureListenerThreads.reserve(numberOfSources);
    for (size_t i = 0; i < numberOfSources; ++i)
    {
        backpressureListenerThreads.emplace_back(
            [&, i](const std::stop_token& stopToken)
            {
                readyToWait.count_down();
                backpressureListener.wait(stopToken);
                completed[i].set_value();
            });
    }
    readyToWait.wait();
    for (auto& completion : completions)
    {
        EXPECT_EQ(completion.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    }

    EXPECT_TRUE(backpressureController.releasePressure());
    /// Join explicitly: requesting stop would also unblock ingestion and mask a release failure.
    for (auto& thread : backpressureListenerThreads)
    {
        thread.join();
    }
    for (auto& completion : completions)
    {
        completion.get();
    }
}

/// Stress test with multiple Backpressure Controllers and backpressureListeners in a multithreaded environment
TEST_F(BackpressureChannelTest, MultithreadedStressTest)
{
    constexpr int numChannels = 10;
    constexpr int numIngestionsPerChannel = 5;
    constexpr int testDurationMs = 1000;

    std::vector<std::pair<BackpressureController, BackpressureListener>> channels;
    channels.reserve(numChannels);

    /// Create multiple backpressure channels
    for (int i = 0; i < numChannels; ++i)
    {
        channels.emplace_back(createBackpressureChannel());
    }

    std::atomic totalOperations{0};
    std::atomic successfulWaits{0};

    /// Barrier to synchronize all threads
    std::barrier syncBarrier{1 + (numChannels * numIngestionsPerChannel) + numChannels};

    /// Start ingestion threads
    std::vector<std::jthread> ingestionThreads;
    for (int channelId = 0; channelId < numChannels; ++channelId)
    {
        for (int ingestionId = 0; ingestionId < numIngestionsPerChannel; ++ingestionId)
        {
            ingestionThreads.emplace_back(
                [&, channelId](const std::stop_token& stopToken)
                {
                    syncBarrier.arrive_and_wait();

                    /// Ensure activity even if this thread is first scheduled after the test duration.
                    do
                    {
                        channels[channelId].second.wait(stopToken);
                        successfulWaits.fetch_add(1);
                    } while (!stopToken.stop_requested());
                });
        }
    }

    /// Start Backpressure Controller operation threads
    std::vector<std::jthread> backpressureControllerThreads;
    backpressureControllerThreads.reserve(numChannels);
    for (int channelId = 0; channelId < numChannels; ++channelId)
    {
        backpressureControllerThreads.emplace_back(
            [&, channelId](const std::stop_token& stopToken)
            {
                syncBarrier.arrive_and_wait();

                std::mt19937 rng(channelId);
                std::uniform_int_distribution<> dist(0, 1);

                /// Perform at least one operation before honoring a stop request.
                do
                {
                    if (dist(rng) == 0)
                    {
                        channels[channelId].first.applyPressure();
                    }
                    else
                    {
                        channels[channelId].first.releasePressure();
                    }

                    totalOperations.fetch_add(1);
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                } while (!stopToken.stop_requested());
            });
    }


    /// Run the test for the specified duration
    syncBarrier.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(testDurationMs));
    ingestionThreads.clear();
    backpressureControllerThreads.clear();

    /// Verify we had some activity
    EXPECT_GT(totalOperations.load(), 0);
    EXPECT_GT(successfulWaits.load(), 0);

    /// All channels should still be functional
    for (int i = 0; i < numChannels; ++i)
    {
        channels[i].first.applyPressure();
        EXPECT_TRUE(channels[i].first.releasePressure());
    }
}

/// Test Backpressure Controller destruction behavior
TEST_F(BackpressureChannelTest, BackpressureControllerDestruction)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");

    auto [backpressureController, ingestion] = createBackpressureChannel();

    /// Apply pressure
    EXPECT_TRUE(backpressureController.applyPressure());

    std::barrier syncBarrier{2};

    /// Backpressure Controller Thread keeps Backpressure Controller alive until barrier is reached
    const std::jthread ingestionThread(
        [&, backpressureController = std::move(backpressureController)]
        {
            syncBarrier.arrive_and_wait();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        });

    syncBarrier.arrive_and_wait();
    EXPECT_DEATH_DEBUG(ingestion.wait({}), "");
}

/// Test stop token functionality
TEST_F(BackpressureChannelTest, StopTokenFunctionality)
{
    auto [backpressureController, ingestion] = createBackpressureChannel();

    /// Apply pressure
    EXPECT_TRUE(backpressureController.applyPressure());

    std::latch ingestionStarted{1};
    std::atomic ingestionStopped{false};

    /// Start ingestion thread
    std::jthread ingestionThread(
        [&](const std::stop_token& stopToken)
        {
            ingestionStarted.count_down();
            ingestion.wait(stopToken);
            ingestionStopped = true;
        });

    /// Wait for ingestion to start
    ingestionStarted.wait();
    EXPECT_FALSE(ingestionStopped);

    /// Stop thread, should trigger stop token
    ingestionThread = {};
    EXPECT_TRUE(ingestionStopped);
    EXPECT_TRUE(backpressureController.releasePressure());
}

}
