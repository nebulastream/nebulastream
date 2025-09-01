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
    std::barrier syncBarrier{2};
    std::atomic backpressureListenerCounter{0};

    auto [backpressureController, backpressureListener] = createBackpressureChannel();

    /// Start backpressureListener without applying pressure
    std::jthread backpressureListenerThread(
        [&](const std::stop_token& stopToken)
        {
            syncBarrier.arrive_and_wait();
            while (!stopToken.stop_requested())
            {
                backpressureListener.wait(stopToken);
                backpressureListenerCounter.fetch_add(1, std::memory_order::relaxed);
            }
        });

    syncBarrier.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    backpressureListenerThread = {};

    /// This is a guess, however 100 sounds very doable on any real hardware
    EXPECT_GT(backpressureListenerCounter.load(), 100);
}

/// Test that backpressureListener is blocked when pressure is applied
TEST_F(BackpressureChannelTest, BackpressureListenerProceedsWithPressure)
{
    std::barrier syncBarrier{2};
    std::atomic backpressureListenerCounter{0};

    auto [backpressureController, backpressureListener] = createBackpressureChannel();

    /// Start backpressureListener without applying pressure
    std::jthread backpressureListenerThread(
        [&](const std::stop_token& stopToken)
        {
            syncBarrier.arrive_and_wait();
            while (!stopToken.stop_requested())
            {
                backpressureListener.wait(stopToken);
                backpressureListenerCounter.fetch_add(1, std::memory_order::relaxed);
            }
        });

    syncBarrier.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    /// This is a guess, however 100 sounds very doable on any real hardware
    EXPECT_GT(backpressureListenerCounter.load(), 100);
    EXPECT_TRUE(backpressureController.applyPressure());

    /// Expect that the backpressureListener does not increase any further after pressure has been applied
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const auto current = backpressureListenerCounter.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(current, backpressureListenerCounter.load());
}

/// Test that backpressureListener waits when pressure is applied
TEST_F(BackpressureChannelTest, IngestionWaitsWhenPressureApplied)
{
    constexpr size_t numberOfSources = 5;
    /// Read on main thread only happens after the backpressureListenerThreads have been stopped.
    std::vector<std::chrono::milliseconds> durations(numberOfSources);

    std::barrier syncBeforeWait{numberOfSources + 1};
    std::barrier syncAfterWait{numberOfSources + 1};

    auto [backpressureController, backpressureListener] = createBackpressureChannel();

    /// Apply pressure before starting backpressureListener
    backpressureController.applyPressure();

    std::vector<std::jthread> backpressureListenerThreads;
    backpressureListenerThreads.reserve(numberOfSources);
    for (size_t i = 0; i < numberOfSources; ++i)
    {
        /// Start a thread that will try to ingest
        backpressureListenerThreads.emplace_back(
            [&, i](const std::stop_token& stopToken)
            {
                syncBeforeWait.arrive_and_wait();
                auto start = std::chrono::steady_clock::now();

                /// This should block until pressure is released
                backpressureListener.wait(stopToken);

                auto end = std::chrono::steady_clock::now();

                syncAfterWait.arrive_and_wait();

                /// Report time spend waiting
                durations[i] = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            });
    }
    syncBeforeWait.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    /// Release pressure
    EXPECT_TRUE(backpressureController.releasePressure());
    syncAfterWait.arrive_and_wait();

    /// Stop thread to ensure that there is no data race on duration
    backpressureListenerThreads.clear();

    /// Expect time to have passed while waiting for backpressure release. This is a guess, we cannot predict an actual duration
    for (auto duration : durations)
    {
        EXPECT_GT(duration, std::chrono::milliseconds(50));
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

                    while (!stopToken.stop_requested())
                    {
                        channels[channelId].second.wait(stopToken);
                        successfulWaits.fetch_add(1);
                    }
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

                while (!stopToken.stop_requested())
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
                }
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

    std::atomic ingestionStarted{false};
    std::atomic ingestionStopped{false};

    /// Start ingestion thread
    std::jthread ingestionThread(
        [&](const std::stop_token& stopToken)
        {
            ingestionStarted = true;
            ingestion.wait(stopToken);
            ingestionStopped = true;
        });

    /// Wait for ingestion to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(ingestionStarted);
    EXPECT_FALSE(ingestionStopped);

    /// Stop thread, should trigger stop token
    ingestionThread = {};
    EXPECT_TRUE(ingestionStopped);
    EXPECT_TRUE(backpressureController.releasePressure());
}

}
