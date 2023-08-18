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
#include <BaseUnitTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/NonBlockingMonotonicSeqQueue.hpp>
#include <algorithm>
#include <atomic>
#include <gtest/gtest.h>
#include <iostream>
#include <random>
#include <thread>

using namespace std;
namespace NES {

class NonBlockingMonotonicSeqQueueTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NonBlockingMonotonicSeqQueueTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup NonBlockingMonotonicSeqQueueTest test class.");
    }
};

/**
 * @brief A single thread test for the lock free watermark processor.
 * We create a sequential list of 10k updates, monotonically increasing from 1 to 10k and push them to the watermark processor.
 * Assumption:
 * As we insert all updates in a sequential fashion we assume that the getCurrentWatermark is equal to the latest processed update.
 */
TEST_F(NonBlockingMonotonicSeqQueueTest, singleThreadSequentialUpdaterTest) {
    auto updates = 10000;
    auto watermarkProcessor = Util::NonBlockingMonotonicSeqQueue<uint64_t>();
    // preallocate watermarks for each transaction
    std::vector<std::tuple<uint64_t, uint64_t>> watermarkBarriers;
    for (int i = 1; i <= updates; i++) {
        watermarkBarriers.emplace_back(/*ts*/ i,
                                       /*sequence number*/ i);
    }
    for (auto i = 0; i < updates; i++) {
        auto currentWatermarkBarrier = watermarkBarriers[i];
        auto oldWatermark = watermarkProcessor.getCurrentValue();
        ASSERT_LT(oldWatermark, std::get<0>(currentWatermarkBarrier));
        watermarkProcessor.emplace(std::get<0>(currentWatermarkBarrier), std::get<1>(currentWatermarkBarrier));
        ASSERT_EQ(watermarkProcessor.getCurrentValue(), std::get<0>(currentWatermarkBarrier));
    }
    ASSERT_EQ(watermarkProcessor.getCurrentValue(), std::get<0>(watermarkBarriers.back()));
}

/**
 * @brief A single thread test for the lock free watermark processor.
 * We create a reverse sequential list of 10k updates, monotonically decreasing from 10k to 1 and push them to the watermark processor.
 * Assumption:
 * As we insert all updates in a sequential fashion we assume that the getCurrentWatermark is equal to the latest processed update.
 */
TEST_F(NonBlockingMonotonicSeqQueueTest, singleThreadReversSequentialUpdaterTest) {
    auto updates = 10000;
    auto watermarkProcessor = Util::NonBlockingMonotonicSeqQueue<uint64_t>();
    // preallocate watermarks for each transaction
    std::vector<std::tuple<uint64_t, uint64_t>> watermarkBarriers;
    for (int i = 1; i <= updates; i++) {
        watermarkBarriers.emplace_back(/*ts*/ i,
                                       /*sequence number*/ i);
    }
    // reverse updates
    std::reverse(watermarkBarriers.begin(), watermarkBarriers.end());

    for (auto i = 0; i < updates - 1; i++) {
        auto currentWatermarkBarrier = watermarkBarriers[i];
        auto oldWatermark = watermarkProcessor.getCurrentValue();
        ASSERT_LT(oldWatermark, std::get<0>(currentWatermarkBarrier));
        watermarkProcessor.emplace(std::get<0>(currentWatermarkBarrier), std::get<1>(currentWatermarkBarrier));
        ASSERT_EQ(watermarkProcessor.getCurrentValue(), 0);
    }
    // add the last remaining watermark, as a result we now apply all remaining watermarks.
    watermarkProcessor.emplace(std::get<0>(watermarkBarriers.back()), std::get<1>(watermarkBarriers.back()));
    ASSERT_EQ(watermarkProcessor.getCurrentValue(), std::get<0>(watermarkBarriers.front()));
}

/**
 * @brief A single thread test for the lock free watermark processor.
 * We create a reverse sequential list of 10k updates, monotonically decreasing from 10k to 1 and push them to the watermark processor.
 * Assumption:
 * As we insert all updates in a sequential fashion we assume that the getCurrentWatermark is equal to the latest processed update.
 */
TEST_F(NonBlockingMonotonicSeqQueueTest, singleThreadRandomeUpdaterTest) {
    auto updates = 100;
    auto watermarkProcessor = Util::NonBlockingMonotonicSeqQueue<uint64_t>();
    // preallocate watermarks for each transaction
    std::vector<std::tuple<uint64_t, uint64_t>> watermarkBarriers;
    for (int i = 1; i <= updates; i++) {
        watermarkBarriers.emplace_back(/*ts*/ i,
                                       /*sequence number*/ i);
    }
    std::mt19937 randomGenerator(42);
    auto value = randomGenerator();
    std::shuffle(watermarkBarriers.begin(), watermarkBarriers.end(), randomGenerator);

    std::vector<std::tuple<uint64_t, uint64_t>> observedUpdates;

    for (auto i = 0; i < updates; i++) {
        auto currentWatermarkBarrier = watermarkBarriers[i];
        auto oldWatermark = watermarkProcessor.getCurrentValue();
        ASSERT_LT(oldWatermark, std::get<0>(currentWatermarkBarrier));
        watermarkProcessor.emplace(std::get<0>(currentWatermarkBarrier), std::get<1>(currentWatermarkBarrier));
        auto currentValue = watermarkProcessor.getCurrentValue();
        observedUpdates.push_back(std::make_tuple(std::get<0>(currentWatermarkBarrier), currentValue));
    }
    // add the last remaining watermark, as a result we now apply all remaining watermarks.
    ASSERT_EQ(watermarkProcessor.getCurrentValue(), updates);
}

TEST_F(NonBlockingMonotonicSeqQueueTest, concurrentLockFreeWatermarkUpdaterTest) {
    const auto updates = 100000;
    const auto threadsCount = 10;
    auto watermarkProcessor = Util::NonBlockingMonotonicSeqQueue<uint64_t, 10000>();

    // preallocate watermarks for each transaction
    std::vector<std::tuple<uint64_t, uint64_t>> watermarkBarriers;
    for (int i = 1; i <= updates * threadsCount; i++) {
        watermarkBarriers.emplace_back(/*ts*/ i,
                                       /*sequence number*/ i);
    }
    std::atomic<uint64_t> globalUpdateCounter = 0;
    std::vector<std::thread> threads;
    threads.reserve(threadsCount);
    for (int threadId = 0; threadId < threadsCount; threadId++) {
        threads.emplace_back(thread([&watermarkProcessor, &watermarkBarriers, &globalUpdateCounter]() {
            // each thread processes a particular update
            for (auto i = 0; i < updates; i++) {
                auto currentWatermark = watermarkBarriers[globalUpdateCounter++];
                auto oldWatermark = watermarkProcessor.getCurrentValue();
                // check if the watermark manager does not return a watermark higher than the current one
                ASSERT_LT(oldWatermark, std::get<0>(currentWatermark));
                watermarkProcessor.emplace(std::get<0>(currentWatermark), std::get<1>(currentWatermark));
                // check that the watermark manager returns a watermark that is <= to the max watermark
                auto globalCurrentWatermark = watermarkProcessor.getCurrentValue();
                auto maxCurrentWatermark = watermarkBarriers[globalUpdateCounter - 1];
                ASSERT_LE(globalCurrentWatermark, std::get<0>(maxCurrentWatermark));
            }
        }));
    }

    for (auto& thread : threads) {
        thread.join();
    }
    ASSERT_EQ(watermarkProcessor.getCurrentValue(), std::get<0>(watermarkBarriers.back()));
}

TEST_F(NonBlockingMonotonicSeqQueueTest, concurrentUpdatesWithLostUpdateThreadTest) {
    const auto updates = 10000;
    const auto lostUpdate = 666;
    const auto threadsCount = 10;
    auto watermarkProcessor = Util::NonBlockingMonotonicSeqQueue<uint64_t, 1000>();

    // preallocate watermarks for each transaction
    std::vector<std::tuple<uint64_t, uint64_t>> watermarkBarriers;
    for (int i = 1; i <= updates * threadsCount; i++) {
        watermarkBarriers.emplace_back(/*ts*/ i,
                                       /*sequence number*/ i);
    }
    std::atomic<uint64_t> globalUpdateCounter = 0;
    std::vector<std::thread> threads;
    threads.reserve(threadsCount);
    for (int threadId = 0; threadId < threadsCount; threadId++) {
        threads.emplace_back(thread([&watermarkProcessor, &watermarkBarriers, &globalUpdateCounter]() {
            // each thread processes a particular update
            for (auto i = 0; i < updates; i++) {
                auto nextUpdate = globalUpdateCounter++;
                if (nextUpdate == lostUpdate) {
                    continue;
                }
                auto currentWatermark = watermarkBarriers[nextUpdate];
                auto oldWatermark = watermarkProcessor.getCurrentValue();
                // check if the watermark manager does not return a watermark higher than the current one
                ASSERT_LT(oldWatermark, std::get<0>(watermarkBarriers[lostUpdate]));
                watermarkProcessor.emplace(std::get<0>(currentWatermark), std::get<1>(currentWatermark));
                // check that the watermark manager returns a watermark that is <= to the max watermark
                auto globalCurrentWatermark = watermarkProcessor.getCurrentValue();
                auto maxCurrentWatermark = watermarkBarriers[globalUpdateCounter - 1];
                ASSERT_LE(globalCurrentWatermark, std::get<0>(watermarkBarriers[lostUpdate]));
            }
        }));
    }

    for (auto& thread : threads) {
        thread.join();
    }
    auto currentValue = watermarkProcessor.getCurrentValue();
    ASSERT_EQ(currentValue, std::get<0>(watermarkBarriers[lostUpdate - 1]));
    watermarkProcessor.emplace(std::get<0>(watermarkBarriers[lostUpdate]), std::get<1>(watermarkBarriers[lostUpdate]));

    ASSERT_EQ(watermarkProcessor.getCurrentValue(), std::get<0>(watermarkBarriers.back()));
}

}// namespace NES