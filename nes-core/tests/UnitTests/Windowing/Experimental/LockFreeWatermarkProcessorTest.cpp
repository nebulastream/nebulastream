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
#include <Util/Logger/Logger.hpp>
#include <Windowing/Experimental/LockFreeMultiOriginWatermarkProcessor.hpp>
#include <Windowing/Experimental/LockFreeWatermarkProcessor.hpp>
#include <Windowing/Watermark/MultiOriginWatermarkProcessor.hpp>
#include <algorithm>
#include <atomic>
#include <gtest/gtest.h>
#include <iostream>
#include <random>
#include <thread>

using namespace std;
namespace NES {

class LockFreeWatermarkProcessorTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup LockFreeWatermarkProcessorTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Logger::setupLogging("LockFreeWatermarkProcessorTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup LockFreeWatermarkProcessorTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down LockFreeWatermarkProcessorTest test case." << std::endl; }
};

/**
 * @brief A single thread test for the lock free watermark processor.
 * We create a sequential list of 10k updates, monotonically increasing from 1 to 10k and push them to the watermark processor.
 * Assumption:
 * As we insert all updates in a sequential fashion we assume that the getCurrentWatermark is equal to the latest processed update.
 */
TEST_F(LockFreeWatermarkProcessorTest, singleThreadSequentialUpdaterTest) {
    auto updates = 10000;
    auto watermarkProcessor = std::make_shared<Experimental::LockFreeWatermarkProcessor<>>();
    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceNumber>> watermarkBarriers;
    for (int i = 1; i <= updates; i++) {
        watermarkBarriers.emplace_back(/*ts*/ i,
                                       /*sequence number*/ i);
    }
    for (auto i = 0; i < updates; i++) {
        auto currentWatermarkBarrier = watermarkBarriers[i];
        auto oldWatermark = watermarkProcessor->getCurrentWatermark();
        ASSERT_LT(oldWatermark, std::get<0>(currentWatermarkBarrier));
        watermarkProcessor->updateWatermark(std::get<0>(currentWatermarkBarrier), std::get<1>(currentWatermarkBarrier));
        ASSERT_EQ(watermarkProcessor->getCurrentWatermark(), std::get<0>(currentWatermarkBarrier));
    }
    ASSERT_EQ(watermarkProcessor->getCurrentWatermark(), std::get<0>(watermarkBarriers.back()));
}

/**
 * @brief A single thread test for the lock free watermark processor.
 * We create a randomly ordered list of 10k updates and push them to the watermark processor.
 * Assumption:
 * As we insert all updates in a random fashion we can assume that that getCurrentWatermark is smaller or equal to the
 * latest processed update.
 */
TEST_F(LockFreeWatermarkProcessorTest, singleThreadRandomWatermarkUpdaterTest) {
    auto updates = 1000;
    auto watermarkProcessor = std::make_shared<Experimental::LockFreeWatermarkProcessor<>>();
    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceNumber>> watermarkBarriers;
    for (int i = 1; i <= updates; i++) {
        watermarkBarriers.emplace_back(/*ts*/ i,
                                       /*sequence number*/ i);
    }

    // shuffle barriers to mimic out of order processing
    std::mt19937 randomGenerator(42);
    std::shuffle(watermarkBarriers.begin(), watermarkBarriers.end(), randomGenerator);
    uint64_t maxWatermark = 0;
    for (auto i = 0; i < updates; i++) {
        auto currentWatermarkBarrier = watermarkBarriers[i];
        auto oldWatermark = watermarkProcessor->getCurrentWatermark();
        maxWatermark = std::max(maxWatermark, std::get<0>(currentWatermarkBarrier));
        ASSERT_LE(oldWatermark, maxWatermark);
        watermarkProcessor->updateWatermark(std::get<0>(currentWatermarkBarrier), std::get<1>(currentWatermarkBarrier));
        ASSERT_LE(watermarkProcessor->getCurrentWatermark(), maxWatermark);
    }
    ASSERT_EQ(watermarkProcessor->getCurrentWatermark(), 999);
}

TEST_F(LockFreeWatermarkProcessorTest, concurrentLockFreeWatermarkUpdaterTest) {
    const auto updates = 100000;
    const auto threadsCount = 10;
    auto watermarkManager = std::make_shared<Experimental::LockFreeWatermarkProcessor<>>();

    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceNumber>> watermarkBarriers;
    for (int i = 1; i <= updates * threadsCount; i++) {
        watermarkBarriers.emplace_back(/*ts*/ i,
                                       /*sequence number*/ i);
    }
    std::atomic<uint64_t> globalUpdateCounter = 0;
    std::vector<std::thread> threads;
    threads.reserve(threadsCount);
    for (int threadId = 0; threadId < threadsCount; threadId++) {
        threads.emplace_back(thread([&watermarkManager, &watermarkBarriers, &globalUpdateCounter]() {
            // each thread processes a particular update
            for (auto i = 0; i < updates; i++) {
                auto currentWatermark = watermarkBarriers[globalUpdateCounter++];
                auto oldWatermark = watermarkManager->getCurrentWatermark();
                // check if the watermark manager does not return a watermark higher than the current one
                ASSERT_LT(oldWatermark, std::get<0>(currentWatermark));
                watermarkManager->updateWatermark(std::get<0>(currentWatermark), std::get<1>(currentWatermark));
                // check that the watermark manager returns a watermark that is <= to the max watermark
                auto globalCurrentWatermark = watermarkManager->getCurrentWatermark();
                auto maxCurrentWatermark = watermarkBarriers[globalUpdateCounter - 1];
                ASSERT_LE(globalCurrentWatermark, std::get<0>(maxCurrentWatermark));
            }
        }));
    }

    for (auto& thread : threads) {
        thread.join();
    }
    ASSERT_EQ(watermarkManager->getCurrentWatermark(), std::get<0>(watermarkBarriers.back()));
}

}// namespace NES