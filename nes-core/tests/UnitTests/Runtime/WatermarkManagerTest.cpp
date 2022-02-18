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
#include <Util/Logger.hpp>
#include <Windowing/Watermark/MultiOriginWatermarkProcessor.hpp>
#include <algorithm>
#include <atomic>
#include <gtest/gtest.h>
#include <NesBaseTest.hpp>
#include <iostream>
#include <random>
#include <thread>

using namespace std;
namespace NES {

class WatermarkManagerTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup WatermarkManagerTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::setupLogging("WatermarkManagerTest.log", NES::LOG_DEBUG);
        std::cout << "Setup WatermarkManagerTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down WatermarkManagerTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down WatermarkManagerTest test class." << std::endl; }
};

TEST_F(WatermarkManagerTest, singleThreadWatermarkUpdaterTest) {
    auto updates = 10000;
    auto watermarkManager = Windowing::MultiOriginWatermarkProcessor::create(/*origins*/ 1);
    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceNumber, OriginId>> watermarkBarriers;
    for (int i = 1; i <= updates; i++) {
        watermarkBarriers.emplace_back(/*ts*/ i,
                                       /*sequence number*/ i,
                                       /*origin*/ 0);
    }
    for (auto i = 0; i < updates; i++) {
        auto currentWatermarkBarrier = watermarkBarriers[i];
        auto oldWatermark = watermarkManager->getCurrentWatermark();
        ASSERT_LT(oldWatermark, std::get<0>(currentWatermarkBarrier));
        watermarkManager->updateWatermark(std::get<0>(currentWatermarkBarrier),
                                          std::get<1>(currentWatermarkBarrier),
                                          std::get<2>(currentWatermarkBarrier));
        ASSERT_LE(watermarkManager->getCurrentWatermark(), std::get<0>(currentWatermarkBarrier));
    }
    ASSERT_EQ(watermarkManager->getCurrentWatermark(), std::get<0>(watermarkBarriers.back()));
}

TEST_F(WatermarkManagerTest, concurrentWatermarkUpdaterTest) {
    const auto updates = 100000;
    const auto threadsCount = 10;
    auto watermarkManager = Windowing::MultiOriginWatermarkProcessor::create(/*origins*/ 1);

    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceNumber, OriginId>> watermarkBarriers;
    for (int i = 1; i <= updates * threadsCount; i++) {
        watermarkBarriers.emplace_back(/*ts*/ i,
                                       /*sequence number*/ i,
                                       /*origin*/ 0);
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
                // check if the watermark manager dose not return a watermark higher then the current one
                ASSERT_LT(oldWatermark, std::get<0>(currentWatermark));
                watermarkManager->updateWatermark(std::get<0>(currentWatermark),
                                                  std::get<1>(currentWatermark),
                                                  std::get<2>(currentWatermark));
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

TEST_F(WatermarkManagerTest, singleThreadWatermarkUpdaterMultipleOriginsTest) {
    auto updates = 10000;
    auto origins = 10;
    auto watermarkManager = Windowing::MultiOriginWatermarkProcessor::create(/*origins*/ origins);
    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceNumber, OriginId>> watermarkBarriers;
    for (int i = 1; i <= updates; i++) {
        for (int o = 0; o < origins; o++) {
            watermarkBarriers.emplace_back(/*ts*/ i,
                                           /*sequence number*/ i,
                                           /*origin*/ o);
        }
    }

    for (auto currentWatermarkBarrier : watermarkBarriers) {
        auto oldWatermark = watermarkManager->getCurrentWatermark();
        ASSERT_LT(oldWatermark, std::get<0>(currentWatermarkBarrier));
        watermarkManager->updateWatermark(std::get<0>(currentWatermarkBarrier),
                                          std::get<1>(currentWatermarkBarrier),
                                          std::get<2>(currentWatermarkBarrier));
        ASSERT_LE(watermarkManager->getCurrentWatermark(), std::get<0>(currentWatermarkBarrier));
    }
    ASSERT_EQ(watermarkManager->getCurrentWatermark(), std::get<0>(watermarkBarriers.back()));
}

TEST_F(WatermarkManagerTest, singleThreadWatermarkUpdaterMultipleOriginsOutofOrderTest) {
    auto updates = 10000u;
    auto origins = 10;
    auto watermarkManager = Windowing::MultiOriginWatermarkProcessor::create(/*origins*/ origins);

    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceNumber, OriginId>> watermarkBarriers;
    for (auto i{1u}; i <= updates; ++i) {
        for (int o = 0; o < origins; o++) {
            watermarkBarriers.emplace_back(/*ts*/ i,
                                           /*sequence number*/ i,
                                           /*origin*/ o);
        }
    }

    // shuffle barriers to mimic out of order processing
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(watermarkBarriers.begin(), watermarkBarriers.end(), g);

    for (auto currentWatermarkBarrier : watermarkBarriers) {
        auto oldWatermark = watermarkManager->getCurrentWatermark();
        ASSERT_LT(oldWatermark, std::get<0>(currentWatermarkBarrier));
        watermarkManager->updateWatermark(std::get<0>(currentWatermarkBarrier),
                                          std::get<1>(currentWatermarkBarrier),
                                          std::get<2>(currentWatermarkBarrier));
    }
    ASSERT_EQ(watermarkManager->getCurrentWatermark(), updates);
}

TEST_F(WatermarkManagerTest, concurrentWatermarkUpdaterMultipleOriginsTest) {
    const auto updates = 100000;
    const auto origins = 10;
    const auto threadsCount = 10;
    auto watermarkManager = Windowing::MultiOriginWatermarkProcessor::create(/*origins*/ origins);

    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceNumber, OriginId>> watermarkBarriers;
    for (int i = 1; i <= updates; i++) {
        for (int o = 0; o < origins; o++) {
            watermarkBarriers.emplace_back(/*ts*/ i,
                                           /*sequence number*/ i,
                                           /*origin*/ o);
        }
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
                // check if the watermark manager dose not return a watermark higher then the current one
                ASSERT_LT(oldWatermark, std::get<0>(currentWatermark));
                watermarkManager->updateWatermark(std::get<0>(currentWatermark),
                                                  std::get<1>(currentWatermark),
                                                  std::get<2>(currentWatermark));
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