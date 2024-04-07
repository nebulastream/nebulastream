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
#include <BaseIntegrationTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/Experimental/LockFreeMultiOriginWatermarkProcessor.hpp>
#include <algorithm>
#include <atomic>
#include <gtest/gtest.h>
#include <iostream>
#include <thread>

using namespace std;
namespace NES {

using WatermarkTs = uint64_t;
using SequenceNumber = uint64_t;
using OriginId = uint64_t;

class LockFreeWatermarkManagerTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LockFreeWatermarkManagerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup LockFreeWatermarkManagerTest test class.");
    }
};

TEST_F(LockFreeWatermarkManagerTest, singleThreadWatermarkUpdaterTest) {
    auto updates = 10000;
    auto watermarkManager = Experimental::LockFreeMultiOriginWatermarkProcessor::create({0});
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

TEST_F(LockFreeWatermarkManagerTest, concurrentWatermarkUpdaterTest) {
    const auto updates = 100000;
    const auto threadsCount = 10;
    auto watermarkManager = Experimental::LockFreeMultiOriginWatermarkProcessor::create({0});

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
                // check if the watermark manager does not return a watermark higher than the current one
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

TEST_F(LockFreeWatermarkManagerTest, singleThreadWatermarkUpdaterMultipleOriginsTest) {
    auto updates = 10000;
    auto origins = 10;
    auto watermarkManager = Experimental::LockFreeMultiOriginWatermarkProcessor::create({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
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

TEST_F(LockFreeWatermarkManagerTest, concurrentWatermarkUpdaterMultipleOriginsTest) {
    const auto updates = 100000;
    const auto origins = 10;
    const auto threadsCount = 10;
    auto watermarkManager = Experimental::LockFreeMultiOriginWatermarkProcessor::create({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

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
                // check if the watermark manager does not return a watermark higher than the current one
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

TEST_F(LockFreeWatermarkManagerTest, singleThreadWatermarkUpdaterOutofOrderTest) {

    class Slice {
      public:
        Slice(uint64_t start) : start(start) {}
        uint64_t start;
    };
    auto vec = std::vector<std::unique_ptr<Slice>>();

    vec.emplace_back(std::make_unique<Slice>(10));
    vec.emplace_back(std::make_unique<Slice>(12));
    auto& s = vec[0];
    NES_DEBUG("{}", s->start);
    auto s2 = std::move(vec[0]);
    NES_DEBUG("{}", s2->start);
}

}// namespace NES