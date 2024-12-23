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
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <atomic>
#include <gtest/gtest.h>
#include <iostream>
#include <random>
#include <thread>

using namespace std;
namespace NES::Runtime::Execution::Operators {

class MultiOriginWatermarkProcessorTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MultiOriginWatermarkProcessorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup MultiOriginWatermarkProcessorTest test class.");
    }
    /**
     * @brief generates random integer from minValue to maxValue
     */
    static uint64_t createRandomInt(uint64_t minValue = 1, uint64_t maxValue = 1000) {
        std::random_device rd;
        std::mt19937 mt(rd());
        std::uniform_int_distribution<uint64_t> dist(minValue, maxValue);
        return dist(mt);
    }
};

TEST_F(MultiOriginWatermarkProcessorTest, singleThreadWatermarkUpdaterTest) {
    auto updates = 10000_u64;
    auto watermarkManager = MultiOriginWatermarkProcessor::create(std::vector{INVALID_ORIGIN_ID});
    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceData, OriginId>> watermarkBarriers;
    for (auto i = 1_u64; i <= updates; i++) {
        watermarkBarriers.emplace_back(std::tuple<WatermarkTs, SequenceData, OriginId>(i, {i, 1, true}, INVALID_ORIGIN_ID));
    }
    for (auto i = 0_u64; i < updates; i++) {
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

TEST_F(MultiOriginWatermarkProcessorTest, concurrentWatermarkUpdaterTest) {
    const auto updates = 100000_u64;
    const auto threadsCount = 10;
    auto watermarkManager = MultiOriginWatermarkProcessor::create({INVALID_ORIGIN_ID});

    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceData, OriginId>> watermarkBarriers;
    for (auto i = 1_u64; i <= updates * threadsCount; i++) {
        watermarkBarriers.emplace_back(std::tuple<WatermarkTs, SequenceData, OriginId>(i, {i, 1, true}, INVALID_ORIGIN_ID));
    }
    std::atomic<uint64_t> globalUpdateCounter = 0;
    std::vector<std::thread> threads;
    threads.reserve(threadsCount);
    for (int threadId = 0; threadId < threadsCount; threadId++) {
        threads.emplace_back(thread([&watermarkManager, &watermarkBarriers, &globalUpdateCounter]() {
            // each thread processes a particular update
            for (auto i = 0_u64; i < updates; i++) {
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

TEST_F(MultiOriginWatermarkProcessorTest, singleThreadWatermarkUpdaterMultipleOriginsTest) {
    auto updates = 10000_u64;
    auto origins = 10;
    auto watermarkManager = MultiOriginWatermarkProcessor::create({INVALID_ORIGIN_ID,
                                                                   OriginId(1),
                                                                   OriginId(2),
                                                                   OriginId(3),
                                                                   OriginId(4),
                                                                   OriginId(5),
                                                                   OriginId(6),
                                                                   OriginId(7),
                                                                   OriginId(8),
                                                                   OriginId(9)});
    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceData, OriginId>> watermarkBarriers;
    for (auto i = 1_u64; i <= updates; i++) {
        for (int o = 0; o < origins; o++) {
            watermarkBarriers.emplace_back(std::tuple<WatermarkTs, SequenceData, OriginId>(i, {i, 1, true}, OriginId(o)));
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

TEST_F(MultiOriginWatermarkProcessorTest, concurrentWatermarkUpdaterMultipleOriginsTest) {
    const auto updates = 100000;
    const auto origins = 10;
    const auto threadsCount = 10;
    auto watermarkManager = MultiOriginWatermarkProcessor::create({INVALID_ORIGIN_ID,
                                                                   OriginId(1),
                                                                   OriginId(2),
                                                                   OriginId(3),
                                                                   OriginId(4),
                                                                   OriginId(5),
                                                                   OriginId(6),
                                                                   OriginId(7),
                                                                   OriginId(8),
                                                                   OriginId(9)});

    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceData, OriginId>> watermarkBarriers;
    for (auto i = 1_u64; i <= updates; i++) {
        for (int o = 0; o < origins; o++) {
            watermarkBarriers.emplace_back(std::tuple<WatermarkTs, SequenceData, OriginId>(i, {i, 1, true}, OriginId(o)));
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

TEST_F(MultiOriginWatermarkProcessorTest, watermarksSerializtionMultipleOriginsSimpleTest) {
    auto updates = 5000_u64;
    auto origins = 10;
    auto watermarkProcessorToMigrate = MultiOriginWatermarkProcessor::create({INVALID_ORIGIN_ID,
                                                                              OriginId(1),
                                                                              OriginId(2),
                                                                              OriginId(3),
                                                                              OriginId(4),
                                                                              OriginId(5),
                                                                              OriginId(6),
                                                                              OriginId(7),
                                                                              OriginId(8),
                                                                              OriginId(9)});
    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceData, OriginId>> watermarkBarriers;
    for (auto i = 1_u64; i <= updates; i++) {
        for (int o = 0; o < origins; o++) {
            watermarkBarriers.emplace_back(std::tuple<WatermarkTs, SequenceData, OriginId>(i, {i, 1, true}, OriginId(o)));
        }
    }

    for (auto currentWatermarkBarrier : watermarkBarriers) {
        auto oldWatermark = watermarkProcessorToMigrate->getCurrentWatermark();
        ASSERT_LT(oldWatermark, std::get<0>(currentWatermarkBarrier));
        watermarkProcessorToMigrate->updateWatermark(std::get<0>(currentWatermarkBarrier),
                                                     std::get<1>(currentWatermarkBarrier),
                                                     std::get<2>(currentWatermarkBarrier));
        ASSERT_LE(watermarkProcessorToMigrate->getCurrentWatermark(), std::get<0>(currentWatermarkBarrier));
    }
    ASSERT_EQ(watermarkProcessorToMigrate->getCurrentWatermark(), std::get<0>(watermarkBarriers.back()));

    auto bufferManager = std::make_shared<BufferManager>(1024, 5000);
    auto watermarks = watermarkProcessorToMigrate->serializeWatermarks(bufferManager);

    auto recreatedWatermarkProcessor = MultiOriginWatermarkProcessor::create({INVALID_ORIGIN_ID,
                                                                              OriginId(1),
                                                                              OriginId(2),
                                                                              OriginId(3),
                                                                              OriginId(4),
                                                                              OriginId(5),
                                                                              OriginId(6),
                                                                              OriginId(7),
                                                                              OriginId(8),
                                                                              OriginId(9)});
    recreatedWatermarkProcessor->restoreWatermarks(watermarks);

    ASSERT_EQ(watermarkProcessorToMigrate->getCurrentWatermark(), recreatedWatermarkProcessor->getCurrentWatermark());
}

TEST_F(MultiOriginWatermarkProcessorTest, watermarksSerializtionMultipleOriginsOutOfOrderSeqNumbersTest) {
    auto updates = 50_u64;
    auto origins = 10;
    auto watermarkProcessorToMigrate = MultiOriginWatermarkProcessor::create({INVALID_ORIGIN_ID,
                                                                              OriginId(1),
                                                                              OriginId(2),
                                                                              OriginId(3),
                                                                              OriginId(4),
                                                                              OriginId(5),
                                                                              OriginId(6),
                                                                              OriginId(7),
                                                                              OriginId(8),
                                                                              OriginId(9)});
    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceData, OriginId>> watermarkBarriers;
    for (auto i = 1_u64; i <= updates; i++) {
        for (int o = 0; o < origins; o++) {
            watermarkBarriers.emplace_back(std::tuple<WatermarkTs, SequenceData, OriginId>(i, {i, 1, true}, OriginId(o)));
        }
    }

    // add more sequence numbers to processors so that they will have random current sequence number
    auto originMin = std::vector<SequenceNumber>(origins);
    for (int o = 0; o < origins; o++) {
        auto nextSeqNumber = createRandomInt(51, 500);
        originMin[o] = nextSeqNumber + 1;
        for (auto seqNumber = updates + 1; seqNumber <= nextSeqNumber; seqNumber++) {
            uint64_t randomChunks = createRandomInt(2, 50);
            for (uint64_t chunk = 1; chunk < randomChunks; chunk++) {
                watermarkBarriers.emplace_back(
                    std::tuple<WatermarkTs, SequenceData, OriginId>(seqNumber, {seqNumber, chunk, false}, OriginId(o)));
            }
            watermarkBarriers.emplace_back(
                std::tuple<WatermarkTs, SequenceData, OriginId>(seqNumber, {seqNumber, randomChunks, true}, OriginId(o)));
        }
    }

    // add more sequence numbers out of order after the current sequence number
    auto originMax = std::vector<SequenceNumber>(origins, 100001);
    for (int o = 0; o < origins; o++) {
        auto seenSeqNumbers = std::map<SequenceNumber, bool>();
        for (uint64_t i = 0; i < createRandomInt(50, 1000); i++) {
            auto nextSeqNumber = createRandomInt(1000, 100000);
            if (!seenSeqNumbers.contains(nextSeqNumber + 1)) {
                originMax[o] = min(originMax[o], nextSeqNumber);
                if (!seenSeqNumbers.contains(nextSeqNumber)) {
                    uint64_t randomChunks = createRandomInt(2, 50);
                    for (uint64_t chunk = 1; chunk < randomChunks; chunk++) {
                        watermarkBarriers.emplace_back(
                            std::tuple<WatermarkTs, SequenceData, OriginId>(nextSeqNumber,
                                                                            {nextSeqNumber, chunk, false},
                                                                            OriginId(o)));
                    }
                    watermarkBarriers.emplace_back(
                        std::tuple<WatermarkTs, SequenceData, OriginId>(nextSeqNumber,
                                                                        {nextSeqNumber, randomChunks, true},
                                                                        OriginId(o)));
                }
            }
        }
    }

    for (auto currentWatermarkBarrier : watermarkBarriers) {
        auto oldWatermark = watermarkProcessorToMigrate->getCurrentWatermark();
        ASSERT_LT(oldWatermark, std::get<0>(currentWatermarkBarrier));
        watermarkProcessorToMigrate->updateWatermark(std::get<0>(currentWatermarkBarrier),
                                                     std::get<1>(currentWatermarkBarrier),
                                                     std::get<2>(currentWatermarkBarrier));
        ASSERT_LE(watermarkProcessorToMigrate->getCurrentWatermark(), std::get<0>(currentWatermarkBarrier));
    }

    auto bufferManager = std::make_shared<BufferManager>(1024, 5000);
    auto watermarks = watermarkProcessorToMigrate->serializeWatermarks(bufferManager);

    auto recreatedWatermarkProcessor = MultiOriginWatermarkProcessor::create({INVALID_ORIGIN_ID,
                                                                              OriginId(1),
                                                                              OriginId(2),
                                                                              OriginId(3),
                                                                              OriginId(4),
                                                                              OriginId(5),
                                                                              OriginId(6),
                                                                              OriginId(7),
                                                                              OriginId(8),
                                                                              OriginId(9)});
    recreatedWatermarkProcessor->restoreWatermarks(watermarks);
    ASSERT_EQ(watermarkProcessorToMigrate->getCurrentWatermark(), recreatedWatermarkProcessor->getCurrentWatermark());

    // add sequence number till the first one, which came out of order
    for (int o = 0; o < origins; o++) {
        for (uint64_t i = originMin[o]; i < originMax[o]; i++) {
            recreatedWatermarkProcessor->updateWatermark(i, {i, 1, true}, OriginId(o));
            watermarkProcessorToMigrate->updateWatermark(i, {i, 1, true}, OriginId(o));
            ASSERT_EQ(watermarkProcessorToMigrate->getCurrentWatermark(), recreatedWatermarkProcessor->getCurrentWatermark());
        }
    }

    // check that current sequence number is changed accordingly
    auto expectedCurrentWatermark = std::min_element(originMax.begin(), originMax.end());
    ASSERT_EQ(recreatedWatermarkProcessor->getCurrentWatermark(), *expectedCurrentWatermark);
}
}// namespace NES::Runtime::Execution::Operators
