/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <NodeEngine/Transactional/CombinedLatchWatermarkManager.hpp>
#include <NodeEngine/Transactional/CombinedWatermarkManager.hpp>
#include <NodeEngine/Transactional/LatchVectorWatermarkManager.hpp>
#include <NodeEngine/Transactional/LatchWatermarkManager.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <thread>

using namespace std;
namespace NES {

class WatermarkManagerTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup WatermarkManagerTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("WatermarkManagerTest.log", NES::LOG_DEBUG);
        std::cout << "Setup WatermarkManagerTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() { std::cout << "Tear down WatermarkManagerTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down WatermarkManagerTest test class." << std::endl; }
};

TEST_F(WatermarkManagerTest, singleThreadLatchBasedWatermarkTest) {
    auto updates = 10000;
    auto watermarkManager = NodeEngine::Transactional::LatchWatermarkManager::create();
    uint64_t transactionIdOracle = 0;
    uint64_t watermarkOracle = 0;
    for (auto i = 1; i < updates; i++) {
        auto currentTransaction = NodeEngine::Transactional::TransactionId(++transactionIdOracle);
        auto currentWatermark = ++watermarkOracle;
        auto oldWatermark = watermarkManager->getCurrentWatermark(currentTransaction);
        ASSERT_TRUE(oldWatermark < currentWatermark);
        watermarkManager->updateWatermark(currentTransaction, currentWatermark);
        ASSERT_TRUE(watermarkManager->getCurrentWatermark(currentTransaction) <= currentWatermark);
    }
    auto currentTransaction = NodeEngine::Transactional::TransactionId(++transactionIdOracle);
    ASSERT_TRUE(watermarkManager->getCurrentWatermark(currentTransaction) == watermarkOracle);
}

TEST_F(WatermarkManagerTest, singleThreadLatchBasedWatermarkDuplicatedWatermarkTest) {
    auto updates = 10000;
    auto watermarkManager = NodeEngine::Transactional::LatchWatermarkManager::create();
    uint64_t transactionIdOracle = 0;
    uint64_t watermarkOracle = 10;
    for (auto i = 1; i < updates; i++) {
        auto currentTransaction = NodeEngine::Transactional::TransactionId(++transactionIdOracle);
        auto currentWatermark = (++watermarkOracle) / 10;
        auto oldWatermark = watermarkManager->getCurrentWatermark(currentTransaction);
        ASSERT_TRUE(oldWatermark <= currentWatermark);
        watermarkManager->updateWatermark(currentTransaction, currentWatermark);
        ASSERT_TRUE(watermarkManager->getCurrentWatermark(currentTransaction) <= currentWatermark);
    }
    auto currentTransaction = NodeEngine::Transactional::TransactionId(++transactionIdOracle);
    ASSERT_TRUE(watermarkManager->getCurrentWatermark(currentTransaction) == (watermarkOracle) / 10);
}

TEST_F(WatermarkManagerTest, concurrentLatchBasedWatermarkTest) {
    auto updates = 100000;
    auto threadsCount = 10;
    auto watermarkManager = NodeEngine::Transactional::LatchWatermarkManager::create();
    std::atomic<uint64_t> transactionIdOracle = 0;

    // preallocate watermarks for each transaction
    std::vector<NodeEngine::Transactional::WatermarkTs> watermarks;
    for (int i = 1; i <= updates * threadsCount; i++) {
        watermarks.emplace_back(i);
    }

    std::vector<std::thread> threads;
    for (int threadId = 0; threadId < threadsCount; threadId++) {
        threads.emplace_back(thread([updates, &watermarkManager, &watermarks, &transactionIdOracle]() {
            for (auto i = 0; i < updates; i++) {
                auto currentTransaction = NodeEngine::Transactional::TransactionId(++transactionIdOracle);
                auto currentWatermark = watermarks[currentTransaction.id - 1];
                auto oldWatermark = watermarkManager->getCurrentWatermark(currentTransaction);
                // check if the watermark manager dose not return a watermark higher then the current one
                ASSERT_LT(oldWatermark, currentWatermark);
                watermarkManager->updateWatermark(currentTransaction, currentWatermark);
                // check that the watermark manager returns a watermark that is <= to the max watermark
                auto globalCurrentWatermark = watermarkManager->getCurrentWatermark(currentTransaction);
                auto maxCurrentWatermark = watermarks[transactionIdOracle - 1];
                ASSERT_LE(globalCurrentWatermark, maxCurrentWatermark);
            }
        }));
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto currentTransaction = NodeEngine::Transactional::TransactionId(++transactionIdOracle);
    ASSERT_EQ(watermarkManager->getCurrentWatermark(currentTransaction), watermarks.back());
}

TEST_F(WatermarkManagerTest, singleThreadVectorLatchBasedWatermarkTest) {
    auto updates = 10000;
    auto watermarkManager = NodeEngine::Transactional::LatchVectorWatermarkManager::create(1, 1);
    uint64_t transactionIdOracle = 0;
    uint64_t watermarkOracle = 0;
    for (auto i = 1; i < updates; i++) {
        auto currentTransaction = NodeEngine::Transactional::TransactionId(++transactionIdOracle, 0, 0);
        auto currentWatermark = ++watermarkOracle;
        auto oldWatermark = watermarkManager->getCurrentWatermark(currentTransaction);
        ASSERT_TRUE(oldWatermark < currentWatermark);
        watermarkManager->updateWatermark(currentTransaction, currentWatermark);
        ASSERT_TRUE(watermarkManager->getCurrentWatermark(currentTransaction) <= currentWatermark);
    }
    auto currentTransaction = NodeEngine::Transactional::TransactionId(++transactionIdOracle);
    ASSERT_TRUE(watermarkManager->getCurrentWatermark(currentTransaction) == watermarkOracle);
}

TEST_F(WatermarkManagerTest, concurrentThreadVectorLatchBasedWatermarkTest) {
    auto updates = 100000;
    auto threadsCount = 2;
    auto watermarkManager = NodeEngine::Transactional::LatchVectorWatermarkManager::create(1, 2);
    std::atomic<uint64_t> transactionIdOracle = 0;

    // preallocate watermarks for each transaction
    std::vector<NodeEngine::Transactional::WatermarkTs> watermarks;
    for (int i = 1; i <= updates * threadsCount; i++) {
        watermarks.emplace_back(i);
    }

    std::vector<std::thread> threads;
    for (int threadId = 0; threadId < threadsCount; threadId++) {
        threads.emplace_back(thread([threadId, updates, &watermarkManager, &watermarks, &transactionIdOracle]() {
            for (auto i = 0; i < updates; i++) {
                auto currentTransaction = NodeEngine::Transactional::TransactionId(++transactionIdOracle, 0, threadId);
                auto currentWatermark = watermarks[currentTransaction.id - 1];
                auto oldWatermark = watermarkManager->getCurrentWatermark(currentTransaction);
                // check if the watermark manager dose not return a watermark higher then the current one
                ASSERT_LT(oldWatermark, currentWatermark);
                watermarkManager->updateWatermark(currentTransaction, currentWatermark);
                // check that the watermark manager returns a watermark that is <= to the max watermark
                auto globalCurrentWatermark = watermarkManager->getCurrentWatermark(currentTransaction);
                auto maxCurrentWatermark = watermarks[transactionIdOracle - 1];
                ASSERT_LE(globalCurrentWatermark, maxCurrentWatermark);
            }
        }));
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto currentTransaction = NodeEngine::Transactional::TransactionId(++transactionIdOracle);
    ASSERT_EQ(watermarkManager->getCurrentWatermark(currentTransaction), watermarks.back());
}

TEST_F(WatermarkManagerTest, combinedWatermarkManagerTest) {
    auto updates = 10000;
    uint64_t transactionIdOracle = 0;
    uint64_t watermarkOracle = 0;
    auto watermarkManager = NodeEngine::Transactional::CombinedLatchWatermarkManager::create(2);
    for (auto i = 1; i < updates; i++) {
        auto nextTransactionId = ++transactionIdOracle;
        auto nextOriginId = transactionIdOracle % 2;
        auto threadId = 0;
        auto currentTransaction = NodeEngine::Transactional::TransactionId(nextTransactionId, nextOriginId, threadId);
        auto currentWatermark = ++watermarkOracle;
        auto oldWatermark = watermarkManager->getCurrentWatermark(currentTransaction);
        ASSERT_TRUE(oldWatermark < currentWatermark);
        watermarkManager->updateWatermark(currentTransaction, currentWatermark);
        ASSERT_TRUE(watermarkManager->getCurrentWatermark(currentTransaction) <= currentWatermark);
    }
    auto currentTransaction = NodeEngine::Transactional::TransactionId(++transactionIdOracle);
    ASSERT_TRUE(watermarkManager->getCurrentWatermark(currentTransaction) == watermarkOracle - 1);
}

TEST_F(WatermarkManagerTest, concurrentCombinedWatermarkTest) {
    auto updates = 10000;
    const auto threadsCount = 10;
    const auto originCount = 10;
    auto watermarkManager = NodeEngine::Transactional::CombinedLatchWatermarkManager::create(originCount);
    std::atomic<uint64_t> transactionIdOracle = 0;

    // preallocate watermarks for each transaction
    std::vector<NodeEngine::Transactional::WatermarkTs> watermarks;
    for (int i = 1; i <= updates * threadsCount; i++) {
        watermarks.emplace_back(i);
    }

    std::vector<std::thread> threads;
    for (int threadId = 0; threadId < threadsCount; threadId++) {
        threads.emplace_back(thread([threadId, updates, &watermarkManager, &watermarks, &transactionIdOracle]() {
            for (auto i = 0; i < updates; i++) {
                auto nextOriginId = transactionIdOracle % originCount;
                auto currentTransaction = NodeEngine::Transactional::TransactionId(++transactionIdOracle, nextOriginId, threadId);
                auto currentWatermark = watermarks[currentTransaction.id - 1];
                auto oldWatermark = watermarkManager->getCurrentWatermark(currentTransaction);
                // check if the watermark manager dose not return a watermark higher then the current one
                ASSERT_LT(oldWatermark, currentWatermark);
                watermarkManager->updateWatermark(currentTransaction, currentWatermark);

                auto globalCurrentWatermark = watermarkManager->getCurrentWatermark(currentTransaction);
                // check that the watermark manager returns a watermark that is <= to the max watermark
                auto maxCurrentWatermark = watermarks[transactionIdOracle - 1];
                ASSERT_LE(globalCurrentWatermark, maxCurrentWatermark);
            }
        }));
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto currentTransaction = NodeEngine::Transactional::TransactionId(++transactionIdOracle);
    ASSERT_EQ(watermarkManager->getCurrentWatermark(currentTransaction), watermarks.back() - originCount + 1);
}

}// namespace NES