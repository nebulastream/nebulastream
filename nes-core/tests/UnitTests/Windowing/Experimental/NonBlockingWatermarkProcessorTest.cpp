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
#include <Windowing/Experimental/NonBlockingWatermarkProcessor.hpp>
#include <Windowing/Watermark/MultiOriginWatermarkProcessor.hpp>
#include <algorithm>
#include <atomic>
#include <gtest/gtest.h>
#include <iostream>
#include <random>
#include <thread>

using namespace std;
namespace NES {

class NonBlockingWatermarkProcessorTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup NonBlockingWatermarkProcessorTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Logger::setupLogging("NonBlockingWatermarkProcessorTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup NonBlockingWatermarkProcessorTest test case." << std::endl;
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
TEST_F(NonBlockingWatermarkProcessorTest, singleThreadSequentialUpdaterTest) {
    auto updates = 10000;
    auto watermarkProcessor = Experimental::NonBlockingMonotonicSeqQueue<uint64_t>();
    // preallocate watermarks for each transaction
    std::vector<std::tuple<WatermarkTs, SequenceNumber>> watermarkBarriers;
    for (int i = 1; i <= updates; i++) {
        watermarkBarriers.emplace_back(/*ts*/ i,
                                       /*sequence number*/ i);
    }
    for (auto i = 0; i < updates; i++) {
        auto currentWatermarkBarrier = watermarkBarriers[i];
        auto oldWatermark = watermarkProcessor.getCurrentValue();
        ASSERT_LT(oldWatermark, std::get<0>(currentWatermarkBarrier));
        watermarkProcessor.update(std::get<0>(currentWatermarkBarrier), std::get<1>(currentWatermarkBarrier));
        ASSERT_EQ(watermarkProcessor.getCurrentValue(), std::get<0>(currentWatermarkBarrier));
    }
    ASSERT_EQ(watermarkProcessor.getCurrentValue(), std::get<0>(watermarkBarriers.back()));
}

}// namespace NES