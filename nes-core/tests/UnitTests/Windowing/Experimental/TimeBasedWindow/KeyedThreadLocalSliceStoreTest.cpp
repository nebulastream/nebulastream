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
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Experimental/HashMap.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Windowing/Watermark/MultiOriginWatermarkProcessor.hpp>
#include <gtest/gtest.h>
#include <iostream>
using namespace std;
namespace NES::Windowing::Experimental {

class KeyedThreadLocalSliceStoreTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("KeyedThreadLocalSliceStoreTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup KeyedThreadLocalSliceStoreTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        NES_DEBUG("Setup KeyedThreadLocalSliceStoreTest test case.");
        auto bufferManager = std::make_shared<Runtime::BufferManager>();
        size_t keySize = 8;
        size_t valueSize = 8;
        size_t nrEntries = 100;
        hashMapFactory = std::make_shared<NES::Experimental::HashMapFactory>(bufferManager, keySize, valueSize, nrEntries);
    }

  public:
    std::shared_ptr<NES::Experimental::HashMapFactory> hashMapFactory;
};

TEST_F(KeyedThreadLocalSliceStoreTest, assignTumblingWindow) {
    auto tumblingWindowSize = 100;
    auto sliceStore = KeyedThreadLocalSliceStore(hashMapFactory, tumblingWindowSize, tumblingWindowSize);
    for (uint64_t ts = 1; ts < 1000; ts = ts + 10) {
        auto& slice = sliceStore.findSliceByTs(ts);
        ASSERT_EQ(slice->getStart(), (ts / tumblingWindowSize) * tumblingWindowSize);
        ASSERT_EQ(slice->getEnd(), ((ts / tumblingWindowSize) * tumblingWindowSize) + tumblingWindowSize);
    }
    ASSERT_EQ(sliceStore.getNumberOfSlices(), 10);
    sliceStore.removeSlicesUntilTs(1000);
    ASSERT_EQ(sliceStore.getNumberOfSlices(), 0);
}

TEST_F(KeyedThreadLocalSliceStoreTest, assignSlidingWindow) {
    auto slidingWindowSize = 100;
    auto slidingWindowSlide = 10;
    auto sliceStore = KeyedThreadLocalSliceStore(hashMapFactory, slidingWindowSize, slidingWindowSlide);
    for (uint64_t ts = 1; ts < 1000; ts = ts + 10) {
        auto& slice = sliceStore.findSliceByTs(ts);
        ASSERT_TRUE(slice->getStart() % slidingWindowSize == 0 || slice->getStart() % slidingWindowSlide == 0);
        ASSERT_TRUE(slice->getEnd() % slidingWindowSize == 0 || slice->getEnd() % slidingWindowSlide == 0);
    }
    ASSERT_EQ(sliceStore.getNumberOfSlices(), 100);
}

TEST_F(KeyedThreadLocalSliceStoreTest, assignSlidingWindowIregularSlide) {
    auto slidingWindowSize = 100;
    auto slidingWindowSlide = 30;
    auto sliceStore = KeyedThreadLocalSliceStore(hashMapFactory, slidingWindowSize, slidingWindowSlide);

    for (uint64_t ts = 1; ts < 1000; ts = ts + 10) {
        auto& slice = sliceStore.findSliceByTs(ts);
    }
    ASSERT_EQ(sliceStore.getNumberOfSlices(), 64);
}

TEST_F(KeyedThreadLocalSliceStoreTest, invalidTs) {
    auto tumblingWindowSize = 100;
    auto sliceStore = KeyedThreadLocalSliceStore(hashMapFactory, tumblingWindowSize, tumblingWindowSize);
    sliceStore.setLastWatermark(42);
    ASSERT_ANY_THROW(sliceStore.findSliceByTs(10));
}

TEST_F(KeyedThreadLocalSliceStoreTest, assignAndDeleteTest) {
    auto slidingWindowSize = 100;
    auto slidingWindowSlide = 10;
    auto sliceStore = KeyedThreadLocalSliceStore(hashMapFactory, slidingWindowSize, slidingWindowSlide);
    // assign 100 slices
    for (uint64_t ts = 1; ts < 1000; ts = ts + 10) {
        sliceStore.findSliceByTs(ts);
    }
    ASSERT_EQ(sliceStore.getNumberOfSlices(), 100);
    sliceStore.removeSlicesUntilTs(500);
    ASSERT_EQ(sliceStore.getNumberOfSlices(), 50);
    for (uint64_t ts = 1000; ts < 1500; ts = ts + 10) {
        sliceStore.findSliceByTs(ts);
    }
    sliceStore.removeSlicesUntilTs(1500);
    ASSERT_EQ(sliceStore.getNumberOfSlices(), 0);
}

}// namespace NES::Windowing::Experimental