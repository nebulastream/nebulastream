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
#include <gtest/gtest.h>

#include <Util/CircularBuffer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

class CircularBufferTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("CircularBufferTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup CircularBufferTest test class.");
    }

    uint64_t testCapacity = 3;
    uint64_t zeroCapacity = 0;
    int testValue = 1;
};

TEST_F(CircularBufferTest, initialState) {
    CircularBuffer<int> circularBuffer(testCapacity);
    EXPECT_EQ(circularBuffer.capacity(), testCapacity);
    EXPECT_EQ(circularBuffer.size(), 0u);
    EXPECT_EQ(circularBuffer.front(), 0);
    EXPECT_EQ(circularBuffer.back(), 0);
    EXPECT_FALSE(circularBuffer.full());
    EXPECT_TRUE(circularBuffer.empty());
}

TEST_F(CircularBufferTest, randomAccessInitial) {
    CircularBuffer<int> circularBuffer(testCapacity);
    EXPECT_EQ(circularBuffer.at(0), 0);
    EXPECT_EQ(circularBuffer[0], circularBuffer.at(0));
}

TEST_F(CircularBufferTest, pushFrontOnce) {
    CircularBuffer<int> circularBuffer(testCapacity);
    circularBuffer.push(testValue);
    EXPECT_EQ(circularBuffer.size(), 1u);
    EXPECT_FALSE(circularBuffer.full());
    EXPECT_FALSE(circularBuffer.empty());
}

TEST_F(CircularBufferTest, emplaceFrontOnce) {
    CircularBuffer<int> circularBuffer(testCapacity);
    circularBuffer.emplace(testValue);
    EXPECT_EQ(circularBuffer.size(), 1u);
    EXPECT_FALSE(circularBuffer.full());
    EXPECT_FALSE(circularBuffer.empty());
}

TEST_F(CircularBufferTest, emplaceFrontFull) {
    CircularBuffer<int> circularBuffer(testCapacity);

    circularBuffer.emplace(testValue);    // at 2 in the end
    circularBuffer.emplace(testValue + 1);// at 1
    circularBuffer.emplace(testValue + 2);// at 0

    EXPECT_TRUE(circularBuffer.full());
    EXPECT_EQ(circularBuffer.size(), testCapacity);
    EXPECT_EQ(circularBuffer.at(0), testValue + 2);
    EXPECT_EQ(circularBuffer.at(1), testValue + 1);
    EXPECT_EQ(circularBuffer.at(2), testValue);
}

TEST_F(CircularBufferTest, emplaceFrontOneOverCapacity) {
    CircularBuffer<int> circularBuffer(testCapacity);

    circularBuffer.emplace(testValue);
    circularBuffer.emplace(testValue + 1);// at 2
    circularBuffer.emplace(testValue + 2);// at 1
    circularBuffer.emplace(testValue + 3);// at 0

    EXPECT_TRUE(circularBuffer.full());
    EXPECT_EQ(circularBuffer.size(), testCapacity);
    EXPECT_EQ(circularBuffer.at(0), testValue + 3);
    EXPECT_EQ(circularBuffer.at(1), testValue + 2);
    EXPECT_EQ(circularBuffer.at(2), testValue + 1);
}

TEST_F(CircularBufferTest, pushUntilFull) {
    CircularBuffer<int> circularBuffer(testCapacity);

    while (!circularBuffer.full()) {
        circularBuffer.push(testValue);
    }

    EXPECT_EQ(circularBuffer.size(), testCapacity);
    EXPECT_TRUE(circularBuffer.full());
    EXPECT_FALSE(circularBuffer.empty());
}

TEST_F(CircularBufferTest, emplaceUntilFull) {
    CircularBuffer<int> circularBuffer(testCapacity);

    while (!circularBuffer.full()) {
        circularBuffer.emplace(testValue);
    }

    EXPECT_EQ(circularBuffer.size(), testCapacity);
    EXPECT_TRUE(circularBuffer.full());
    EXPECT_FALSE(circularBuffer.empty());
}

TEST_F(CircularBufferTest, iterateAutoNotFull) {
    CircularBuffer<int> circularBuffer(testCapacity);

    circularBuffer.push(testValue + 1);
    circularBuffer.push(testValue);

    EXPECT_FALSE(circularBuffer.full());

    int i = 0;
    for (auto c : circularBuffer) {
        EXPECT_EQ(c, testValue + i++);
    }
    EXPECT_EQ(static_cast<std::size_t>(i), circularBuffer.size());
}

TEST_F(CircularBufferTest, popAfterPushOnce) {
    CircularBuffer<int> circularBuffer(testCapacity);
    circularBuffer.push(testValue);
    circularBuffer.pop();
    EXPECT_FALSE(circularBuffer.full());
    EXPECT_TRUE(circularBuffer.empty());
}

TEST_F(CircularBufferTest, popAfterTwoPushes) {
    CircularBuffer<int> circularBuffer(testCapacity);
    circularBuffer.push(testValue + 1);
    circularBuffer.push(testValue);
    circularBuffer.pop();
    EXPECT_FALSE(circularBuffer.full());
    EXPECT_EQ(circularBuffer.size(), 1U);
    EXPECT_EQ(circularBuffer.capacity(), testCapacity);
}

TEST_F(CircularBufferTest, popOnEmpty) {
    CircularBuffer<int> circularBuffer(testCapacity);
    auto val = circularBuffer.pop();
    EXPECT_TRUE(circularBuffer.empty());
    EXPECT_EQ(val, 0);
}

TEST_F(CircularBufferTest, toVector) {
    CircularBuffer<int> circularBuffer(testCapacity);
    circularBuffer.push(testValue + 1);
    auto retVec = circularBuffer.toVector();
    EXPECT_FALSE(retVec.empty());
    EXPECT_EQ(retVec.capacity(), retVec.size());
    EXPECT_EQ(retVec.size(), 1);
    EXPECT_EQ(retVec.capacity(), circularBuffer.size());
    EXPECT_EQ(retVec.size(), circularBuffer.size());
    for (uint64_t itemIdx = 0; itemIdx < retVec.size(); ++itemIdx) {
        ASSERT_EQ(retVec[itemIdx], circularBuffer.at(itemIdx));
    }
    circularBuffer.push(testValue);
    EXPECT_NE(retVec.capacity(), circularBuffer.size());
    EXPECT_NE(retVec.size(), circularBuffer.size());
    retVec = circularBuffer.toVector();
    EXPECT_EQ(retVec.capacity(), circularBuffer.size());
    EXPECT_EQ(retVec.size(), 2);
    EXPECT_EQ(retVec.capacity(), circularBuffer.size());
    EXPECT_EQ(retVec.size(), circularBuffer.size());
    for (uint64_t itemIdx = 0; itemIdx < retVec.size(); ++itemIdx) {
        ASSERT_EQ(retVec[itemIdx], circularBuffer.at(itemIdx));
    }
}

}// namespace NES