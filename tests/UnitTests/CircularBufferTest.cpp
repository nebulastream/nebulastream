#include <gtest/gtest.h>

#include <Util/Logger.hpp>
#include <Util/CircularBuffer.hpp>

namespace NES {

class CircularBufferTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup CircularBufferTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("CircularBufferTest.log", NES::LOG_DEBUG);
        std::cout << "Setup CircularBufferTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Tear down CircularBufferTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down CircularBufferTest test class." << std::endl;
    }

    size_t testCapacity = 3;
    int testValue = 1;
    int testAdditionalValue = 2;
};

TEST_F(CircularBufferTest, initialState) {
    CircularBuffer<int> circularBuffer(testCapacity);
    EXPECT_EQ(circularBuffer.capacity(), testCapacity);
    EXPECT_EQ(circularBuffer.size(), 0);
    EXPECT_FALSE(circularBuffer.isFull());
    EXPECT_TRUE(circularBuffer.isEmpty());
}

TEST_F(CircularBufferTest, writeOnce) {
    CircularBuffer<int> circularBuffer(testCapacity);
    circularBuffer.write(testValue);
    EXPECT_EQ(circularBuffer.size(), 1);
    EXPECT_FALSE(circularBuffer.isFull());
    EXPECT_FALSE(circularBuffer.isEmpty());
}

TEST_F(CircularBufferTest, readOnce) {
    CircularBuffer<int> circularBuffer(testCapacity);

    circularBuffer.write(testValue);

    EXPECT_EQ(circularBuffer.read(), testValue);
    EXPECT_TRUE(circularBuffer.isEmpty());
}

TEST_F(CircularBufferTest, writeUntilFull) {
    CircularBuffer<int> circularBuffer(testCapacity);

    while (!circularBuffer.isFull()) {
        circularBuffer.write(testValue);
    }

    EXPECT_EQ(circularBuffer.size(), testCapacity);
    EXPECT_TRUE(circularBuffer.isFull());
    EXPECT_FALSE(circularBuffer.isEmpty());
}

TEST_F(CircularBufferTest, writeUntilFullReadUntilEmpty) {
    CircularBuffer<int> circularBuffer(testCapacity);

    while (!circularBuffer.isFull()) {
        circularBuffer.write(testValue);
    }

    while (!circularBuffer.isEmpty()) {
        EXPECT_EQ(circularBuffer.read(), testValue);
    }

    EXPECT_TRUE(circularBuffer.isEmpty());
}

TEST_F(CircularBufferTest, writeOnceOverCapacity) {
    CircularBuffer<int> circularBuffer(testCapacity);

    int i = 0;
    while (!circularBuffer.isFull()) {
        circularBuffer.write(i++);
    }
    circularBuffer.write(i++);

    // 1 is the second item inserted
    EXPECT_EQ(circularBuffer.read(), 1);
}

TEST_F(CircularBufferTest, readOnceWhenEmpty) {
    CircularBuffer<int> circularBuffer(testCapacity);

    EXPECT_TRUE(circularBuffer.isEmpty());

    // 0 comes from T();
    EXPECT_EQ(circularBuffer.read(), 0);
}

TEST_F(CircularBufferTest, resetOnceAfterOneWrite) {
    CircularBuffer<int> circularBuffer(testCapacity);

    circularBuffer.write(testValue);
    circularBuffer.reset();

    // 0 comes from T();
    EXPECT_EQ(circularBuffer.read(), 0);
}

}// namespace NES