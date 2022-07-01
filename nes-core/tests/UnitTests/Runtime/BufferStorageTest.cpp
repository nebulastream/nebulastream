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
#include <Runtime/BufferStorage.hpp>
#include <Util/Logger/Logger.hpp>
#include <thread>

namespace NES {
const size_t buffersInserted = 101;
const size_t emptyBuffer = 0;
const size_t oneBuffer = 1;
const size_t numberOfThreads = 5;

class BufferStorageTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    Runtime::BufferManagerPtr bufferManager;
    Runtime::BufferStoragePtr bufferStorage;

  protected:
    virtual void SetUp() { bufferManager = std::make_shared<Runtime::BufferManager>(1024, 1); }
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::Logger::setupLogging("BufferStorageTest.log", NES::LogLevel::LOG_DEBUG); }
};

/**
     * @brief test inserts buffers to different queues and checks after every insertion that queue
     * size increased on one. After the insertion is fully done the size of the buffer storage is checked to be buffersInserted
*/
TEST_F(BufferStorageTest, bufferInsertionInBufferStorage) {
    for (size_t i = 0; i < buffersInserted; i++) {
        Network::NesPartition nesPartition = Network::NesPartition(i, i, i, i);
        bufferStorage = std::make_shared<Runtime::BufferStorage>(nesPartition);
        auto buffer = bufferManager->getUnpooledBuffer(16384);
        bufferStorage->insertBuffer(nesPartition, buffer.value());
        ASSERT_EQ(bufferStorage->getStorageSize(), i + 1);
        ASSERT_EQ(bufferStorage->getQueueSize(nesPartition), oneBuffer);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffersInserted);
}

/**
     * @brief test inserts buffersInserted amount of buffers into one queue but starts from the biggest watermark.
     * The queue is then checked to be sorted to be exact to have the biggest watermark value at the top.
*/
TEST_F(BufferStorageTest, sortedInsertionInBufferStorage) {
    Network::NesPartition nesPartition = Network::NesPartition(0, 0, 0, 0);
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>(nesPartition);
    for (int i = buffersInserted - 1; i >= 0; i--) {
        auto buffer = bufferManager->getUnpooledBuffer(16384);
        buffer->setWatermark(i);
        bufferStorage->insertBuffer(nesPartition, buffer.value());
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffersInserted);
    for (uint64_t i = 0; i < buffersInserted - 1; i++) {
        bufferStorage->trimBuffer(nesPartition, i + 1);
        ASSERT_EQ(bufferStorage->getTopElementFromQueue(nesPartition)->getWatermark(), i + 1);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), oneBuffer);
}

/**
     * @brief test checks that if trimming is called on an empty buffer it doesn't cause an error
*/
TEST_F(BufferStorageTest, emptyBufferCheck) {
    Network::NesPartition nesPartition = Network::NesPartition(0, 0, 0, 0);
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>(nesPartition);
    ASSERT_EQ(bufferStorage->trimBuffer(nesPartition, 0), false);
}

/**
     * @brief test tries to delete non existing element
*/
TEST_F(BufferStorageTest, trimmingOfNonExistingNesPartition) {
    Network::NesPartition nesPartition = Network::NesPartition(0, 0, 0, 0);
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>(nesPartition);
    for (int i = buffersInserted - 1; i > 0; i--) {
        Network::NesPartition nesPartition = Network::NesPartition(i, i, i, i);
        auto bufferStorage = std::make_shared<Runtime::BufferStorage>(nesPartition);
        auto buffer = bufferManager->getUnpooledBuffer(16384);
        buffer->setWatermark(i);
        bufferStorage->insertBuffer(nesPartition, buffer.value());
    }
    ASSERT_EQ(bufferStorage->trimBuffer(nesPartition, buffersInserted + buffersInserted), false);
}

/**
     * @brief test inserts one buffer and deletes it
*/
TEST_F(BufferStorageTest, oneBufferDeletionFromBufferStorage) {
    Network::NesPartition nesPartition = Network::NesPartition(0, 0, 0, 0);
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>(nesPartition);
    auto buffer = bufferManager->getUnpooledBuffer(16384);
    buffer->setWatermark(0);
    bufferStorage->insertBuffer(nesPartition, buffer.value());
    ASSERT_EQ(bufferStorage->getStorageSize(), oneBuffer);
    ASSERT_EQ(bufferStorage->getQueueSize(nesPartition), oneBuffer);
    bufferStorage->trimBuffer(nesPartition, 1);
    ASSERT_EQ(bufferStorage->getQueueSize(nesPartition), emptyBuffer);
}

/**
     * @brief test inserts buffersInserted buffers in different queues and deletes them.
*/
TEST_F(BufferStorageTest, manyBufferDeletionFromBufferStorage) {
    for (size_t i = 0; i < buffersInserted; i++) {
        Network::NesPartition nesPartition = Network::NesPartition(i, i, i, i);
        auto bufferStorage = std::make_shared<Runtime::BufferStorage>(nesPartition);
        auto buffer = bufferManager->getUnpooledBuffer(16384);
        bufferStorage->insertBuffer(nesPartition, buffer.value());
        ASSERT_EQ(bufferStorage->getQueueSize(nesPartition), oneBuffer);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffersInserted);
    for (size_t i = 0; i < buffersInserted; i++) {
        Network::NesPartition nesPartition = Network::NesPartition(i, i, i, i);
        bufferStorage->trimBuffer(nesPartition, i + 1);
        ASSERT_EQ(bufferStorage->getQueueSize(nesPartition), emptyBuffer);
    }
}

/**
     * @brief test inserts buffersInserted buffers in one queue and leaves only one after trimming. The test checks that
     * the deleted buffers are smaller that passed timestamp.
*/
TEST_F(BufferStorageTest, smallerBufferDeletionFromBufferStorage) {
    Network::NesPartition nesPartition = Network::NesPartition(0, 0, 0, 0);
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>(nesPartition);
    for (size_t i = 0; i < buffersInserted; i++) {
        auto buffer = bufferManager->getUnpooledBuffer(16384);
        buffer->setWatermark(i);
        bufferStorage->insertBuffer(nesPartition, buffer.value());
        ASSERT_EQ(bufferStorage->getQueueSize(nesPartition), i + 1);
    }
    bufferStorage->trimBuffer(nesPartition, buffersInserted - 1);
    ASSERT_EQ(bufferStorage->getQueueSize(nesPartition), oneBuffer);
}

}// namespace NES
