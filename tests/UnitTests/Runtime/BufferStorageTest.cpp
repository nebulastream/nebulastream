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

#include <Runtime/BufferManager.hpp>
#include <Runtime/BufferStorage.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <thread>
namespace NES {
const size_t buffersInserted = 21;
const size_t emptyBuffer = 0;
const size_t oneBuffer = 1;
const size_t expectedStorageSize = 2;
const size_t numberOfThreads = 5;

class BufferStorageTest : public testing::Test {
  public:
    Runtime::BufferManagerPtr bufferManager;

  protected:
    virtual void SetUp() { bufferManager = std::make_shared<Runtime::BufferManager>(1024, 1); }
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::setupLogging("BufferStorageTest.log", NES::LOG_DEBUG); }
};

/**
     * @brief test inserts buffers to different queues and checks after every insertion that queue
     * size increased on one. After the insertion is fully done the site of the buffer storage is checked to be five
*/
TEST_F(BufferStorageTest, bufferInsertionInBufferStorage) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    for (size_t i = 0; i < buffersInserted; i++) {
        auto buffer = bufferManager->getUnpooledBuffer(16384);
        bufferStorage->insertBuffer(BufferSequenceNumber(i, i), buffer.value());
        ASSERT_EQ(bufferStorage->getStorageSize(), i + 1);
        ASSERT_EQ(bufferStorage->getStorageSizeForQueue(i), oneBuffer);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffersInserted);
}

/**
     * @brief test checks that in case of out of order insertion the sequence number tracker is aware
     * of the highest linear increasing sequnce number
*/
TEST_F(BufferStorageTest, bufferInsertionOutOfOrderInBufferStorage) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    for (size_t i = 0; i < buffersInserted; i++) {
        auto buffer = bufferManager->getUnpooledBuffer(16384);
        bufferStorage->insertBuffer(BufferSequenceNumber(i, 0), buffer.value());
        ASSERT_EQ(bufferStorage->getStorageSize(), i + 1);
        ASSERT_EQ(bufferStorage->getStorageSizeForQueue(0), i + 1);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffersInserted);
    auto buffer1 = bufferManager->getUnpooledBuffer(16384);
    bufferStorage->insertBuffer(BufferSequenceNumber(buffersInserted + 2, 0), buffer1.value());
    auto buffer2 = bufferManager->getUnpooledBuffer(16384);
    ASSERT_EQ(bufferStorage->getSequenceNumberTracker()[0]->getSequenceNumberTrackerPriorityQueue()->size(), 1);
    ASSERT_EQ(bufferStorage->getSequenceNumberTracker()[0]->getCurrentHighestSequenceNumber(), buffersInserted - 1);
    bufferStorage->insertBuffer(BufferSequenceNumber(buffersInserted + 1, 0), buffer2.value());
    ASSERT_EQ(bufferStorage->getSequenceNumberTracker()[0]->getSequenceNumberTrackerPriorityQueue()->size(), 2);
    ASSERT_EQ(bufferStorage->getSequenceNumberTracker()[0]->getCurrentHighestSequenceNumber(), buffersInserted - 1);
    auto buffer3 = bufferManager->getUnpooledBuffer(16384);
    bufferStorage->insertBuffer(BufferSequenceNumber(buffersInserted, 0), buffer3.value());
    ASSERT_EQ(bufferStorage->getSequenceNumberTracker()[0]->getSequenceNumberTrackerPriorityQueue()->size(), 0);
    ASSERT_EQ(bufferStorage->getSequenceNumberTracker()[0]->getCurrentHighestSequenceNumber(), buffersInserted + 2);
}

/**
     * @brief test checks that if trimming is called on an empty buffer it doesn't cause an error
*/
TEST_F(BufferStorageTest, emptyBufferCheck) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    ASSERT_EQ(bufferStorage->trimBuffer(BufferSequenceNumber(0, 0)), false);
}

/**
     * @brief test tries to delete non existing element
*/
TEST_F(BufferStorageTest, trimmingOfNonExistingElement) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    auto buffer = bufferManager->getUnpooledBuffer(16384);
    for (int i = buffersInserted - 1; i >= 0; i--) {
        bufferStorage->insertBuffer(BufferSequenceNumber(i, 0), buffer.value());
    }
    ASSERT_EQ(bufferStorage->trimBuffer(BufferSequenceNumber(0, 6)), false);
}

/**
     * @brief test inserts one buffer and deletes it
*/
TEST_F(BufferStorageTest, oneBufferTrimmingFromBufferStorage) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    auto buffer = bufferManager->getUnpooledBuffer(16384);
    bufferStorage->insertBuffer(BufferSequenceNumber(0, 0), buffer.value());
    ASSERT_EQ(bufferStorage->getStorageSize(), oneBuffer);
    ASSERT_EQ(bufferStorage->getStorageSizeForQueue(0), oneBuffer);
    bufferStorage->trimBuffer(BufferSequenceNumber(1, 0));
    ASSERT_EQ(bufferStorage->getStorageSizeForQueue(0), emptyBuffer);
}

/**
     * @brief test inserts five buffers in different queues and deletes them.
*/
TEST_F(BufferStorageTest, manyBufferTrimmingFromBufferStorage) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    for (size_t i = 0; i < buffersInserted; i++) {
        auto buffer = bufferManager->getUnpooledBuffer(16384);
        bufferStorage->insertBuffer(BufferSequenceNumber(i, i), buffer.value());
        ASSERT_EQ(bufferStorage->getStorageSizeForQueue(i), oneBuffer);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffersInserted);
    for (size_t i = 0; i < buffersInserted; i++) {
        bufferStorage->trimBuffer(BufferSequenceNumber(i + 1, i));
        ASSERT_EQ(bufferStorage->getStorageSizeForQueue(i), emptyBuffer);
    }
}

/**
     * @brief test inserts buffers in different queues concurrently.
*/
TEST_F(BufferStorageTest, multithreadInsertionInBufferStorage) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    auto buffer = bufferManager->getUnpooledBuffer(16384);
    std::vector<std::thread> t;
    for (uint32_t i = 0; i < buffersInserted; i++) {
        t.emplace_back([bufferStorage, buffer, i]() {
            bufferStorage->insertBuffer(BufferSequenceNumber(i, i), buffer.value());
            ASSERT_EQ(bufferStorage->getStorageSizeForQueue(i), oneBuffer);
        });
    }
    for (auto& thread : t) {
        thread.join();
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffersInserted);
}

/**
     * @brief test inserts five buffers in different queues and deletes four of them concurrently.
*/
TEST_F(BufferStorageTest, multithreadTrimmingInBufferStorage) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    auto buffer = bufferManager->getUnpooledBuffer(16384);
    std::vector<std::thread> t;
    for (uint64_t i = 0; i < numberOfThreads; i++) {
        t.emplace_back([bufferStorage, buffer, i]() {
            for (uint64_t j = 0; j < buffersInserted; j++) {
                if (j && j % 4 == 0) {
                    bufferStorage->trimBuffer(BufferSequenceNumber(j, i));
                } else {
                    bufferStorage->insertBuffer(BufferSequenceNumber(j, i), buffer.value());
                }
            }
        });
    }
    for (auto& thread : t) {
        thread.join();
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), emptyBuffer);
}
}// namespace NES
