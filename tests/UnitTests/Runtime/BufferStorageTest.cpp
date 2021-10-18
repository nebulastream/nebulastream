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
const size_t buffersInserted = 5;
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
     * @brief test inserts five buffers to different queues and checks after every insertion that queue
     * size increased on one. After the insertion is fully done the site of the buffer storage is checked to be five
*/
TEST_F(BufferStorageTest, bufferInsertionInBufferStorage) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    auto buffer = bufferManager->getUnpooledBuffer(16384);
    for (size_t i = 0; i < buffersInserted; i++) {
        bufferStorage->insertBuffer(BufferSequenceNumber(i, i), buffer.value());
        ASSERT_EQ(bufferStorage->getStorageSize(), i + 1);
        ASSERT_EQ(bufferStorage->getQueueSize(i), oneBuffer);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffersInserted);
}

/**
     * @brief test inserts five buffers into one queue but starts from the biggest sequence number.
     * The queue is then checked to be sorted to be exact to have biggest elements at the top.
*/
TEST_F(BufferStorageTest, sortedInsertionInBufferStorage) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    auto buffer = bufferManager->getUnpooledBuffer(16384);
    for (int i = buffersInserted - 1; i >= 0; i--) {
        bufferStorage->insertBuffer(BufferSequenceNumber(i, 0), buffer.value());
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffersInserted);
    for (uint64_t i = 0; i < buffersInserted - 1; i++) {
        bufferStorage->trimBuffer(BufferSequenceNumber(i + 1, 0));
        ASSERT_EQ(bufferStorage->getTopElementFromQueue(0).getSequenceNumber().getSequenceNumber(), i + 1);
    }
}

/**
     * @brief test checks that if trimming is called on an empty buffer it doesn't cause an error
*/
TEST_F(BufferStorageTest, emptyBufferCheck) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    ASSERT_EQ(bufferStorage->trimBuffer(BufferSequenceNumber(0, 0)), false);
}

/**
     * @brief test inserts one buffer and deletes it
*/
TEST_F(BufferStorageTest, oneBufferDeletionFromBufferStorage) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    auto buffer = bufferManager->getUnpooledBuffer(16384);
    bufferStorage->insertBuffer(BufferSequenceNumber(0, 0), buffer.value());
    ASSERT_EQ(bufferStorage->getStorageSize(), oneBuffer);
    ASSERT_EQ(bufferStorage->getQueueSize(0), oneBuffer);
    bufferStorage->trimBuffer(BufferSequenceNumber(1, 0));
    ASSERT_EQ(bufferStorage->getQueueSize(0), emptyBuffer);
}

/**
     * @brief test inserts five buffers in different queues and deletes them.
*/
TEST_F(BufferStorageTest, manyBufferDeletionFromBufferStorage) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    for (size_t i = 0; i < buffersInserted; i++) {
        auto buffer = bufferManager->getUnpooledBuffer(16384);
        bufferStorage->insertBuffer(BufferSequenceNumber(i, i), buffer.value());
        ASSERT_EQ(bufferStorage->getQueueSize(i), oneBuffer);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffersInserted);
    for (size_t i = 0; i < buffersInserted; i++) {
        bufferStorage->trimBuffer(BufferSequenceNumber(i + 1, i));
        ASSERT_EQ(bufferStorage->getQueueSize(i), emptyBuffer);
    }
}

/**
     * @brief test inserts five buffers in one queue and deletes three of them. The test checks that
     * the deleted buffers are smaller that passed id.
*/
TEST_F(BufferStorageTest, smallerBufferDeletionFromBufferStorage) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    auto buffer = bufferManager->getUnpooledBuffer(16384);
    for (size_t i = 0; i < buffersInserted; i++) {
        bufferStorage->insertBuffer(BufferSequenceNumber(i, 0), buffer.value());
        ASSERT_EQ(bufferStorage->getQueueSize(0), i + 1);
    }
    bufferStorage->trimBuffer(BufferSequenceNumber(3, 0));
    ASSERT_EQ(bufferStorage->getQueueSize(0), expectedStorageSize);
}

/**
     * @brief test inserts five buffers in different queues concurrently.
*/
TEST_F(BufferStorageTest, multithreadInsertionInBufferStorage) {
    auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
    auto buffer = bufferManager->getUnpooledBuffer(16384);
    std::vector<std::thread> t;
    for (uint32_t i = 0; i < numberOfThreads; i++) {
        t.emplace_back([bufferStorage, buffer, i](){
            bufferStorage->insertBuffer(BufferSequenceNumber(i, i), buffer.value());
            ASSERT_EQ(bufferStorage->getQueueSize(i), oneBuffer);
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
    for (uint32_t i = 0; i < numberOfThreads; i++) {
        t.emplace_back([bufferStorage, buffer, i](){
            bufferStorage->insertBuffer(BufferSequenceNumber(i, i), buffer.value());
            ASSERT_EQ(bufferStorage->getQueueSize(i), oneBuffer);
        });
    }
    for (uint32_t i = 0; i < numberOfThreads - 1; i++) {
        t.emplace_back([bufferStorage, buffer, i](){
            bufferStorage->trimBuffer(BufferSequenceNumber(i + 1, i));
        });
    }
    for (auto& thread : t) {
        thread.join();
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), oneBuffer);
}
}// namespace NES
