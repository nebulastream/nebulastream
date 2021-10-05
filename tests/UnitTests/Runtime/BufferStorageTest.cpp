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

#include <Runtime/BufferStorage.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
namespace NES {
const size_t buffers_inserted = 5;
const size_t empty_buffer = 0;
const size_t expected_storage_size = 2;
class BufferStorageTest : public testing::Test {
  public:
    Runtime::BufferManagerPtr bufferManager;

  protected:
    virtual void SetUp() {
        bufferManager = std::make_shared<Runtime::BufferManager>(1024, 1);
    }
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::setupLogging("BufferStorageTest.log", NES::LOG_DEBUG); }
};

TEST_F(BufferStorageTest, bufferInsertionInBufferStorage) {
    auto bufferStorage = std::make_shared<BufferStorage>();
    Runtime::TupleBuffer buffer;
    for (size_t i = 0; i < buffers_inserted; i++) {
        bufferStorage->insertBuffer(i, buffer);
        ASSERT_EQ(bufferStorage->getStorageSize(), i + 1);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffers_inserted);
}

TEST_F(BufferStorageTest, bufferDeletionFromBufferStorage) {
    auto bufferStorage = std::make_shared<BufferStorage>();
    Runtime::TupleBuffer buffer;
    for (size_t i = 0; i < buffers_inserted; i++) {
        bufferStorage->insertBuffer(i, buffer);
        ASSERT_EQ(bufferStorage->getStorageSize(), i + 1);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffers_inserted);
    for (size_t i = 0; i <= buffers_inserted; i++) {
        bufferStorage->trimBuffer(i);
        ASSERT_EQ(bufferStorage->getStorageSize(), buffers_inserted - i);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), empty_buffer);
}

TEST_F(BufferStorageTest, smallerBufferDeletionFromBufferStorage) {
    auto bufferStorage = std::make_shared<BufferStorage>();
    Runtime::TupleBuffer buffer;
    for (size_t i = 0; i < buffers_inserted; i++) {
        bufferStorage->insertBuffer(i, buffer);
        ASSERT_EQ(bufferStorage->getStorageSize(), i + 1);
    }
    bufferStorage->trimBuffer(3);
    ASSERT_EQ(bufferStorage->getStorageSize(), expected_storage_size);
}

TEST_F(BufferStorageTest, emptyBufferCheck) {
    auto bufferStorage = std::make_shared<BufferStorage>();
    ASSERT_EQ(bufferStorage->trimBuffer(0), false);
}
}// namespace NES
