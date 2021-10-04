//
// Created by Anastasiia Kozar on 01.10.21.
//
#include <Runtime/BufferStorage.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
namespace NES {
const size_t buffers_inserted = 5;
const size_t empty_buffer = 0;
class BufferStorageTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::setupLogging("BufferStorageTest.log", NES::LOG_DEBUG); }
};

TEST_F(BufferStorageTest, bufferInsertionInBufferStorage) {
    auto bufferStorage = std::make_shared<BufferStorage>();
    auto bufferManager = std::make_shared<Runtime::BufferManager>(1024, 1);
    auto buffer = bufferManager->getUnpooledBuffer(16384);
    for (size_t i = 0; i < buffers_inserted; i++) {
        bufferStorage->insertBuffer(i, buffer.value());
        ASSERT_EQ(bufferStorage->getStorageSize(), i + 1);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffers_inserted);
}

TEST_F(BufferStorageTest, bufferDeletionFromBufferStorage) {
    auto bufferStorage = std::make_shared<BufferStorage>();
    auto bufferManager = std::make_shared<Runtime::BufferManager>(1024, 1);
    auto buffer = bufferManager->getUnpooledBuffer(16384);
    for (size_t i = 0; i < buffers_inserted; i++) {
        bufferStorage->insertBuffer(i, buffer.value());
        ASSERT_EQ(bufferStorage->getStorageSize(), i + 1);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), buffers_inserted);
    for (size_t i = buffers_inserted; i >= 0; i--) {
        bufferStorage->trimBuffer(i);
        ASSERT_EQ(bufferStorage->getStorageSize(), i);
    }
    ASSERT_EQ(bufferStorage->getStorageSize(), empty_buffer);
}

TEST_F(BufferStorageTest, emptyBufferCheck) {
    auto bufferStorage = std::make_shared<BufferStorage>();
    ASSERT_EQ(bufferStorage->trimBuffer(0), false);
}
}// namespace NES
