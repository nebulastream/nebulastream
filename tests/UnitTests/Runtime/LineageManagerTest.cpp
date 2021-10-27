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
#include <Runtime/LineageManager.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <thread>

namespace NES {

const size_t buffersInserted = 21;
const size_t emptyBuffer = 0;
const size_t oneBuffer = 1;

class LineageManagerTest : public testing::Test {

  protected:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::setupLogging("LineageManagerTest.log", NES::LOG_DEBUG); }
};

/**
     * @brief test inserts one buffer into lineage manager and checks that it was successfully inserted
*/
TEST_F(LineageManagerTest, OneBufferInsertionInLineageManager) {
    auto lineageManager = std::make_shared<Runtime::LineageManager>();
    lineageManager->insertIntoLineage(BufferSequenceNumber(0, 0), BufferSequenceNumber(1, 1));
    ASSERT_EQ(lineageManager->getLineageSize(), oneBuffer);
}

/**
     * @brief test inserts buffers into lineage manager and checks after every insertion that the table
     * size increased on one.
*/
TEST_F(LineageManagerTest, bufferInsertionInLineageManager) {
    auto lineageManager = std::make_shared<Runtime::LineageManager>();
    for (size_t i = 0; i < buffersInserted; i++) {
        lineageManager->insertIntoLineage(BufferSequenceNumber(i, i), BufferSequenceNumber(i + 1, i + 1));
        ASSERT_EQ(lineageManager->getLineageSize(), i + 1);
    }
    ASSERT_EQ(lineageManager->getLineageSize(), buffersInserted);
}

/**
     * @brief test tries to delete from an empty lineage
*/
TEST_F(LineageManagerTest, deletionFromAnEmptyLineageManager) {
    auto lineageManager = std::make_shared<Runtime::LineageManager>();
    ASSERT_EQ(lineageManager->trimLineage(BufferSequenceNumber(0, 0)), false);
}

/**
     * @brief test trims buffers from a lineage manager and checks after every deletion that the table
     * size decreased on one.
*/
TEST_F(LineageManagerTest, bufferDeletionFromLineageManager) {
    auto lineageManager = std::make_shared<Runtime::LineageManager>();
    for (size_t i = 0; i < buffersInserted; i++) {
        lineageManager->insertIntoLineage(BufferSequenceNumber(i, i), BufferSequenceNumber(i + 1, i + 1));
        ASSERT_EQ(lineageManager->getLineageSize(), i + 1);
    }
    ASSERT_EQ(lineageManager->getLineageSize(), buffersInserted);
    for (size_t i = 0; i < buffersInserted; i++) {
        lineageManager->trimLineage(BufferSequenceNumber(i, i));
        ASSERT_EQ(lineageManager->getLineageSize(), buffersInserted - 1 - i);
    }
    ASSERT_EQ(lineageManager->getLineageSize(), emptyBuffer);
}

/**
     * @brief test inserts five buffers in different queues concurrently.
*/
TEST_F(LineageManagerTest, multithreadInsertionInLineage) {
    auto lineageManager = std::make_shared<Runtime::LineageManager>();
    std::vector<std::thread> t;
    for (uint32_t i = 0; i < buffersInserted; i++) {
        t.emplace_back([lineageManager, i]() {
            lineageManager->insertIntoLineage(BufferSequenceNumber(i, i), BufferSequenceNumber(i + 1, i + 1));
            ASSERT_EQ(lineageManager->getLineageSize(), i + 1);
        });
    }
    for (auto& thread : t) {
        thread.join();
    }
    ASSERT_EQ(lineageManager->getLineageSize(), buffersInserted);
}
}