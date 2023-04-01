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

#include "API/Schema.hpp"
#include "NesBaseTest.hpp"
#include "Runtime/BufferManager.hpp"
#include "Runtime/MemoryLayout/ColumnLayout.hpp"
#include "Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp"
#include "Runtime/MemoryLayout/MemoryLayout.hpp"
#include "Runtime/MemoryLayout/RowLayout.hpp"
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <iostream>
#include <lz4.h>
#include <snappy.h>
#include <vector>

namespace NES::Runtime::MemoryLayouts {
class CompressionTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    BufferManagerPtr bufferManager;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("DynamicMemoryLayoutTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DynamicMemoryLayoutTest test class.");
    }
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        bufferManager = std::make_shared<BufferManager>(4096, 10);
    }
};

// ====================================================================================================
// Snappy
// ====================================================================================================
TEST_F(CompressionTest, compressDecompressSnappyFullBufferColumnLayout) {// TODO
    GTEST_SKIP();
}

// ====================================================================================================
// compressLz4
// ====================================================================================================
// ===================================
// Full Buffer
// ===================================
TEST_F(CompressionTest, compressDecompressLZ4FullBufferRowLayoutSingleColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    int i = 0;
    for (; i < 3; i++) {
        buffer[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        buffer[i][0].write<uint8_t>(71);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(71);
    }
    // TODO deep copy
    //auto bufferOrig = buffer;
    auto bufferOrig = CompressedDynamicTupleBuffer(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::LZ4);
    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strcmp(contentOrig, contentCompressed) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strcmp(contentOrig, contentCompressed) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

}// namespace NES::Runtime::MemoryLayouts
