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

// TODO: tests always have the same layout → refactor such that we simply have to set the compression method in the test (setup and validation somewhere else)
// ====================================================================================================
// LZ4
// ====================================================================================================
// ===================================
// Horizontal
// ===================================
TEST_F(CompressionTest, lz4RowLayoutHorizontaltSingleColumnUint8) {
    // compressDecompressLZ4FullBufferRowLayoutSingleColumnUint8
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
        buffer[i][0].write<uint8_t>(72);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::LZ4);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, lz4RowLayoutHorizontalMultiColumnUint8) {
    // compressDecompressLZ4FullBufferRowLayoutMultiColumnUint8
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    // first column: 6A2B2A
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(offset);
    }
    buffer[6][0].write<uint8_t>(offset + 1);
    buffer[7][0].write<uint8_t>(offset + 1);

    // second column: 6B4C
    for (int i = 0; i < 6; i++) {
        buffer[i][1].write<uint8_t>(offset + 1);
    }
    for (int i = 6; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][1].write<uint8_t>(offset + 2);
    }

    // third column: 10D
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][2].write<uint8_t>(offset + 3);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::LZ4);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, lz4ColumnLayoutHorizontalSingleColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    int i = 0;
    for (; i < 3; i++) {
        buffer[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        buffer[i][0].write<uint8_t>(71);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(72);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::LZ4, CompressionMode::HORIZONTAL);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, lz4ColumnLayoutHorizontalMultiColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    // first column: 6A2B2A
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(offset);
    }
    buffer[6][0].write<uint8_t>(offset + 1);
    buffer[7][0].write<uint8_t>(offset + 1);

    // second column: 6B4C
    for (int i = 0; i < 6; i++) {
        buffer[i][1].write<uint8_t>(offset + 1);
    }
    for (int i = 6; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][1].write<uint8_t>(offset + 2);
    }

    // third column: 10D
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][2].write<uint8_t>(offset + 3);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::LZ4, CompressionMode::HORIZONTAL);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

// ===================================
// Vertical
// ===================================
TEST_F(CompressionTest, lz4RowLayoutVertical) {
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

    // compress
    try {
        buffer.compress(CompressionAlgorithm::LZ4, CompressionMode::VERTICAL);
    } catch (Exceptions::RuntimeException const& err) {
        using ::testing::HasSubstr;
        EXPECT_THAT(err.what(), HasSubstr("Vertical compression cannot be performed on row layout."));
    } catch (...) {
        FAIL();
    }
}

TEST_F(CompressionTest, lz4ColumnLayoutVerticalSingleColumnUint8) {
    // compressDecompressLZ4FullBufferColumnLayoutSingleColumnUint8
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    int i = 0;
    for (; i < 3; i++) {
        buffer[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        buffer[i][0].write<uint8_t>(71);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(72);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::LZ4);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, lz4ColumnLayoutVerticalMultiColumnUint8) {
    // compressDecompressLZ4FullBufferColumnLayoutMultiColumnUint8
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    // first column: 6A2B2A
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(offset);
    }
    buffer[6][0].write<uint8_t>(offset + 1);
    buffer[7][0].write<uint8_t>(offset + 1);

    // second column: 6B4C
    for (int i = 0; i < 6; i++) {
        buffer[i][1].write<uint8_t>(offset + 1);
    }
    for (int i = 6; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][1].write<uint8_t>(offset + 2);
    }

    // third column: 10D
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][2].write<uint8_t>(offset + 3);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::LZ4);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

// ====================================================================================================
// Snappy
// ====================================================================================================
// ===================================
// Horizontal
// ===================================
TEST_F(CompressionTest, snappyRowLayoutHorizontalSingleColumnUint8) {
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
        buffer[i][0].write<uint8_t>(72);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::SNAPPY);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, snappyRowLayoutHorizontalMultiColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    // first column: 6A2B2A
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(offset);
    }
    buffer[6][0].write<uint8_t>(offset + 1);
    buffer[7][0].write<uint8_t>(offset + 1);

    // second column: 6B4C
    for (int i = 0; i < 6; i++) {
        buffer[i][1].write<uint8_t>(offset + 1);
    }
    for (int i = 6; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][1].write<uint8_t>(offset + 2);
    }

    // third column: 10D
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][2].write<uint8_t>(offset + 3);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::SNAPPY);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, snappyColumnLayoutHorizontalSingleColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    int i = 0;
    for (; i < 3; i++) {
        buffer[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        buffer[i][0].write<uint8_t>(71);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(72);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::SNAPPY, CompressionMode::HORIZONTAL);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, snappyColumnLayoutHorizontalMultiColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    // first column: 6A2B2A
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(offset);
    }
    buffer[6][0].write<uint8_t>(offset + 1);
    buffer[7][0].write<uint8_t>(offset + 1);

    // second column: 6B4C
    for (int i = 0; i < 6; i++) {
        buffer[i][1].write<uint8_t>(offset + 1);
    }
    for (int i = 6; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][1].write<uint8_t>(offset + 2);
    }

    // third column: 10D
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][2].write<uint8_t>(offset + 3);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::SNAPPY, CompressionMode::HORIZONTAL);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

// ===================================
// Vertical
// ===================================
TEST_F(CompressionTest, snappyRowLayoutVertical) {
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

    // compress
    try {
        buffer.compress(CompressionAlgorithm::SNAPPY, CompressionMode::VERTICAL);
    } catch (Exceptions::RuntimeException const& err) {
        using ::testing::HasSubstr;
        EXPECT_THAT(err.what(), HasSubstr("Vertical compression cannot be performed on row layout."));
    } catch (...) {
        FAIL();
    }
}

TEST_F(CompressionTest, snappyColumnLayoutVerticalSingleColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    int i = 0;
    for (; i < 3; i++) {
        buffer[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        buffer[i][0].write<uint8_t>(71);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(72);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::SNAPPY);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, snappyColumnLayoutVerticalMultiColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    // first column: 6A2B2A
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(offset);
    }
    buffer[6][0].write<uint8_t>(offset + 1);
    buffer[7][0].write<uint8_t>(offset + 1);

    // second column: 6B4C
    for (int i = 0; i < 6; i++) {
        buffer[i][1].write<uint8_t>(offset + 1);
    }
    for (int i = 6; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][1].write<uint8_t>(offset + 2);
    }

    // third column: 10D
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][2].write<uint8_t>(offset + 3);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::SNAPPY);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

// ====================================================================================================
// ====================================================================================================
// ====================================================================================================

// ====================================================================================================
// FSST
// ====================================================================================================
// ===================================
// Horizontal
// ===================================
TEST_F(CompressionTest, fsstRowLayoutHorizontalSingleColumnUint8) {
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
        buffer[i][0].write<uint8_t>(72);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::FSST);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, fsstRowLayoutHorizontalMultiColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    // first column: 6A2B2A
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(offset);
    }
    buffer[6][0].write<uint8_t>(offset + 1);
    buffer[7][0].write<uint8_t>(offset + 1);

    // second column: 6B4C
    for (int i = 0; i < 6; i++) {
        buffer[i][1].write<uint8_t>(offset + 1);
    }
    for (int i = 6; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][1].write<uint8_t>(offset + 2);
    }

    // third column: 10D
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][2].write<uint8_t>(offset + 3);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::FSST);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, fsstColumnLayoutHorizontalSingleColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    int i = 0;
    for (; i < 3; i++) {
        buffer[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        buffer[i][0].write<uint8_t>(71);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(72);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::FSST, CompressionMode::HORIZONTAL);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, fsstColumnLayoutHorizontalMultiColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    // first column: 6A2B2A
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(offset);
    }
    buffer[6][0].write<uint8_t>(offset + 1);
    buffer[7][0].write<uint8_t>(offset + 1);

    // second column: 6B4C
    for (int i = 0; i < 6; i++) {
        buffer[i][1].write<uint8_t>(offset + 1);
    }
    for (int i = 6; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][1].write<uint8_t>(offset + 2);
    }

    // third column: 10D
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][2].write<uint8_t>(offset + 3);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::FSST, CompressionMode::HORIZONTAL);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

// ===================================
// Vertical
// ===================================
TEST_F(CompressionTest, fsstRowLayoutVerticalSingleColumnUint8) {
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

    // compress
    try {
        buffer.compress(CompressionAlgorithm::FSST, CompressionMode::VERTICAL);
    } catch (Exceptions::RuntimeException const& err) {
        using ::testing::HasSubstr;
        EXPECT_THAT(err.what(), HasSubstr("Vertical compression cannot be performed on row layout."));
    } catch (...) {
        FAIL();
    }
}

TEST_F(CompressionTest, fsstColumnLayoutVerticalSingleColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    int i = 0;
    for (; i < 3; i++) {
        buffer[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        buffer[i][0].write<uint8_t>(71);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(72);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::FSST);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, fsstColumnLayoutVerticalMultiColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    buffer.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    // first column: 6A2B2A
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][0].write<uint8_t>(offset);
    }
    buffer[6][0].write<uint8_t>(offset + 1);
    buffer[7][0].write<uint8_t>(offset + 1);

    // second column: 6B4C
    for (int i = 0; i < 6; i++) {
        buffer[i][1].write<uint8_t>(offset + 1);
    }
    for (int i = 6; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][1].write<uint8_t>(offset + 2);
    }

    // third column: 10D
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        buffer[i][2].write<uint8_t>(offset + 3);
    }
    // copy for comparison
    auto bufferOrig = DynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    memcpy(bufferOrig.getBuffer().getBuffer(), buffer.getBuffer().getBuffer(), bufferOrig.getCapacity());

    // compress
    buffer.compress(CompressionAlgorithm::FSST);

    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    bool contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);

    //decompress
    buffer.decompress();

    // evaluate
    // raw content
    contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    contentCompressed = reinterpret_cast<const char*>(buffer.getBuffer().getBuffer());
    contentIsEqual = strncmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), buffer[i][0].read<uint8_t>());
    }
}

}// namespace NES::Runtime::MemoryLayouts
