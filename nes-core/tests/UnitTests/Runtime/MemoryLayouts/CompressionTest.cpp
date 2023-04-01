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
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    int i = 0;
    for (; i < 3; i++) {
        bufferOrig[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        bufferOrig[i][0].write<uint8_t>(71);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(71);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed =
        CompressedDynamicTupleBuffer(rowLayout, tupleBuffer, CompressionAlgorithm::LZ4, CompressionMode::FULL_BUFFER);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Compressor::compress(bufferOrig, bufferCompressed);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompressor::decompress(bufferCompressed, bufferDecompressed);

    // evaluate
    // raw content
    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentDecompressed = reinterpret_cast<const char*>(bufferDecompressed.getBuffer().getBuffer());
    bool contentIsEqual = strcmp(contentOrig, contentDecompressed) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), bufferDecompressed[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, compressDecompressLZ4FullBufferColumnLayoutSingleColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    int i = 0;
    for (; i < 3; i++) {
        bufferOrig[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        bufferOrig[i][0].write<uint8_t>(71);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(72);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed =
        CompressedDynamicTupleBuffer(columnLayout, tupleBuffer, CompressionAlgorithm::LZ4, CompressionMode::FULL_BUFFER);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Compressor::compress(bufferOrig, bufferCompressed);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompressor::decompress(bufferCompressed, bufferDecompressed);

    // evaluate
    // raw content
    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentDecompressed = reinterpret_cast<const char*>(bufferDecompressed.getBuffer().getBuffer());
    bool contentIsEqual = strcmp(contentOrig, contentDecompressed) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), bufferDecompressed[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, compressDecompressLZ4FullBufferRowLayoutMultiColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    // first column: 6A2B2A
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(offset);
    }
    bufferOrig[6][0].write<uint8_t>(offset + 1);
    bufferOrig[7][0].write<uint8_t>(offset + 1);

    // second column: 6B4C
    for (int i = 0; i < 6; i++) {
        bufferOrig[i][1].write<uint8_t>(offset + 1);
    }
    for (int i = 6; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][1].write<uint8_t>(offset + 2);
    }

    // third column: 10D
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][2].write<uint8_t>(offset + 3);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed =
        CompressedDynamicTupleBuffer(rowLayout, tupleBuffer, CompressionAlgorithm::LZ4, CompressionMode::FULL_BUFFER);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Compressor::compress(bufferOrig, bufferCompressed);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompressor::decompress(bufferCompressed, bufferDecompressed);

    // evaluate
    // raw content
    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentDecompressed = reinterpret_cast<const char*>(bufferDecompressed.getBuffer().getBuffer());
    bool contentIsEqual = strcmp(contentOrig, contentDecompressed) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), bufferDecompressed[i][0].read<uint8_t>());
        ASSERT_EQ(bufferOrig[i][1].read<uint8_t>(), bufferDecompressed[i][1].read<uint8_t>());
        ASSERT_EQ(bufferOrig[i][2].read<uint8_t>(), bufferDecompressed[i][2].read<uint8_t>());
    }
}

TEST_F(CompressionTest, compressDecompressLZ4FullBufferColumnLayoutMultiColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    // first column: 6A2B2A
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(offset);
    }
    bufferOrig[6][0].write<uint8_t>(offset + 1);
    bufferOrig[7][0].write<uint8_t>(offset + 1);

    // second column: 6B4C
    for (int i = 0; i < 6; i++) {
        bufferOrig[i][1].write<uint8_t>(offset + 1);
    }
    for (int i = 6; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][1].write<uint8_t>(offset + 2);
    }

    // third column: 10D
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][2].write<uint8_t>(offset + 3);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed =
        CompressedDynamicTupleBuffer(columnLayout, tupleBuffer, CompressionAlgorithm::LZ4, CompressionMode::FULL_BUFFER);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Compressor::compress(bufferOrig, bufferCompressed);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompressor::decompress(bufferCompressed, bufferDecompressed);

    // evaluate
    // raw content
    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentDecompressed = reinterpret_cast<const char*>(bufferDecompressed.getBuffer().getBuffer());
    bool contentIsEqual = strcmp(contentOrig, contentDecompressed) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), bufferDecompressed[i][0].read<uint8_t>());
        ASSERT_EQ(bufferOrig[i][1].read<uint8_t>(), bufferDecompressed[i][1].read<uint8_t>());
        ASSERT_EQ(bufferOrig[i][2].read<uint8_t>(), bufferDecompressed[i][2].read<uint8_t>());
    }
}

// ===================================
// Column wise
// ===================================
TEST_F(CompressionTest, compressDecompressLZ4ColumnWiseRowLayoutSingleColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    int i = 0;
    for (; i < 3; i++) {
        bufferOrig[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        bufferOrig[i][0].write<uint8_t>(71);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(71);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed =
        CompressedDynamicTupleBuffer(rowLayout, tupleBuffer, CompressionAlgorithm::LZ4, CompressionMode::COLUMN_WISE);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    try {
        Compressor::compress(bufferOrig, bufferCompressed);
    } catch (Exceptions::RuntimeException const& err) {
        using ::testing::HasSubstr;
        EXPECT_THAT(err.what(), HasSubstr("Only ColumnLayout supported for row-wise compression"));
    } catch (...) {
        FAIL();
    }
}

TEST_F(CompressionTest, compressDecompressLZ4ColumnWiseColumnLayoutSingleColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    int i = 0;
    for (; i < 3; i++) {
        bufferOrig[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        bufferOrig[i][0].write<uint8_t>(71);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(72);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed =
        CompressedDynamicTupleBuffer(columnLayout, tupleBuffer, CompressionAlgorithm::LZ4, CompressionMode::COLUMN_WISE);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Compressor::compress(bufferOrig, bufferCompressed);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompressor::decompress(bufferCompressed, bufferDecompressed);

    // evaluate
    // raw content
    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentDecompressed = reinterpret_cast<const char*>(bufferDecompressed.getBuffer().getBuffer());
    bool contentIsEqual = strcmp(contentOrig, contentDecompressed) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), bufferDecompressed[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, compressDecompressLZ4ColumnWiseRowLayoutMultiColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    // first column: 6A2B2A
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(offset);
    }
    bufferOrig[6][0].write<uint8_t>(offset + 1);
    bufferOrig[7][0].write<uint8_t>(offset + 1);

    // second column: 6B4C
    for (int i = 0; i < 6; i++) {
        bufferOrig[i][1].write<uint8_t>(offset + 1);
    }
    for (int i = 6; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][1].write<uint8_t>(offset + 2);
    }

    // third column: 10D
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][2].write<uint8_t>(offset + 3);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed =
        CompressedDynamicTupleBuffer(rowLayout, tupleBuffer, CompressionAlgorithm::LZ4, CompressionMode::COLUMN_WISE);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    try {
        Compressor::compress(bufferOrig, bufferCompressed);
    } catch (Exceptions::RuntimeException const& err) {
        using ::testing::HasSubstr;
        EXPECT_THAT(err.what(), HasSubstr("Only ColumnLayout supported for row-wise compression"));
    } catch (...) {
        FAIL();
    }
}

TEST_F(CompressionTest, compressDecompressLZ4ColumnWiseColumnLayoutMultiColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    // first column: 6A2B2A
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(offset);
    }
    bufferOrig[6][0].write<uint8_t>(offset + 1);
    bufferOrig[7][0].write<uint8_t>(offset + 1);

    // second column: 6B4C
    for (int i = 0; i < 6; i++) {
        bufferOrig[i][1].write<uint8_t>(offset + 1);
    }
    for (int i = 6; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][1].write<uint8_t>(offset + 2);
    }

    // third column: 10D
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][2].write<uint8_t>(offset + 3);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed =
        CompressedDynamicTupleBuffer(columnLayout, tupleBuffer, CompressionAlgorithm::LZ4, CompressionMode::COLUMN_WISE);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Compressor::compress(bufferOrig, bufferCompressed);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed =
        CompressedDynamicTupleBuffer(columnLayout, tupleBuffer, CompressionAlgorithm::LZ4, CompressionMode::COLUMN_WISE);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompressor::decompress(bufferCompressed, bufferDecompressed);

    // evaluate field values
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), bufferDecompressed[i][0].read<uint8_t>());
        ASSERT_EQ(bufferOrig[i][1].read<uint8_t>(), bufferDecompressed[i][1].read<uint8_t>());
        ASSERT_EQ(bufferOrig[i][2].read<uint8_t>(), bufferDecompressed[i][2].read<uint8_t>());
    }
}
// ====================================================================================================
// RLE
// ====================================================================================================
/*
TEST_F(CompressionTest, compressDecompressRLEFullBufferRowLayoutSingleColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    int i = 0;
    for (; i < 3; i++) {
        bufferOrig[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        bufferOrig[i][0].write<uint8_t>(71);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(70);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer, CompressionAlgorithm::RLE);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Compressor::compress(bufferOrig, bufferCompressed);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompressor::decompress(bufferCompressed, bufferDecompressed);

    // evaluate
    // raw content
    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentDecompressed = reinterpret_cast<const char*>(bufferDecompressed.getBuffer().getBuffer());
    bool contentIsEqual = strcmp(contentOrig, contentDecompressed) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), bufferDecompressed[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, compressDecompressRLEFullBufferColumnLayoutSingleColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    int i = 0;
    for (; i < 3; i++) {
        bufferOrig[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        bufferOrig[i][0].write<uint8_t>(71);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(70);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer, CompressionAlgorithm::RLE);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Compressor::compress(bufferOrig, bufferCompressed);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompressor::decompress(bufferCompressed, bufferDecompressed);

    // evaluate
    // raw content
    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentDecompressed = reinterpret_cast<const char*>(bufferDecompressed.getBuffer().getBuffer());
    bool contentIsEqual = strcmp(contentOrig, contentDecompressed) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), bufferDecompressed[i][0].read<uint8_t>());
    }
}

TEST_F(CompressionTest, compressDecompressRLEFullBufferRowLayoutMultiColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", UINT8)->addField("t2", UINT8)->addField("t3", UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    int i = 0;
    for (; i < 3; i++) {
        bufferOrig[i][0].write<uint8_t>(offset);
        bufferOrig[i][1].write<uint8_t>(offset);
        bufferOrig[i][2].write<uint8_t>(offset);
    }
    for (; i < 7; i++) {
        bufferOrig[i][0].write<uint8_t>(offset + 1);
        bufferOrig[i][1].write<uint8_t>(offset + 1);
        bufferOrig[i][2].write<uint8_t>(offset + 1);
    }
    for (; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(offset + 2);
        bufferOrig[i][1].write<uint8_t>(offset + 2);
        bufferOrig[i][2].write<uint8_t>(offset + 2);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer, CompressionAlgorithm::RLE);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Compressor::compress(bufferOrig, bufferCompressed);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = CompressedDynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompressor::decompress(bufferCompressed, bufferDecompressed);

    // evaluate
    // raw content
    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentDecompressed = reinterpret_cast<const char*>(bufferDecompressed.getBuffer().getBuffer());
    bool contentIsEqual = strcmp(contentOrig, contentDecompressed) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (i = 0; i < 10; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), bufferDecompressed[i][0].read<uint8_t>());
        ASSERT_EQ(bufferOrig[i][1].read<uint8_t>(), bufferDecompressed[i][1].read<uint8_t>());
        ASSERT_EQ(bufferOrig[i][2].read<uint8_t>(), bufferDecompressed[i][2].read<uint8_t>());
    }
}

TEST_F(CompressionTest, compressDecompressRLEFullBufferColumnLayoutMultiColumnUint8) {
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", UINT8)->addField("t2", UINT8)->addField("t3", UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(offset);
        bufferOrig[i][1].write<uint8_t>(offset + 1);
        bufferOrig[i][2].write<uint8_t>(offset + 2);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer, CompressionAlgorithm::RLE);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Compressor::compress(bufferOrig, bufferCompressed);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = CompressedDynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompressor::decompress(bufferCompressed, bufferDecompressed);

    // evaluate
    // raw content
    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentDecompressed = reinterpret_cast<const char*>(bufferDecompressed.getBuffer().getBuffer());
    bool contentIsEqual = strcmp(contentOrig, contentDecompressed) == 0;
    ASSERT_TRUE(contentIsEqual);
    // field values
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(bufferOrig[i][0].read<uint8_t>(), bufferDecompressed[i][0].read<uint8_t>());
        ASSERT_EQ(bufferOrig[i][1].read<uint8_t>(), bufferDecompressed[i][1].read<uint8_t>());
        ASSERT_EQ(bufferOrig[i][2].read<uint8_t>(), bufferDecompressed[i][2].read<uint8_t>());
    }
}
*/
}// namespace NES::Runtime::MemoryLayouts
