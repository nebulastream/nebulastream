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

#include "Runtime/MemoryLayout/Compression.hpp"
#include "API/Schema.hpp"
#include "Common/ExecutableType/Array.hpp"
#include "NesBaseTest.hpp"
#include "Runtime/BufferManager.hpp"
#include "Runtime/MemoryLayout/ColumnLayout.hpp"
#include "Runtime/MemoryLayout/ColumnLayoutField.hpp"
#include "Runtime/MemoryLayout/DynamicTupleBuffer.hpp"
#include "Runtime/MemoryLayout/MemoryLayout.hpp"
#include "Runtime/MemoryLayout/RowLayout.hpp"
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
    SchemaPtr schema = Schema::create()->addField("t1", UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = DynamicTupleBuffer(columnLayout, tupleBuffer);
    int i = 0;
    for (; i < 3; i++) {
        bufferOrig[i][0].write<uint8_t>(70);
    }
    for (; i < 6; i++) {
        bufferOrig[i][0].write<uint8_t>(71);
    }
    for (; i < 10; i++) {
        bufferOrig[i][0].write<uint8_t>(70);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed = DynamicTupleBuffer(columnLayout, tupleBuffer);
    Compress::Snappy(bufferOrig, bufferCompressed);

    /*
    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = DynamicTupleBuffer(columnLayout, tupleBuffer);
    Decompress::RLE(bufferCompressed, bufferDecompressed);

    // evaluate
    const char* contentOrig = reinterpret_cast<const char*>(bufferOrig.getBuffer().getBuffer());
    const char* contentDecompressed = reinterpret_cast<const char*>(bufferDecompressed.getBuffer().getBuffer());
    bool valuesAreEqual = strcmp(contentOrig, contentDecompressed) == 0;
    ASSERT_TRUE(valuesAreEqual);
    */
    ASSERT_TRUE(false);
}

// ====================================================================================================
// LZ4
// ====================================================================================================
TEST_F(CompressionTest, compressDecompressLZ4FullBufferRowLayoutSingleColumnUint8) {// works
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = DynamicTupleBuffer(rowLayout, tupleBuffer);
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
    auto bufferCompressed = DynamicTupleBuffer(rowLayout, tupleBuffer);
    // bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);// TODO?
    int compressedSize = Compress::LZ4(bufferOrig, bufferCompressed);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = DynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompress::LZ4(bufferCompressed, bufferDecompressed, compressedSize);

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

TEST_F(CompressionTest, compressDecompressLZ4FullBufferColumnLayoutSingleColumnUint8) {// works
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = DynamicTupleBuffer(columnLayout, tupleBuffer);
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
    auto bufferCompressed = DynamicTupleBuffer(columnLayout, tupleBuffer);
    // bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);// TODO?
    auto compressedSizes = Compress::LZ4(bufferOrig, bufferCompressed, offsets);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = DynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompress::LZ4(bufferCompressed, bufferDecompressed, offsets, compressedSizes);

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

TEST_F(CompressionTest, compressDecompressLZ4FullBufferRowLayoutMultiColumnUint8) {// works
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", UINT8)->addField("t2", UINT8)->addField("t3", UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = DynamicTupleBuffer(rowLayout, tupleBuffer);
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
    auto bufferCompressed = DynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);// TODO?
    int compressedSize = Compress::LZ4(bufferOrig, bufferCompressed);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = DynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompress::LZ4(bufferCompressed, bufferDecompressed, compressedSize);

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

TEST_F(CompressionTest, compressDecompressLZ4FullBufferColumnLayoutMultiColumnUint8) {// works
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", UINT8)->addField("t2", UINT8)->addField("t3", UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = DynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(offset);
        bufferOrig[i][1].write<uint8_t>(offset + 1);
        bufferOrig[i][2].write<uint8_t>(offset + 2);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed = DynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);// TODO?
    auto compressedSizes = Compress::LZ4(bufferOrig, bufferCompressed, offsets);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = DynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompress::LZ4(bufferCompressed, bufferDecompressed, offsets, compressedSizes);

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

// ====================================================================================================
// RLE
// ====================================================================================================
TEST_F(CompressionTest, compressDecompressRLEFullBufferRowLayoutSingleColumnUint8) {// works
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = DynamicTupleBuffer(rowLayout, tupleBuffer);
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
    auto bufferCompressed = DynamicTupleBuffer(rowLayout, tupleBuffer);
    // bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);// TODO?
    Compress::RLE(bufferOrig, bufferCompressed);
    //std::cout << bufferCompressed.toString(schema) << std::endl;

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = DynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompress::RLE(bufferCompressed, bufferDecompressed);

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

TEST_F(CompressionTest, compressDecompressRLEFullBufferColumnLayoutSingleColumnUint8) {// works
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    // generate data
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = DynamicTupleBuffer(columnLayout, tupleBuffer);
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
    auto bufferCompressed = DynamicTupleBuffer(columnLayout, tupleBuffer);
    // bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);// TODO?
    Compress::RLE(bufferOrig, bufferCompressed, offsets);
    //std::cout << bufferCompressed.toString(schema) << std::endl;

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = DynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompress::RLE(bufferCompressed, bufferDecompressed, offsets);

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

TEST_F(CompressionTest, compressDecompressRLEFullBufferRowLayoutMultiColumnUint8) {// works
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", UINT8)->addField("t2", UINT8)->addField("t3", UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = DynamicTupleBuffer(rowLayout, tupleBuffer);
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
    auto bufferCompressed = DynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);// TODO?
    Compress::RLE(bufferOrig, bufferCompressed);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = DynamicTupleBuffer(rowLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompress::RLE(bufferCompressed, bufferDecompressed);

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

TEST_F(CompressionTest, compressDecompressRLEFullBufferColumnLayoutMultiColumnUint8) {// works
    int NUMBER_OF_TUPLES_IN_BUFFER = 10;
    SchemaPtr schema = Schema::create()->addField("t1", UINT8)->addField("t2", UINT8)->addField("t3", UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
    auto offsets = columnLayout->getColumnOffsets();

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferOrig = DynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferOrig.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);

    const int offset = 65;// == 'A'
    for (int i = 0; i < NUMBER_OF_TUPLES_IN_BUFFER; i++) {
        bufferOrig[i][0].write<uint8_t>(offset);
        bufferOrig[i][1].write<uint8_t>(offset + 1);
        bufferOrig[i][2].write<uint8_t>(offset + 2);
    }

    // compress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferCompressed = DynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferCompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);// TODO?
    Compress::RLE(bufferOrig, bufferCompressed, offsets);

    // decompress
    tupleBuffer = bufferManager->getBufferBlocking();
    auto bufferDecompressed = DynamicTupleBuffer(columnLayout, tupleBuffer);
    bufferDecompressed.setNumberOfTuples(NUMBER_OF_TUPLES_IN_BUFFER);
    Decompress::RLE(bufferCompressed, bufferDecompressed, offsets);

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

}// namespace NES::Runtime::MemoryLayouts
