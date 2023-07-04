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

#include "API/AttributeField.hpp"
#include "API/Schema.hpp"
#include "Common/DataTypes/DataType.hpp"
#include "NesBaseTest.hpp"
#include "Runtime/BufferManager.hpp"
#include "Runtime/MemoryLayout/ColumnLayout.hpp"
#include "Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp"
#include "Runtime/MemoryLayout/RowLayout.hpp"
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <iostream>
#include <vector>

// ======================================================================
// NOTE: all tests succeed when executed individually
// TODO: fix bug when executing all tests at once
// ======================================================================

namespace NES::Runtime::MemoryLayouts {
class CompressionTest : public Testing::TestWithErrorHandling {
  public:
    BufferManagerPtr bufferManager;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("DynamicMemoryLayoutTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DynamicMemoryLayoutTest test class.");
    }
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        bufferManager = std::make_shared<BufferManager>(4096, 2);
    }

  protected:
    static void fillBufferSingleColumn(CompressedDynamicTupleBuffer& buffer);
    static void fillBufferMultiColumn(CompressedDynamicTupleBuffer& buffer);
    static void fillBufferMultiColumnRle(CompressedDynamicTupleBuffer& buffer);
    static void fillBuffer4Columns(CompressedDynamicTupleBuffer& buffer);
    void verifyCompressed(DynamicTupleBuffer& orig, DynamicTupleBuffer& compressed);
    void verifyDecompressed(DynamicTupleBuffer& orig, DynamicTupleBuffer& decompressed);
};

void CompressionTest::fillBufferSingleColumn(CompressedDynamicTupleBuffer& buffer) {
    const int numTuples = 1000;
    buffer.setNumberOfTuples(numTuples);
    const int character = 70;// == 'F'
    int i = 0;
    for (; i < 300; i++) {
        buffer[i][0].write<uint8_t>(character);
    }
    for (; i < 600; i++) {
        buffer[i][0].write<uint8_t>(0);
    }
    for (; i < numTuples; i++) {
        buffer[i][0].write<uint8_t>(character + 2);
    }
}
void CompressionTest::fillBufferMultiColumn(CompressedDynamicTupleBuffer& buffer) {
    const int numTuples = 500;
    buffer.setNumberOfTuples(numTuples);
    const int character = 65;// == 'A'
    // first column: 500A
    for (int i = 0; i < numTuples; i++) {
        buffer[i][0].write<uint8_t>(character);
    }
    // second column: 200B100'0'200C
    for (int i = 0; i < 200; i++) {
        buffer[i][1].write<uint8_t>(character + 1);
    }
    for (int i = 3; i < 300; i++) {
        buffer[i][1].write<uint8_t>(0);
    }
    for (int i = 7; i < numTuples; i++) {
        buffer[i][1].write<uint8_t>(character + 2);
    }
    // third column: 500A
    for (int i = 0; i < numTuples; i++) {
        buffer[i][2].write<uint8_t>(character);
    }
}

void CompressionTest::fillBufferMultiColumnRle(CompressedDynamicTupleBuffer& buffer) {
    const size_t numTuples = 1000;
    buffer.setNumberOfTuples(numTuples);
    for (size_t col = 0; col < buffer.getOffsets().size(); col++) {
        size_t row = 0;
        while (row < numTuples) {
            if ((row % 4) == 0)
                buffer[row][col].write<uint8_t>(0xff);
            else
                buffer[row][col].write<uint8_t>(0x00);
            row++;
        }
    }
}

void CompressionTest::fillBuffer4Columns(CompressedDynamicTupleBuffer& buffer) {
    const int numTuples = 500;
    buffer.setNumberOfTuples(numTuples);
    const int character = 65;// == 'A'
    // first column: 500A
    for (int i = 0; i < numTuples; i++) {
        buffer[i][0].write<uint8_t>(character);
    }
    // second column: 500B
    for (int i = 0; i < numTuples; i++) {
        buffer[i][1].write<uint8_t>(character + 1);
    }
    // third column: 500C
    for (int i = 0; i < numTuples; i++) {
        buffer[i][2].write<uint8_t>(character + 2);
    }
    // fourth column: 500D
    for (int i = 0; i < numTuples; i++) {
        buffer[i][3].write<uint8_t>(character + 3);
    }
}

void CompressionTest::verifyCompressed(DynamicTupleBuffer& orig, DynamicTupleBuffer& compressed) {
    const char* contentOrig = reinterpret_cast<const char*>(orig.getBuffer().getBuffer());
    const char* contentCompressed = reinterpret_cast<const char*>(compressed.getBuffer().getBuffer());
    bool contentIsEqual = memcmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_FALSE(contentIsEqual);
}
void CompressionTest::verifyDecompressed(DynamicTupleBuffer& orig, DynamicTupleBuffer& decompressed) {
    auto contentOrig = reinterpret_cast<const char*>(orig.getBuffer().getBuffer());
    auto contentCompressed = reinterpret_cast<const char*>(decompressed.getBuffer().getBuffer());
    bool contentIsEqual = memcmp(contentOrig, contentCompressed, bufferManager->getBufferSize()) == 0;
    ASSERT_TRUE(contentIsEqual);
}

// ====================================================================================================
// No compression
// ====================================================================================================
// ===================================
// Horizontal
// ===================================
TEST_F(CompressionTest, noneRowLayoutHorizontaltSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::NONE);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, noneRowLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());

    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::NONE);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, noneColumnLayoutHorizontalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::NONE);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, noneColumnLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::NONE);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ===================================
// Vertical
// ===================================
TEST_F(CompressionTest, noneRowLayoutVertical) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    try {
        buffer.compress(CompressionMode::VERTICAL, CompressionAlgorithm::NONE);
    } catch (Exceptions::RuntimeException const& err) {
        using ::testing::HasSubstr;
        EXPECT_THAT(err.what(), HasSubstr("Vertical compression cannot be performed on row layout."));
    } catch (...) {
        FAIL();
    }
}

TEST_F(CompressionTest, noneColumnLayoutVerticalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::NONE);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, noneColumnLayoutVerticalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress({CompressionAlgorithm::NONE, CompressionAlgorithm::NONE, CompressionAlgorithm::NONE});

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ====================================================================================================
// LZ4
// ====================================================================================================
// ===================================
// Horizontal
// ===================================
TEST_F(CompressionTest, lz4RowLayoutHorizontaltSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::LZ4);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, lz4RowLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());

    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::LZ4);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, lz4ColumnLayoutHorizontalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::LZ4);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, lz4ColumnLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::LZ4);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ===================================
// Vertical
// ===================================
TEST_F(CompressionTest, lz4RowLayoutVertical) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    try {
        buffer.compress(CompressionMode::VERTICAL, CompressionAlgorithm::LZ4);
    } catch (Exceptions::RuntimeException const& err) {
        using ::testing::HasSubstr;
        EXPECT_THAT(err.what(), HasSubstr("Vertical compression cannot be performed on row layout."));
    } catch (...) {
        FAIL();
    }
}

TEST_F(CompressionTest, lz4ColumnLayoutVerticalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::LZ4);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, lz4ColumnLayoutVerticalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress({CompressionAlgorithm::LZ4, CompressionAlgorithm::LZ4, CompressionAlgorithm::LZ4});
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ====================================================================================================
// Snappy
// ====================================================================================================
// ===================================
// Horizontal
// ===================================
TEST_F(CompressionTest, snappyRowLayoutHorizontalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::SNAPPY);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, snappyRowLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::SNAPPY);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, snappyColumnLayoutHorizontalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::SNAPPY);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, snappyColumnLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::SNAPPY);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ===================================
// Vertical
// ===================================
TEST_F(CompressionTest, snappyRowLayoutVertical) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    try {
        buffer.compress(CompressionMode::VERTICAL, CompressionAlgorithm::SNAPPY);
    } catch (Exceptions::RuntimeException const& err) {
        using ::testing::HasSubstr;
        EXPECT_THAT(err.what(), HasSubstr("Vertical compression cannot be performed on row layout."));
    } catch (...) {
        FAIL();
    }
}

TEST_F(CompressionTest, snappyColumnLayoutVerticalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::SNAPPY);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, snappyColumnLayoutVerticalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress({CompressionAlgorithm::SNAPPY, CompressionAlgorithm::SNAPPY, CompressionAlgorithm::SNAPPY});
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ====================================================================================================
// rle
// ====================================================================================================
// ===================================
// Horizontal
// ===================================
TEST_F(CompressionTest, rleRowLayoutHorizontalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::RLE);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, rleRowLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumnRle(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumnRle(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::RLE);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, rleColumnLayoutHorizontalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::RLE);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, rleColumnLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::RLE);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ===================================
// Vertical
// ===================================
TEST_F(CompressionTest, rleRowLayoutVertical) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    try {
        buffer.compress(CompressionMode::VERTICAL, CompressionAlgorithm::RLE);
    } catch (Exceptions::RuntimeException const& err) {
        using ::testing::HasSubstr;
        EXPECT_THAT(err.what(), HasSubstr("Vertical compression cannot be performed on row layout."));
    } catch (...) {
        FAIL();
    }
}

TEST_F(CompressionTest, rleColumnLayoutVerticalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::RLE);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, rleColumnLayoutVerticalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress({CompressionAlgorithm::RLE, CompressionAlgorithm::RLE, CompressionAlgorithm::RLE});
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ====================================================================================================
// BINARY_RLE
// ====================================================================================================
// ===================================
// Horizontal
// ===================================
TEST_F(CompressionTest, binaryRleRowLayoutHorizontalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::BINARY_RLE);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, binaryRleRowLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumnRle(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumnRle(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::BINARY_RLE);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, binaryRleColumnLayoutHorizontalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::BINARY_RLE);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, binaryRleColumnLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumnRle(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumnRle(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::BINARY_RLE);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ===================================
// Vertical
// ===================================
TEST_F(CompressionTest, binaryRleRowLayoutVertical) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    try {
        buffer.compress(CompressionMode::VERTICAL, CompressionAlgorithm::BINARY_RLE);
    } catch (Exceptions::RuntimeException const& err) {
        using ::testing::HasSubstr;
        EXPECT_THAT(err.what(), HasSubstr("Vertical compression cannot be performed on row layout."));
    } catch (...) {
        FAIL();
    }
}

TEST_F(CompressionTest, binaryRleColumnLayoutVerticalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::BINARY_RLE);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, binaryRleColumnLayoutVerticalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumnRle(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumnRle(buffer);

    // compress
    buffer.compress({CompressionAlgorithm::BINARY_RLE, CompressionAlgorithm::BINARY_RLE, CompressionAlgorithm::BINARY_RLE});
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ====================================================================================================
// FSST
// ====================================================================================================
// ===================================
// Horizontal
// ===================================
TEST_F(CompressionTest, fsstRowLayoutHorizontalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::FSST);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, fsstRowLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::FSST);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, fsstColumnLayoutHorizontalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::FSST);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, fsstColumnLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::FSST);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ===================================
// Vertical
// ===================================
TEST_F(CompressionTest, fsstRowLayoutVertical) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    try {
        buffer.compress(CompressionMode::VERTICAL, CompressionAlgorithm::FSST);
    } catch (Exceptions::RuntimeException const& err) {
        using ::testing::HasSubstr;
        EXPECT_THAT(err.what(), HasSubstr("Vertical compression cannot be performed on row layout."));
    } catch (...) {
        FAIL();
    }
}

TEST_F(CompressionTest, fsstColumnLayoutVerticalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::FSST);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, fsstColumnLayoutVerticalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress({CompressionAlgorithm::FSST, CompressionAlgorithm::FSST, CompressionAlgorithm::FSST});
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ====================================================================================================
// Sprintz
// ====================================================================================================
// ===================================
// Horizontal
// ===================================
TEST_F(CompressionTest, sprintzRowLayoutHorizontalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::SPRINTZ);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, sprintzRowLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::SPRINTZ);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, sprintzColumnLayoutHorizontalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::SPRINTZ);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, sprintzColumnLayoutHorizontalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress(CompressionMode::HORIZONTAL, CompressionAlgorithm::SPRINTZ);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ===================================
// Vertical
// ===================================
TEST_F(CompressionTest, sprintzRowLayoutVerticalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    try {
        buffer.compress(CompressionMode::VERTICAL, CompressionAlgorithm::SPRINTZ);
    } catch (Exceptions::RuntimeException const& err) {
        using ::testing::HasSubstr;
        EXPECT_THAT(err.what(), HasSubstr("Vertical compression cannot be performed on row layout."));
    } catch (...) {
        FAIL();
    }
}

TEST_F(CompressionTest, sprintzColumnLayoutVerticalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    buffer.compress(CompressionAlgorithm::SPRINTZ);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, sprintzColumnLayoutVerticalMultiColumnUint8) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

    // compress
    buffer.compress({CompressionAlgorithm::SPRINTZ, CompressionAlgorithm::SPRINTZ, CompressionAlgorithm::SPRINTZ});
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ===================================
// Mixed
// ===================================
TEST_F(CompressionTest, mixedColumnLayoutVerticalMultiColumnUint8) {
    SchemaPtr schema = Schema::create()
                           ->addField("t1", BasicType::UINT8)
                           ->addField("t2", BasicType::UINT8)
                           ->addField("t3", BasicType::UINT8)
                           ->addField("t4", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBuffer4Columns(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBuffer4Columns(buffer);

    // compress
    buffer.compress(
        {CompressionAlgorithm::LZ4, CompressionAlgorithm::SNAPPY, CompressionAlgorithm::NONE, CompressionAlgorithm::SPRINTZ});
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

TEST_F(CompressionTest, multiFsstSprintzColumnLayoutVerticalMultiColumnUint8) {
    SchemaPtr schema = Schema::create()
                           ->addField("t1", BasicType::UINT8)
                           ->addField("t2", BasicType::UINT8)
                           ->addField("t3", BasicType::UINT8)
                           ->addField("t4", BasicType::UINT8);
    ColumnLayoutPtr columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    auto bufferOrig = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBuffer4Columns(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(columnLayout, bufferManager->getBufferBlocking());
    fillBuffer4Columns(buffer);

    // compress
    buffer.compress(
        {CompressionAlgorithm::NONE, CompressionAlgorithm::FSST, CompressionAlgorithm::NONE, CompressionAlgorithm::FSST});
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

}// namespace NES::Runtime::MemoryLayouts
