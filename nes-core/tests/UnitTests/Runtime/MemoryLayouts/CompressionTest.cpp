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
// NOTE: tests work when executed individually
// TODO: fix bug when executing all tests at once
// ======================================================================

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

  protected:
    void fillBufferSingleColumn(CompressedDynamicTupleBuffer& buffer);
    void fillBufferMultiColumn(CompressedDynamicTupleBuffer& buffer);
    void verifyCompressed(DynamicTupleBuffer& orig, DynamicTupleBuffer& compressed);
    void verifyDecompressed(DynamicTupleBuffer& orig, DynamicTupleBuffer& decompressed);
};

void CompressionTest::fillBufferSingleColumn(CompressedDynamicTupleBuffer& buffer) {
    const int numTuples = 100;
    buffer.setNumberOfTuples(numTuples);
    const int character = 70;// == 'F'
    int i = 0;
    for (; i < 30; i++) {
        buffer[i][0].write<uint8_t>(character);
    }
    for (; i < 60; i++) {
        buffer[i][0].write<uint8_t>(0);
    }
    for (; i < numTuples; i++) {
        buffer[i][0].write<uint8_t>(character + 2);
    }
}
void CompressionTest::fillBufferMultiColumn(CompressedDynamicTupleBuffer& buffer) {
    const int numTuples = 120;
    buffer.setNumberOfTuples(numTuples);
    const int character = 65;// == 'A'
    // first column: 10A
    for (int i = 0; i < numTuples; i++) {
        buffer[i][2].write<uint8_t>(character);
    }
    // second column: 30B40'0'50C
    for (int i = 0; i < 30; i++) {
        buffer[i][1].write<uint8_t>(character + 1);
    }
    for (int i = 3; i < 70; i++) {
        buffer[i][1].write<uint8_t>(0);
    }
    for (int i = 7; i < numTuples; i++) {
        buffer[i][1].write<uint8_t>(character + 2);
    }
    // third column: 10A
    for (int i = 0; i < numTuples; i++) {
        buffer[i][2].write<uint8_t>(character);
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
    for (size_t i = 0; i < orig.getNumberOfTuples(); i++) {
        ASSERT_EQ(orig[i][0].read<uint8_t>(), decompressed[i][0].read<uint8_t>());
    }
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
    buffer.compress(CompressionAlgorithm::LZ4, CompressionMode::HORIZONTAL);
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
    buffer.compress(CompressionAlgorithm::LZ4, CompressionMode::HORIZONTAL);
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
        buffer.compress(CompressionAlgorithm::LZ4, CompressionMode::VERTICAL);
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
    buffer.compress(CompressionAlgorithm::LZ4);
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
    buffer.compress(CompressionAlgorithm::SNAPPY, CompressionMode::HORIZONTAL);
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
    buffer.compress(CompressionAlgorithm::SNAPPY, CompressionMode::HORIZONTAL);
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
        buffer.compress(CompressionAlgorithm::SNAPPY, CompressionMode::VERTICAL);
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
    buffer.compress(CompressionAlgorithm::SNAPPY);
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
    buffer.compress(CompressionAlgorithm::FSST, CompressionMode::HORIZONTAL);
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
    buffer.compress(CompressionAlgorithm::FSST, CompressionMode::HORIZONTAL);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ===================================
// Vertical
// ===================================
TEST_F(CompressionTest, fsstRowLayoutVerticalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

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
    buffer.compress(CompressionAlgorithm::FSST);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ====================================================================================================
// RLE
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
    fillBufferMultiColumn(bufferOrig);
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferMultiColumn(buffer);

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
    buffer.compress(CompressionAlgorithm::RLE, CompressionMode::HORIZONTAL);
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
    buffer.compress(CompressionAlgorithm::RLE, CompressionMode::HORIZONTAL);
    verifyCompressed(bufferOrig, buffer);

    //decompress
    buffer.decompress();
    verifyDecompressed(bufferOrig, buffer);
}

// ===================================
// Vertical
// ===================================
TEST_F(CompressionTest, rleRowLayoutVerticalSingleColumnUint8) {
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8);
    RowLayoutPtr rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());
    auto buffer = CompressedDynamicTupleBuffer(rowLayout, bufferManager->getBufferBlocking());
    fillBufferSingleColumn(buffer);

    // compress
    try {
        buffer.compress(CompressionAlgorithm::RLE, CompressionMode::VERTICAL);
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
    buffer.compress(CompressionAlgorithm::RLE);
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
    buffer.compress(CompressionAlgorithm::SPRINTZ);

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

// ===================================
// Vertical
// ===================================
TEST_F(CompressionTest, sprintzRowLayoutVerticalSingleColumnUint8) {
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
        buffer.compress(CompressionAlgorithm::SPRINTZ, CompressionMode::VERTICAL);
    } catch (Exceptions::RuntimeException const& err) {
        using ::testing::HasSubstr;
        EXPECT_THAT(err.what(), HasSubstr("Vertical compression cannot be performed on row layout."));
    } catch (...) {
        FAIL();
    }
}
}// namespace NES::Runtime::MemoryLayouts
