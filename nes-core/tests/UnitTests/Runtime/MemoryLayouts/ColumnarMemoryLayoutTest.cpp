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

#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutTupleBuffer.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>
#include <vector>

namespace NES::Runtime::MemoryLayouts {
class ColumnarMemoryLayoutTest : public testing::Test {
  public:
    BufferManagerPtr bufferManager;

    void SetUp() override {
        NESLogger::getInstance()->removeAllAppenders();
        NES::setupLogging("ColumnarMemoryLayoutTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ColumnarMemoryLayoutTest test class.");
        bufferManager = std::make_shared<BufferManager>(4096, 10);
    }
    static void TearDownTestCase() {
        std::cout << "Tear down ColumnarMemoryLayoutTest class." << std::endl;
        NESLogger::getInstance()->removeAllAppenders();
    }
};

TEST_F(ColumnarMemoryLayoutTest, columnLayoutCreateTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
}

TEST_F(ColumnarMemoryLayoutTest, columnLayoutMapCalcOffsetTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto bindedColumnLayout = columnLayout->bind(tupleBuffer);

    auto capacity = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();
    ASSERT_EQ(bindedColumnLayout->getCapacity(), capacity);
    ASSERT_EQ(bindedColumnLayout->getNumberOfRecords(), 0u);
    ASSERT_EQ(columnLayout->getFieldOffset(1, 2), capacity * 1 + capacity * 2 + 1 * 4);
    ASSERT_EQ(columnLayout->getFieldOffset(5, 1), capacity * 1 + 5 * 2);
    ASSERT_EQ(columnLayout->getFieldOffset(4, 0), capacity * 0 + 4);
}

TEST_F(ColumnarMemoryLayoutTest, columnLayoutPushRecordAndReadRecordTestOneRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    ColumnLayoutBufferPtr bindedColumnLayout =
        std::dynamic_pointer_cast<ColumnLayoutTupleBuffer>(columnLayout->bind(tupleBuffer));

    std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
    bindedColumnLayout->pushRecord<true>(writeRecord);

    std::tuple<uint8_t, uint16_t, uint32_t> readRecord = bindedColumnLayout->readRecord<true, uint8_t, uint16_t, uint32_t>(0);

    ASSERT_EQ(writeRecord, readRecord);
    ASSERT_EQ(bindedColumnLayout->getNumberOfRecords(), 1UL);
}

TEST_F(ColumnarMemoryLayoutTest, columnLayoutPushRecordAndReadRecordTestMultipleRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto bindedColumnLayout = std::dynamic_pointer_cast<ColumnLayoutTupleBuffer>(columnLayout->bind(tupleBuffer));

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedColumnLayout->pushRecord<true>(writeRecord);
    }

    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> readRecord = bindedColumnLayout->readRecord<true, uint8_t, uint16_t, uint32_t>(i);
        ASSERT_EQ(allTuples[i], readRecord);
    }

    ASSERT_EQ(bindedColumnLayout->getNumberOfRecords(), NUM_TUPLES);
}

TEST_F(ColumnarMemoryLayoutTest, columnLayoutLayoutFieldSimple) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    ColumnLayoutBufferPtr bindedColumnLayout =
        std::dynamic_pointer_cast<ColumnLayoutTupleBuffer>(columnLayout->bind(tupleBuffer));

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedColumnLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = ColumnLayoutField<uint8_t, true>::create(0, columnLayout, tupleBuffer);
    auto field1 = ColumnLayoutField<uint16_t, true>::create(1, columnLayout, tupleBuffer);
    auto field2 = ColumnLayoutField<uint32_t, true>::create(2, columnLayout, tupleBuffer);

    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }
}

TEST_F(ColumnarMemoryLayoutTest, columnLayoutLayoutFieldBoundaryCheck) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    ColumnLayoutBufferPtr bindedColumnLayout =
        std::dynamic_pointer_cast<ColumnLayoutTupleBuffer>(columnLayout->bind(tupleBuffer));

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedColumnLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = ColumnLayoutField<uint8_t, true>::create(0, columnLayout, tupleBuffer);
    auto field1 = ColumnLayoutField<uint16_t, true>::create(1, columnLayout, tupleBuffer);
    auto field2 = ColumnLayoutField<uint32_t, true>::create(2, columnLayout, tupleBuffer);
    ASSERT_THROW((ColumnLayoutField<uint8_t, true>::create(3, columnLayout, tupleBuffer)), NES::NesRuntimeException);
    ASSERT_THROW((ColumnLayoutField<uint16_t, true>::create(4, columnLayout, tupleBuffer)), NES::NesRuntimeException);
    ASSERT_THROW((ColumnLayoutField<uint32_t, true>::create(5, columnLayout, tupleBuffer)), NES::NesRuntimeException);

    size_t i = 0;
    for (; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }

    ASSERT_THROW(field0[i], NES::NesRuntimeException);
    ASSERT_THROW(field1[i], NES::NesRuntimeException);
    ASSERT_THROW(field2[i], NES::NesRuntimeException);

    ASSERT_THROW(field0[++i], NES::NesRuntimeException);
    ASSERT_THROW(field1[i], NES::NesRuntimeException);
    ASSERT_THROW(field2[i], NES::NesRuntimeException);
}

TEST_F(ColumnarMemoryLayoutTest, DISABLED_columnLayoutLayoutFieldBoundaryNoCheck) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    ColumnLayoutBufferPtr bindedColumnLayout =
        std::dynamic_pointer_cast<ColumnLayoutTupleBuffer>(columnLayout->bind(tupleBuffer));

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedColumnLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = ColumnLayoutField<uint8_t, false>::create(0, columnLayout, tupleBuffer);
    auto field1 = ColumnLayoutField<uint16_t, false>::create(1, columnLayout, tupleBuffer);
    auto field2 = ColumnLayoutField<uint32_t, false>::create(2, columnLayout, tupleBuffer);

    try {
        ColumnLayoutField<uint32_t, false>::create(3, columnLayout, tupleBuffer);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        ColumnLayoutField<uint32_t, false>::create(4, columnLayout, tupleBuffer);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        ColumnLayoutField<uint32_t, false>::create(5, columnLayout, tupleBuffer);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    size_t i = 0;
    for (; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }

    try {
        ((void) field0[i]);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        ((void) field2[i]);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        ((void) field2[i]);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        ((void) field0[++i]);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        ((void) field1[i]);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        ((void) field2[i]);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }
}

TEST_F(ColumnarMemoryLayoutTest, pushRecordTooManyRecordsColumnLayout) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    ColumnLayoutBufferPtr bindedColumnLayout =
        std::dynamic_pointer_cast<ColumnLayoutTupleBuffer>(columnLayout->bind(tupleBuffer));

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    size_t i = 0;
    for (; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        ASSERT_TRUE(bindedColumnLayout->pushRecord<true>(writeRecord));
    }

    for (; i < NUM_TUPLES + 10; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        ASSERT_FALSE(bindedColumnLayout->pushRecord<true>(writeRecord));
    }

    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> readRecord = bindedColumnLayout->readRecord<true, uint8_t, uint16_t, uint32_t>(i);
        ASSERT_EQ(allTuples[i], readRecord);
    }

    ASSERT_EQ(bindedColumnLayout->getNumberOfRecords(), NUM_TUPLES);
}

TEST_F(ColumnarMemoryLayoutTest, getFieldViaFieldNameColumnLayout) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    ColumnLayoutBufferPtr bindedColumnLayout =
        std::dynamic_pointer_cast<ColumnLayoutTupleBuffer>(columnLayout->bind(tupleBuffer));

    ASSERT_NO_THROW((ColumnLayoutField<uint8_t, true>::create("t1", columnLayout, tupleBuffer)));
    ASSERT_NO_THROW((ColumnLayoutField<uint16_t, true>::create("t2", columnLayout, tupleBuffer)));
    ASSERT_NO_THROW((ColumnLayoutField<uint32_t, true>::create("t3", columnLayout, tupleBuffer)));

    ASSERT_THROW((ColumnLayoutField<uint32_t, true>::create("t4", columnLayout, tupleBuffer)), NES::NesRuntimeException);
    ASSERT_THROW((ColumnLayoutField<uint32_t, true>::create("t5", columnLayout, tupleBuffer)), NES::NesRuntimeException);
    ASSERT_THROW((ColumnLayoutField<uint32_t, true>::create("t6", columnLayout, tupleBuffer)), NES::NesRuntimeException);
}

TEST_F(ColumnarMemoryLayoutTest, accessDynamicColumnBufferTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = DynamicTupleBuffer(columnLayout, tupleBuffer);
    uint32_t numberOfRecords = 10;
    for (uint32_t i = 0; i < numberOfRecords; i++) {
        auto record = buffer[i];
        record[0].write<uint8_t>(i);
        record[1].write<uint16_t>(i);
        record[2].write<uint32_t>(i);
    }

    for (uint32_t i = 0; i < numberOfRecords; i++) {
        auto record = buffer[i];
        ASSERT_EQ(record[0].read<uint8_t>(), i);
        ASSERT_EQ(record[1].read<uint16_t>(), i);
        ASSERT_EQ(record[2].read<uint32_t>(), i);
    }
}

}// namespace NES::Runtime::MemoryLayouts
