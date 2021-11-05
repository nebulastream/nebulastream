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

#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <QueryCompiler/GeneratableTypes/Array.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutTupleBuffer.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>

#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>
#include <vector>

namespace NES::Runtime::MemoryLayouts {
class DynamicMemoryLayoutTest : public testing::Test {
  public:
    BufferManagerPtr bufferManager;

    void SetUp() override {
        NESLogger::getInstance()->removeAllAppenders();
        NES::setupLogging("DynamicMemoryLayoutTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup DynamicMemoryLayoutTest test class.");
        bufferManager = std::make_shared<BufferManager>(4096, 10);
    }
    static void TearDownTestCase() {
        std::cout << "Tear down DynamicMemoryLayoutTest class." << std::endl;
        NESLogger::getInstance()->removeAllAppenders();
    }
};

TEST_F(DynamicMemoryLayoutTest, rowLayoutCreateTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, true));
    ASSERT_EQ(rowLayout->isCheckBoundaryFieldChecks(), true);
    ASSERT_NE(rowLayout, nullptr);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutCreateTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, true));
    ASSERT_EQ(columnLayout->isCheckBoundaryFieldChecks(), true);
    ASSERT_NE(columnLayout, nullptr);
}

TEST_F(DynamicMemoryLayoutTest, rowLayoutMapCalcOffsetTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto bindedRowLayout = rowLayout->bind(tupleBuffer);

    ASSERT_EQ(bindedRowLayout->getCapacity(), tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());
    ASSERT_EQ(bindedRowLayout->getNumberOfRecords(), 0u);
    ASSERT_EQ(bindedRowLayout->calcOffset(1, 2, true), schema->getSchemaSizeInBytes() * 1 + (1 + 2));
    ASSERT_EQ(bindedRowLayout->calcOffset(4, 0, true), schema->getSchemaSizeInBytes() * 4 + 0);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutMapCalcOffsetTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto bindedColumnLayout = columnLayout->bind(tupleBuffer);

    auto capacity = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();
    ASSERT_EQ(bindedColumnLayout->getCapacity(), capacity);
    ASSERT_EQ(bindedColumnLayout->getNumberOfRecords(), 0u);
    ASSERT_EQ(bindedColumnLayout->calcOffset(1, 2, true), capacity * 1 + capacity * 2 + 1 * 4);
    ASSERT_EQ(bindedColumnLayout->calcOffset(5, 1, true), capacity * 1 + 5 * 2);
    ASSERT_EQ(bindedColumnLayout->calcOffset(4, 0, true), capacity * 0 + 4);
}

TEST_F(DynamicMemoryLayoutTest, rowLayoutPushRecordAndReadRecordTestOneRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto bindedRowLayout = std::dynamic_pointer_cast<RowLayoutTupleBuffer>(rowLayout->bind(tupleBuffer));

    std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(1, 2, 3);
    bindedRowLayout->pushRecord<true>(writeRecord);

    std::tuple<uint8_t, uint16_t, uint32_t> readRecord = bindedRowLayout->readRecord<true, uint8_t, uint16_t, uint32_t>(0);

    ASSERT_EQ(writeRecord, readRecord);
    ASSERT_EQ(bindedRowLayout->getNumberOfRecords(), 1UL);
}

TEST_F(DynamicMemoryLayoutTest, rowLayoutPushRecordAndReadRecordTestMultipleRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = std::dynamic_pointer_cast<RowLayoutTupleBuffer>(rowLayout->bind(tupleBuffer));

    size_t NUM_TUPLES = 230;//tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedRowLayout->pushRecord<true>(writeRecord);
    }

    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> readRecord = bindedRowLayout->readRecord<true, uint8_t, uint16_t, uint32_t>(i);
        ASSERT_EQ(allTuples[i], readRecord);
    }

    ASSERT_EQ(bindedRowLayout->getNumberOfRecords(), NUM_TUPLES);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutPushRecordAndReadRecordTestOneRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, true));
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

TEST_F(DynamicMemoryLayoutTest, columnLayoutPushRecordAndReadRecordTestMultipleRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, true));
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

TEST_F(DynamicMemoryLayoutTest, rowLayoutLayoutFieldSimple) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = std::dynamic_pointer_cast<RowLayoutTupleBuffer>(rowLayout->bind(tupleBuffer));

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedRowLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = RowLayoutField<uint8_t, true>::create(0, bindedRowLayout);
    auto field1 = RowLayoutField<uint16_t, true>::create(1, bindedRowLayout);
    auto field2 = RowLayoutField<uint32_t, true>::create(2, bindedRowLayout);

    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutLayoutFieldSimple) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, true));
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

    auto field0 = ColumnLayoutField<uint8_t, true>::create(0, bindedColumnLayout);
    auto field1 = ColumnLayoutField<uint16_t, true>::create(1, bindedColumnLayout);
    auto field2 = ColumnLayoutField<uint32_t, true>::create(2, bindedColumnLayout);

    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }
}

TEST_F(DynamicMemoryLayoutTest, rowLayoutLayoutFieldBoundaryCheck) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = std::dynamic_pointer_cast<RowLayoutTupleBuffer>(rowLayout->bind(tupleBuffer));

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedRowLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = RowLayoutField<uint8_t, true>::create(0, bindedRowLayout);
    auto field1 = RowLayoutField<uint16_t, true>::create(1, bindedRowLayout);
    auto field2 = RowLayoutField<uint32_t, true>::create(2, bindedRowLayout);

    ASSERT_THROW((RowLayoutField<uint32_t, true>::create(3, bindedRowLayout)), NES::NesRuntimeException);
    ASSERT_THROW((RowLayoutField<uint32_t, true>::create(4, bindedRowLayout)), NES::NesRuntimeException);
    ASSERT_THROW((RowLayoutField<uint32_t, true>::create(5, bindedRowLayout)), NES::NesRuntimeException);

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

TEST_F(DynamicMemoryLayoutTest, columnLayoutLayoutFieldBoundaryCheck) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, true));
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

    auto field0 = ColumnLayoutField<uint8_t, true>::create(0, bindedColumnLayout);
    auto field1 = ColumnLayoutField<uint16_t, true>::create(1, bindedColumnLayout);
    auto field2 = ColumnLayoutField<uint32_t, true>::create(2, bindedColumnLayout);
    ASSERT_THROW((ColumnLayoutField<uint8_t, true>::create(3, bindedColumnLayout)), NES::NesRuntimeException);
    ASSERT_THROW((ColumnLayoutField<uint16_t, true>::create(4, bindedColumnLayout)), NES::NesRuntimeException);
    ASSERT_THROW((ColumnLayoutField<uint32_t, true>::create(5, bindedColumnLayout)), NES::NesRuntimeException);

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

TEST_F(DynamicMemoryLayoutTest, rowLayoutLayoutFieldBoundaryNoCheck) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = std::dynamic_pointer_cast<RowLayoutTupleBuffer>(rowLayout->bind(tupleBuffer));

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedRowLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = RowLayoutField<uint8_t, false>::create(0, bindedRowLayout);
    auto field1 = RowLayoutField<uint16_t, false>::create(1, bindedRowLayout);
    auto field2 = RowLayoutField<uint32_t, false>::create(2, bindedRowLayout);

    try {
        RowLayoutField<uint32_t, false>::create(3, bindedRowLayout);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        RowLayoutField<uint32_t, false>::create(4, bindedRowLayout);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        RowLayoutField<uint32_t, false>::create(5, bindedRowLayout);
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

TEST_F(DynamicMemoryLayoutTest, columnLayoutLayoutFieldBoundaryNoCheck) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, true));
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

    auto field0 = ColumnLayoutField<uint8_t, false>::create(0, bindedColumnLayout);
    auto field1 = ColumnLayoutField<uint16_t, false>::create(1, bindedColumnLayout);
    auto field2 = ColumnLayoutField<uint32_t, false>::create(2, bindedColumnLayout);

    try {
        ColumnLayoutField<uint32_t, false>::create(3, bindedColumnLayout);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        ColumnLayoutField<uint32_t, false>::create(4, bindedColumnLayout);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        ColumnLayoutField<uint32_t, false>::create(5, bindedColumnLayout);
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

TEST_F(DynamicMemoryLayoutTest, pushRecordTooManyRecordsRowLayout) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = std::dynamic_pointer_cast<RowLayoutTupleBuffer>(rowLayout->bind(tupleBuffer));

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    size_t i = 0;
    for (; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        ASSERT_TRUE(bindedRowLayout->pushRecord<true>(writeRecord));
    }

    for (; i < NUM_TUPLES + 10; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        ASSERT_FALSE(bindedRowLayout->pushRecord<true>(writeRecord));
    }

    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> readRecord = bindedRowLayout->readRecord<true, uint8_t, uint16_t, uint32_t>(i);
        ASSERT_EQ(allTuples[i], readRecord);
    }

    ASSERT_EQ(bindedRowLayout->getNumberOfRecords(), NUM_TUPLES);
}

TEST_F(DynamicMemoryLayoutTest, pushRecordTooManyRecordsColumnLayout) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, true));
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

TEST_F(DynamicMemoryLayoutTest, getFieldViaFieldNameRowLayout) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = std::dynamic_pointer_cast<RowLayoutTupleBuffer>(rowLayout->bind(tupleBuffer));

    ASSERT_NO_THROW((RowLayoutField<uint8_t, true>::create("t1", bindedRowLayout)));
    ASSERT_NO_THROW((RowLayoutField<uint16_t, true>::create("t2", bindedRowLayout)));
    ASSERT_NO_THROW((RowLayoutField<uint32_t, true>::create("t3", bindedRowLayout)));

    ASSERT_THROW((RowLayoutField<uint32_t, true>::create("t4", bindedRowLayout)), NES::NesRuntimeException);
    ASSERT_THROW((RowLayoutField<uint32_t, true>::create("t5", bindedRowLayout)), NES::NesRuntimeException);
    ASSERT_THROW((RowLayoutField<uint32_t, true>::create("t6", bindedRowLayout)), NES::NesRuntimeException);
}

TEST_F(DynamicMemoryLayoutTest, getFieldViaFieldNameColumnLayout) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    ColumnLayoutBufferPtr bindedColumnLayout =
        std::dynamic_pointer_cast<ColumnLayoutTupleBuffer>(columnLayout->bind(tupleBuffer));

    ASSERT_NO_THROW((ColumnLayoutField<uint8_t, true>::create("t1", bindedColumnLayout)));
    ASSERT_NO_THROW((ColumnLayoutField<uint16_t, true>::create("t2", bindedColumnLayout)));
    ASSERT_NO_THROW((ColumnLayoutField<uint32_t, true>::create("t3", bindedColumnLayout)));

    ASSERT_THROW((ColumnLayoutField<uint32_t, true>::create("t4", bindedColumnLayout)), NES::NesRuntimeException);
    ASSERT_THROW((ColumnLayoutField<uint32_t, true>::create("t5", bindedColumnLayout)), NES::NesRuntimeException);
    ASSERT_THROW((ColumnLayoutField<uint32_t, true>::create("t6", bindedColumnLayout)), NES::NesRuntimeException);
}

TEST_F(DynamicMemoryLayoutTest, accessDynamicColumnBufferTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    ColumnLayoutBufferPtr bindedColumnLayout =
        std::dynamic_pointer_cast<ColumnLayoutTupleBuffer>(columnLayout->bind(tupleBuffer));
    auto buffer = DynamicTupleBuffer(bindedColumnLayout);
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

TEST_F(DynamicMemoryLayoutTest, accessDynamicBufferExceptionTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    ColumnLayoutBufferPtr bindedColumnLayout =
        std::dynamic_pointer_cast<ColumnLayoutTupleBuffer>(columnLayout->bind(tupleBuffer));
    auto buffer = DynamicTupleBuffer(bindedColumnLayout);
    ASSERT_ANY_THROW(buffer[10000000L]);
    ASSERT_ANY_THROW(buffer[0][5]);
}

TEST_F(DynamicMemoryLayoutTest, accessArrayDynamicBufferTest) {
    SchemaPtr schema = Schema::create()->addField("t1", DataTypeFactory::createArray(10, DataTypeFactory::createInt64()));

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    ColumnLayoutBufferPtr bindedColumnLayout =
        std::dynamic_pointer_cast<ColumnLayoutTupleBuffer>(columnLayout->bind(tupleBuffer));
    auto buffer = DynamicTupleBuffer(bindedColumnLayout);
    auto array = NES::QueryCompilation::Array<int64_t, 10>();
    for (int i = 0; i < 10; i++) {
        array[i] = i * 2;
    }
    buffer[0][0].write<NES::QueryCompilation::Array<int64_t, 10>>(array);
    auto value = buffer[0][0].read<NES::QueryCompilation::Array<int64_t, 10>>();
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(value[i], i * 2);
    }

    auto* arrayPtr = buffer[0][0].read<int64_t*>();
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(arrayPtr[i], i * 2);
    }
}

}// namespace NES::Runtime::MemoryLayouts
