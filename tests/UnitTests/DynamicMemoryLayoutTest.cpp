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

#include <gtest/gtest.h>

#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/MemoryLayout/DynamicColumnLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicColumnLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicColumnLayoutField.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutField.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>

#include <cstdlib>
#include <iostream>
#include <vector>

namespace NES::NodeEngine::DynamicMemoryLayout {
class DynamicMemoryLayoutTest : public testing::Test {
  public:
    BufferManagerPtr bufferManager;

    void SetUp() override {
        NESLogger->removeAllAppenders();
        NES::setupLogging("DynamicMemoryLayoutTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup DynamicMemoryLayoutTest test class.");
        bufferManager = std::make_shared<BufferManager>(4096, 10);
    }
    static void TearDownTestCase() {
        std::cout << "Tear down DynamicMemoryLayoutTest class." << std::endl;
        NESLogger->removeAllAppenders();
    }
};

TEST_F(DynamicMemoryLayoutTest, rowLayoutCreateTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_EQ(rowLayout->isCheckBoundaryFieldChecks(), true);
    ASSERT_NE(rowLayout, nullptr);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutCreateTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_EQ(columnLayout->isCheckBoundaryFieldChecks(), true);
    ASSERT_NE(columnLayout, nullptr);
}

TEST_F(DynamicMemoryLayoutTest, rowLayoutMapCalcOffsetTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);

    ASSERT_EQ(bindedRowLayout->getCapacity(), tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());
    ASSERT_EQ(bindedRowLayout->getNumberOfRecords(), 0u);
    ASSERT_EQ(bindedRowLayout->calcOffset(1, 2, true), schema->getSchemaSizeInBytes() * 1 + (1 + 2));
    ASSERT_EQ(bindedRowLayout->calcOffset(4, 0, true), schema->getSchemaSizeInBytes() * 4 + 0);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutMapCalcOffsetTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);

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
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);

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
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);

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

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);

    std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
    bindedColumnLayout->pushRecord<true>(writeRecord);

    std::tuple<uint8_t, uint16_t, uint32_t> readRecord = bindedColumnLayout->readRecord<true, uint8_t, uint16_t, uint32_t>(0);

    ASSERT_EQ(writeRecord, readRecord);
    ASSERT_EQ(bindedColumnLayout->getNumberOfRecords(), 1UL);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutPushRecordAndReadRecordTestMultipleRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);

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
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedRowLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = DynamicRowLayoutField<uint8_t, true>::create(0, bindedRowLayout);
    auto field1 = DynamicRowLayoutField<uint16_t, true>::create(1, bindedRowLayout);
    auto field2 = DynamicRowLayoutField<uint32_t, true>::create(2, bindedRowLayout);

    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutLayoutFieldSimple) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedColumnLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = DynamicColumnLayoutField<uint8_t, true>::create(0, bindedColumnLayout);
    auto field1 = DynamicColumnLayoutField<uint16_t, true>::create(1, bindedColumnLayout);
    auto field2 = DynamicColumnLayoutField<uint32_t, true>::create(2, bindedColumnLayout);

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
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedRowLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = DynamicRowLayoutField<uint8_t, true>::create(0, bindedRowLayout);
    auto field1 = DynamicRowLayoutField<uint16_t, true>::create(1, bindedRowLayout);
    auto field2 = DynamicRowLayoutField<uint32_t, true>::create(2, bindedRowLayout);

    ASSERT_THROW((DynamicRowLayoutField<uint32_t, true>::create(3, bindedRowLayout)), NES::NesRuntimeException);
    ASSERT_THROW((DynamicRowLayoutField<uint32_t, true>::create(4, bindedRowLayout)), NES::NesRuntimeException);
    ASSERT_THROW((DynamicRowLayoutField<uint32_t, true>::create(5, bindedRowLayout)), NES::NesRuntimeException);

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

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedColumnLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = DynamicColumnLayoutField<uint8_t, true>::create(0, bindedColumnLayout);
    auto field1 = DynamicColumnLayoutField<uint16_t, true>::create(1, bindedColumnLayout);
    auto field2 = DynamicColumnLayoutField<uint32_t, true>::create(2, bindedColumnLayout);
    ASSERT_THROW((DynamicColumnLayoutField<uint8_t, true>::create(3, bindedColumnLayout)), NES::NesRuntimeException);
    ASSERT_THROW((DynamicColumnLayoutField<uint16_t, true>::create(4, bindedColumnLayout)), NES::NesRuntimeException);
    ASSERT_THROW((DynamicColumnLayoutField<uint32_t, true>::create(5, bindedColumnLayout)), NES::NesRuntimeException);

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
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedRowLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = DynamicRowLayoutField<uint8_t, false>::create(0, bindedRowLayout);
    auto field1 = DynamicRowLayoutField<uint16_t, false>::create(1, bindedRowLayout);
    auto field2 = DynamicRowLayoutField<uint32_t, false>::create(2, bindedRowLayout);

    try {
        DynamicRowLayoutField<uint32_t, false>::create(3, bindedRowLayout);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        DynamicRowLayoutField<uint32_t, false>::create(4, bindedRowLayout);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        DynamicRowLayoutField<uint32_t, false>::create(5, bindedRowLayout);
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

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedColumnLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = DynamicColumnLayoutField<uint8_t, false>::create(0, bindedColumnLayout);
    auto field1 = DynamicColumnLayoutField<uint16_t, false>::create(1, bindedColumnLayout);
    auto field2 = DynamicColumnLayoutField<uint32_t, false>::create(2, bindedColumnLayout);

    try {
        DynamicColumnLayoutField<uint32_t, false>::create(3, bindedColumnLayout);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        DynamicColumnLayoutField<uint32_t, false>::create(4, bindedColumnLayout);
    } catch (NES::NesRuntimeException& e) {
        EXPECT_TRUE(false);
    } catch (Exception& e) {
        EXPECT_TRUE(true);
    }

    try {
        DynamicColumnLayoutField<uint32_t, false>::create(5, bindedColumnLayout);
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
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);

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

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);

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
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);

    ASSERT_NO_THROW((DynamicRowLayoutField<uint8_t, true>::create("t1", bindedRowLayout)));
    ASSERT_NO_THROW((DynamicRowLayoutField<uint16_t, true>::create("t2", bindedRowLayout)));
    ASSERT_NO_THROW((DynamicRowLayoutField<uint32_t, true>::create("t3", bindedRowLayout)));

    ASSERT_THROW((DynamicRowLayoutField<uint32_t, true>::create("t4", bindedRowLayout)), NES::NesRuntimeException);
    ASSERT_THROW((DynamicRowLayoutField<uint32_t, true>::create("t5", bindedRowLayout)), NES::NesRuntimeException);
    ASSERT_THROW((DynamicRowLayoutField<uint32_t, true>::create("t6", bindedRowLayout)), NES::NesRuntimeException);
}

TEST_F(DynamicMemoryLayoutTest, getFieldViaFieldNameColumnLayout) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);

    ASSERT_NO_THROW((DynamicColumnLayoutField<uint8_t, true>::create("t1", bindedColumnLayout)));
    ASSERT_NO_THROW((DynamicColumnLayoutField<uint16_t, true>::create("t2", bindedColumnLayout)));
    ASSERT_NO_THROW((DynamicColumnLayoutField<uint32_t, true>::create("t3", bindedColumnLayout)));

    ASSERT_THROW((DynamicColumnLayoutField<uint32_t, true>::create("t4", bindedColumnLayout)), NES::NesRuntimeException);
    ASSERT_THROW((DynamicColumnLayoutField<uint32_t, true>::create("t5", bindedColumnLayout)), NES::NesRuntimeException);
    ASSERT_THROW((DynamicColumnLayoutField<uint32_t, true>::create("t6", bindedColumnLayout)), NES::NesRuntimeException);
}

}// namespace NES::NodeEngine::DynamicMemoryLayout
