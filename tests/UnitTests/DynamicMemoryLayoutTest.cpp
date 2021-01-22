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
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/MemoryLayout/DynamicColumnLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicColumnLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutField.hpp>
#include <NodeEngine/MemoryLayout/DynamicColumnLayoutField.hpp>

#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>
#include <vector>

namespace NES::NodeEngine {
class DynamicMemoryLayoutTest : public testing::Test {
  public:
    BufferManagerPtr bufferManager;

    void SetUp() {
        NESLogger->removeAllAppenders();
        NES::setupLogging("DynamicMemoryLayoutTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup DynamicMemoryLayoutTest test class.");
        bufferManager = std::make_shared<BufferManager>(4096, 10);
    }
    static void TearDownTestCase() { std::cout << "Tear down DynamicMemoryLayoutTest class." << std::endl; NESLogger->removeAllAppenders(); }
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

    DynamicRowLayoutBufferPtr mappedRowLayout;
    ASSERT_NO_THROW(mappedRowLayout = std::unique_ptr<DynamicRowLayoutBuffer>(static_cast<DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedRowLayout, nullptr);

    ASSERT_EQ(mappedRowLayout->getCapacity(), tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());
    ASSERT_EQ(mappedRowLayout->getNumberOfRecords(), 0);
    ASSERT_EQ(mappedRowLayout->calcOffset(1, 2, false), schema->getSchemaSizeInBytes() * 1 + (1 + 2));
    ASSERT_EQ(mappedRowLayout->calcOffset(4, 0, false), schema->getSchemaSizeInBytes() * 4 + 0);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutMapCalcOffsetTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr mappedColumnLayout;
    ASSERT_NO_THROW(mappedColumnLayout = std::unique_ptr<DynamicColumnLayoutBuffer>(static_cast<DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedColumnLayout, nullptr);

    auto capacity = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();
    ASSERT_EQ(mappedColumnLayout->getCapacity(), capacity);
    ASSERT_EQ(mappedColumnLayout->getNumberOfRecords(), 0);
    ASSERT_EQ(mappedColumnLayout->calcOffset(1, 2, false), capacity * 1 + capacity * 2 + 1 * 4);
    ASSERT_EQ(mappedColumnLayout->calcOffset(5, 1, false), capacity * 1 + 5 * 2);
    ASSERT_EQ(mappedColumnLayout->calcOffset(4, 0, false), capacity * 0 + 4);
}

TEST_F(DynamicMemoryLayoutTest, rowLayoutPushRecordAndReadRecordTestOneRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr mappedRowLayout;
    ASSERT_NO_THROW(mappedRowLayout = std::unique_ptr<DynamicRowLayoutBuffer>(static_cast<DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedRowLayout, nullptr);

    std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(1, 2, 3);
    mappedRowLayout->pushRecord<false>(writeRecord);

    std::tuple<uint8_t, uint16_t, uint32_t> readRecord = mappedRowLayout->readRecord<false, uint8_t, uint16_t, uint32_t>(0);

    ASSERT_EQ(writeRecord, readRecord);
    ASSERT_EQ(mappedRowLayout->getNumberOfRecords(), 1);
}

TEST_F(DynamicMemoryLayoutTest, rowLayoutPushRecordAndReadRecordTestMultipleRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr mappedRowLayout;
    ASSERT_NO_THROW(mappedRowLayout = std::unique_ptr<DynamicRowLayoutBuffer>(static_cast<DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedRowLayout, nullptr);

    size_t NUM_TUPLES = 230; //tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        mappedRowLayout->pushRecord<false>(writeRecord);
    }

    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> readRecord = mappedRowLayout->readRecord<false, uint8_t, uint16_t, uint32_t>(i);
        ASSERT_EQ(allTuples[i], readRecord);
    }

    ASSERT_EQ(mappedRowLayout->getNumberOfRecords(), NUM_TUPLES);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutPushRecordAndReadRecordTestOneRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr mappedColumnLayout;
    ASSERT_NO_THROW(mappedColumnLayout = std::unique_ptr<DynamicColumnLayoutBuffer>(static_cast<DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedColumnLayout, nullptr);

    std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
    mappedColumnLayout->pushRecord<false>(writeRecord);

    std::tuple<uint8_t, uint16_t, uint32_t> readRecord = mappedColumnLayout->readRecord<false, uint8_t, uint16_t, uint32_t>(0);

    ASSERT_EQ(writeRecord, readRecord);
    ASSERT_EQ(mappedColumnLayout->getNumberOfRecords(), 1);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutPushRecordAndReadRecordTestMultipleRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr mappedColumnLayout;
    ASSERT_NO_THROW(mappedColumnLayout = std::unique_ptr<DynamicColumnLayoutBuffer>(static_cast<DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedColumnLayout, nullptr);

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        mappedColumnLayout->pushRecord<false>(writeRecord);
    }


    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> readRecord = mappedColumnLayout->readRecord<false, uint8_t, uint16_t, uint32_t>(i);
        ASSERT_EQ(allTuples[i], readRecord);
    }

    ASSERT_EQ(mappedColumnLayout->getNumberOfRecords(), NUM_TUPLES);
}

TEST_F(DynamicMemoryLayoutTest, rowLayoutLayoutFieldSimple) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr mappedRowLayout;
    ASSERT_NO_THROW(mappedRowLayout = std::unique_ptr<DynamicRowLayoutBuffer>(static_cast<DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedRowLayout, nullptr);

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        mappedRowLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = DynamicRowLayoutField<uint8_t, false>::create(0, mappedRowLayout);
    auto field1 = DynamicRowLayoutField<uint16_t, false>::create(1, mappedRowLayout);
    auto field2 = DynamicRowLayoutField<uint32_t, false>::create(2, mappedRowLayout);

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

    DynamicColumnLayoutBufferPtr mappedColumnLayout;
    ASSERT_NO_THROW(mappedColumnLayout = std::unique_ptr<DynamicColumnLayoutBuffer>(static_cast<DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedColumnLayout, nullptr);

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        mappedColumnLayout->pushRecord<false>(writeRecord);
    }


    auto field0 = DynamicColumnLayoutField<uint8_t, false>::create(0, mappedColumnLayout);
    auto field1 = DynamicColumnLayoutField<uint16_t, false>::create(1, mappedColumnLayout);
    auto field2 = DynamicColumnLayoutField<uint32_t, false>::create(2, mappedColumnLayout);

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

    DynamicRowLayoutBufferPtr mappedRowLayout;
    ASSERT_NO_THROW(mappedRowLayout = std::unique_ptr<DynamicRowLayoutBuffer>(static_cast<DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedRowLayout, nullptr);

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        mappedRowLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = DynamicRowLayoutField<uint8_t, true>::create(0, mappedRowLayout);
    auto field1 = DynamicRowLayoutField<uint16_t, true>::create(1, mappedRowLayout);
    auto field2 = DynamicRowLayoutField<uint32_t, true>::create(2, mappedRowLayout);

    ASSERT_ANY_THROW((DynamicRowLayoutField<uint32_t, true>::create(3, mappedRowLayout)));
    ASSERT_ANY_THROW((DynamicRowLayoutField<uint32_t, true>::create(4, mappedRowLayout)));
    ASSERT_ANY_THROW((DynamicRowLayoutField<uint32_t, true>::create(5, mappedRowLayout)));

    size_t i = 0;
    for (; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }
    ASSERT_ANY_THROW(field0[i]);
    ASSERT_ANY_THROW(field1[i]);
    ASSERT_ANY_THROW(field2[i]);

    ASSERT_ANY_THROW(field0[++i]);
    ASSERT_ANY_THROW(field1[i]);
    ASSERT_ANY_THROW(field2[i]);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutLayoutFieldBoundaryCheck) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr mappedColumnLayout;
    ASSERT_NO_THROW(mappedColumnLayout = std::unique_ptr<DynamicColumnLayoutBuffer>(static_cast<DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedColumnLayout, nullptr);

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        mappedColumnLayout->pushRecord<true>(writeRecord);
    }


    auto field0 = DynamicColumnLayoutField<uint8_t, true>::create(0, mappedColumnLayout);
    auto field1 = DynamicColumnLayoutField<uint16_t, true>::create(1, mappedColumnLayout);
    auto field2 = DynamicColumnLayoutField<uint32_t, true>::create(2, mappedColumnLayout);
    ASSERT_ANY_THROW((DynamicColumnLayoutField<uint8_t, true>::create(3, mappedColumnLayout)));
    ASSERT_ANY_THROW((DynamicColumnLayoutField<uint16_t, true>::create(4, mappedColumnLayout)));
    ASSERT_ANY_THROW((DynamicColumnLayoutField<uint32_t, true>::create(5, mappedColumnLayout)));

    size_t i = 0;
    for (; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }
    ASSERT_ANY_THROW(field0[i]);
    ASSERT_ANY_THROW(field1[i]);
    ASSERT_ANY_THROW(field2[i]);

    ASSERT_ANY_THROW(field0[++i]);
    ASSERT_ANY_THROW(field1[i]);
    ASSERT_ANY_THROW(field2[i]);
}

TEST_F(DynamicMemoryLayoutTest, rowLayoutLayoutFieldBoundaryNoCheck) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr mappedRowLayout;
    ASSERT_NO_THROW(mappedRowLayout = std::unique_ptr<DynamicRowLayoutBuffer>(static_cast<DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedRowLayout, nullptr);

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        mappedRowLayout->pushRecord<true>(writeRecord);
    }


    auto field0 = DynamicRowLayoutField<uint8_t, false>::create(0, mappedRowLayout);
    auto field1 = DynamicRowLayoutField<uint16_t, false>::create(1, mappedRowLayout);
    auto field2 = DynamicRowLayoutField<uint32_t, false>::create(2, mappedRowLayout);

    ASSERT_NO_THROW((DynamicRowLayoutField<uint32_t, false>::create(3, mappedRowLayout)));
    ASSERT_NO_THROW((DynamicRowLayoutField<uint32_t, false>::create(4, mappedRowLayout)));
    ASSERT_NO_THROW((DynamicRowLayoutField<uint32_t, false>::create(5, mappedRowLayout)));

    size_t i = 0;
    for (; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }
    ASSERT_NO_THROW(field0[i]);
    ASSERT_NO_THROW(field1[i]);
    ASSERT_NO_THROW(field2[i]);

    ASSERT_NO_THROW(field0[++i]);
    ASSERT_NO_THROW(field1[i]);
    ASSERT_NO_THROW(field2[i]);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutLayoutFieldBoundaryNoCheck) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr mappedColumnLayout;
    ASSERT_NO_THROW(mappedColumnLayout = std::unique_ptr<DynamicColumnLayoutBuffer>(static_cast<DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedColumnLayout, nullptr);

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        mappedColumnLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = DynamicColumnLayoutField<uint8_t, false>::create(0, mappedColumnLayout);
    auto field1 = DynamicColumnLayoutField<uint16_t, false>::create(1, mappedColumnLayout);
    auto field2 = DynamicColumnLayoutField<uint32_t, false>::create(2, mappedColumnLayout);

    ASSERT_NO_THROW((DynamicColumnLayoutField<uint32_t, false>::create(3, mappedColumnLayout)));
    ASSERT_NO_THROW((DynamicColumnLayoutField<uint32_t, false>::create(4, mappedColumnLayout)));
    ASSERT_NO_THROW((DynamicColumnLayoutField<uint32_t, false>::create(5, mappedColumnLayout)));
    ASSERT_NO_THROW((DynamicColumnLayoutField<uint32_t, false>::create(5, mappedColumnLayout)));

    size_t i = 0;
    for (; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }
    ASSERT_NO_THROW(field0[i]);
    ASSERT_NO_THROW(field1[i]);
    ASSERT_NO_THROW(field2[i]);

    ASSERT_NO_THROW(field0[++i]);
    ASSERT_NO_THROW(field1[i]);
    ASSERT_NO_THROW(field2[i]);
}



}// namespace NES
