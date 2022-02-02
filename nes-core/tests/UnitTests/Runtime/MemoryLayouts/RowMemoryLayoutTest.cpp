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
#include "../../../util/NesBaseTest.hpp"

#include <cstdlib>
#include <iostream>
#include <vector>

namespace NES::Runtime::MemoryLayouts {
class RowMemoryLayoutTest : public Testing::NESBaseTest {
  public:
    BufferManagerPtr bufferManager;

    void SetUp() override {
        NESLogger::getInstance()->removeAllAppenders();
        NES::setupLogging("RowMemoryLayoutTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup RowMemoryLayoutTest test class.");
        bufferManager = std::make_shared<BufferManager>(4096, 10);
    }
    static void TearDownTestCase() {
        std::cout << "Tear down RowMemoryLayoutTest class." << std::endl;
        NESLogger::getInstance()->removeAllAppenders();
    }
};

TEST_F(RowMemoryLayoutTest, rowLayoutCreateTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);
}

TEST_F(RowMemoryLayoutTest, rowLayoutMapCalcOffsetTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto bindedRowLayout = rowLayout->bind(tupleBuffer);

    ASSERT_EQ(bindedRowLayout->getCapacity(), tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());
    ASSERT_EQ(bindedRowLayout->getNumberOfRecords(), 0u);
    ASSERT_EQ(rowLayout->getFieldOffset(1, 2), schema->getSchemaSizeInBytes() * 1 + (1 + 2));
    ASSERT_EQ(rowLayout->getFieldOffset(4, 0), schema->getSchemaSizeInBytes() * 4 + 0);
}

TEST_F(RowMemoryLayoutTest, rowLayoutPushRecordAndReadRecordTestOneRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto bindedRowLayout = std::dynamic_pointer_cast<RowLayoutTupleBuffer>(rowLayout->bind(tupleBuffer));

    std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(1, 2, 3);
    bindedRowLayout->pushRecord<true>(writeRecord);

    std::tuple<uint8_t, uint16_t, uint32_t> readRecord = bindedRowLayout->readRecord<true, uint8_t, uint16_t, uint32_t>(0);

    ASSERT_EQ(writeRecord, readRecord);
    ASSERT_EQ(bindedRowLayout->getNumberOfRecords(), 1UL);
}

TEST_F(RowMemoryLayoutTest, rowLayoutPushRecordAndReadRecordTestMultipleRecord) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto bindedRowLayout = std::dynamic_pointer_cast<RowLayoutTupleBuffer>(rowLayout->bind(tupleBuffer));

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

TEST_F(RowMemoryLayoutTest, rowLayoutLayoutFieldSimple) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto bindedRowLayout = std::dynamic_pointer_cast<RowLayoutTupleBuffer>(rowLayout->bind(tupleBuffer));

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedRowLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = RowLayoutField<uint8_t, true>::create(0, rowLayout, tupleBuffer);
    auto field1 = RowLayoutField<uint16_t, true>::create(1, rowLayout, tupleBuffer);
    auto field2 = RowLayoutField<uint32_t, true>::create(2, rowLayout, tupleBuffer);

    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }
}

TEST_F(RowMemoryLayoutTest, rowLayoutLayoutFieldBoundaryCheck) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto bindedRowLayout = std::dynamic_pointer_cast<RowLayoutTupleBuffer>(rowLayout->bind(tupleBuffer));

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        bindedRowLayout->pushRecord<true>(writeRecord);
    }

    auto field0 = RowLayoutField<uint8_t, true>::create(0, rowLayout, tupleBuffer);
    auto field1 = RowLayoutField<uint16_t, true>::create(1, rowLayout, tupleBuffer);
    auto field2 = RowLayoutField<uint32_t, true>::create(2, rowLayout, tupleBuffer);

    ASSERT_THROW((RowLayoutField<uint32_t, true>::create(3, rowLayout, tupleBuffer)), NES::RuntimeException);
    ASSERT_THROW((RowLayoutField<uint32_t, true>::create(4, rowLayout, tupleBuffer)), NES::RuntimeException);
    ASSERT_THROW((RowLayoutField<uint32_t, true>::create(5, rowLayout, tupleBuffer)), NES::RuntimeException);

    size_t i = 0;
    for (; i < NUM_TUPLES; ++i) {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }
    ASSERT_THROW(field0[i], NES::RuntimeException);
    ASSERT_THROW(field1[i], NES::RuntimeException);
    ASSERT_THROW(field2[i], NES::RuntimeException);

    ASSERT_THROW(field0[++i], NES::RuntimeException);
    ASSERT_THROW(field1[i], NES::RuntimeException);
    ASSERT_THROW(field2[i], NES::RuntimeException);
}

TEST_F(RowMemoryLayoutTest, pushRecordTooManyRecordsRowLayout) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto bindedRowLayout = std::dynamic_pointer_cast<RowLayoutTupleBuffer>(rowLayout->bind(tupleBuffer));

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

TEST_F(RowMemoryLayoutTest, getFieldViaFieldNameRowLayout) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    RowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    ASSERT_NO_THROW((RowLayoutField<uint8_t, true>::create("t1", rowLayout, tupleBuffer)));
    ASSERT_NO_THROW((RowLayoutField<uint16_t, true>::create("t2", rowLayout, tupleBuffer)));
    ASSERT_NO_THROW((RowLayoutField<uint32_t, true>::create("t3", rowLayout, tupleBuffer)));

    ASSERT_THROW((RowLayoutField<uint32_t, true>::create("t4", rowLayout, tupleBuffer)), NES::RuntimeException);
    ASSERT_THROW((RowLayoutField<uint32_t, true>::create("t5", rowLayout, tupleBuffer)), NES::RuntimeException);
    ASSERT_THROW((RowLayoutField<uint32_t, true>::create("t6", rowLayout, tupleBuffer)), NES::RuntimeException);
}

}// namespace NES::Runtime::MemoryLayouts
