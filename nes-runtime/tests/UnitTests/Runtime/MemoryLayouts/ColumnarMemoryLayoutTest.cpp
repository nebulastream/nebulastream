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

#include <cstdlib>
#include <iostream>
#include <vector>
#include <API/Schema.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES::Memory::MemoryLayouts
{
class ColumnarMemoryLayoutTest : public Testing::BaseUnitTest
{
public:
    Memory::BufferManagerPtr bufferManager;
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("ColumnarMemoryLayoutTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ColumnarMemoryLayoutTest test class.");
    }

    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        bufferManager = Memory::BufferManager::create(4096, 10);
    }
};

/**
 * @brief Tests that we can construct a column layout.
 */
TEST_F(ColumnarMemoryLayoutTest, columnLayoutCreateTest)
{
    SchemaPtr schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);
}

/**
 * @brief Tests that the field offsets are are calculated correctly using a TestTupleBuffer.
 */
TEST_F(ColumnarMemoryLayoutTest, columnLayoutMapCalcOffsetTest)
{
    SchemaPtr schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    auto capacity = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();
    ASSERT_EQ(testBuffer->getCapacity(), capacity);
    ASSERT_EQ(testBuffer->getNumberOfTuples(), 0u);
    ASSERT_EQ(columnLayout->getFieldOffset(1, 2), capacity * 1 + capacity * 2 + 1 * 4);
    ASSERT_EQ(columnLayout->getFieldOffset(5, 1), capacity * 1 + 5 * 2);
    ASSERT_EQ(columnLayout->getFieldOffset(4, 0), capacity * 0 + 4);
}

/**
 * @brief Tests that we can write a single record to and read from a TestTupleBuffer correctly.
 */
TEST_F(ColumnarMemoryLayoutTest, columnLayoutPushRecordAndReadRecordTestOneRecord)
{
    SchemaPtr schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
    testBuffer->pushRecordToBuffer(writeRecord);

    std::tuple<uint8_t, uint16_t, uint32_t> readRecord = testBuffer->readRecordFromBuffer<uint8_t, uint16_t, uint32_t>(0);

    ASSERT_EQ(writeRecord, readRecord);
    ASSERT_EQ(testBuffer->getNumberOfTuples(), 1UL);
}

/**
 * @brief Tests that we can write many records to and read from a TestTupleBuffer correctly.
 */
TEST_F(ColumnarMemoryLayoutTest, columnLayoutPushRecordAndReadRecordTestMultipleRecord)
{
    SchemaPtr schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    for (size_t i = 0; i < NUM_TUPLES; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> readRecord = testBuffer->readRecordFromBuffer<uint8_t, uint16_t, uint32_t>(i);
        ASSERT_EQ(allTuples[i], readRecord);
    }

    ASSERT_EQ(testBuffer->getNumberOfTuples(), NUM_TUPLES);
}

/**
 * @brief Tests that we can access fields of a TupleBuffer that is used in a TestTupleBuffer correctly.
 */
TEST_F(ColumnarMemoryLayoutTest, columnLayoutLayoutFieldSimple)
{
    SchemaPtr schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    auto field0 = ColumnLayoutField<uint8_t, true>::create(0, columnLayout, tupleBuffer);
    auto field1 = ColumnLayoutField<uint16_t, true>::create(1, columnLayout, tupleBuffer);
    auto field2 = ColumnLayoutField<uint32_t, true>::create(2, columnLayout, tupleBuffer);

    for (size_t i = 0; i < NUM_TUPLES; ++i)
    {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }
}

/**
 * @brief Tests whether whether an error is thrown if we try to access non-existing fields of a TupleBuffer.
 */
TEST_F(ColumnarMemoryLayoutTest, columnLayoutLayoutFieldBoundaryCheck)
{
    SchemaPtr schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    auto field0 = ColumnLayoutField<uint8_t, true>::create(0, columnLayout, tupleBuffer);
    auto field1 = ColumnLayoutField<uint16_t, true>::create(1, columnLayout, tupleBuffer);
    auto field2 = ColumnLayoutField<uint32_t, true>::create(2, columnLayout, tupleBuffer);
    ASSERT_DEATH((ColumnLayoutField<uint8_t, true>::create(3, columnLayout, tupleBuffer)), "Invariant violated:.*");
    ASSERT_DEATH((ColumnLayoutField<uint16_t, true>::create(4, columnLayout, tupleBuffer)), "Invariant violated:.*");
    ASSERT_DEATH((ColumnLayoutField<uint32_t, true>::create(5, columnLayout, tupleBuffer)), "Invariant violated:.*");

    size_t i = 0;
    for (; i < NUM_TUPLES; ++i)
    {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }

    ASSERT_DEATH(field0[i], "Invariant violated:.*");
    ASSERT_DEATH(field1[i], "Invariant violated:.*");
    ASSERT_DEATH(field2[i], "Invariant violated:.*");

    ASSERT_DEATH(field0[++i], "Invariant violated:.*");
    ASSERT_DEATH(field1[i], "Invariant violated:.*");
    ASSERT_DEATH(field2[i], "Invariant violated:.*");
}

/**
 * @brief Tests whether we can only access the correct fields.
 */
TEST_F(ColumnarMemoryLayoutTest, getFieldViaFieldNameColumnLayout)
{
    SchemaPtr schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    ASSERT_NO_THROW((ColumnLayoutField<uint8_t, true>::create("t1", columnLayout, tupleBuffer)));
    ASSERT_NO_THROW((ColumnLayoutField<uint16_t, true>::create("t2", columnLayout, tupleBuffer)));
    ASSERT_NO_THROW((ColumnLayoutField<uint32_t, true>::create("t3", columnLayout, tupleBuffer)));

    ASSERT_DEATH((ColumnLayoutField<uint32_t, true>::create("t4", columnLayout, tupleBuffer)), "Invariant violated:.*");
    ASSERT_DEATH((ColumnLayoutField<uint32_t, true>::create("t5", columnLayout, tupleBuffer)), "Invariant violated:.*");
    ASSERT_DEATH((ColumnLayoutField<uint32_t, true>::create("t6", columnLayout, tupleBuffer)), "Invariant violated:.*");
}

/**
 * @brief Tests whether reading from the TestTupleBuffer works correctly.
 */
TEST_F(ColumnarMemoryLayoutTest, accessDynamicColumnBufferTest)
{
    SchemaPtr schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = TestTupleBuffer(columnLayout, tupleBuffer);
    uint32_t numberOfRecords = 10;
    for (uint32_t i = 0; i < numberOfRecords; i++)
    {
        auto record = buffer[i];
        record[0].write<uint8_t>(i);
        record[1].write<uint16_t>(i);
        record[2].write<uint32_t>(i);
    }

    for (uint32_t i = 0; i < numberOfRecords; i++)
    {
        auto record = buffer[i];
        ASSERT_EQ(record[0].read<uint8_t>(), i);
        ASSERT_EQ(record[1].read<uint16_t>(), i);
        ASSERT_EQ(record[2].read<uint32_t>(), i);
    }
}

/**
 * @brief Tests if an error is thrown if more tuples are added to a TupleBuffer than the TupleBuffer can store.
 */
TEST_F(ColumnarMemoryLayoutTest, pushRecordTooManyRecordsColumnLayout)
{
    SchemaPtr schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    size_t i = 0;
    for (; i < NUM_TUPLES; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    for (; i < NUM_TUPLES + 1; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        ASSERT_EXCEPTION_ERRORCODE(testBuffer->pushRecordToBuffer(writeRecord), ErrorCode::CannotAccessBuffer);
    }

    for (size_t i = 0; i < NUM_TUPLES; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> readRecord = testBuffer->readRecordFromBuffer<uint8_t, uint16_t, uint32_t>(i);
        ASSERT_EQ(allTuples[i], readRecord);
    }

    ASSERT_EQ(testBuffer->getNumberOfTuples(), NUM_TUPLES);
}

}
