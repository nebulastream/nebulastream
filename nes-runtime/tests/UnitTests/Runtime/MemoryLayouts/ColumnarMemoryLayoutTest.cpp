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

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <tuple>
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
    std::shared_ptr<Memory::BufferManager> bufferManager;
    std::mt19937 rng;
    std::uniform_int_distribution<std::mt19937::result_type> dist;

    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("ColumnarMemoryLayoutTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ColumnarMemoryLayoutTest test class.");
    }

    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        bufferManager = Memory::BufferManager::create(4096, 10);
        rng = std::mt19937(std::random_device()());
        dist = std::uniform_int_distribution<std::mt19937::result_type>(0, std::numeric_limits<std::mt19937::result_type>::max());
    }
};

/**
 * @brief Tests that we can construct a column layout.
 */
TEST_F(ColumnarMemoryLayoutTest, columnLayoutCreateTest)
{
    const std::shared_ptr<Schema> schema
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
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    const auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    const auto capacity = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();
    ASSERT_EQ(testBuffer->getCapacity(), capacity);
    ASSERT_EQ(testBuffer->getNumberOfTuples(), 0U);
    ASSERT_EQ(columnLayout->getFieldOffset(1, 2), (capacity * 1) + (capacity * 2 + 1 * 4));
    ASSERT_EQ(columnLayout->getFieldOffset(5, 1), (capacity * 1) + (5 * 2));
    ASSERT_EQ(columnLayout->getFieldOffset(4, 0), (capacity * 0) + 4);
}

/**
 * @brief Tests that we can write a single record to and read from a TestTupleBuffer correctly.
 */
TEST_F(ColumnarMemoryLayoutTest, columnLayoutPushRecordAndReadRecordTestOneRecord)
{
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    const auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    const std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(dist(rng), dist(rng), dist(rng));
    testBuffer->pushRecordToBuffer(writeRecord);

    const std::tuple<uint8_t, uint16_t, uint32_t> readRecord = testBuffer->readRecordFromBuffer<uint8_t, uint16_t, uint32_t>(0);

    ASSERT_EQ(writeRecord, readRecord);
    ASSERT_EQ(testBuffer->getNumberOfTuples(), 1UL);
}

/**
 * @brief Tests that we can write many records to and read from a TestTupleBuffer correctly.
 */
TEST_F(ColumnarMemoryLayoutTest, columnLayoutPushRecordAndReadRecordTestMultipleRecord)
{
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    const auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    const size_t numTuples = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < numTuples; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(dist(rng), dist(rng), dist(rng));
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    for (size_t i = 0; i < numTuples; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> readRecord = testBuffer->readRecordFromBuffer<uint8_t, uint16_t, uint32_t>(i);
        ASSERT_EQ(allTuples[i], readRecord);
    }

    ASSERT_EQ(testBuffer->getNumberOfTuples(), numTuples);
}

/**
 * @brief Tests that we can access fields of a TupleBuffer that is used in a TestTupleBuffer correctly.
 */
TEST_F(ColumnarMemoryLayoutTest, columnLayoutLayoutFieldSimple)
{
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    const auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    const size_t numTuples = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < numTuples; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(dist(rng), dist(rng), dist(rng));
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    auto field0 = ColumnLayoutField<uint8_t, true>::create(0, columnLayout, tupleBuffer);
    auto field1 = ColumnLayoutField<uint16_t, true>::create(1, columnLayout, tupleBuffer);
    auto field2 = ColumnLayoutField<uint32_t, true>::create(2, columnLayout, tupleBuffer);

    for (size_t i = 0; i < numTuples; ++i)
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
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const std::shared_ptr<Schema> schema
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
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(dist(rng), dist(rng), dist(rng));
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    auto field0 = ColumnLayoutField<uint8_t, true>::create(0, columnLayout, tupleBuffer);
    auto field1 = ColumnLayoutField<uint16_t, true>::create(1, columnLayout, tupleBuffer);
    auto field2 = ColumnLayoutField<uint32_t, true>::create(2, columnLayout, tupleBuffer);
    ASSERT_DEATH_DEBUG((ColumnLayoutField<uint8_t, true>::create(3, columnLayout, tupleBuffer)), "");
    ASSERT_DEATH_DEBUG((ColumnLayoutField<uint16_t, true>::create(4, columnLayout, tupleBuffer)), "");
    ASSERT_DEATH_DEBUG((ColumnLayoutField<uint32_t, true>::create(5, columnLayout, tupleBuffer)), "");

    size_t i = 0;
    for (; i < NUM_TUPLES; ++i)
    {
        ASSERT_EQ(std::get<0>(allTuples[i]), field0[i]);
        ASSERT_EQ(std::get<1>(allTuples[i]), field1[i]);
        ASSERT_EQ(std::get<2>(allTuples[i]), field2[i]);
    }

    ASSERT_DEATH_DEBUG(field0[i], "");
    ASSERT_DEATH_DEBUG(field1[i], "");
    ASSERT_DEATH_DEBUG(field2[i], "");

    ASSERT_DEATH_DEBUG(field0[++i], "");
    ASSERT_DEATH_DEBUG(field1[i], "");
    ASSERT_DEATH_DEBUG(field2[i], "");
}

/**
 * @brief Tests whether we can only access the correct fields.
 */
TEST_F(ColumnarMemoryLayoutTest, getFieldViaFieldNameColumnLayout)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    ASSERT_NO_THROW((ColumnLayoutField<uint8_t, true>::create("t1", columnLayout, tupleBuffer)));
    ASSERT_NO_THROW((ColumnLayoutField<uint16_t, true>::create("t2", columnLayout, tupleBuffer)));
    ASSERT_NO_THROW((ColumnLayoutField<uint32_t, true>::create("t3", columnLayout, tupleBuffer)));

    ASSERT_DEATH_DEBUG((ColumnLayoutField<uint32_t, true>::create("t4", columnLayout, tupleBuffer)), "");
    ASSERT_DEATH_DEBUG((ColumnLayoutField<uint32_t, true>::create("t5", columnLayout, tupleBuffer)), "");
    ASSERT_DEATH_DEBUG((ColumnLayoutField<uint32_t, true>::create("t6", columnLayout, tupleBuffer)), "");
}

/**
 * @brief Tests whether reading from the TestTupleBuffer works correctly.
 */
TEST_F(ColumnarMemoryLayoutTest, accessDynamicColumnBufferTest)
{
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    const auto tupleBuffer = bufferManager->getBufferBlocking();
    const auto buffer = TestTupleBuffer(columnLayout, tupleBuffer);
    const uint32_t numberOfRecords = 10;
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
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<ColumnLayout> columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    const auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    const size_t numTuples = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    size_t i = 0;
    for (; i < numTuples; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(dist(rng), dist(rng), dist(rng));
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    for (; i < numTuples + 1; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(dist(rng), dist(rng), dist(rng));
        allTuples.emplace_back(writeRecord);
        ASSERT_EXCEPTION_ERRORCODE(testBuffer->pushRecordToBuffer(writeRecord), ErrorCode::CannotAccessBuffer);
    }

    for (size_t i = 0; i < numTuples; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> readRecord = testBuffer->readRecordFromBuffer<uint8_t, uint16_t, uint32_t>(i);
        ASSERT_EQ(allTuples[i], readRecord);
    }

    ASSERT_EQ(testBuffer->getNumberOfTuples(), numTuples);
}

TEST_F(ColumnarMemoryLayoutTest, getFieldOffset)
{
    const auto schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    const auto columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());

    ASSERT_EXCEPTION_ERRORCODE(auto result = columnLayout->getFieldOffset(2, 4), ErrorCode::CannotAccessBuffer);
    ASSERT_EXCEPTION_ERRORCODE(auto result = columnLayout->getFieldOffset(1000000000, 2), ErrorCode::CannotAccessBuffer);
}

TEST_F(ColumnarMemoryLayoutTest, deepCopy)
{
    const auto schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    auto columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize());

    const auto deepCopy = columnLayout->deepCopy();

    ASSERT_NE(deepCopy, columnLayout);
    ASSERT_EQ(*deepCopy, *columnLayout);

    /// checking if changing the schema does not affect the deep copy
    const auto schema2 = Schema::create()->addField("r1", BasicType::UINT8);
    columnLayout = ColumnLayout::create(schema2, bufferManager->getBufferSize());

    ASSERT_TRUE(deepCopy->getSchema() != columnLayout->getSchema());
}

}
