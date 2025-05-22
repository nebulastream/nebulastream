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
#include <memory>
#include <tuple>
#include <vector>
#include <API/Schema.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <MemoryLayout/RowLayoutField.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES::Memory::MemoryLayouts
{
class RowMemoryLayoutTest : public Testing::BaseUnitTest
{
public:
    std::shared_ptr<Memory::BufferManager> bufferManager;

    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("RowMemoryLayoutTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup RowMemoryLayoutTest test class.");
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
TEST_F(RowMemoryLayoutTest, rowLayoutCreateTest)
{
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    std::shared_ptr<RowLayout> rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);
}

/**
 * @brief Tests that the field offsets are are calculated correctly using a TestTupleBuffer.
 */
TEST_F(RowMemoryLayoutTest, rowLayoutMapCalcOffsetTest)
{
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<RowLayout> rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    const auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);

    ASSERT_EQ(testBuffer->getCapacity(), tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());
    ASSERT_EQ(testBuffer->getNumberOfTuples(), 0U);
    ASSERT_EQ(rowLayout->getFieldOffset(1, 2), (schema->getSchemaSizeInBytes() * 1) + (1 + 2));
    ASSERT_EQ(rowLayout->getFieldOffset(4, 0), (schema->getSchemaSizeInBytes() * 4) + 0);
}

/**
 * @brief Tests that we can write a single record to and read from a TestTupleBuffer correctly.
 */
TEST_F(RowMemoryLayoutTest, rowLayoutPushRecordAndReadRecordTestOneRecord)
{
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<RowLayout> rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    const auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);

    const std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(1, 2, 3);
    testBuffer->pushRecordToBuffer(writeRecord);

    const std::tuple<uint8_t, uint16_t, uint32_t> readRecord = testBuffer->readRecordFromBuffer<uint8_t, uint16_t, uint32_t>(0);

    ASSERT_EQ(writeRecord, readRecord);
    ASSERT_EQ(testBuffer->getNumberOfTuples(), 1UL);
}

/**
 * @brief Tests that we can write many records to and read from a TestTupleBuffer correctly.
 */
TEST_F(RowMemoryLayoutTest, rowLayoutPushRecordAndReadRecordTestMultipleRecord)
{
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<RowLayout> rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    const auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);

    const size_t numTuples = 230; ///tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < numTuples; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
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
TEST_F(RowMemoryLayoutTest, rowLayoutLayoutFieldSimple)
{
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<RowLayout> rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    const auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);

    const size_t numTuples = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < numTuples; ++i)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    auto field0 = RowLayoutField<uint8_t, true>::create(0, rowLayout, tupleBuffer);
    auto field1 = RowLayoutField<uint16_t, true>::create(1, rowLayout, tupleBuffer);
    auto field2 = RowLayoutField<uint32_t, true>::create(2, rowLayout, tupleBuffer);

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
TEST_F(RowMemoryLayoutTest, rowLayoutLayoutFieldBoundaryCheck)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<RowLayout> rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);

    size_t NUM_TUPLES = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    for (size_t i = 0; i < NUM_TUPLES; ++i)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    auto field0 = RowLayoutField<uint8_t, true>::create(0, rowLayout, tupleBuffer);
    auto field1 = RowLayoutField<uint16_t, true>::create(1, rowLayout, tupleBuffer);
    auto field2 = RowLayoutField<uint32_t, true>::create(2, rowLayout, tupleBuffer);

    ASSERT_DEATH_DEBUG((RowLayoutField<uint32_t, true>::create(3, rowLayout, tupleBuffer)), "");
    ASSERT_DEATH_DEBUG((RowLayoutField<uint32_t, true>::create(4, rowLayout, tupleBuffer)), "");
    ASSERT_DEATH_DEBUG((RowLayoutField<uint32_t, true>::create(5, rowLayout, tupleBuffer)), "");

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
TEST_F(RowMemoryLayoutTest, getFieldViaFieldNameRowLayout)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<RowLayout> rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    ASSERT_NO_THROW((RowLayoutField<uint8_t, true>::create("t1", rowLayout, tupleBuffer)));
    ASSERT_NO_THROW((RowLayoutField<uint16_t, true>::create("t2", rowLayout, tupleBuffer)));
    ASSERT_NO_THROW((RowLayoutField<uint32_t, true>::create("t3", rowLayout, tupleBuffer)));

    ASSERT_DEATH_DEBUG((RowLayoutField<uint32_t, true>::create("t4", rowLayout, tupleBuffer)), "");
    ASSERT_DEATH_DEBUG((RowLayoutField<uint32_t, true>::create("t5", rowLayout, tupleBuffer)), "");
    ASSERT_DEATH_DEBUG((RowLayoutField<uint32_t, true>::create("t6", rowLayout, tupleBuffer)), "");
}

/**
 * @brief Tests if an error is thrown if more tuples are added to a TupleBuffer than the TupleBuffer can store.
 */
TEST_F(RowMemoryLayoutTest, pushRecordTooManyRecordsRowLayout)
{
    const std::shared_ptr<Schema> schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    std::shared_ptr<RowLayout> rowLayout;
    ASSERT_NO_THROW(rowLayout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    const auto testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(rowLayout, tupleBuffer);

    const size_t numTuples = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();

    std::vector<std::tuple<uint8_t, uint16_t, uint32_t>> allTuples;
    size_t i = 0;
    for (; i < numTuples; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
        allTuples.emplace_back(writeRecord);
        testBuffer->pushRecordToBuffer(writeRecord);
    }

    for (; i < numTuples + 1; i++)
    {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(rand(), rand(), rand());
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

TEST_F(RowMemoryLayoutTest, getFieldOffset)
{
    const auto schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    const auto columnLayout = RowLayout::create(schema, bufferManager->getBufferSize());

    ASSERT_EXCEPTION_ERRORCODE(auto result = columnLayout->getFieldOffset(2, 4), ErrorCode::CannotAccessBuffer);
    ASSERT_EXCEPTION_ERRORCODE(auto result = columnLayout->getFieldOffset(1000000000, 2), ErrorCode::CannotAccessBuffer);
}

TEST_F(RowMemoryLayoutTest, deepCopy)
{
    const auto schema
        = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    auto rowLayout = RowLayout::create(schema, bufferManager->getBufferSize());

    const auto deepCopy = std::dynamic_pointer_cast<RowLayout>(rowLayout->deepCopy());

    ASSERT_NE(deepCopy.get(), rowLayout.get());
    ASSERT_EQ(*deepCopy, *rowLayout);

    /// checking if changing the schema does not affect the deep copy
    const auto schema2 = Schema::create()->addField("r1", BasicType::UINT8);
    rowLayout = RowLayout::create(schema2, bufferManager->getBufferSize());

    ASSERT_NE(deepCopy->getSchema(), rowLayout->getSchema());
}

}
