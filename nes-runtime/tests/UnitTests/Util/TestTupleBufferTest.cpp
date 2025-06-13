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

#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <tuple>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/StdInt.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES::Memory::MemoryLayouts
{

#define VAR_SIZED_DATA_TYPES uint16_t, std::string, double, std::string
#define FIXED_SIZED_DATA_TYPES uint16_t, bool, double

using VarSizeDataTuple = std::tuple<VAR_SIZED_DATA_TYPES>;
using FixedSizedDataTuple = std::tuple<FIXED_SIZED_DATA_TYPES>;

class TestTupleBufferTest : public Testing::BaseUnitTest, public testing::WithParamInterface<Schema::MemoryLayoutType>
{
public:
    std::shared_ptr<Memory::BufferManager> bufferManager;
    Schema schema, varSizedDataSchema;
    std::unique_ptr<TestTupleBuffer> testBuffer, testBufferVarSize;

    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("TestTupleBufferTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TestTupleBufferTest test class.");
    }
    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        const auto memoryLayout = GetParam();
        bufferManager = Memory::BufferManager::create(4096, 10);
        schema = Schema{memoryLayout}
                     .addField("test$t1", DataType::Type::UINT16)
                     .addField("test$t2", DataType::Type::BOOLEAN)
                     .addField("test$t3", DataType::Type::FLOAT64);

        varSizedDataSchema = Schema{memoryLayout}
                                 .addField("test$t1", DataType::Type::UINT16)
                                 .addField("test$t2", DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
                                 .addField("test$t3", DataType::Type::FLOAT64)
                                 .addField("test$t4", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));

        auto tupleBuffer = bufferManager->getBufferBlocking();
        auto tupleBufferVarSizedData = bufferManager->getBufferBlocking();

        testBuffer = std::make_unique<TestTupleBuffer>(TestTupleBuffer::createTestTupleBuffer(tupleBuffer, schema));
        testBufferVarSize
            = std::make_unique<TestTupleBuffer>(TestTupleBuffer::createTestTupleBuffer(tupleBufferVarSizedData, varSizedDataSchema));
    }
};

TEST_P(TestTupleBufferTest, throwErrorIfEmpty)
{
    const std::shared_ptr<Memory::BufferManager> bufferManager = Memory::BufferManager::create(4096, 2);
    auto tupleBuffer1 = bufferManager->getBufferBlocking();
    auto tupleBuffer2 = bufferManager->getBufferBlocking();
    try
    {
        auto tupleBuffer3 = bufferManager->getBufferBlocking();
    }
    catch (const Exception& e)
    {
        if (e.code() == ErrorCode::BufferAllocationFailure)
        {
            tryLogCurrentException();
            SUCCEED();
            return;
        }
    }
    FAIL();
}

TEST_P(TestTupleBufferTest, throwErrorIfEmptyAfterSequenceOfPulls)
{
    const std::shared_ptr<Memory::BufferManager> bufferManager = Memory::BufferManager::create(4096, 3);
    auto tupleBuffer1 = bufferManager->getBufferBlocking();
    auto tupleBuffer2 = bufferManager->getBufferBlocking();
    auto tupleBuffer3 = bufferManager->getBufferBlocking();

    tupleBuffer1.release();
    tupleBuffer2.release();
    tupleBuffer3.release();

    tupleBuffer1 = bufferManager->getBufferBlocking();
    tupleBuffer2 = bufferManager->getBufferBlocking();
    tupleBuffer3 = bufferManager->getBufferBlocking();

    try
    {
        auto tupleBuffer4 = bufferManager->getBufferBlocking();
    }
    catch (const Exception& e)
    {
        if (e.code() == ErrorCode::BufferAllocationFailure)
        {
            tryLogCurrentException();
            SUCCEED();
            return;
        }
    }
    FAIL();
}

TEST_P(TestTupleBufferTest, readWritetestBufferTest)
{
    /// Reading and writing a full tuple
    for (int i = 0; i < 10; ++i)
    {
        auto testTuple = std::make_tuple((uint16_t)i, true, i * 2.0);
        testBuffer->pushRecordToBuffer(testTuple);
        ASSERT_EQ((testBuffer->readRecordFromBuffer<FIXED_SIZED_DATA_TYPES>(i)), testTuple);
    }

    /// Reading and writing a full tuple via DynamicTuple and DynamicField
    for (auto i = 0; i < 10; ++i)
    {
        (*testBuffer)[i]["test$t1"].write<uint16_t>(i);
        (*testBuffer)[i]["test$t2"].write<bool>(i % 2);
        (*testBuffer)[i]["test$t3"].write<double_t>(i * 42.0);

        ASSERT_EQ((*testBuffer)[i]["test$t1"].read<uint16_t>(), i);
        ASSERT_EQ((*testBuffer)[i]["test$t2"].read<bool>(), i % 2);
        ASSERT_EQ((*testBuffer)[i]["test$t3"].read<double_t>(), i * 42.0);
    }
}

TEST_P(TestTupleBufferTest, readWritetestBufferTestVarSizeData)
{
    for (int i = 0; i < 10; ++i)
    {
        auto testTuple = std::make_tuple((uint16_t)i, "" + std::to_string(i) + std::to_string(i), i * 2.0, std::to_string(i));
        testBufferVarSize->pushRecordToBuffer(testTuple, bufferManager.get());
        ASSERT_EQ((testBufferVarSize->readRecordFromBuffer<VAR_SIZED_DATA_TYPES>(i)), testTuple);
    }

    /// Reading and writing a full tuple via DynamicTuple and DynamicField
    for (auto i = 0_u64; i < 10; ++i)
    {
        (*testBufferVarSize)[i]["test$t1"].write<uint16_t>(i);
        (*testBufferVarSize)[i]["test$t3"].write<double_t>(i * 42.0);
        (*testBufferVarSize)[i].writeVarSized("test$t2", "" + std::to_string(i) + std::to_string(i), *bufferManager);
        (*testBufferVarSize)[i].writeVarSized("test$t4", std::to_string(i), *bufferManager);

        ASSERT_EQ((*testBufferVarSize)[i]["test$t1"].read<uint16_t>(), i);
        ASSERT_EQ((*testBufferVarSize)[i]["test$t3"].read<double_t>(), i * 42.0);
        ASSERT_EQ((*testBufferVarSize)[i].readVarSized("test$t2"), "" + std::to_string(i) + std::to_string(i));
        ASSERT_EQ((*testBufferVarSize)[i].readVarSized("test$t4"), std::to_string(i));
    }
}

TEST_P(TestTupleBufferTest, readWritetestBufferTestFullBuffer)
{
    for (auto i = 0_u64; i < testBuffer->getCapacity(); ++i)
    {
        auto testTuple = std::make_tuple((uint16_t)i, true, i * 2.0);
        testBuffer->pushRecordToBuffer(testTuple);
        ASSERT_EQ((testBuffer->readRecordFromBuffer<FIXED_SIZED_DATA_TYPES>(i)), testTuple);
    }

    /// Reading and writing a full tuple via DynamicTuple and DynamicField
    for (auto i = 0_u64; i < testBuffer->getCapacity(); ++i)
    {
        (*testBuffer)[i]["test$t1"].write<uint16_t>(i);
        (*testBuffer)[i]["test$t2"].write<bool>(i % 2);
        (*testBuffer)[i]["test$t3"].write<double_t>(i * 42.0);

        ASSERT_EQ((*testBuffer)[i]["test$t1"].read<uint16_t>(), i);
        ASSERT_EQ((*testBuffer)[i]["test$t2"].read<bool>(), i % 2);
        ASSERT_EQ((*testBuffer)[i]["test$t3"].read<double_t>(), i * 42.0);
    }
}

TEST_P(TestTupleBufferTest, readWritetestBufferTestFullBufferVarSizeData)
{
    for (auto i = 0_u64; i < testBufferVarSize->getCapacity(); ++i)
    {
        auto testTuple = std::make_tuple((uint16_t)i, "" + std::to_string(i) + std::to_string(i), i * 2.0, std::to_string(i));
        testBufferVarSize->pushRecordToBuffer(testTuple, bufferManager.get());
        ASSERT_EQ((testBufferVarSize->readRecordFromBuffer<VAR_SIZED_DATA_TYPES>(i)), testTuple);
    }

    /// Reading and writing a full tuple via DynamicTuple and DynamicField
    for (auto i = 0_u64; i < testBufferVarSize->getCapacity(); ++i)
    {
        (*testBufferVarSize)[i]["test$t1"].write<uint16_t>(i);
        (*testBufferVarSize)[i]["test$t3"].write<double_t>(i * 42.0);
        (*testBufferVarSize)[i].writeVarSized("test$t2", "" + std::to_string(i) + std::to_string(i), *bufferManager);
        (*testBufferVarSize)[i].writeVarSized("test$t4", std::to_string(i), *bufferManager);

        ASSERT_EQ((*testBufferVarSize)[i]["test$t1"].read<uint16_t>(), i);
        ASSERT_EQ((*testBufferVarSize)[i]["test$t3"].read<double_t>(), i * 42.0);
        ASSERT_EQ((*testBufferVarSize)[i].readVarSized("test$t2"), "" + std::to_string(i) + std::to_string(i));
        ASSERT_EQ((*testBufferVarSize)[i].readVarSized("test$t4"), std::to_string(i));
    }
}

TEST_P(TestTupleBufferTest, countOccurrencesTest)
{
    struct TupleOccurrences
    {
        FixedSizedDataTuple tuple;
        uint64_t occurrences;
    };

    std::vector<TupleOccurrences> vec
        = {{.tuple = {1, true, rand()}, .occurrences = 5},
           {.tuple = {2, false, rand()}, .occurrences = 6},
           {.tuple = {3, false, rand()}, .occurrences = 20},
           {.tuple = {4, true, rand()}, .occurrences = 5}};

    auto posTuple = 0_u64;
    for (auto item : vec)
    {
        for (auto i = 0_u64; i < item.occurrences; ++i)
        {
            testBuffer->pushRecordToBuffer(item.tuple);
            ASSERT_EQ((testBuffer->readRecordFromBuffer<FIXED_SIZED_DATA_TYPES>(testBuffer->getNumberOfTuples() - 1)), item.tuple);
        }

        auto dynamicTuple = (*testBuffer)[posTuple];
        ASSERT_EQ(item.occurrences, testBuffer->countOccurrences(dynamicTuple));
        posTuple += item.occurrences;
    }
}

TEST_P(TestTupleBufferTest, countOccurrencesTestVarSizeData)
{
    struct TupleOccurrences
    {
        VarSizeDataTuple tuple;
        uint64_t occurrences;
    };

    std::vector<TupleOccurrences> vec
        = {{.tuple = {1, "true", rand(), "aaaaa"}, .occurrences = 5},
           {.tuple = {2, "false", rand(), "bbbbb"}, .occurrences = 6},
           {.tuple = {4, "true", rand(), "ccccc"}, .occurrences = 20},
           {.tuple = {3, "false", rand(), "ddddd"}, .occurrences = 5}};

    auto posTuple = 0_u64;
    for (auto item : vec)
    {
        for (auto i = 0_u64; i < item.occurrences; ++i)
        {
            testBufferVarSize->pushRecordToBuffer(item.tuple, bufferManager.get());
            ASSERT_EQ(
                (testBufferVarSize->readRecordFromBuffer<VAR_SIZED_DATA_TYPES>(testBufferVarSize->getNumberOfTuples() - 1)), item.tuple);
        }

        auto dynamicTuple = (*testBufferVarSize)[posTuple];
        ASSERT_EQ(item.occurrences, testBufferVarSize->countOccurrences(dynamicTuple));
        posTuple += item.occurrences;
    }
}

TEST_P(TestTupleBufferTest, DynamicTupleCompare)
{
    std::vector<std::tuple<FIXED_SIZED_DATA_TYPES>> tuples = {
        {1, true, 42}, /// 0
        {2, false, 43}, /// 1
        {1, true, 42}, /// 2
        {2, false, 43}, /// 3

    };

    for (const auto& tuple : tuples)
    {
        testBuffer->pushRecordToBuffer(tuple);
        ASSERT_EQ((testBuffer->readRecordFromBuffer<FIXED_SIZED_DATA_TYPES>(testBuffer->getNumberOfTuples() - 1)), tuple);
    }

    /// Check if the same tuple is equal to itself
    ASSERT_TRUE((*testBuffer)[0] == (*testBuffer)[0]);
    ASSERT_TRUE((*testBuffer)[1] == (*testBuffer)[1]);
    ASSERT_TRUE((*testBuffer)[2] == (*testBuffer)[2]);
    ASSERT_TRUE((*testBuffer)[3] == (*testBuffer)[3]);

    /// Check that a tuple is not equal to another tuple that is different
    ASSERT_TRUE((*testBuffer)[0] != (*testBuffer)[1]);
    ASSERT_TRUE((*testBuffer)[1] != (*testBuffer)[2]);
    ASSERT_TRUE((*testBuffer)[2] != (*testBuffer)[3]);
    ASSERT_TRUE((*testBuffer)[3] != (*testBuffer)[0]);

    /// Check if tuple have the same values but at different positions
    ASSERT_TRUE((*testBuffer)[0] == (*testBuffer)[2]);
    ASSERT_TRUE((*testBuffer)[1] == (*testBuffer)[3]);
}

TEST_P(TestTupleBufferTest, DynamicTupleCompareVarSizeData)
{
    std::vector<std::tuple<VAR_SIZED_DATA_TYPES>> tuples = {
        {1, "true", 42, "aaaaa"}, /// 0
        {2, "false", 43, "bbbbb"}, /// 1
        {1, "true", 42, "aaaaa"}, /// 2
        {2, "false", 43, "bbbbb"}, /// 3
    };

    for (auto tuple : tuples)
    {
        testBufferVarSize->pushRecordToBuffer(tuple, bufferManager.get());
        ASSERT_EQ((testBufferVarSize->readRecordFromBuffer<VAR_SIZED_DATA_TYPES>(testBufferVarSize->getNumberOfTuples() - 1)), tuple);
    }

    /// Check if the same tuple is equal to itself
    ASSERT_TRUE((*testBufferVarSize)[0] == (*testBufferVarSize)[0]);
    ASSERT_TRUE((*testBufferVarSize)[1] == (*testBufferVarSize)[1]);
    ASSERT_TRUE((*testBufferVarSize)[2] == (*testBufferVarSize)[2]);
    ASSERT_TRUE((*testBufferVarSize)[3] == (*testBufferVarSize)[3]);

    /// Check that a tuple is not equal to another tuple that is different
    ASSERT_TRUE((*testBufferVarSize)[0] != (*testBufferVarSize)[1]);
    ASSERT_TRUE((*testBufferVarSize)[1] != (*testBufferVarSize)[2]);
    ASSERT_TRUE((*testBufferVarSize)[2] != (*testBufferVarSize)[3]);
    ASSERT_TRUE((*testBufferVarSize)[3] != (*testBufferVarSize)[0]);

    /// Check if tuple have the same values but at different positions
    ASSERT_TRUE((*testBufferVarSize)[0] == (*testBufferVarSize)[2]);
    ASSERT_TRUE((*testBufferVarSize)[1] == (*testBufferVarSize)[3]);
}

INSTANTIATE_TEST_SUITE_P(
    TestInputs,
    TestTupleBufferTest,
    ::testing::Values(Schema::MemoryLayoutType::COLUMNAR_LAYOUT, Schema::MemoryLayoutType::ROW_LAYOUT),
    [](const testing::TestParamInfo<TestTupleBufferTest::ParamType>& info) { return std::string(magic_enum::enum_name(info.param)); });
}
