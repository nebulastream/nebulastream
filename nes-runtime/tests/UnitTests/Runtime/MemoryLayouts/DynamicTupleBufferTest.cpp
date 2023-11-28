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
#include <BaseIntegrationTest.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::MemoryLayouts {

#define VAR_SIZED_DATA_TYPES  uint16_t, std::string, double, std::string
#define FIXED_SIZED_DATA_TYPES  uint16_t, bool, double

using VarSizeDataTuple = std::tuple<VAR_SIZED_DATA_TYPES>;
using FixedSizedDataTuple = std::tuple<FIXED_SIZED_DATA_TYPES>;

class DynamicTupleBufferTest : public Testing::BaseUnitTest,
                               public testing::WithParamInterface<Schema::MemoryLayoutType> {
  public:
    BufferManagerPtr bufferManager;
    SchemaPtr schema, varSizedDataSchema;
    std::unique_ptr<DynamicTupleBuffer> dynamicBuffer, dynamicBufferVarSize;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("DynamicMemoryLayoutTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DynamicMemoryLayoutTest test class.");
    }
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        const auto memoryLayout = GetParam();
        bufferManager = std::make_shared<BufferManager>(4096, 10);
        schema = Schema::create(memoryLayout)
                     ->addField("test$t1", BasicType::UINT16)
                     ->addField("test$t2", BasicType::BOOLEAN)
                     ->addField("test$t3", BasicType::FLOAT64);

        varSizedDataSchema = Schema::create(memoryLayout)
                     ->addField("test$t1", BasicType::UINT16)
                     ->addField("test$t2", BasicType::TEXT)
                     ->addField("test$t3", BasicType::FLOAT64)
                     ->addField("test$t4", BasicType::TEXT);

        auto tupleBuffer = bufferManager->getBufferBlocking();
        auto tupleBufferVarSizedData = bufferManager->getBufferBlocking();

        dynamicBuffer = std::make_unique<DynamicTupleBuffer>(DynamicTupleBuffer::createDynamicTupleBuffer(tupleBuffer, schema));
        dynamicBufferVarSize = std::make_unique<DynamicTupleBuffer>(DynamicTupleBuffer::createDynamicTupleBuffer(tupleBufferVarSizedData, varSizedDataSchema));
    }
};

TEST_P(DynamicTupleBufferTest, readWriteDynamicBufferTest) {
    for (int i = 0; i < 10; ++i) {
        auto testTuple = std::make_tuple((uint16_t) i, true, i * 2.0);
        dynamicBuffer->pushRecordToBuffer(testTuple);
        ASSERT_EQ((dynamicBuffer->readRecordFromBuffer<FIXED_SIZED_DATA_TYPES>(i)), testTuple);
    }
}

TEST_P(DynamicTupleBufferTest, readWriteDynamicBufferTestVarSizeData) {
    for (int i = 0; i < 10; ++i) {
        auto testTuple = std::make_tuple((uint16_t) i, "" + std::to_string(i) + std::to_string(i), i * 2.0, std::to_string(i));
        dynamicBufferVarSize->pushRecordToBuffer(testTuple, bufferManager.get());
        ASSERT_EQ((dynamicBufferVarSize->readRecordFromBuffer<VAR_SIZED_DATA_TYPES>(i)), testTuple);
    }
}

TEST_P(DynamicTupleBufferTest, readWriteDynamicBufferTestFullBuffer) {
    for (auto i = 0_u64; i < dynamicBuffer->getCapacity(); ++i) {
        auto testTuple = std::make_tuple((uint16_t) i, true, i * 2.0);
        dynamicBuffer->pushRecordToBuffer(testTuple);
        ASSERT_EQ((dynamicBuffer->readRecordFromBuffer<FIXED_SIZED_DATA_TYPES>(i)), testTuple);
    }
}

TEST_P(DynamicTupleBufferTest, readWriteDynamicBufferTestFullBufferVarSizeData) {
    for (auto i = 0_u64; i < dynamicBufferVarSize->getCapacity(); ++i) {
        auto testTuple = std::make_tuple((uint16_t) i, "" + std::to_string(i) + std::to_string(i), i * 2.0, std::to_string(i));
        dynamicBufferVarSize->pushRecordToBuffer(testTuple, bufferManager.get());
        ASSERT_EQ((dynamicBufferVarSize->readRecordFromBuffer<VAR_SIZED_DATA_TYPES>(i)), testTuple);
    }
}

TEST_P(DynamicTupleBufferTest, countOccurrencesTest) {
    struct TupleOccurrences {
        FixedSizedDataTuple tuple;
        uint64_t occurrences;
    };

    std::vector<TupleOccurrences> vec = {
        {{1, true,  rand()},  5},
        {{2, false, rand()},  6},
        {{3, false, rand()}, 20},
        {{4, true,  rand()},  5}
    };

    auto posTuple = 0_u64;
    for (auto item : vec) {
        for (auto i = 0_u64; i < item.occurrences; ++i) {
            dynamicBuffer->pushRecordToBuffer(item.tuple);
            ASSERT_EQ((dynamicBuffer->readRecordFromBuffer<FIXED_SIZED_DATA_TYPES>(dynamicBuffer->getNumberOfTuples() - 1)), item.tuple);
        }

        auto dynamicTuple = (*dynamicBuffer)[posTuple];
        ASSERT_EQ(item.occurrences, dynamicBuffer->countOccurrences(dynamicTuple));
        posTuple += item.occurrences;
    }
}

TEST_P(DynamicTupleBufferTest, countOccurrencesTestVarSizeData) {
    struct TupleOccurrences {
        VarSizeDataTuple tuple;
        uint64_t occurrences;
    };

    std::vector<TupleOccurrences> vec = {
        {{1, "true",  rand(), "aaaaa"}, 5},
        {{2, "false", rand(), "bbbbb"}, 6},
        {{4, "true",  rand(), "ccccc"}, 20},
        {{3, "false", rand(), "ddddd"}, 5}
    };

    auto posTuple = 0_u64;
    for (auto item : vec) {
        for (auto i = 0_u64; i < item.occurrences; ++i) {
            dynamicBufferVarSize->pushRecordToBuffer(item.tuple, bufferManager.get());
            ASSERT_EQ((dynamicBufferVarSize->readRecordFromBuffer<VAR_SIZED_DATA_TYPES>(dynamicBufferVarSize->getNumberOfTuples() - 1)), item.tuple);
        }

        auto dynamicTuple = (*dynamicBufferVarSize)[posTuple];
        ASSERT_EQ(item.occurrences, dynamicBufferVarSize->countOccurrences(dynamicTuple));
        posTuple += item.occurrences;
    }
}

TEST_P(DynamicTupleBufferTest, DynamicTupleCompare) {
    std::vector<std::tuple<FIXED_SIZED_DATA_TYPES>> tuples = {
        {1, true,  42}, // 0
        {2, false, 43}, // 1
        {1, true,  42}, // 2
        {2, false, 43}, // 3

    };

    for (const auto& tuple : tuples) {
        dynamicBuffer->pushRecordToBuffer(tuple);
        ASSERT_EQ((dynamicBuffer->readRecordFromBuffer<FIXED_SIZED_DATA_TYPES>(dynamicBuffer->getNumberOfTuples() - 1)), tuple);
    }

    // Check if the same tuple is equal to itself
    ASSERT_TRUE((*dynamicBuffer)[0] == (*dynamicBuffer)[0]);
    ASSERT_TRUE((*dynamicBuffer)[1] == (*dynamicBuffer)[1]);
    ASSERT_TRUE((*dynamicBuffer)[2] == (*dynamicBuffer)[2]);
    ASSERT_TRUE((*dynamicBuffer)[3] == (*dynamicBuffer)[3]);

    // Check that a tuple is not equal to another tuple that is different
    ASSERT_TRUE((*dynamicBuffer)[0] != (*dynamicBuffer)[1]);
    ASSERT_TRUE((*dynamicBuffer)[1] != (*dynamicBuffer)[2]);
    ASSERT_TRUE((*dynamicBuffer)[2] != (*dynamicBuffer)[3]);
    ASSERT_TRUE((*dynamicBuffer)[3] != (*dynamicBuffer)[0]);

    // Check if tuple have the same values but at different positions
    ASSERT_TRUE((*dynamicBuffer)[0] == (*dynamicBuffer)[2]);
    ASSERT_TRUE((*dynamicBuffer)[1] == (*dynamicBuffer)[3]);
}


TEST_P(DynamicTupleBufferTest, DynamicTupleCompareVarSizeData) {
    std::vector<std::tuple<VAR_SIZED_DATA_TYPES>> tuples = {
        {1, "true",  42, "aaaaa"}, // 0
        {2, "false", 43, "bbbbb"}, // 1
        {1, "true",  42, "aaaaa"}, // 2
        {2, "false", 43, "bbbbb"}, // 3
    };

    for (auto tuple : tuples) {
        dynamicBufferVarSize->pushRecordToBuffer(tuple, bufferManager.get());
        ASSERT_EQ((dynamicBufferVarSize->readRecordFromBuffer<VAR_SIZED_DATA_TYPES>(dynamicBufferVarSize->getNumberOfTuples() - 1)), tuple);
    }

    // Check if the same tuple is equal to itself
    ASSERT_TRUE((*dynamicBufferVarSize)[0] == (*dynamicBufferVarSize)[0]);
    ASSERT_TRUE((*dynamicBufferVarSize)[1] == (*dynamicBufferVarSize)[1]);
    ASSERT_TRUE((*dynamicBufferVarSize)[2] == (*dynamicBufferVarSize)[2]);
    ASSERT_TRUE((*dynamicBufferVarSize)[3] == (*dynamicBufferVarSize)[3]);

    // Check that a tuple is not equal to another tuple that is different
    ASSERT_TRUE((*dynamicBufferVarSize)[0] != (*dynamicBufferVarSize)[1]);
    ASSERT_TRUE((*dynamicBufferVarSize)[1] != (*dynamicBufferVarSize)[2]);
    ASSERT_TRUE((*dynamicBufferVarSize)[2] != (*dynamicBufferVarSize)[3]);
    ASSERT_TRUE((*dynamicBufferVarSize)[3] != (*dynamicBufferVarSize)[0]);

    // Check if tuple have the same values but at different positions
    ASSERT_TRUE((*dynamicBufferVarSize)[0] == (*dynamicBufferVarSize)[2]);
    ASSERT_TRUE((*dynamicBufferVarSize)[1] == (*dynamicBufferVarSize)[3]);
}


INSTANTIATE_TEST_CASE_P(TestInputs,
                        DynamicTupleBufferTest,
                        ::testing::Values(Schema::MemoryLayoutType::COLUMNAR_LAYOUT, Schema::MemoryLayoutType::ROW_LAYOUT),
                        [](const testing::TestParamInfo<DynamicTupleBufferTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });
}// namespace NES::Runtime::MemoryLayouts