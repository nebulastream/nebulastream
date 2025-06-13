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
#include <memory>
#include <tuple>
#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <magic_enum/magic_enum.hpp>
#include <BaseUnitTest.hpp>

namespace NES::Memory::MemoryLayouts
{

class DynamicMemoryLayoutTestParameterized : public Testing::BaseUnitTest, public testing::WithParamInterface<Schema::MemoryLayoutType>
{
public:
    std::shared_ptr<Memory::BufferManager> bufferManager;
    Schema schema;
    std::unique_ptr<TestTupleBuffer> testBuffer;
    Schema::MemoryLayoutType memoryLayoutType = GetParam();

    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("DynamicMemoryLayoutTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DynamicMemoryLayoutTest test class.");
    }
    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        bufferManager = Memory::BufferManager::create(4096, 10);

        schema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT}
                     .addField("t1", DataType::Type::UINT16)
                     .addField("t2", DataType::Type::BOOLEAN)
                     .addField("t3", DataType::Type::FLOAT64);
        if (GetParam() == Schema::MemoryLayoutType::ROW_LAYOUT)
        {
            std::shared_ptr<RowLayout> layout;
            ASSERT_NO_THROW(layout = RowLayout::create(bufferManager->getBufferSize(), schema));
            ASSERT_NE(layout, nullptr);

            auto tupleBuffer = bufferManager->getBufferBlocking();
            testBuffer = std::make_unique<TestTupleBuffer>(layout, tupleBuffer);
        }
        else
        {
            std::shared_ptr<ColumnLayout> layout;
            ASSERT_NO_THROW(layout = ColumnLayout::create(bufferManager->getBufferSize(), schema));
            ASSERT_NE(layout, nullptr);

            auto tupleBuffer = bufferManager->getBufferBlocking();
            testBuffer = std::make_unique<TestTupleBuffer>(layout, tupleBuffer);
        }
    }
};

TEST_P(DynamicMemoryLayoutTestParameterized, readWriteColumnartestBufferTest)
{
    for (int i = 0; i < 10; i++)
    {
        auto testTuple = std::make_tuple(static_cast<uint16_t>(i), true, i * 2.0);
        testBuffer->pushRecordToBuffer(testTuple);
        ASSERT_EQ((testBuffer->readRecordFromBuffer<uint16_t, bool, double>(i)), testTuple);
    }
}

TEST_P(DynamicMemoryLayoutTestParameterized, iteratetestBufferTest)
{
    for (int i = 0; i < 10; i++)
    {
        testBuffer->pushRecordToBuffer(std::make_tuple(static_cast<uint16_t>(42), true, 42 * 2.0));
    }

    for (auto tuple : *testBuffer)
    {
        ASSERT_EQ(tuple[0].read<uint16_t>(), 42);
        ASSERT_EQ(tuple["t2"].read<bool>(), true);
        ASSERT_EQ(tuple["t3"].read<double>(), 42 * 2.0);
    }
}

TEST_P(DynamicMemoryLayoutTestParameterized, toStringTestRowLayout)
{
    for (uint32_t i = 0; i < 10; i++)
    {
        testBuffer->pushRecordToBuffer(std::tuple<uint16_t, bool, double>{i, true, i * 2.0});
    }

    const std::string expectedOutput = "+----------------------------------------------------+\n"
                                       "|t1:UINT16|t2:BOOLEAN|t3:FLOAT64|\n"
                                       "+----------------------------------------------------+\n"
                                       "0|1|0.0\n"
                                       "1|1|2.0\n"
                                       "2|1|4.0\n"
                                       "3|1|6.0\n"
                                       "4|1|8.0\n"
                                       "5|1|10.0\n"
                                       "6|1|12.0\n"
                                       "7|1|14.0\n"
                                       "8|1|16.0\n"
                                       "9|1|18.0\n"
                                       "+----------------------------------------------------+";

    EXPECT_EQ(testBuffer->toString(schema), expectedOutput);
}

INSTANTIATE_TEST_SUITE_P(
    TestInputs,
    DynamicMemoryLayoutTestParameterized,
    ::testing::Values(Schema::MemoryLayoutType::COLUMNAR_LAYOUT, Schema::MemoryLayoutType::ROW_LAYOUT),
    [](const testing::TestParamInfo<DynamicMemoryLayoutTestParameterized::ParamType>& info)
    { return std::string(magic_enum::enum_name(info.param)); });
}
