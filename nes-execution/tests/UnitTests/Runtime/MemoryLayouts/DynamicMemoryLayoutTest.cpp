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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Util/magicenum/magic_enum.hpp>
namespace NES::Runtime::MemoryLayouts {

class DynamicMemoryLayoutTestParameterized : public Testing::BaseUnitTest,
                                             public testing::WithParamInterface<Schema::MemoryLayoutType> {
  public:
    BufferManagerPtr bufferManager;
    SchemaPtr schema;
    std::unique_ptr<DynamicTupleBuffer> dynamicBuffer;
    Schema::MemoryLayoutType memoryLayoutType = GetParam();

    static void SetUpTestCase() {
        NES::Logger::setupLogging("DynamicMemoryLayoutTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DynamicMemoryLayoutTest test class.");
    }
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        bufferManager = std::make_shared<BufferManager>(4096, 10);

        schema = Schema::create()
                     ->addField("t1", BasicType::UINT16)
                     ->addField("t2", BasicType::BOOLEAN)
                     ->addField("t3", BasicType::FLOAT64);
        if (GetParam() == Schema::MemoryLayoutType::ROW_LAYOUT) {
            RowLayoutPtr layout;
            ASSERT_NO_THROW(layout = RowLayout::create(schema, bufferManager->getBufferSize()));
            ASSERT_NE(layout, nullptr);

            auto tupleBuffer = bufferManager->getBufferBlocking();
            dynamicBuffer = std::make_unique<DynamicTupleBuffer>(layout, tupleBuffer);
        } else {
            ColumnLayoutPtr layout;
            ASSERT_NO_THROW(layout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
            ASSERT_NE(layout, nullptr);

            auto tupleBuffer = bufferManager->getBufferBlocking();
            dynamicBuffer = std::make_unique<DynamicTupleBuffer>(layout, tupleBuffer);
        }
    }
};

TEST_P(DynamicMemoryLayoutTestParameterized, readWriteColumnarDynamicBufferTest) {
    for (int i = 0; i < 10; i++) {
        auto testTuple = std::make_tuple((uint16_t) i, true, i * 2.0);
        dynamicBuffer->pushRecordToBuffer(testTuple);
        ASSERT_EQ((dynamicBuffer->readRecordFromBuffer<uint16_t, bool, double>(i)), testTuple);
    }
}

TEST_P(DynamicMemoryLayoutTestParameterized, iterateDynamicBufferTest) {
    for (int i = 0; i < 10; i++) {
        dynamicBuffer->pushRecordToBuffer(std::make_tuple(42_u16, true, 42 * 2.0));
    }

    for (auto tuple : *dynamicBuffer) {
        ASSERT_EQ(tuple[0].read<uint16_t>(), 42);
        ASSERT_EQ(tuple["t2"].read<bool>(), true);
        ASSERT_EQ(tuple["t3"].read<double>(), 42 * 2.0);
    }
}

TEST_P(DynamicMemoryLayoutTestParameterized, toStringTestRowLayout) {
    for (uint32_t i = 0; i < 10; i++) {
        dynamicBuffer->pushRecordToBuffer(std::tuple<uint16_t, bool, double>{i, true, i * 2.0});
    }

    std::string expectedOutput = "+----------------------------------------------------+\n"
                                 "|t1:UINT16|t2:BOOLEAN|t3:FLOAT64|\n"
                                 "+----------------------------------------------------+\n"
                                 "|0|1|0.000000|\n"
                                 "|1|1|2.000000|\n"
                                 "|2|1|4.000000|\n"
                                 "|3|1|6.000000|\n"
                                 "|4|1|8.000000|\n"
                                 "|5|1|10.000000|\n"
                                 "|6|1|12.000000|\n"
                                 "|7|1|14.000000|\n"
                                 "|8|1|16.000000|\n"
                                 "|9|1|18.000000|\n"
                                 "+----------------------------------------------------+";

    EXPECT_EQ(dynamicBuffer->toString(schema), expectedOutput);
}

INSTANTIATE_TEST_CASE_P(TestInputs,
                        DynamicMemoryLayoutTestParameterized,
                        ::testing::Values(Schema::MemoryLayoutType::COLUMNAR_LAYOUT, Schema::MemoryLayoutType::ROW_LAYOUT),
                        [](const testing::TestParamInfo<DynamicMemoryLayoutTestParameterized::ParamType>& info) {
                            return std::string(magic_enum::enum_name(info.param));
                        });
}// namespace NES::Runtime::MemoryLayouts
