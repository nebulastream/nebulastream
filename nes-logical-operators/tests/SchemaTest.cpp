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
#include <random>
#include <vector>
#include <Schema/Schema.hpp>
#include <Schema/Field.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/StdInt.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{
class SchemaTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("SchemaTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("SchemaTest test class SetUpTestCase.");
    }

    static void TearDownTestCase() { NES_INFO("SchemaTest test class TearDownTestCase."); }

    auto getRandomFields(const auto numberOfFields)
    {
        auto getRandomBasicType = [](const unsigned long rndPos)
        {
            const auto& values = magic_enum::enum_values<DataType::Type>();
            std::uniform_int_distribution<unsigned long>(0, values.size() - 1);
            return values[rndPos % values.size()];
        };
        constexpr auto RND_SEED = 42;
        std::random_device rd;
        std::mt19937 mt(RND_SEED);

        std::vector<Field> rndFields;
        for (auto fieldCnt = 0_u64; fieldCnt < numberOfFields; ++fieldCnt)
        {
            const auto fieldName = fmt::format("field{}", fieldCnt);
            const auto basicType = getRandomBasicType(mt());
            rndFields.emplace_back(Field{fieldName, DataTypeProvider::provideDataType(basicType)});
        }

        return rndFields;
    }
};



// TEST_F(SchemaTest, getSchemaSizeInBytesTest)
// {
//     {
//         /// Calculating the schema size for each data type
//         for (const auto& basicTypeVal : magic_enum::enum_values<DataType::Type>())
//         {
//             Schema testSchema;
//             ASSERT_NO_THROW(testSchema = Schema{Schema::MemoryLayoutType::COLUMNAR_LAYOUT});
//             ASSERT_EQ(testSchema.memoryLayoutType, Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
//             testSchema.addField("field", basicTypeVal);
//             ASSERT_EQ(testSchema.getNumberOfFields(), 1);
//             ASSERT_EQ(testSchema.getFieldAt(0).name, "field");
//             ASSERT_EQ(testSchema.getFieldAt(0).dataType, DataTypeProvider::provideDataType(basicTypeVal));
//             ASSERT_EQ(testSchema.getSizeOfSchemaInBytes(), DataTypeProvider::provideDataType(basicTypeVal).getSizeInBytes());
//         }
//     }
//
//     {
//         using enum DataType::Type;
//         /// Calculating the schema size for multiple fields
//         auto testSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT}
//                               .addField("field1", UINT8)
//                               .addField("field2", UINT16)
//                               .addField("field3", INT32)
//                               .addField("field4", FLOAT32)
//                               .addField("field5", FLOAT64);
//         EXPECT_EQ(testSchema.getSizeOfSchemaInBytes(), 1 + 2 + 4 + 4 + 8);
//     }
// }


}
