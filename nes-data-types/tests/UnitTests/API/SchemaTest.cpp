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
#include <memory>
#include <random>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <fmt/core.h>
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
        NES::Logger::setupLogging("SchemaTest.log", NES::LogLevel::LOG_DEBUG);
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

        std::vector<Schema::Field> rndFields;
        for (auto fieldCnt = 0_u64; fieldCnt < numberOfFields; ++fieldCnt)
        {
            const auto fieldName = fmt::format("field{}", fieldCnt);
            const auto basicType = getRandomBasicType(mt());
            rndFields.emplace_back(Schema::Field{fieldName, DataTypeProvider::provideDataType(basicType)});
        }

        return rndFields;
    }
};

TEST_F(SchemaTest, createTest)
{
    {
        /// Checking for default values
        Schema testSchema;
        ASSERT_NO_THROW(testSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT});
        ASSERT_EQ(testSchema.memoryLayoutType, Schema::MemoryLayoutType::ROW_LAYOUT);
    }

    {
        /// Checking with row memory layout
        Schema testSchema;
        ASSERT_NO_THROW(testSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT});
        ASSERT_EQ(testSchema.memoryLayoutType, Schema::MemoryLayoutType::ROW_LAYOUT);
    }

    {
        /// Checking with col memory layout
        Schema testSchema;
        ASSERT_NO_THROW(testSchema = Schema{Schema::MemoryLayoutType::COLUMNAR_LAYOUT});
        ASSERT_EQ(testSchema.memoryLayoutType, Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
    }
}

TEST_F(SchemaTest, addFieldTest)
{
    {
        /// Adding one field
        for (const auto& basicTypeVal : magic_enum::enum_values<DataType::Type>())
        {
            Schema testSchema;
            ASSERT_NO_THROW(testSchema = Schema{Schema::MemoryLayoutType::COLUMNAR_LAYOUT});
            ASSERT_EQ(testSchema.memoryLayoutType, Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
            testSchema.addField("field", basicTypeVal);
            ASSERT_EQ(testSchema.getNumberOfFields(), 1);
            ASSERT_EQ(testSchema.getFieldAt(0).name, "field");
            ASSERT_EQ(testSchema.getFieldAt(0).dataType, DataTypeProvider::provideDataType(basicTypeVal));
        }
    }

    {
        /// Adding multiple fields
        constexpr auto NUM_FIELDS = 10_u64;
        auto rndFields = getRandomFields(NUM_FIELDS);
        auto testSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
        for (const auto& field : rndFields)
        {
            testSchema.addField(field.name, field.dataType);
        }

        ASSERT_EQ(testSchema.getNumberOfFields(), rndFields.size());
        for (auto fieldCnt = 0_u64; fieldCnt < rndFields.size(); ++fieldCnt)
        {
            const auto& curField = rndFields[fieldCnt];
            EXPECT_TRUE(testSchema.getFieldAt(fieldCnt) == curField);
            EXPECT_TRUE(testSchema.getFieldByName(curField.name).has_value());
            if (testSchema.getFieldByName(curField.name).has_value())
            {
                EXPECT_TRUE(testSchema.getFieldByName(curField.name).value() == curField);
            }
        }
    }
}

TEST_F(SchemaTest, replaceFieldTest)
{
    {
        /// Replacing one field with a random one
        for (const auto& basicTypeVal : magic_enum::enum_values<DataType::Type>())
        {
            Schema testSchema;
            ASSERT_NO_THROW(testSchema = Schema{Schema::MemoryLayoutType::COLUMNAR_LAYOUT});
            ASSERT_EQ(testSchema.memoryLayoutType, Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
            testSchema.addField("field", basicTypeVal);
            ASSERT_EQ(testSchema.getNumberOfFields(), 1);
            ASSERT_EQ(testSchema.getFieldAt(0).name, "field");
            ASSERT_EQ(testSchema.getFieldAt(0).dataType, DataTypeProvider::provideDataType(basicTypeVal));

            /// Replacing field
            const auto newDataType = getRandomFields(1_u64)[0].dataType;
            ASSERT_NO_THROW(testSchema.replaceTypeOfField("field", newDataType));
            ASSERT_EQ(testSchema.getFieldAt(0).dataType, newDataType);
        }
    }

    {
        /// Adding multiple fields
        constexpr auto NUM_FIELDS = 10_u64;
        auto rndFields = getRandomFields(NUM_FIELDS);
        auto testSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
        for (const auto& field : rndFields)
        {
            testSchema.addField(field.name, field.dataType);
        }

        ASSERT_EQ(testSchema.getNumberOfFields(), rndFields.size());
        for (auto fieldCnt = 0_u64; fieldCnt < rndFields.size(); ++fieldCnt)
        {
            const auto& curField = rndFields[fieldCnt];
            EXPECT_TRUE(testSchema.getFieldAt(fieldCnt) == curField);
            EXPECT_TRUE(testSchema.getFieldByName(curField.name).has_value());
            if (testSchema.getFieldByName(curField.name).has_value())
            {
                EXPECT_TRUE(testSchema.getFieldByName(curField.name).value() == curField);
            }
        }

        /// Replacing multiple fields with new data types
        auto replacingFields = getRandomFields(NUM_FIELDS);
        for (const auto& replaceField : replacingFields)
        {
            testSchema.replaceTypeOfField(replaceField.name, replaceField.dataType);
        }

        for (auto fieldCnt = 0_u64; fieldCnt < replacingFields.size(); ++fieldCnt)
        {
            const auto& curField = replacingFields[fieldCnt];
            EXPECT_TRUE(testSchema.getFieldAt(fieldCnt) == curField);
            EXPECT_TRUE(testSchema.getFieldByName(curField.name).has_value());
            if (testSchema.getFieldByName(curField.name).has_value())
            {
                EXPECT_TRUE(testSchema.getFieldByName(curField.name).value() == curField);
            }
        }
    }
}

TEST_F(SchemaTest, getSchemaSizeInBytesTest)
{
    {
        /// Calculating the schema size for each data type
        for (const auto& basicTypeVal : magic_enum::enum_values<DataType::Type>())
        {
            Schema testSchema;
            ASSERT_NO_THROW(testSchema = Schema{Schema::MemoryLayoutType::COLUMNAR_LAYOUT});
            ASSERT_EQ(testSchema.memoryLayoutType, Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
            testSchema.addField("field", basicTypeVal);
            ASSERT_EQ(testSchema.getNumberOfFields(), 1);
            ASSERT_EQ(testSchema.getFieldAt(0).name, "field");
            ASSERT_EQ(testSchema.getFieldAt(0).dataType, DataTypeProvider::provideDataType(basicTypeVal));
            ASSERT_EQ(testSchema.getSizeOfSchemaInBytes(), DataTypeProvider::provideDataType(basicTypeVal).getSizeInBytes());
        }
    }

    {
        using enum NES::DataType::Type;
        /// Calculating the schema size for multiple fields
        auto testSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT}
                              .addField("field1", UINT8)
                              .addField("field2", UINT16)
                              .addField("field3", INT32)
                              .addField("field4", FLOAT32)
                              .addField("field5", FLOAT64);
        EXPECT_EQ(testSchema.getSizeOfSchemaInBytes(), 1 + 2 + 4 + 4 + 8);
    }
}

TEST_F(SchemaTest, containsTest)
{
    using enum NES::DataType::Type;
    {
        /// Checking contains for one fieldName
        auto testSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT}.addField("field1", UINT8);
        EXPECT_TRUE(testSchema.contains("field1"));
        EXPECT_FALSE(testSchema.contains("notExistingField1"));
    }

    {
        /// Checking contains with multiple fields
        auto testSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT}
                              .addField("field1", UINT8)
                              .addField("field2", UINT16)
                              .addField("field3", INT32)
                              .addField("field4", FLOAT32)
                              .addField("field5", FLOAT64);

        /// Existing fields
        EXPECT_TRUE(testSchema.contains("field3"));

        /// Not existing fields
        EXPECT_FALSE(testSchema.contains("notExistingField3"));
    }
}

TEST_F(SchemaTest, getFieldByNameTestInSchemaWithSourceName)
{
    using enum DataType::Type;

    /// Checking contains for one fieldName but with fields containing already a source name, e.g., after a join
    const auto testSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT}
                                .addField("streamstream2$start", UINT64)
                                .addField("streamstream2$end", UINT64)
                                .addField("stream$id", UINT64)
                                .addField("stream$value", UINT64)
                                .addField("stream$timestamp", UINT64)
                                .addField("stream2$id2", UINT64)
                                .addField("stream2$value2", UINT64)
                                .addField("stream2$timestamp", UINT64);
    EXPECT_TRUE(testSchema.getFieldByName("id"));
    EXPECT_FALSE(testSchema.getFieldByName("notExistingField1"));
}

TEST_F(SchemaTest, getSourceNameQualifierTest)
{
    using enum NES::DataType::Type;
    /// TODO once #4355 is done, we can use updateSourceName(source1) here
    const auto sourceName = std::string("source1");
    auto testSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT}
                          .addField(sourceName + "$field1", UINT8)
                          .addField(sourceName + "$field2", UINT16)
                          .addField(sourceName + "$field3", INT32)
                          .addField(sourceName + "$field4", FLOAT32)
                          .addField(sourceName + "$field5", FLOAT64);

    EXPECT_EQ(testSchema.getSourceNameQualifier(), sourceName);
}

TEST_F(SchemaTest, copyTest)
{
    const auto testSchema
        = Schema{Schema::MemoryLayoutType::ROW_LAYOUT}.addField("field1", DataType::Type::UINT8).addField("field2", DataType::Type::UINT16);
    const auto testSchemaCopy = testSchema;

    ASSERT_EQ(testSchema.getSizeOfSchemaInBytes(), testSchemaCopy.getSizeOfSchemaInBytes());
    ASSERT_EQ(testSchema.memoryLayoutType, testSchemaCopy.memoryLayoutType);
    ASSERT_EQ(testSchema.getNumberOfFields(), testSchemaCopy.getNumberOfFields());

    /// Comparing fields
    for (auto fieldCnt = 0_u64; fieldCnt < testSchemaCopy.getNumberOfFields(); ++fieldCnt)
    {
        const auto& curField = testSchemaCopy.getFieldAt(fieldCnt);
        EXPECT_TRUE(testSchema.getFieldAt(fieldCnt) == curField);
        EXPECT_TRUE(testSchema.getFieldByName(curField.name).has_value());
        if (testSchema.getFieldByName(curField.name).has_value())
        {
            EXPECT_TRUE(testSchema.getFieldByName(curField.name).value() == curField);
        }
    }
}

}
