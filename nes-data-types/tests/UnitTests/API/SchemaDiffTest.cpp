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
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{
using FieldMismatches = std::vector<SchemaDiff::FieldMismatch>;

class SchemaDiffTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SchemaDiffTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("SchemaDiffTest test class SetUpTestCase.");
    }

    static void TearDownTestSuite() { NES_INFO("SchemaDiffTest test class TearDownTestCase."); }
};

TEST_F(SchemaDiffTest, identicalSchemasIsDifferentTest)
{
    /// Test 1: Identical schemas - should return false
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);
    expectedSchema.addField("field2", DataType::Type::FLOAT32);

    Schema actualSchema;
    actualSchema.addField("field1", DataType::Type::INT64);
    actualSchema.addField("field2", DataType::Type::FLOAT32);

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);
    EXPECT_FALSE(diff.isDifferent());
}

TEST_F(SchemaDiffTest, missingActualFieldsIsDifferentTest)
{
    /// Test 2: Schema with missing fields - should return true
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);
    expectedSchema.addField("field2", DataType::Type::FLOAT32);

    Schema actualSchema;
    actualSchema.addField("field1", DataType::Type::INT64);
    /// field2 is missing

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);
    EXPECT_TRUE(diff.isDifferent());
}

TEST_F(SchemaDiffTest, additionalActualFieldsIsDifferentTest)
{
    /// Test 3: Schema with extra fields - should return true
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);

    Schema actualSchema;
    actualSchema.addField("field1", DataType::Type::INT64);
    actualSchema.addField("field2", DataType::Type::FLOAT32); /// Extra field

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);
    EXPECT_TRUE(diff.isDifferent());
}

TEST_F(SchemaDiffTest, typeMismatchIsDifferentTest)
{
    /// Test 4: Schema with type mismatches - should return true
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);

    Schema actualSchema;
    actualSchema.addField("field1", DataType::Type::INT32); /// Different type

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);
    EXPECT_TRUE(diff.isDifferent());
}

TEST_F(SchemaDiffTest, emptySchemasIsDifferentTest)
{
    /// Test 5: Empty schemas - should return false
    const Schema expectedSchema;
    const Schema actualSchema;

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);
    EXPECT_FALSE(diff.isDifferent());
}

TEST_F(SchemaDiffTest, emptyActualSchemaIsDifferentTest)
{
    /// Test 6: One empty schema - should return true
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);

    const Schema actualSchema;

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);
    EXPECT_TRUE(diff.isDifferent());
}

TEST_F(SchemaDiffTest, multipleMismatchesIsDifferentTest)
{
    /// Test 7: Complex scenario with multiple differences - should return true
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);
    expectedSchema.addField("field2", DataType::Type::FLOAT32);
    expectedSchema.addField("field3", DataType::Type::BOOLEAN);

    Schema actualSchema;
    actualSchema.addField("field1", DataType::Type::INT32); /// Type mismatch
    actualSchema.addField("field2", DataType::Type::FLOAT32); /// Same
    /// field3 is missing
    actualSchema.addField("field4", DataType::Type::VARSIZED); /// Extra field

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);
    EXPECT_TRUE(diff.isDifferent());
}

TEST_F(SchemaDiffTest, identicalSchemasTest)
{
    /// Test 8: Identical schemas - should return empty diff
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);
    expectedSchema.addField("field2", DataType::Type::FLOAT32);
    expectedSchema.addField("field3", DataType::Type::BOOLEAN);

    Schema actualSchema;
    actualSchema.addField("field1", DataType::Type::INT64);
    actualSchema.addField("field2", DataType::Type::FLOAT32);
    actualSchema.addField("field3", DataType::Type::BOOLEAN);

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);

    EXPECT_TRUE(std::holds_alternative<SchemaDiff::NoMismatch>(diff.result));
}

TEST_F(SchemaDiffTest, missingActualFieldsTest)
{
    /// Test 9: Schema with missing fields in actual - should cause a size mismatch.
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);
    expectedSchema.addField("field2", DataType::Type::FLOAT32);
    expectedSchema.addField("field3", DataType::Type::BOOLEAN);

    Schema actualSchema;
    actualSchema.addField("field1", DataType::Type::INT64);
    /// field2 is missing
    actualSchema.addField("field3", DataType::Type::BOOLEAN);

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);

    /// We do not check for field mismatches if a size mismatch is detected.
    ASSERT_TRUE(std::holds_alternative<SchemaDiff::SizeMismatch>(diff.result));

    auto sizeMismatch = std::get<1>(diff.result);
    EXPECT_EQ(sizeMismatch.expectedSize, 3);
    EXPECT_EQ(sizeMismatch.actualSize, 2);
}

TEST_F(SchemaDiffTest, additionalActualFieldsTest)
{
    /// Test 10: Schema with extra fields in actual - should cause size mismatch.
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);
    expectedSchema.addField("field2", DataType::Type::FLOAT32);

    Schema actualSchema;
    actualSchema.addField("field1", DataType::Type::INT64);
    actualSchema.addField("field2", DataType::Type::FLOAT32);
    actualSchema.addField("field3", DataType::Type::BOOLEAN); /// Extra field

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);

    ASSERT_TRUE(std::holds_alternative<SchemaDiff::SizeMismatch>(diff.result));

    auto sizeMismatch = std::get<1>(diff.result);
    EXPECT_EQ(sizeMismatch.expectedSize, 2);
    EXPECT_EQ(sizeMismatch.actualSize, 3);
}

TEST_F(SchemaDiffTest, typeMismatchesTest)
{
    /// Test 11: Schema with type mismatches - should show modified fields
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);
    expectedSchema.addField("field2", DataType::Type::FLOAT32);

    Schema actualSchema;
    actualSchema.addField("field1", DataType::Type::INT32); /// Different type
    actualSchema.addField("field2", DataType::Type::FLOAT32);

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);

    ASSERT_TRUE(std::holds_alternative<FieldMismatches>(diff.result));

    auto fieldMismatches = std::get<2>(diff.result);
    EXPECT_EQ(fieldMismatches.size(), 1);
    EXPECT_EQ(fieldMismatches[0].index, 0);
    EXPECT_EQ(fieldMismatches[0].expectedField.name, "field1");
    EXPECT_EQ(fieldMismatches[0].expectedField.dataType.type, DataType::Type::INT64);
    EXPECT_EQ(fieldMismatches[0].actualField.name, "field1");
    EXPECT_EQ(fieldMismatches[0].actualField.dataType.type, DataType::Type::INT32);
}

TEST_F(SchemaDiffTest, identicalSchemasWithDuplicateFieldsTest)
{
    /// Test 12: Schema with duplicate fields - should handle duplicates correctly
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);
    expectedSchema.addField("field1", DataType::Type::INT64); /// Duplicate
    expectedSchema.addField("field2", DataType::Type::FLOAT32);

    Schema actualSchema;
    actualSchema.addField("field1", DataType::Type::INT64);
    actualSchema.addField("field1", DataType::Type::INT64); /// Duplicate
    actualSchema.addField("field2", DataType::Type::FLOAT32);

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);

    EXPECT_TRUE(std::holds_alternative<SchemaDiff::NoMismatch>(diff.result));
}

TEST_F(SchemaDiffTest, schemasWithDuplicateFieldsAndDifferentSizeTest)
{
    /// Test 13: Schema with duplicate fields but different counts - should show size mismatch
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);
    expectedSchema.addField("field1", DataType::Type::INT64); /// Two instances

    Schema actualSchema;
    actualSchema.addField("field1", DataType::Type::INT64); /// Only one instance

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);

    ASSERT_TRUE(std::holds_alternative<SchemaDiff::SizeMismatch>(diff.result));

    auto sizeMismatch = std::get<1>(diff.result);
    EXPECT_EQ(sizeMismatch.expectedSize, 2);
    EXPECT_EQ(sizeMismatch.actualSize, 1);
}

TEST_F(SchemaDiffTest, schemasWithDuplicateFieldsAndTypeMismatchTest)
{
    /// Test 14: Schema with duplicate fields and type mismatches
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);
    expectedSchema.addField("field1", DataType::Type::INT64);

    Schema actualSchema;
    actualSchema.addField("field1", DataType::Type::INT32); /// Different type
    actualSchema.addField("field1", DataType::Type::INT64);

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);

    ASSERT_TRUE(std::holds_alternative<FieldMismatches>(diff.result));

    auto fieldMismatches = std::get<2>(diff.result);
    EXPECT_EQ(fieldMismatches.size(), 1);
    EXPECT_EQ(fieldMismatches[0].index, 0);
    EXPECT_EQ(fieldMismatches[0].expectedField.name, "field1");
    EXPECT_EQ(fieldMismatches[0].expectedField.dataType.type, DataType::Type::INT64);
    EXPECT_EQ(fieldMismatches[0].actualField.name, "field1");
    EXPECT_EQ(fieldMismatches[0].actualField.dataType.type, DataType::Type::INT32);
}

TEST_F(SchemaDiffTest, emptySchemasTest)
{
    /// Test 15: Empty schemas
    const Schema expectedSchema;
    const Schema actualSchema;

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);

    EXPECT_TRUE(std::holds_alternative<SchemaDiff::NoMismatch>(diff.result));
}

TEST_F(SchemaDiffTest, emptyActualSchemaTest)
{
    /// Test 16: One empty schema
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);

    const Schema actualSchema;

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);

    ASSERT_TRUE(std::holds_alternative<SchemaDiff::SizeMismatch>(diff.result));

    auto sizeMismatch = std::get<1>(diff.result);
    EXPECT_EQ(sizeMismatch.expectedSize, 1);
    EXPECT_EQ(sizeMismatch.actualSize, 0);
}

TEST_F(SchemaDiffTest, multipleMismatchesTest)
{
    /// Test 17: Complex schema with multiple differences
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);
    expectedSchema.addField("field2", DataType::Type::FLOAT32);
    expectedSchema.addField("field3", DataType::Type::BOOLEAN);
    expectedSchema.addField("field4", DataType::Type::VARSIZED);

    Schema actualSchema;
    actualSchema.addField("field1", DataType::Type::INT32); /// Type mismatch
    actualSchema.addField("field2", DataType::Type::FLOAT32); /// Same
    actualSchema.addField("field4", DataType::Type::VARSIZED); /// Name mismatch
    actualSchema.addField("field4", DataType::Type::VARSIZED); /// Same

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);

    ASSERT_TRUE(std::holds_alternative<FieldMismatches>(diff.result));

    auto fieldMismatches = std::get<2>(diff.result);
    EXPECT_EQ(fieldMismatches.size(), 2);
    EXPECT_EQ(fieldMismatches[0].index, 0);
    EXPECT_EQ(fieldMismatches[0].expectedField.name, "field1");
    EXPECT_EQ(fieldMismatches[0].expectedField.dataType.type, DataType::Type::INT64);
    EXPECT_EQ(fieldMismatches[0].actualField.name, "field1");
    EXPECT_EQ(fieldMismatches[0].actualField.dataType.type, DataType::Type::INT32);

    EXPECT_EQ(fieldMismatches[1].index, 2);
    EXPECT_EQ(fieldMismatches[1].expectedField.name, "field3");
    EXPECT_EQ(fieldMismatches[1].expectedField.dataType.type, DataType::Type::BOOLEAN);
    EXPECT_EQ(fieldMismatches[1].actualField.name, "field4");
    EXPECT_EQ(fieldMismatches[1].actualField.dataType.type, DataType::Type::VARSIZED);
}

TEST_F(SchemaDiffTest, sameFieldsDifferentOrderTest)
{
    /// Test 18: Fields out of order - should be treated like mismatches
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);
    expectedSchema.addField("field2", DataType::Type::FLOAT32);
    expectedSchema.addField("field3", DataType::Type::BOOLEAN);

    Schema actualSchema;
    actualSchema.addField("field3", DataType::Type::BOOLEAN); /// Different order
    actualSchema.addField("field1", DataType::Type::INT64); /// Different order
    actualSchema.addField("field2", DataType::Type::FLOAT32); /// Different order

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);

    ASSERT_TRUE(std::holds_alternative<FieldMismatches>(diff.result));

    auto fieldMismatches = std::get<2>(diff.result);
    EXPECT_EQ(fieldMismatches.size(), 3);
    EXPECT_EQ(fieldMismatches[0].index, 0);
    EXPECT_EQ(fieldMismatches[0].expectedField.name, "field1");
    EXPECT_EQ(fieldMismatches[0].expectedField.dataType.type, DataType::Type::INT64);
    EXPECT_EQ(fieldMismatches[0].actualField.name, "field3");
    EXPECT_EQ(fieldMismatches[0].actualField.dataType.type, DataType::Type::BOOLEAN);

    EXPECT_EQ(fieldMismatches[1].index, 1);
    EXPECT_EQ(fieldMismatches[1].expectedField.name, "field2");
    EXPECT_EQ(fieldMismatches[1].expectedField.dataType.type, DataType::Type::FLOAT32);
    EXPECT_EQ(fieldMismatches[1].actualField.name, "field1");
    EXPECT_EQ(fieldMismatches[1].actualField.dataType.type, DataType::Type::INT64);

    EXPECT_EQ(fieldMismatches[2].index, 2);
    EXPECT_EQ(fieldMismatches[2].expectedField.name, "field3");
    EXPECT_EQ(fieldMismatches[2].expectedField.dataType.type, DataType::Type::BOOLEAN);
    EXPECT_EQ(fieldMismatches[2].actualField.name, "field2");
    EXPECT_EQ(fieldMismatches[2].actualField.dataType.type, DataType::Type::FLOAT32);
}

TEST_F(SchemaDiffTest, sameFieldsDifferentOrderWithDuplicatesTest)
{
    /// Test 19: Fields out of order with duplicates - Should cause mismatches
    Schema expectedSchema;
    expectedSchema.addField("field1", DataType::Type::INT64);
    expectedSchema.addField("field1", DataType::Type::INT64); /// Duplicate
    expectedSchema.addField("field2", DataType::Type::FLOAT32);

    Schema actualSchema;
    actualSchema.addField("field2", DataType::Type::FLOAT32); /// mismatch
    actualSchema.addField("field1", DataType::Type::INT64); /// same
    actualSchema.addField("field1", DataType::Type::INT64); /// mismatch

    auto diff = SchemaDiff::of(expectedSchema, actualSchema);

    ASSERT_TRUE(std::holds_alternative<FieldMismatches>(diff.result));

    auto fieldMismatches = std::get<2>(diff.result);
    EXPECT_EQ(fieldMismatches.size(), 2);
    EXPECT_EQ(fieldMismatches[0].index, 0);
    EXPECT_EQ(fieldMismatches[0].expectedField.name, "field1");
    EXPECT_EQ(fieldMismatches[0].expectedField.dataType.type, DataType::Type::INT64);
    EXPECT_EQ(fieldMismatches[0].actualField.name, "field2");
    EXPECT_EQ(fieldMismatches[0].actualField.dataType.type, DataType::Type::FLOAT32);

    EXPECT_EQ(fieldMismatches[1].index, 2);
    EXPECT_EQ(fieldMismatches[1].expectedField.name, "field2");
    EXPECT_EQ(fieldMismatches[1].expectedField.dataType.type, DataType::Type::FLOAT32);
    EXPECT_EQ(fieldMismatches[1].actualField.name, "field1");
    EXPECT_EQ(fieldMismatches[1].actualField.dataType.type, DataType::Type::INT64);
}
}
