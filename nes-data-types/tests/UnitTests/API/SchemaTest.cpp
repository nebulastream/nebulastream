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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

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
            const auto& values = magic_enum::enum_values<BasicType>();
            std::uniform_int_distribution<unsigned long>(0, values.size() - 1);
            return values[rndPos % values.size()];
        };
        constexpr auto RND_SEED = 42;
        std::random_device rd;
        std::mt19937 mt(RND_SEED);

        std::vector<std::shared_ptr<AttributeField>> rndFields;
        for (auto fieldCnt = 0_u64; fieldCnt < numberOfFields; ++fieldCnt)
        {
            const auto fieldName = fmt::format("field{}", fieldCnt);
            const auto basicType = getRandomBasicType(mt());
            rndFields.emplace_back(AttributeField::create(fieldName, DataTypeProvider::provideBasicType(basicType)));
        }

        return rndFields;
    }
};

TEST_F(SchemaTest, createTest)
{
    {
        /// Checking for default values
        std::shared_ptr<Schema> testSchema;
        ASSERT_NO_THROW(testSchema = Schema::create());
        ASSERT_TRUE(testSchema);
        ASSERT_EQ(testSchema->getLayoutType(), Schema::MemoryLayoutType::ROW_LAYOUT);
    }

    {
        /// Checking with row memory layout
        std::shared_ptr<Schema> testSchema;
        ASSERT_NO_THROW(testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT));
        ASSERT_TRUE(testSchema);
        ASSERT_EQ(testSchema->getLayoutType(), Schema::MemoryLayoutType::ROW_LAYOUT);
    }

    {
        /// Checking with col memory layout
        std::shared_ptr<Schema> testSchema;
        ASSERT_NO_THROW(testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT));
        ASSERT_TRUE(testSchema);
        ASSERT_EQ(testSchema->getLayoutType(), Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
    }
}

TEST_F(SchemaTest, addFieldTest)
{
    {
        /// Adding one field
        for (const auto& basicTypeVal : magic_enum::enum_values<BasicType>())
        {
            std::shared_ptr<Schema> testSchema;
            ASSERT_NO_THROW(testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT));
            ASSERT_EQ(testSchema->getLayoutType(), Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
            ASSERT_TRUE(testSchema->addField("field", basicTypeVal));
            ASSERT_EQ(testSchema->getFieldCount(), 1);
            ASSERT_EQ(testSchema->getFieldByIndex(0)->getName(), "field");
            ASSERT_EQ(*testSchema->getFieldByIndex(0)->getDataType(), *DataTypeProvider::provideBasicType(basicTypeVal));
        }
    }

    {
        /// Adding multiple fields
        constexpr auto NUM_FIELDS = 10_u64;
        auto rndFields = getRandomFields(NUM_FIELDS);
        auto testSchema = Schema::create();
        for (const auto& field : rndFields)
        {
            ASSERT_TRUE(testSchema->addField(field));
        }

        ASSERT_EQ(testSchema->getFieldCount(), rndFields.size());
        for (auto fieldCnt = 0_u64; fieldCnt < rndFields.size(); ++fieldCnt)
        {
            const auto& curField = rndFields[fieldCnt];
            EXPECT_TRUE(testSchema->getFieldByIndex(fieldCnt)->isEqual(curField));
            EXPECT_TRUE(testSchema->getFieldByName(curField->getName()).has_value());
            if (testSchema->getFieldByName(curField->getName()).has_value())
            {
                EXPECT_TRUE(testSchema->getFieldByName(curField->getName()).value()->isEqual(curField));
            }
        }
    }
}

/// Test for the method getFieldByName that calling getFieldByName(bid$start) and getFieldByName(bidbid$start) return two different fields
TEST_F(SchemaTest, getFieldByNameWithSimilarFieldNames)
{
    const auto field1 = AttributeField::create("bidbid$start", DataTypeProvider::provideBasicType(BasicType::UINT64));
    const auto field2 = AttributeField::create("bidbid$end", DataTypeProvider::provideBasicType(BasicType::UINT64));
    const auto field3 = AttributeField::create("bid$start", DataTypeProvider::provideBasicType(BasicType::UINT64));
    const auto field4 = AttributeField::create("auction$id", DataTypeProvider::provideBasicType(BasicType::UINT64));
    const auto field5 = AttributeField::create("auction$initialbid", DataTypeProvider::provideBasicType(BasicType::UINT64));

    const auto schemaUnderTest = Schema::create()->addField(field1)->addField(field2)->addField(field3)->addField(field4)->addField(field5);

    const auto fieldByName1 = schemaUnderTest->getFieldByName("bidbid$start");
    const auto fieldByName2 = schemaUnderTest->getFieldByName("end");
    const auto fieldByName3 = schemaUnderTest->getFieldByName(field3->getName());
    const auto fieldByName4 = schemaUnderTest->getFieldByName("id");
    const auto fieldByName5 = schemaUnderTest->getFieldByName("initialbid");

    EXPECT_TRUE(field1->isEqual(fieldByName1.value()))
        << "Field 1 " << field1->toString() << " and field by name 1 " << fieldByName1.value()->toString() << " are not equal";
    EXPECT_TRUE(field2->isEqual(fieldByName2.value()))
        << "Field 2 " << field2->toString() << " and field by name 2 " << fieldByName2.value()->toString() << " are not equal";
    EXPECT_TRUE(field3->isEqual(fieldByName3.value()))
        << "Field 3 " << field3->toString() << " and field by name 3 " << fieldByName3.value()->toString() << " are not equal";
    EXPECT_TRUE(field4->isEqual(fieldByName4.value()))
        << "Field 4 " << field4->toString() << " and field by name 4 " << fieldByName4.value()->toString() << " are not equal";
    EXPECT_TRUE(field5->isEqual(fieldByName5.value()))
        << "Field 5 " << field5->toString() << " and field by name 5 " << fieldByName5.value()->toString() << " are not equal";

    NES_INFO("fieldByName1: {}", fieldByName1.value()->toString());
    NES_INFO("fieldByName2: {}", fieldByName2.value()->toString());
    NES_INFO("fieldByName3: {}", fieldByName3.value()->toString());
    NES_INFO("fieldByName4: {}", fieldByName4.value()->toString());
    NES_INFO("fieldByName5: {}", fieldByName5.value()->toString());
}

TEST_F(SchemaTest, removeFieldsTest)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    constexpr auto NUM_FIELDS = 10_u64;
    auto rndFields = getRandomFields(NUM_FIELDS);
    auto testSchema = Schema::create();
    for (const auto& field : rndFields)
    {
        ASSERT_TRUE(testSchema->addField(field));
    }

    ASSERT_EQ(testSchema->getFieldCount(), rndFields.size());
    for (auto fieldCnt = 0_u64; fieldCnt < rndFields.size(); ++fieldCnt)
    {
        const auto& curField = rndFields[fieldCnt];
        EXPECT_TRUE(testSchema->getFieldByIndex(fieldCnt)->isEqual(curField));
        EXPECT_TRUE(testSchema->getFieldByName(curField->getName()).has_value());
        if (testSchema->getFieldByName(curField->getName()).has_value())
        {
            EXPECT_TRUE(testSchema->getFieldByName(curField->getName()).value()->isEqual(curField));
        }
    }

    /// Removing fields while we still have one field
    while (testSchema->getFieldCount() > 0)
    {
        const auto rndPos = rand() % testSchema->getFieldCount();
        const auto& fieldToRemove = rndFields[rndPos];
        EXPECT_NO_THROW(testSchema->removeField(fieldToRemove));
        if (testSchema->getFieldCount() < 1)
        {
            EXPECT_DEATH_DEBUG([&]() { auto field = testSchema->getFieldByName(fieldToRemove->getName()); }(), "");
        }
        else
        {
            EXPECT_FALSE(testSchema->getFieldByName(fieldToRemove->getName()));
        }

        rndFields.erase(rndFields.begin() + rndPos);
    }
}

TEST_F(SchemaTest, replaceFieldTest)
{
    {
        /// Replacing one field with a random one
        for (const auto& basicTypeVal : magic_enum::enum_values<BasicType>())
        {
            std::shared_ptr<Schema> testSchema;
            ASSERT_NO_THROW(testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT));
            ASSERT_EQ(testSchema->getLayoutType(), Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
            ASSERT_TRUE(testSchema->addField("field", basicTypeVal));
            ASSERT_EQ(testSchema->getFieldCount(), 1);
            ASSERT_EQ(testSchema->getFieldByIndex(0)->getName(), "field");
            ASSERT_EQ(*testSchema->getFieldByIndex(0)->getDataType(), *DataTypeProvider::provideBasicType(basicTypeVal));

            /// Replacing field
            const auto newDataType = getRandomFields(1_u64)[0]->getDataType();
            ASSERT_NO_THROW(testSchema->replaceField("field", newDataType));
            ASSERT_EQ(*testSchema->getFieldByIndex(0)->getDataType(), *newDataType);
        }
    }

    {
        /// Adding multiple fields
        constexpr auto NUM_FIELDS = 10_u64;
        auto rndFields = getRandomFields(NUM_FIELDS);
        auto testSchema = Schema::create();
        for (const auto& field : rndFields)
        {
            ASSERT_TRUE(testSchema->addField(field));
        }

        ASSERT_EQ(testSchema->getFieldCount(), rndFields.size());
        for (auto fieldCnt = 0_u64; fieldCnt < rndFields.size(); ++fieldCnt)
        {
            const auto& curField = rndFields[fieldCnt];
            EXPECT_TRUE(testSchema->getFieldByIndex(fieldCnt)->isEqual(curField));
            EXPECT_TRUE(testSchema->getFieldByName(curField->getName()).has_value());
            if (testSchema->getFieldByName(curField->getName()).has_value())
            {
                EXPECT_TRUE(testSchema->getFieldByName(curField->getName()).value()->isEqual(curField));
            }
        }

        /// Replacing multiple fields with new data types
        auto replacingFields = getRandomFields(NUM_FIELDS);
        for (const auto& replaceField : replacingFields)
        {
            testSchema->replaceField(replaceField->getName(), replaceField->getDataType());
        }

        for (auto fieldCnt = 0_u64; fieldCnt < replacingFields.size(); ++fieldCnt)
        {
            const auto& curField = replacingFields[fieldCnt];
            EXPECT_TRUE(testSchema->getFieldByIndex(fieldCnt)->isEqual(curField));
            EXPECT_TRUE(testSchema->getFieldByName(curField->getName()).has_value());
            if (testSchema->getFieldByName(curField->getName()).has_value())
            {
                EXPECT_TRUE(testSchema->getFieldByName(curField->getName()).value()->isEqual(curField));
            }
        }
    }
}

TEST_F(SchemaTest, getSchemaSizeInBytesTest)
{
    {
        /// Calculating the schema size for each data type
        DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
        for (const auto& basicTypeVal : magic_enum::enum_values<BasicType>())
        {
            std::shared_ptr<Schema> testSchema;
            ASSERT_NO_THROW(testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT));
            ASSERT_EQ(testSchema->getLayoutType(), Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
            ASSERT_TRUE(testSchema->addField("field", basicTypeVal));
            ASSERT_EQ(testSchema->getFieldCount(), 1);
            ASSERT_EQ(testSchema->getFieldByIndex(0)->getName(), "field");
            ASSERT_EQ(*testSchema->getFieldByIndex(0)->getDataType(), *DataTypeProvider::provideBasicType(basicTypeVal));
            ASSERT_EQ(
                testSchema->getSchemaSizeInBytes(),
                defaultPhysicalTypeFactory.getPhysicalType(DataTypeProvider::provideBasicType(basicTypeVal))->size());
        }
    }

    {
        using enum NES::BasicType;
        /// Calculating the schema size for multiple fields
        auto testSchema = Schema::create()
                              ->addField("field1", UINT8)
                              ->addField("field2", UINT16)
                              ->addField("field3", INT32)
                              ->addField("field4", FLOAT32)
                              ->addField("field5", FLOAT64);
        EXPECT_EQ(testSchema->getSchemaSizeInBytes(), 1 + 2 + 4 + 4 + 8);
    }
}

TEST_F(SchemaTest, containsTest)
{
    using enum NES::BasicType;
    {
        /// Checking contains for one fieldName
        auto testSchema = Schema::create()->addField("field1", UINT8);
        EXPECT_TRUE(testSchema->contains("field1"));
        EXPECT_FALSE(testSchema->contains("notExistingField1"));
    }

    {
        /// Checking contains with multiple fields
        auto testSchema = Schema::create()
                              ->addField("field1", UINT8)
                              ->addField("field2", UINT16)
                              ->addField("field3", INT32)
                              ->addField("field4", FLOAT32)
                              ->addField("field5", FLOAT64);

        /// Existing fields
        EXPECT_TRUE(testSchema->contains("field3"));

        /// Not existing fields
        EXPECT_FALSE(testSchema->contains("notExistingField3"));
    }
}

TEST_F(SchemaTest, getFieldByNameTestInSchemaWithSourceName)
{
    using enum BasicType;

    /// Checking contains for one fieldName but with fields containing already a source name, e.g., after a join
    const auto testSchema = Schema::create()
                                ->addField("streamstream2$start", UINT64)
                                ->addField("streamstream2$end", UINT64)
                                ->addField("stream$id", UINT64)
                                ->addField("stream$value", UINT64)
                                ->addField("stream$timestamp", UINT64)
                                ->addField("stream2$id2", UINT64)
                                ->addField("stream2$value2", UINT64)
                                ->addField("stream2$timestamp", UINT64);
    EXPECT_TRUE(testSchema->getFieldByName("id"));
    EXPECT_FALSE(testSchema->getFieldByName("notExistingField1"));
}

TEST_F(SchemaTest, getSourceNameQualifierTest)
{
    using enum NES::BasicType;
    /// TODO once #4355 is done, we can use updateSourceName(source1) here
    const auto sourceName = std::string("source1");
    auto testSchema = Schema::create()
                          ->addField(sourceName + "$field1", UINT8)
                          ->addField(sourceName + "$field2", UINT16)
                          ->addField(sourceName + "$field3", INT32)
                          ->addField(sourceName + "$field4", FLOAT32)
                          ->addField(sourceName + "$field5", FLOAT64);

    EXPECT_EQ(testSchema->getSourceNameQualifier(), sourceName);
}

TEST_F(SchemaTest, copyTest)
{
    auto testSchema = Schema::create()->addField("field1", BasicType::UINT8)->addField("field2", BasicType::UINT16);
    auto testSchemaCopy = testSchema->copy();

    ASSERT_EQ(testSchema->getSchemaSizeInBytes(), testSchemaCopy->getSchemaSizeInBytes());
    ASSERT_EQ(testSchema->getLayoutType(), testSchemaCopy->getLayoutType());
    ASSERT_EQ(testSchema->getFieldCount(), testSchemaCopy->getFieldCount());

    /// Comparing fields
    for (auto fieldCnt = 0_u64; fieldCnt < testSchemaCopy->getFieldCount(); ++fieldCnt)
    {
        const auto& curField = testSchemaCopy->getFieldByIndex(fieldCnt);
        EXPECT_TRUE(testSchema->getFieldByIndex(fieldCnt)->isEqual(curField));
        EXPECT_TRUE(testSchema->getFieldByName(curField->getName()).has_value());
        if (testSchema->getFieldByName(curField->getName()).has_value())
        {
            EXPECT_TRUE(testSchema->getFieldByName(curField->getName()).value()->isEqual(curField));
        }
    }
}

TEST_F(SchemaTest, updateSourceNameTest)
{
    /// TODO once #4355 is done, we can test updateSourceName(source1) here
}

}
