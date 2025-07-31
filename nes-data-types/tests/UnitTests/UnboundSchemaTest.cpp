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

#include <vector>
#include "DataTypes/UnboundSchema.hpp"


#include "Identifiers/Identifier.hpp"
#include "Util/Logger/Logger.hpp"
#include "Util/Logger/impl/NesLogger.hpp"


#include "BaseUnitTest.hpp"

namespace NES
{

///NOLINTBEGIN(bugprone-unchecked-optional-access)
class UnboundSchemaTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("UnboundSchemaTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("UnboundSchemaTest test class SetUpTestCase.");
    }
};

TEST_F(UnboundSchemaTest, InitListCtorNoConflicts)
{
    const UnboundField field1{Identifier::parse("field1"), DataType::Type::BOOLEAN};
    const UnboundField field2{Identifier::parse("field2"), DataType::Type::INT32};
    const UnboundOrderedSchema unboundSchema{field1, field2};

    const auto found1 = unboundSchema[static_cast<IdentifierList>(Identifier::parse("field1"))];
    const auto found2 = unboundSchema[Identifier::parse("field2")];
    EXPECT_EQ(found1.value(), field1);
    EXPECT_EQ(found2.value(), field2);
    EXPECT_EQ(std::ranges::size(unboundSchema), 2);
}

TEST_F(UnboundSchemaTest, VectorCtorNoConflicts)
{
    const UnboundField field1{Identifier::parse("field1"), DataType::Type::BOOLEAN};
    const UnboundField field2{Identifier::parse("field2"), DataType::Type::INT32};
    const UnboundOrderedSchema unboundSchema{std::vector{field1, field2}};

    const auto found1 = unboundSchema[Identifier::parse("field1")];
    const auto found2 = unboundSchema[Identifier::parse("field2")];
    EXPECT_EQ(found1.value(), field1);
    EXPECT_EQ(found2.value(), field2);
    EXPECT_EQ(std::ranges::size(unboundSchema), 2);
}

TEST_F(UnboundSchemaTest, InitListCtorConflictNoName)
{
    const UnboundField field1{Identifier::parse("field1"), DataType::Type::BOOLEAN};
    const UnboundField field2{Identifier::parse("field1"), DataType::Type::INT32};
    const UnboundOrderedSchema unboundSchema{field1, field2};

    const auto found1 = unboundSchema[Identifier::parse("field1")];
    EXPECT_FALSE(found1.has_value());
    EXPECT_EQ(std::ranges::size(unboundSchema), 2);
}

TEST_F(UnboundSchemaTest, InitListCtorConflictQualifiedResolution)
{
    const UnboundField field1{IdentifierList::tryParse("source1.field1").value(), DataType::Type::BOOLEAN};
    const UnboundField field2{IdentifierList::tryParse("source2.field1").value(), DataType::Type::INT32};
    const UnboundOrderedSchema unboundSchema{field1, field2};

    const auto foundUnqualified = unboundSchema[Identifier::parse("field1")];
    EXPECT_FALSE(foundUnqualified.has_value());
    const auto foundQualified1 = unboundSchema[IdentifierList::tryParse("source1.field1").value()];
    EXPECT_EQ(foundQualified1.value(), field1);
    const auto foundQualified2 = unboundSchema[IdentifierList::tryParse("source2.field1").value()];
    EXPECT_EQ(foundQualified2.value(), field2);

    EXPECT_EQ(std::ranges::size(unboundSchema), 2);
}

TEST_F(UnboundSchemaTest, InitListCtorMultiLevelConflict)
{
    const UnboundField field1{IdentifierList::tryParse("source1.struct1.field1").value(), DataType::Type::BOOLEAN};
    const UnboundField field2{IdentifierList::tryParse("source2.struct1.field1").value(), DataType::Type::INT32};
    const UnboundOrderedSchema unboundSchema{field1, field2};

    const auto foundUnqualifiedField = unboundSchema[Identifier::parse("field1")];
    EXPECT_FALSE(foundUnqualifiedField.has_value());
    const auto foundUnqualifiedStruct = unboundSchema[IdentifierList::tryParse("struct1.field1").value()];
    EXPECT_FALSE(foundUnqualifiedStruct.has_value());
    const auto foundQualified1 = unboundSchema[IdentifierList::tryParse("source1.struct1.field1").value()];
    EXPECT_EQ(foundQualified1.value(), field1);
    const auto foundQualified2 = unboundSchema[IdentifierList::tryParse("source2.struct1.field1").value()];
    EXPECT_EQ(foundQualified2.value(), field2);

    EXPECT_EQ(std::ranges::size(unboundSchema), 2);
}

TEST_F(UnboundSchemaTest, InitListCtorSubsetConflict)
{
    const UnboundField field1{IdentifierList::tryParse("source1.struct1.field1").value(), DataType::Type::BOOLEAN};
    const UnboundField field2{IdentifierList::tryParse("struct1.field1").value(), DataType::Type::INT32};
    const UnboundOrderedSchema unboundSchema{field1, field2};

    const auto foundUnqualifiedField = unboundSchema[Identifier::parse("field1")];
    EXPECT_FALSE(foundUnqualifiedField.has_value());
    const auto foundUnqualifiedStruct = unboundSchema[IdentifierList::tryParse("struct1.field1").value()];
    EXPECT_FALSE(foundUnqualifiedStruct.has_value());
    const auto foundQualified1 = unboundSchema[IdentifierList::tryParse("source1.struct1.field1").value()];
    EXPECT_EQ(foundQualified1.value(), field1);

    EXPECT_EQ(std::ranges::size(unboundSchema), 2);
}

TEST_F(UnboundSchemaTest, InitListCtorIdxAccess)
{
    const UnboundField field1{Identifier::parse("field1"), DataType::Type::BOOLEAN};
    const UnboundField field2{Identifier::parse("field2"), DataType::Type::INT32};
    const UnboundOrderedSchema unboundSchema{field1, field2, field1};

    const auto found1 = unboundSchema[0];
    const auto found2 = unboundSchema[1];
    const auto found3 = unboundSchema[2];
    const auto found4 = unboundSchema[3];
    EXPECT_EQ(found1.value(), field1);
    EXPECT_EQ(found2.value(), field2);
    EXPECT_EQ(found3.value(), field1);
    EXPECT_FALSE(found4.has_value());
    EXPECT_EQ(std::ranges::size(unboundSchema), 3);
}

TEST_F(UnboundSchemaTest, CalcSizeInBytes)
{
    const UnboundField field1{Identifier::parse("field1"), DataType::Type::BOOLEAN};
    const UnboundField field2{Identifier::parse("field2"), DataType::Type::INT32};
    const UnboundOrderedSchema unboundSchema{field1, field2, field1};

    const auto expectedSize = DataTypeProvider::provideDataType(DataType::Type::BOOLEAN).getSizeInBytes() * 2
        + DataTypeProvider::provideDataType(DataType::Type::INT32).getSizeInBytes();
    EXPECT_EQ(unboundSchema.getSizeInBytes(), expectedSize);
}

TEST_F(UnboundSchemaTest, Equality)
{
    const auto unboundSchema1 =
        []
    {
        const UnboundField field1{Identifier::parse("field1"), DataType::Type::BOOLEAN};
        const UnboundField field2{Identifier::parse("field2"), DataType::Type::INT32};
        return UnboundOrderedSchema{field1, field2, field1};
    }();

    const auto unboundSchema2 =
        []
    {
        const UnboundField field1{Identifier::parse("field1"), DataType::Type::BOOLEAN};
        const UnboundField field2{Identifier::parse("field2"), DataType::Type::INT32};
        return UnboundOrderedSchema{std::vector{field1, field2, field1}};
    }();

    EXPECT_EQ(unboundSchema1, unboundSchema2);
}

TEST_F(UnboundSchemaTest, Inequality)
{
    const auto unboundSchema1 =
        []
    {
        const UnboundField field1{Identifier::parse("field1"), DataType::Type::BOOLEAN};
        const UnboundField field2{IdentifierList::tryParse("source1.field2").value(), DataType::Type::INT32};
        return UnboundOrderedSchema{field1, field2, field1};
    }();

    const auto unboundSchema2 =
        []
    {
        const UnboundField field1{Identifier::parse("field1"), DataType::Type::BOOLEAN};
        const UnboundField field2{Identifier::parse("field2"), DataType::Type::INT32};
        return UnboundOrderedSchema{std::vector{field1, field2, field1}};
    }();

    EXPECT_FALSE(unboundSchema1 == unboundSchema2);
}
}

///NOLINTEND(bugprone-unchecked-optional-access)
