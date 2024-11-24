//
// Created by ls on 11/24/24.
//

#include <API/DataType.hpp>
#include <API/Schema.hpp>
#include <gtest/gtest.h>
namespace NES
{
class DataTypeTest : public ::testing::Test
{
};

TEST_F(DataTypeTest, BasicTypes)
{
    EXPECT_EQ(uint64(), uint64());
    EXPECT_NE(uint64(), uint32());
    EXPECT_EQ(uint64(), IntegerType(0, false, std::numeric_limits<uint64_t>::max(), false));
    EXPECT_NE(uint64(), VariableSizedType(IntegerType(0, false, std::numeric_limits<uint64_t>::max(), false)));
    EXPECT_EQ(VariableSizedType(uint64()), VariableSizedType(IntegerType(0, false, std::numeric_limits<uint64_t>::max(), false)));

    EXPECT_TRUE(isNumerical(uint64()));
    EXPECT_FALSE(isNumerical(VariableSizedType(uint64())));
    EXPECT_FALSE(isNumerical(FixedSizedType(uint64(), 32)));
    EXPECT_FALSE(isNumerical(ScalarType(character())));
}

TEST_F(DataTypeTest, SchemaTest)
{
    Schema schema;
    schema.add("A", uint64());

    Schema schema2;
    schema2.add("A", uint64());
    EXPECT_EQ(schema, schema2);

    schema2.add("B", uint32());
    EXPECT_NE(schema, schema2);
}
}