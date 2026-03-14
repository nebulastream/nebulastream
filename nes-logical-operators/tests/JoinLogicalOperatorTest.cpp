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
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace
{
using namespace NES; /// NOLINT(google-build-using-namespace) test file scoped within anonymous namespace

class JoinLogicalOperatorTest : public ::testing::Test
{
protected:
    static constexpr uint64_t TUMBLING_WINDOW_SIZE_MS = 1000;

    static std::shared_ptr<Windowing::WindowType> createTumblingWindow()
    {
        return std::make_shared<Windowing::TumblingWindow>(
            Windowing::TimeCharacteristic::createIngestionTime(), Windowing::TimeMeasure(TUMBLING_WINDOW_SIZE_MS));
    }

    static Schema createLeftSchema()
    {
        Schema schema;
        schema.addField("left$id", DataType::Type::UINT64);
        schema.addField("left$value", DataType::Type::UINT64);
        return schema;
    }

    static Schema createRightSchema()
    {
        Schema schema;
        schema.addField("right$id", DataType::Type::UINT64);
        schema.addField("right$value", DataType::Type::UINT64);
        return schema;
    }

    static LogicalFunction createJoinFunction()
    {
        return LogicalFunction{EqualsLogicalFunction(
            LogicalFunction{FieldAccessLogicalFunction("left$id")}, LogicalFunction{FieldAccessLogicalFunction("right$id")})};
    }

    static JoinLogicalOperator createOperator(JoinLogicalOperator::JoinType joinType)
    {
        return JoinLogicalOperator(createJoinFunction(), createTumblingWindow(), joinType);
    }

    static JoinLogicalOperator createOperatorWithSchema(JoinLogicalOperator::JoinType joinType)
    {
        auto op = createOperator(joinType);
        return op.withInferredSchema({createLeftSchema(), createRightSchema()});
    }
};

TEST_F(JoinLogicalOperatorTest, IsOuterJoin)
{
    EXPECT_TRUE(isOuterJoin(JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN));
    EXPECT_TRUE(isOuterJoin(JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN));
    EXPECT_TRUE(isOuterJoin(JoinLogicalOperator::JoinType::OUTER_FULL_JOIN));
    EXPECT_FALSE(isOuterJoin(JoinLogicalOperator::JoinType::INNER_JOIN));
    EXPECT_FALSE(isOuterJoin(JoinLogicalOperator::JoinType::CARTESIAN_PRODUCT));
}

TEST_F(JoinLogicalOperatorTest, GetJoinType)
{
    EXPECT_EQ(createOperator(JoinLogicalOperator::JoinType::INNER_JOIN).getJoinType(), JoinLogicalOperator::JoinType::INNER_JOIN);
    EXPECT_EQ(createOperator(JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN).getJoinType(), JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN);
    EXPECT_EQ(
        createOperator(JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN).getJoinType(), JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN);
    EXPECT_EQ(createOperator(JoinLogicalOperator::JoinType::OUTER_FULL_JOIN).getJoinType(), JoinLogicalOperator::JoinType::OUTER_FULL_JOIN);
}

TEST_F(JoinLogicalOperatorTest, LeftOuterJoinMarksRightFieldsNullableAndPreservesLeft)
{
    auto op = createOperatorWithSchema(JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN);
    const auto outputSchema = op.getOutputSchema();

    /// NOLINTBEGIN(bugprone-unchecked-optional-access) ASSERT_TRUE(has_value()) guards the access but clang-tidy cannot see through gtest macros
    for (const auto& field : createLeftSchema().getFields())
    {
        const auto outField = outputSchema.getFieldByName(field.name);
        ASSERT_TRUE(outField.has_value()) << "Missing left field: " << field.name;
        EXPECT_FALSE(outField->dataType.nullable) << "Left field should not be nullable: " << field.name;
    }

    for (const auto& field : createRightSchema().getFields())
    {
        const auto outField = outputSchema.getFieldByName(field.name);
        ASSERT_TRUE(outField.has_value()) << "Missing right field: " << field.name;
        EXPECT_TRUE(outField->dataType.nullable) << "Right field should be nullable: " << field.name;
    }
    /// NOLINTEND(bugprone-unchecked-optional-access)
}

TEST_F(JoinLogicalOperatorTest, RightOuterJoinMarksLeftFieldsNullableAndPreservesRight)
{
    auto op = createOperatorWithSchema(JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN);
    const auto outputSchema = op.getOutputSchema();

    /// NOLINTBEGIN(bugprone-unchecked-optional-access) ASSERT_TRUE(has_value()) guards the access but clang-tidy cannot see through gtest macros
    for (const auto& field : createLeftSchema().getFields())
    {
        const auto outField = outputSchema.getFieldByName(field.name);
        ASSERT_TRUE(outField.has_value()) << "Missing left field: " << field.name;
        EXPECT_TRUE(outField->dataType.nullable) << "Left field should be nullable: " << field.name;
    }

    for (const auto& field : createRightSchema().getFields())
    {
        const auto outField = outputSchema.getFieldByName(field.name);
        ASSERT_TRUE(outField.has_value()) << "Missing right field: " << field.name;
        EXPECT_FALSE(outField->dataType.nullable) << "Right field should not be nullable: " << field.name;
    }
    /// NOLINTEND(bugprone-unchecked-optional-access)
}

TEST_F(JoinLogicalOperatorTest, FullOuterJoinMarksBothSidesNullable)
{
    auto op = createOperatorWithSchema(JoinLogicalOperator::JoinType::OUTER_FULL_JOIN);
    const auto outputSchema = op.getOutputSchema();

    /// NOLINTBEGIN(bugprone-unchecked-optional-access) ASSERT_TRUE(has_value()) guards the access but clang-tidy cannot see through gtest macros
    for (const auto& field : createLeftSchema().getFields())
    {
        const auto outField = outputSchema.getFieldByName(field.name);
        ASSERT_TRUE(outField.has_value()) << "Missing left field: " << field.name;
        EXPECT_TRUE(outField->dataType.nullable) << "Left field should be nullable in FULL JOIN: " << field.name;
    }

    for (const auto& field : createRightSchema().getFields())
    {
        const auto outField = outputSchema.getFieldByName(field.name);
        ASSERT_TRUE(outField.has_value()) << "Missing right field: " << field.name;
        EXPECT_TRUE(outField->dataType.nullable) << "Right field should be nullable in FULL JOIN: " << field.name;
    }
    /// NOLINTEND(bugprone-unchecked-optional-access)
}

TEST_F(JoinLogicalOperatorTest, InnerJoinDoesNotMarkAnyFieldsNullable)
{
    auto op = createOperatorWithSchema(JoinLogicalOperator::JoinType::INNER_JOIN);
    const auto outputSchema = op.getOutputSchema();

    /// NOLINTBEGIN(bugprone-unchecked-optional-access) ASSERT_TRUE(has_value()) guards the access but clang-tidy cannot see through gtest macros
    for (const auto& field : createLeftSchema().getFields())
    {
        const auto outField = outputSchema.getFieldByName(field.name);
        ASSERT_TRUE(outField.has_value()) << "Missing left field: " << field.name;
        EXPECT_FALSE(outField->dataType.nullable) << "Inner join left field should not be nullable: " << field.name;
    }

    for (const auto& field : createRightSchema().getFields())
    {
        const auto outField = outputSchema.getFieldByName(field.name);
        ASSERT_TRUE(outField.has_value()) << "Missing right field: " << field.name;
        EXPECT_FALSE(outField->dataType.nullable) << "Inner join right field should not be nullable: " << field.name;
    }
    /// NOLINTEND(bugprone-unchecked-optional-access)
}

TEST_F(JoinLogicalOperatorTest, WindowStartEndFieldsAreNotNullableForOuterJoins)
{
    /// NOLINTBEGIN(bugprone-unchecked-optional-access) ASSERT_TRUE(has_value()) guards the access but clang-tidy cannot see through gtest macros
    for (const auto joinType : {JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN, JoinLogicalOperator::JoinType::OUTER_FULL_JOIN})
    {
        auto op = createOperatorWithSchema(joinType);
        const auto outputSchema = op.getOutputSchema();

        const auto startField = outputSchema.getFieldByName(op.getWindowStartFieldName());
        ASSERT_TRUE(startField.has_value()) << "Missing window start field";
        EXPECT_FALSE(startField->dataType.nullable) << "Window START field must never be nullable";

        const auto endField = outputSchema.getFieldByName(op.getWindowEndFieldName());
        ASSERT_TRUE(endField.has_value()) << "Missing window end field";
        EXPECT_FALSE(endField->dataType.nullable) << "Window END field must never be nullable";
    }
    /// NOLINTEND(bugprone-unchecked-optional-access)
}

TEST_F(JoinLogicalOperatorTest, ReflectionRoundTripPreservesOuterJoinTypes)
{
    for (const auto joinType : {JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN, JoinLogicalOperator::JoinType::OUTER_FULL_JOIN})
    {
        auto op = createOperator(joinType);
        auto reflected = reflect(op);
        auto restored = unreflect<JoinLogicalOperator>(reflected);
        EXPECT_EQ(restored.getJoinType(), joinType);
        EXPECT_NE(restored.getJoinType(), JoinLogicalOperator::JoinType::INNER_JOIN);
    }
}

}
