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

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>

namespace NES
{
namespace
{

/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class JoinLogicalOperatorTest : public ::testing::Test
{
public:
    static constexpr uint64_t TUMBLING_WINDOW_SIZE_MS = 1000;

    static Windowing::TimeBasedWindowType createTumblingWindow()
    {
        return Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure(TUMBLING_WINDOW_SIZE_MS)}};
    }

    static Schema<UnqualifiedUnboundField, Ordered> createLeftSchema()
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            {Identifier::parse("left_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
            {Identifier::parse("left_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};
    }

    static Schema<UnqualifiedUnboundField, Ordered> createRightSchema()
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            {Identifier::parse("right_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
            {Identifier::parse("right_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};
    }

    static std::vector<Identifier> leftFieldNames() { return {Identifier::parse("left_id"), Identifier::parse("left_value")}; }

    static std::vector<Identifier> rightFieldNames() { return {Identifier::parse("right_id"), Identifier::parse("right_value")}; }

    static TypedLogicalOperator<SourceDescriptorLogicalOperator>
    makeSource(std::string_view name, const Schema<UnqualifiedUnboundField, Ordered> &schema) {
        const auto logical = LogicalSource{Identifier::parse(std::string{name}), schema};
        const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("file_path"), "/dev/null"}};
        const auto descriptor = SourceDescriptor::create(
                    PhysicalSourceId{1},
                    logical,
                    Identifier::parse("File"),
                    Host{"localhost"},
                    sourceConfig,
                    {{Identifier::parse("type"), "CSV"}})
                .value();
        return SourceDescriptorLogicalOperator::create(descriptor);
    }

    /// Build a left_id = right_id equi-join over two file sources. Schema inference happens in the children-ctor, so the returned
    /// operator already has its output schema bound.
    static TypedLogicalOperator<JoinLogicalOperator> buildInferredJoin(JoinLogicalOperator::JoinType joinType) {
        const auto leftSource = makeSource("left_src", createLeftSchema());
        const auto rightSource = makeSource("right_src", createRightSchema());

        auto joinFunction = EqualsLogicalFunction{
            FieldAccessLogicalFunction{leftSource->getOutputSchema().getFieldByName(Identifier::parse("left_id")).value()},
            FieldAccessLogicalFunction{rightSource->getOutputSchema().getFieldByName(Identifier::parse("right_id")).value()}};

        auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
            {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
             Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});

        return JoinLogicalOperator::create(
            std::array<LogicalOperator, 2>{leftSource, rightSource},
            joinFunction,
            createTumblingWindow(),
            joinType,
            characteristics.value());
    }

    /// Expected join output schema for the left_src/right_src join, used to declare a matching sink. For outer joins the fields of the side
    /// that may be unmatched become nullable, mirroring JoinLogicalOperator::inferLocalSchema; window START/END are never nullable.
    static Schema<UnqualifiedUnboundField, Ordered> createJoinOutputSchema(const bool leftNullable, const bool rightNullable)
    {
        const auto u64 = DataTypeProvider::provideDataType(DataType::Type::UINT64);
        const auto leftType
            = leftNullable ? DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE) : u64;
        const auto rightType
            = rightNullable ? DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE) : u64;
        return Schema<UnqualifiedUnboundField, Ordered>{
            {Identifier::parse("left_id"), leftType},
            {Identifier::parse("left_value"), leftType},
            {Identifier::parse("right_id"), rightType},
            {Identifier::parse("right_value"), rightType},
            {Identifier::parse("START"), u64},
            {Identifier::parse("END"), u64}};
    }

    static SinkDescriptor createSinkDescriptor(const Identifier &sinkName,
                                               const Schema<UnqualifiedUnboundField, Ordered> &schema) {
        const std::unordered_map<Identifier, std::string> sinkConfig{
            {Identifier::parse("FILE_PATH"), "/dev/null"}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}};
        return SinkDescriptor::createNamed(INVALID_SINK_ID, sinkName, schema, Identifier::parse("file"),
                                           Host{"localhost"}, sinkConfig, {})
                .value();
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
    for (const auto joinType :
         {JoinLogicalOperator::JoinType::INNER_JOIN,
          JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN,
          JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN,
          JoinLogicalOperator::JoinType::OUTER_FULL_JOIN})
    {
        const auto join = buildInferredJoin(joinType);
        EXPECT_EQ(join->getJoinType(), joinType);
    }
}

TEST_F(JoinLogicalOperatorTest, LeftOuterJoinMarksRightFieldsNullableAndPreservesLeft)
{
    const auto join = buildInferredJoin(JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN);
    const auto outputSchema = join->getOutputSchema();

    for (const auto& fieldName : leftFieldNames())
    {
        const auto outField = outputSchema.getFieldByName(fieldName);
        ASSERT_TRUE(outField.has_value()) << "Missing left field: " << fieldName;
        EXPECT_FALSE(outField->getDataType().nullable) << "Left field should not be nullable: " << fieldName;
    }

    for (const auto& fieldName : rightFieldNames())
    {
        const auto outField = outputSchema.getFieldByName(fieldName);
        ASSERT_TRUE(outField.has_value()) << "Missing right field: " << fieldName;
        EXPECT_TRUE(outField->getDataType().nullable) << "Right field should be nullable: " << fieldName;
    }
}

TEST_F(JoinLogicalOperatorTest, RightOuterJoinMarksLeftFieldsNullableAndPreservesRight)
{
    const auto join = buildInferredJoin(JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN);
    const auto outputSchema = join->getOutputSchema();

    for (const auto& fieldName : leftFieldNames())
    {
        const auto outField = outputSchema.getFieldByName(fieldName);
        ASSERT_TRUE(outField.has_value()) << "Missing left field: " << fieldName;
        EXPECT_TRUE(outField->getDataType().nullable) << "Left field should be nullable: " << fieldName;
    }

    for (const auto& fieldName : rightFieldNames())
    {
        const auto outField = outputSchema.getFieldByName(fieldName);
        ASSERT_TRUE(outField.has_value()) << "Missing right field: " << fieldName;
        EXPECT_FALSE(outField->getDataType().nullable) << "Right field should not be nullable: " << fieldName;
    }
}

TEST_F(JoinLogicalOperatorTest, FullOuterJoinMarksBothSidesNullable)
{
    const auto join = buildInferredJoin(JoinLogicalOperator::JoinType::OUTER_FULL_JOIN);
    const auto outputSchema = join->getOutputSchema();

    for (const auto& fieldName : leftFieldNames())
    {
        const auto outField = outputSchema.getFieldByName(fieldName);
        ASSERT_TRUE(outField.has_value()) << "Missing left field: " << fieldName;
        EXPECT_TRUE(outField->getDataType().nullable) << "Left field should be nullable in FULL JOIN: " << fieldName;
    }

    for (const auto& fieldName : rightFieldNames())
    {
        const auto outField = outputSchema.getFieldByName(fieldName);
        ASSERT_TRUE(outField.has_value()) << "Missing right field: " << fieldName;
        EXPECT_TRUE(outField->getDataType().nullable) << "Right field should be nullable in FULL JOIN: " << fieldName;
    }
}

TEST_F(JoinLogicalOperatorTest, InnerJoinDoesNotMarkAnyFieldsNullable)
{
    const auto join = buildInferredJoin(JoinLogicalOperator::JoinType::INNER_JOIN);
    const auto outputSchema = join->getOutputSchema();

    for (const auto& fieldName : leftFieldNames())
    {
        const auto outField = outputSchema.getFieldByName(fieldName);
        ASSERT_TRUE(outField.has_value()) << "Missing left field: " << fieldName;
        EXPECT_FALSE(outField->getDataType().nullable) << "Inner join left field should not be nullable: " << fieldName;
    }

    for (const auto& fieldName : rightFieldNames())
    {
        const auto outField = outputSchema.getFieldByName(fieldName);
        ASSERT_TRUE(outField.has_value()) << "Missing right field: " << fieldName;
        EXPECT_FALSE(outField->getDataType().nullable) << "Inner join right field should not be nullable: " << fieldName;
    }
}

TEST_F(JoinLogicalOperatorTest, WindowStartEndFieldsAreNotNullableForOuterJoins)
{
    for (const auto joinType :
         {JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN,
          JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN,
          JoinLogicalOperator::JoinType::OUTER_FULL_JOIN})
    {
        const auto join = buildInferredJoin(joinType);
        const auto outputSchema = join->getOutputSchema();

        const auto startField = outputSchema.getFieldByName(join->getStartField().getFullyQualifiedName());
        ASSERT_TRUE(startField.has_value()) << "Missing window start field";
        EXPECT_FALSE(startField->getDataType().nullable) << "Window START field must never be nullable";

        const auto endField = outputSchema.getFieldByName(join->getEndField().getFullyQualifiedName());
        ASSERT_TRUE(endField.has_value()) << "Missing window end field";
        EXPECT_FALSE(endField->getDataType().nullable) << "Window END field must never be nullable";
    }
}

TEST_F(JoinLogicalOperatorTest, ReflectionRoundTripPreservesOuterJoinTypes)
{
    for (const auto joinType :
         {JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN,
          JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN,
          JoinLogicalOperator::JoinType::OUTER_FULL_JOIN})
    {
        const auto join = buildInferredJoin(joinType);

        const bool leftNullable
            = joinType == JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN || joinType == JoinLogicalOperator::JoinType::OUTER_FULL_JOIN;
        const bool rightNullable
            = joinType == JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN || joinType == JoinLogicalOperator::JoinType::OUTER_FULL_JOIN;
        const auto sinkDescriptor
                = createSinkDescriptor(Identifier::parse("test_sink"),
                                       createJoinOutputSchema(leftNullable, rightNullable));
        const auto sinkOp = SinkLogicalOperator::create(sinkDescriptor).withChildrenUnsafe({join});
        const LogicalPlan plan{QueryId{1}, {sinkOp->withInferredSchema()}};

        const auto serialized = QueryPlanSerializationUtil::serializeQueryPlan(plan);
        const auto restored = QueryPlanSerializationUtil::deserializeQueryPlan(serialized);

        const auto joins = getOperatorByType<JoinLogicalOperator>(restored);
        ASSERT_EQ(joins.size(), 1U);
        EXPECT_EQ(joins[0]->getJoinType(), joinType);
        EXPECT_NE(joins[0]->getJoinType(), JoinLogicalOperator::JoinType::INNER_JOIN);
    }
}

/// NOLINTEND(bugprone-unchecked-optional-access)

}
}
