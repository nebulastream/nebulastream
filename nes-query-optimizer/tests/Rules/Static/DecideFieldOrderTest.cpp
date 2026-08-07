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

#include <Rules/Static/DecideFieldOrder.hpp>

#include <DataTypes/DataType.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <gtest/gtest.h>
#include <OptimizerTestUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace
{

void assertOrderedFieldsIncludeAllOutputFields(const LogicalOperator& op)
{
    const auto outputSchema = op.getOutputSchema();
    ASSERT_TRUE(op.getTraitSet().contains<FieldOrderingTrait>());
    const auto orderedFields = op.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields();
    ASSERT_EQ(orderedFields.size(), outputSchema.size());
    for (const auto& field : orderedFields)
    {
        EXPECT_TRUE(outputSchema.contains(field.getFullyQualifiedName()));
    }
}
}

class DecideFieldOrderTest : public ::testing::Test
{
public:
    OptimizerTestUtils utils;
};

TEST_F(DecideFieldOrderTest, SingleSink)
{
    /// BEFORE & AFTER: Sink > Select > Source

    auto source = utils.createSource("singleSink", {"a", "b"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}});
    auto sink = utils.createSink(select, "singleSink", {"a", "b"});
    auto plan = utils.createPlan(sink);

    const auto result = DecideFieldOrder{}.apply(plan);

    for (const auto& op : BFSRange(result.getRootOperators().at(0)))
    {
        if (op.tryGetAs<SinkLogicalOperator>())
        {
            continue;
        }
        assertOrderedFieldsIncludeAllOutputFields(op);
    }
}

TEST_F(DecideFieldOrderTest, MultiSink)
{
    /// BEFORE (Sink1, Sink2) < Select < Source
    /// AFTER: (Sink1 < Projection1, Sink2 < Projection2) < Select Source

    auto source = utils.createSource("multiSink", {"a", "b"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}});
    auto sink1 = utils.createSink(select, "multiSink1", {"a", "b"});
    auto sink2 = utils.createSink(select, "multiSink2", {"b", "a"});
    auto plan = utils.createPlan({sink1, sink2});

    const auto result = DecideFieldOrder{}.apply(plan);


    auto op00 = result.getRootOperators().at(0);
    ASSERT_TRUE(op00.tryGetAs<SinkLogicalOperator>());
    auto op01 = op00.getChildren().at(0);
    ASSERT_TRUE(op01.tryGetAs<ProjectionLogicalOperator>());
    auto op02 = op01.getChildren().at(0);
    ASSERT_TRUE(op02.tryGetAs<SelectionLogicalOperator>());

    auto op10 = result.getRootOperators().at(1);
    ASSERT_TRUE(op10.tryGetAs<SinkLogicalOperator>());
    auto op11 = op10.getChildren().at(0);
    ASSERT_TRUE(op11.tryGetAs<ProjectionLogicalOperator>());
    auto op12 = op11.getChildren().at(0);
    ASSERT_TRUE(op12.tryGetAs<SelectionLogicalOperator>());

    ASSERT_FALSE(op11 == op01);
    ASSERT_TRUE(op12 == op02);

    auto op3 = op12.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<SourceDescriptorLogicalOperator>());
}
}

/// NOLINTEND(bugprone-unchecked-optional-access)
