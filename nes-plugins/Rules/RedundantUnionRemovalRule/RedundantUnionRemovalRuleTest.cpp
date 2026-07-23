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

#include <RedundantUnionRemovalRule.hpp>

#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class RedundantUnionRemovalRuleTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("RedundantUnionRemovalRuleTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};

TEST_F(RedundantUnionRemovalRuleTest, OneUnionToRemove)
{
    /// BEFORE: Sink < Selection < Union < Union < Source
    /// AFTER: Sink < Selection < Source

    auto source = utils.createSource("oneUnionSource", {"a"});
    auto unionOp = UnionLogicalOperator::create(std::vector<LogicalOperator>{source});
    auto select = SelectionLogicalOperator::create(
        unionOp,
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{unionOp.getOutputSchema()[Identifier::parse("a")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}});

    auto sink = utils.createSink(select, "oneUnionSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = RedundantUnionRemovalRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());

    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(RedundantUnionRemovalRuleTest, TwoUnionsToRemove)
{
    /// BEFORE: Sink < Selection < Union < Source
    /// AFTER: Sink < Selection < Source

    auto source = utils.createSource("twoUnionSource", {"a"});
    auto union1 = UnionLogicalOperator::create(std::vector<LogicalOperator>{source});
    auto union2 = UnionLogicalOperator::create(std::vector<LogicalOperator>{union1});
    auto select = SelectionLogicalOperator::create(
        union2,
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{union2.getOutputSchema()[Identifier::parse("a")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}});

    auto sink = utils.createSink(select, "twoUnionSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = RedundantUnionRemovalRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());

    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(RedundantUnionRemovalRuleTest, LeaveUnionWithMultipleChildren)
{
    /// BEFORE: Sink < Selection < Union < (source1, source2)
    /// AFTER: Sink < Selection < Union < (source1, source2)

    auto source1 = utils.createSource("multipleChildren1Source", {"a"});
    auto source2 = utils.createSource("multipleChildren2Source", {"a"});
    auto unionOp = UnionLogicalOperator::create(std::vector<LogicalOperator>{source1, source2});
    auto select = SelectionLogicalOperator::create(
        unionOp,
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{unionOp.getOutputSchema()[Identifier::parse("a")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}});

    auto sink = utils.createSink(select, "multipleChildrenSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = RedundantUnionRemovalRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());

    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<UnionLogicalOperator>());
    ASSERT_EQ(op2.getChildren().size(), 2);
    auto op20 = op2.getChildren().at(0);
    ASSERT_TRUE(op20.tryGetAs<SourceDescriptorLogicalOperator>());
    auto op21 = op2.getChildren().at(1);
    ASSERT_TRUE(op21.tryGetAs<SourceDescriptorLogicalOperator>());
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
