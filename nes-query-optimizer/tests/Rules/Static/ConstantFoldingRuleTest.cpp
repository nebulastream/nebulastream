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

#include <Rules/Static/ConstantFoldingRule.hpp>

#include <DataTypes/DataType.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class ConstantFoldingRuleTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ConstantFoldingRuleTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};

TEST_F(ConstantFoldingRuleTest, simpleAdditionInSelection)
{
    /// BEFORE: Sink < Selection(a = 2+2) < Source
    /// AFTER: Sink < Selection(a = 4) < Source

    auto source = utils.createSource("simpleAdditionSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        AddLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}}
    });

    auto sink = utils.createSink(select, "simpleAdditionSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = ConstantFoldingRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    //NES_DEBUG("op1 explain: {}", op1.explain(ExplainVerbosity::Debug));
    //NES_DEBUG("predicate: {}", op1.getAs<SelectionLogicalOperator>()->getPredicate().explain(ExplainVerbosity::Debug));
    //NES_DEBUG("predicate children: {}", op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().size());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>());
    ASSERT_EQ(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue(), "4");
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());

}

TEST_F(ConstantFoldingRuleTest, nestedAdditionInSelection)
{
    /// BEFORE: Sink < Selection(a = (2 + (1+2))) < Source
    /// AFTER: Sink < Selection(a = 5) < Source

    auto source = utils.createSource("simpleAdditionSource", {"a"});
    auto select = SelectionLogicalOperator::create(
        source,
        EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
        AddLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"},
            AddLogicalFunction{ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "1"},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}}}
    });

    auto sink = utils.createSink(select, "simpleAdditionSink", {"a"});
    auto plan = utils.createPlan(sink);

    auto cleaned = ConstantFoldingRule{}.apply(plan);

    auto op0 = cleaned.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>());
    ASSERT_EQ(op1.getAs<SelectionLogicalOperator>()->getPredicate().getChildren().at(1).tryGetAs<ConstantValueLogicalFunction>().value()->getConstantValue(), "5");
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
