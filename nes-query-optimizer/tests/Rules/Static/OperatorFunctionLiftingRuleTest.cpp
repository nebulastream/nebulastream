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
#include <Rules/Static/OperatorFunctionLiftingRule.hpp>

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <Functions/OperatorCallLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/AsynchronousOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/Reflection.hpp>
#include <Util/Reflection/SharedPointerDeserializationState.hpp>
#include <Util/Reflection/SharedPointerSerializationState.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <OperatorFunctionRegistry.hpp>
#include <OptimizerTestUtils.hpp>
#include <QueryId.hpp>

namespace NES
{
namespace
{
size_t constructedOperators = 0;
const DataType INTEGER{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};

OperatorFunctionRegistryEntry sumDefinition()
{
    return {
        .resultName = Identifier::parse("sum"),
        .inferResultType = [](const std::vector<LogicalFunction>& arguments) -> DataType
        {
            if (arguments.size() != 2)
            {
                throw InvalidQuerySyntax("APPEND_SUM expects two arguments");
            }
            auto type = arguments.at(0).getDataType().join(arguments.at(1).getDataType());
            if (!type)
            {
                throw CannotInferSchema("APPEND_SUM argument types cannot be joined");
            }
            return *type;
        },
        .createOperator = [](LogicalOperator child, const std::vector<LogicalFunction>& arguments) -> LogicalOperator
        {
            ++constructedOperators;
            return ProjectionLogicalOperator::create(
                std::move(child),
                {{Identifier::parse("sum"), AddLogicalFunction{arguments.at(0), arguments.at(1)}}},
                ProjectionLogicalOperator::Asterisk{true});
        },
        .getArgumentsFromOperator = {}};
}

LogicalFunction one()
{
    return ConstantValueLogicalFunction{INTEGER, "1"};
}
}

class OperatorFunctionLiftingRuleTest : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        ASSERT_TRUE(OperatorFunctionRegistry::instance().addEntry("APPEND_SUM", sumDefinition()));
        ASSERT_TRUE(OperatorFunctionRegistry::instance().addEntry("CONFLICTING_OPERATOR_FUNCTION", sumDefinition()));
        ASSERT_TRUE(
            LogicalFunctionRegistry::instance().addEntry(
                "CONFLICTING_OPERATOR_FUNCTION",
                [](LogicalFunctionRegistryArguments arguments) -> LogicalFunction
                { return AddLogicalFunction{arguments.children.at(0), arguments.children.at(1)}; }));
        auto invalid = sumDefinition();
        invalid.createOperator = [](LogicalOperator child, const std::vector<LogicalFunction>& arguments) -> LogicalOperator
        {
            return ProjectionLogicalOperator::create(
                std::move(child),
                {{Identifier::parse("sum"), AddLogicalFunction{arguments.at(0), arguments.at(1)}}},
                ProjectionLogicalOperator::Asterisk{false});
        };
        ASSERT_TRUE(OperatorFunctionRegistry::instance().addEntry("DROPS_INPUT_FIELDS", std::move(invalid)));
    }

    void SetUp() override { constructedOperators = 0; }

    OptimizerTestUtils utils;
};

TEST_F(OperatorFunctionLiftingRuleTest, SynchronousOperatorIsDiscoveredWithoutConstructingIt)
{
    const auto input = utils.createSource("input", {"value"});
    const auto field = input.getOutputSchema()[Identifier::parse("value")].value();
    const auto call = LogicalFunctionProvider::provide("APPEND_SUM", {FieldAccessLogicalFunction{field}, one()});
    const auto inferred = call.withInferredDataType(input.getOutputSchema());
    EXPECT_EQ(inferred.getDataType(), INTEGER);
    EXPECT_EQ(constructedOperators, 0);
    const LogicalOperator projection
        = ProjectionLogicalOperator::create(input, {{Identifier::parse("result"), inferred}}, ProjectionLogicalOperator::Asterisk{false});
    const auto lowered = OperatorFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {projection}});
    EXPECT_EQ(constructedOperators, 1);
    const auto schema = lowered.getRootOperators().front().getOutputSchema();
    ASSERT_EQ(schema.size(), 1);
    EXPECT_EQ(schema[Identifier::parse("result")]->getDataType(), INTEGER);
    for (const auto& op : BFSRange(lowered.getRootOperators().front()))
    {
        EXPECT_FALSE(op.tryGetAs<AsynchronousOperator>());
        if (const auto project = op.tryGetAs<ProjectionLogicalOperator>())
        {
            for (const auto& [name, expression] : project.value()->getProjections())
            {
                static_cast<void>(name);
                for (const auto& function : BFSRange(expression))
                {
                    EXPECT_FALSE(function.tryGetAs<OperatorCallLogicalFunction>());
                }
            }
        }
    }
}

TEST_F(OperatorFunctionLiftingRuleTest, NestedCallsUseOrdinaryOperatorsAndProjections)
{
    const auto input = utils.createSource("input", {"value"});
    const auto field = input.getOutputSchema()[Identifier::parse("value")].value();
    const auto inner = LogicalFunctionProvider::provide("APPEND_SUM", {FieldAccessLogicalFunction{field}, one()});
    const auto outer = LogicalFunctionProvider::provide("APPEND_SUM", {inner, one()});
    const LogicalOperator projection
        = ProjectionLogicalOperator::create(input, {{Identifier::parse("result"), outer}}, ProjectionLogicalOperator::Asterisk{false});
    const auto lowered = OperatorFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {projection}});
    EXPECT_EQ(constructedOperators, 2);
    EXPECT_TRUE(lowered.getRootOperators().front().getOutputSchema().contains(Identifier::parse("result")));
}

TEST_F(OperatorFunctionLiftingRuleTest, SharedReferencesExecuteOneOperatorButSeparateCallsRemainDistinct)
{
    const auto input = utils.createSource("input", {"value"});
    const auto field = input.getOutputSchema()[Identifier::parse("value")].value();
    const auto shared = LogicalFunctionProvider::provide("APPEND_SUM", {FieldAccessLogicalFunction{field}, one()});
    const auto distinct = LogicalFunctionProvider::provide("APPEND_SUM", {FieldAccessLogicalFunction{field}, one()});
    const LogicalOperator projection = ProjectionLogicalOperator::create(
        input,
        {{Identifier::parse("first"), shared}, {Identifier::parse("second"), shared}, {Identifier::parse("third"), distinct}},
        ProjectionLogicalOperator::Asterisk{false});
    const auto lowered = OperatorFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {projection}});
    EXPECT_EQ(constructedOperators, 2);
    EXPECT_EQ(lowered.getRootOperators().front().getOutputSchema().size(), 3);
}

TEST_F(OperatorFunctionLiftingRuleTest, CollidingInputNamesArePreservedByOrdinaryProjections)
{
    const auto input = utils.createSource("input", {"sum"});
    const auto field = input.getOutputSchema()[Identifier::parse("sum")].value();
    const auto call = LogicalFunctionProvider::provide("APPEND_SUM", {FieldAccessLogicalFunction{field}, one()});
    const LogicalOperator projection = ProjectionLogicalOperator::create(
        input,
        {{Identifier::parse("original"), FieldAccessLogicalFunction{field}}, {Identifier::parse("result"), call}},
        ProjectionLogicalOperator::Asterisk{false});
    const auto lowered = OperatorFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {projection}});
    const auto output = lowered.getRootOperators().front().getOutputSchema();
    EXPECT_EQ(constructedOperators, 1);
    EXPECT_EQ(output.size(), 2);
    EXPECT_TRUE(output.contains(Identifier::parse("original")));
    EXPECT_TRUE(output.contains(Identifier::parse("result")));
}

TEST_F(OperatorFunctionLiftingRuleTest, ReflectionRetainsSharedCallsWithoutInvokingTheOperatorFactory)
{
    const auto call = LogicalFunctionProvider::provide("APPEND_SUM", {one(), one()});
    const LogicalFunction expression = AddLogicalFunction{call, call};
    const ReflectionContext writer{std::make_shared<SharedPointerSerializationState>()};
    const ReflectionContext reader{std::make_shared<SharedPointerDeserializationState>()};
    const auto restored = reader.unreflect<LogicalFunction>(writer.reflect(expression));
    EXPECT_EQ(restored.getChildren().at(0), restored.getChildren().at(1));
    EXPECT_TRUE(restored.getChildren().at(0).tryGetAs<OperatorCallLogicalFunction>());
    EXPECT_EQ(constructedOperators, 0);
}

TEST_F(OperatorFunctionLiftingRuleTest, RejectsAnOperatorThatDoesNotForwardItsInputFields)
{
    const auto input = utils.createSource("input", {"value"});
    const auto field = input.getOutputSchema()[Identifier::parse("value")].value();
    const auto call = LogicalFunctionProvider::provide("DROPS_INPUT_FIELDS", {FieldAccessLogicalFunction{field}, one()});
    const LogicalOperator projection
        = ProjectionLogicalOperator::create(input, {{Identifier::parse("result"), call}}, ProjectionLogicalOperator::Asterisk{false});
    EXPECT_THROW(static_cast<void>(OperatorFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {projection}})), Exception);
}

TEST_F(OperatorFunctionLiftingRuleTest, ScalarAndOperatorFunctionRegistrationsMustNotBeAmbiguous)
{
    EXPECT_THROW(static_cast<void>(LogicalFunctionProvider::provide("CONFLICTING_OPERATOR_FUNCTION", {one(), one()})), Exception);
}
}
