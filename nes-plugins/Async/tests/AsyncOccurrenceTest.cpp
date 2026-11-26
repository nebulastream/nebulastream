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

#include <algorithm>
#include <mutex>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <AsyncDelayLogicalOperator.hpp>
#include <AsyncDelayAsyncOperator.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/IsNaNLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <Functions/OperatorCallLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/AsynchronousOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Static/OperatorFunctionLiftingRule.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Util/Reflection.hpp>
#include <rfl/Generic.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <OptimizerTestUtils.hpp>
#include <OperatorFunctionRegistry.hpp>
#include <Util/Reflection/SharedPointerDeserializationState.hpp>
#include <Util/Reflection/SharedPointerSerializationState.hpp>

namespace NES
{
namespace
{
LogicalFunction zeroMillis()
{
    return ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"};
}

constexpr std::string_view delayAlias = "TEST_DELAY_ONE";

void registerDelayAlias()
{
    static std::once_flag registered;
    std::call_once(registered,
                   []
                   {
                       const auto entry = OperatorFunctionRegistryEntry{
                           .resultName = Identifier::parse("value"),
                           .inferResultType =
                               [](const std::vector<LogicalFunction>& arguments)
                           {
                               if (arguments.size() != 1)
                               {
                                   throw InvalidQuerySyntax("TEST_DELAY_ONE takes one argument");
                               }
                               return arguments.front().getDataType();
                           },
                           .createOperator =
                               [](LogicalOperator child, const std::vector<LogicalFunction>& arguments)
                           {
                               if (arguments.size() != 1)
                               {
                                   throw InvalidQuerySyntax("TEST_DELAY_ONE takes one argument");
                               }
                               return AsyncDelayLogicalOperator::create(std::move(child), arguments.front(), zeroMillis());
                           },
                           .getArgumentsFromOperator = [](const LogicalOperator& op) -> std::optional<std::vector<LogicalFunction>>
                           {
                               const auto delay = op.tryGetAs<AsyncDelayLogicalOperator>();
                               if (!delay || !StructuralFunctionEqual{}(delay.value()->getMillis(), zeroMillis()))
                               {
                                   return std::nullopt;
                               }
                               return std::vector<LogicalFunction>{delay.value()->getValue()};
                           }};
                       if (!OperatorFunctionRegistry::instance().addEntry(std::string{delayAlias}, entry))
                       {
                           throw InvalidQuerySyntax("Unable to register test-only delay function");
                       }
                   });
}

LogicalFunction call(LogicalFunction argument)
{
    registerDelayAlias();
    return LogicalFunctionProvider::provide(std::string{delayAlias}, {std::move(argument)});
}

LogicalFunction literal(uint64_t value)
{
    return ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, std::to_string(value)};
}

TEST(AsyncOccurrenceTest, OrdinaryDelayLogicalNodeLowersWithoutAFunctionOrRuntimeFactoryNode)
{
    OptimizerTestUtils utils;
    const auto source = utils.createSource("input", std::vector<std::string>{"seed"});
    const auto input = source.getOutputSchema().getFieldByName(Identifier::parse("seed"));
    ASSERT_TRUE(input.has_value());
    const LogicalOperator async = AsyncDelayLogicalOperator::create(source, FieldAccessLogicalFunction{*input}, zeroMillis());
    EXPECT_EQ(async.getOutputSchema().size(), source.getOutputSchema().size() + 1);
    EXPECT_EQ(async.getAs<AsyncDelayLogicalOperator>()->getValue().getDataType(), input->getDataType());
}

TEST(AsyncOccurrenceTest, RuntimeAccessorAndGraphKeepDistinctInvocationsAndProducerDependency)
{
    const Schema<QualifiedUnboundField, Ordered> schema{};
    AsyncCallGraphBuilder builder{schema};
    const auto makeDelay = []
    {
        return AsyncOperator{
            AsyncDelayAsyncOperator{AsyncInputAccessor{RuntimeValue{uint64_t{17}}}, AsyncInputAccessor{RuntimeValue{uint64_t{0}}}}};
    };
    const auto first = builder.call(makeDelay(), {});
    const auto second = builder.call(makeDelay(), {first});
    const auto third = builder.call(makeDelay(), {});
    builder.require(first);
    builder.require(second);
    builder.require(third);
    builder.exportOutput(second->output(0), QualifiedIdentifier::parse("nested"));
    builder.exportOutput(third->output(0), QualifiedIdentifier::parse("independent"));
    const auto graph = builder.build();
    ASSERT_EQ(graph.getCalls().size(), 3);
    ASSERT_EQ(graph.getRequiredCalls().size(), 3);
    EXPECT_NE(graph.getCalls()[0], graph.getCalls()[1]);
    EXPECT_NE(graph.getCalls()[0], graph.getCalls()[2]);
    EXPECT_EQ(graph.getCalls()[1]->getDependencies().size(), 1);
    EXPECT_TRUE(graph.getCalls()[0]->getDependencies().empty());
    EXPECT_TRUE(graph.getCalls()[2]->getDependencies().empty());
}

TEST(AsyncOccurrenceTest, PureLogicalAliasesPreserveSameCallIdentityWithoutStructuralDeduplication)
{
    OptimizerTestUtils utils;
    const auto source = utils.createSource("input", std::vector<std::string>{"value"});
    const auto input = source.getOutputSchema().getFieldByName(Identifier::parse("value"));
    ASSERT_TRUE(input.has_value());
    const LogicalFunction shared = LogicalFunctionProvider::provide("ASYNC_DELAY", {FieldAccessLogicalFunction{*input}, zeroMillis()});
    const LogicalFunction independent = LogicalFunctionProvider::provide("ASYNC_DELAY", {FieldAccessLogicalFunction{*input}, zeroMillis()});
    EXPECT_NE(shared, independent);
    const LogicalFunction alias = shared;
    EXPECT_EQ(alias, shared);
    EXPECT_TRUE(StructuralFunctionEqual{}(alias, independent));
}

TEST(AsyncOccurrenceTest, ProviderCallsHaveDistinctIdentityEvenWithIdenticalArguments)
{
    const auto first = call(literal(7));
    const auto second = call(literal(7));
    EXPECT_NE(first, second);
    EXPECT_EQ(first, first);
    EXPECT_TRUE(StructuralFunctionEqual{}(first, second));
}

TEST(AsyncOccurrenceTest, TypeInferenceContextPreservesSharedDagNodesAndDistinctInstances)
{
    OptimizerTestUtils utils;
    const auto schema = utils.createSource("input", std::vector<std::string>{"x"}).getOutputSchema();
    const auto shared = call(literal(7));
    const auto distinct = call(literal(7));
    const LogicalFunction one = literal(1);
    const LogicalFunction two = literal(2);
    const LogicalFunction between = AndLogicalFunction{GreaterEqualsLogicalFunction{shared, one}, LessEqualsLogicalFunction{shared, two}};
    const auto inferredBetween = between.withInferredDataType(schema);
    EXPECT_EQ(inferredBetween.getChildren().at(0).getChildren().at(0), inferredBetween.getChildren().at(1).getChildren().at(0));

    const LogicalFunction sharedAdd = AddLogicalFunction{shared, one};
    const LogicalFunction inExpr = OrLogicalFunction{EqualsLogicalFunction{sharedAdd, one}, EqualsLogicalFunction{sharedAdd, two}};
    const auto inferredIn = inExpr.withInferredDataType(schema);
    const auto left = inferredIn.getChildren().at(0).getChildren().at(0);
    const auto right = inferredIn.getChildren().at(1).getChildren().at(0);
    EXPECT_EQ(left, right);
    EXPECT_EQ(left.getChildren().at(0), right.getChildren().at(0));

    const auto sum = LogicalFunction{AddLogicalFunction{shared, distinct}}.withInferredDataType(schema);
    EXPECT_NE(sum.getChildren().at(0), sum.getChildren().at(1));
    EXPECT_TRUE(StructuralFunctionEqual{}(sum.getChildren().at(0), sum.getChildren().at(1)));
}

TEST(AsyncOccurrenceTest, ProjectionInferencePreservesSharingAcrossExpressionRoots)
{
    OptimizerTestUtils utils;
    const auto child = utils.createSource("input", std::vector<std::string>{"x"});
    const auto shared = call(literal(7));
    const auto distinct = call(literal(7));
    const auto projection = ProjectionLogicalOperator::create(
        child,
        {{Identifier::parse("first"), shared}, {Identifier::parse("second"), shared}, {Identifier::parse("third"), distinct}},
        ProjectionLogicalOperator::Asterisk{false});
    const auto rewritten = projection.withChildren({child});
    const auto expressions = rewritten.getAs<ProjectionLogicalOperator>()->getProjections();
    ASSERT_EQ(expressions.size(), 3);
    EXPECT_EQ(expressions[0].second, expressions[1].second);
    EXPECT_NE(expressions[0].second, expressions[2].second);

    const auto lifted = OperatorFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {rewritten}});
    EXPECT_EQ(std::ranges::count_if(BFSRange(lifted.getRootOperators().front()),
                                    [](const LogicalOperator& op) { return op.tryGetAs<AsynchronousOperator>().has_value(); }),
              2);
    const auto outputs = lifted.getRootOperators().front().getAs<ProjectionLogicalOperator>()->getProjections();
    ASSERT_EQ(outputs.size(), 3);
    const auto first = outputs[0].second.getAs<FieldAccessLogicalFunction>()->getField().getFullyQualifiedName();
    const auto second = outputs[1].second.getAs<FieldAccessLogicalFunction>()->getField().getFullyQualifiedName();
    const auto third = outputs[2].second.getAs<FieldAccessLogicalFunction>()->getField().getFullyQualifiedName();
    EXPECT_EQ(first, second);
    EXPECT_NE(first, third);
}

TEST(AsyncOccurrenceTest, TypeInferenceContextRetainsSuccessfulChildrenAfterParentFailure)
{
    OptimizerTestUtils utils;
    TypeInferenceContext context{utils.createSource("input", std::vector<std::string>{"x"}).getOutputSchema()};
    const auto async = call(literal(7));
    const LogicalFunction text
        = ConstantValueLogicalFunction{DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "not numeric"};
    const LogicalFunction invalid = AddLogicalFunction{async, text};
    EXPECT_THROW(static_cast<void>(context.inferType(invalid)), Exception);
    const auto inferredCall = context.inferType(async);
    const auto valid = context.inferType(LogicalFunction{AddLogicalFunction{async, literal(1)}});
    ASSERT_EQ(valid.getChildren().size(), 2);
    EXPECT_EQ(valid.getChildren().front(), inferredCall);
    EXPECT_THROW(static_cast<void>(context.inferType(invalid)), Exception);
    EXPECT_EQ(context.inferType(async), inferredCall);
}

TEST(AsyncOccurrenceTest, IsNaNDesugaringPreservesSharedChildIdentity)
{
    OptimizerTestUtils utils;
    const auto source = utils.createSource("input", std::vector<std::string>{"x"});
    const auto x = source.getOutputSchema().getFieldByName(Identifier::parse("x"));
    ASSERT_TRUE(x.has_value());
    const LogicalFunction operand = call(AddLogicalFunction{literal(7), FieldAccessLogicalFunction{*x}});
    TypeInferenceContext context{source.getOutputSchema()};
    const auto inferred = context.inferType(IsNaNLogicalFunction{operand});
    const auto equality = inferred.getChildren().at(0);
    EXPECT_EQ(equality.getChildren().at(0), equality.getChildren().at(1));
}

TEST(AsyncOccurrenceTest, IntegerIsNaNPreservesOperandEffectsAndNullability)
{
    OptimizerTestUtils utils;
    const auto source = utils.createSource("input", std::vector<std::string>{"x"});
    const auto x = source.getOutputSchema().getFieldByName(Identifier::parse("x"));
    ASSERT_TRUE(x.has_value());
    const auto isNaN = IsNaNLogicalFunction{call(FieldAccessLogicalFunction{*x})};
    TypeInferenceContext inference{source.getOutputSchema()};
    const auto inferred = inference.inferType(isNaN);
    const auto operands = inferred.getChildren().at(0).getChildren();
    ASSERT_EQ(operands.size(), 2);
    EXPECT_EQ(operands.at(0), operands.at(1));
    ASSERT_TRUE(operands.front().tryGetAs<OperatorCallLogicalFunction>().has_value());
    const LogicalOperator selection = SelectionLogicalOperator::create(source, isNaN);
    const auto lifted = OperatorFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {selection}});
    EXPECT_EQ(std::ranges::count_if(BFSRange(lifted.getRootOperators().front()),
                                    [](const LogicalOperator& op) { return op.tryGetAs<AsynchronousOperator>().has_value(); }),
              1);
    const auto nullable = ConstantValueLogicalFunction{DataType{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE}, "1"};
    TypeInferenceContext nullableContext{Schema<Field, Unordered>{}};
    const auto nullableResult = nullableContext.inferType(IsNaNLogicalFunction{nullable});
    EXPECT_TRUE(nullableResult.getDataType().nullable);
    EXPECT_EQ(nullableResult.getChildren().at(0).getChildren().size(), 2);
}

TEST(AsyncOccurrenceTest, LogicalFunctionReflectionPreservesSharedDagNodes)
{
    const auto shared = call(literal(1));
    const auto distinct = call(literal(1));
    const LogicalFunction one = literal(1);
    const LogicalFunction two = literal(2);
    const LogicalFunction dag = AndLogicalFunction{
        EqualsLogicalFunction{shared, one}, AndLogicalFunction{EqualsLogicalFunction{shared, two}, EqualsLogicalFunction{distinct, one}}};
    EXPECT_THROW(static_cast<void>(ReflectionContext{}.reflect(dag)), Exception);
    const ReflectionContext writer{std::make_shared<SharedPointerSerializationState>()};
    const ReflectionContext reader{std::make_shared<SharedPointerDeserializationState>()};
    const auto wire = writer.reflect(dag);
    EXPECT_THROW(static_cast<void>(ReflectionContext{}.unreflect<LogicalFunction>(wire)), Exception);
    const auto restored = reader.unreflect<LogicalFunction>(wire);
    EXPECT_TRUE(*restored == *dag);
    const auto restoredLeft = restored.getChildren().at(0).getChildren().at(0);
    const auto restoredRight = restored.getChildren().at(1).getChildren().at(0).getChildren().at(0);
    const auto restoredSeparate = restored.getChildren().at(1).getChildren().at(1).getChildren().at(0);
    EXPECT_EQ(restoredLeft, restoredRight);
    EXPECT_NE(restoredLeft, restoredSeparate);
    EXPECT_TRUE(*restoredLeft == *restoredSeparate);
}

TEST(AsyncOccurrenceTest, LogicalFunctionIdentityDecodeRejectsUnknownConflictingAndWrongPointeeDefinitions)
{
    const auto original = call(literal(7)).withInferredDataType(Schema<Field, Unordered>{});
    const ReflectionContext writer{std::make_shared<SharedPointerSerializationState>()};
    const auto encoded = writer.reflect(original);
    auto unknown = *encoded;
    auto& definition = std::get<rfl::Generic::Object>(std::get<rfl::Generic::Object>(unknown.variant())["value"].variant());
    definition["functionType"] = std::string{"UNREGISTERED_LOGICAL_FUNCTION"};
    const ReflectionContext reader{std::make_shared<SharedPointerDeserializationState>()};
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(reader.unreflect<LogicalFunction>(unknown)), ErrorCode::CannotDeserialize);
    /// A failed registry construction must not leave the identity marked as pending.
    const auto decoded = reader.unreflect<LogicalFunction>(encoded);
    EXPECT_EQ(reader.unreflect<LogicalFunction>(encoded), decoded);
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(reader.unreflect<LogicalFunction>(unknown)), ErrorCode::CannotDeserialize);
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(reader.unreflect<std::shared_ptr<int>>(encoded)), ErrorCode::CannotDeserialize);
}

TEST(AsyncOccurrenceTest, ScalarLiftingPreservesACollidingInputUsedAsAnArgument)
{
    OptimizerTestUtils utils;
    const auto source = utils.createSource("input", std::vector<std::string>{"value"});
    const auto input = source.getOutputSchema().getFieldByName(Identifier::parse("value"));
    ASSERT_TRUE(input.has_value());
    const auto expression = call(FieldAccessLogicalFunction{*input});
    const auto projection = ProjectionLogicalOperator::create(
        source,
        {{Identifier::parse("original"), FieldAccessLogicalFunction{*input}}, {Identifier::parse("result"), expression}},
        ProjectionLogicalOperator::Asterisk{false});
    const auto lifted = OperatorFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {projection}});
    EXPECT_EQ(std::ranges::count_if(BFSRange(lifted.getRootOperators().front()),
                                    [](const LogicalOperator& op) { return op.tryGetAs<AsynchronousOperator>().has_value(); }),
              1);
    const auto asyncNodes = getOperatorByType<AsyncDelayLogicalOperator>(lifted);
    ASSERT_EQ(asyncNodes.size(), 1);
    const auto asyncNode = asyncNodes.front();
    const auto argument = asyncNode->getValue().getAs<FieldAccessLogicalFunction>();
    EXPECT_NE(argument->getField().getLastName(), Identifier::parse("value"));
    EXPECT_FALSE(asyncNode->getChild().getOutputSchema().contains(Identifier::parse("value")));
    EXPECT_TRUE(asyncNode->getOutputSchema().contains(Identifier::parse("value")));
    const auto& outputSchema = lifted.getRootOperators().front().getOutputSchema();
    EXPECT_EQ(outputSchema.size(), 2);
    EXPECT_EQ(outputSchema[Identifier::parse("original")]->getDataType(), input->getDataType());
    EXPECT_EQ(outputSchema[Identifier::parse("result")]->getDataType(), input->getDataType());
}

TEST(AsyncOccurrenceTest, ConstantAccessorWorksWithAnEmptyStandaloneRow)
{
    AsyncInputAccessor accessor{RuntimeValue{uint64_t{42}}};
    const AsyncRowHandle row;
    EXPECT_EQ(accessor.getDataType().type, DataType::Type::UINT64);
    const auto value = accessor(row);
    EXPECT_EQ(value.view().get<uint64_t>(), 42);
    EXPECT_FALSE(accessor.getProducer());
}
}
}
