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

/// NOLINTBEGIN(readability-magic-numbers) Numeric values are fixture data in these tests.
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>
#include <Configurations/Util.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/AsyncCallLogicalFunction.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/IsNaNLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/AsyncLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Rules/Static/AsyncFunctionLiftingRule.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncError.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncOperatorReflection.hpp>
#include <Sources/AsyncSourceConfig.hpp>
#include <Sources/PreparedAsyncCall.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Reflection.hpp>
#include <Util/Reflection/SharedPointerDeserializationState.hpp>
#include <Util/Reflection/SharedPointerSerializationState.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp> /// NOLINT(misc-include-cleaner) Public Asio provider; cleaner misattributes co_spawn.
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <AsyncGraphExecution.hpp>
#include <AsyncOperatorRegistry.hpp>
#include <AsyncOperatorUnreflectionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <QueryId.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

/// NOLINTBEGIN(bugprone-unchecked-optional-access) GoogleTest fatal assertions guard optional reads
namespace NES::Test
{
namespace
{
/// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables) test instrumentation counter records factory invocations
inline size_t customFactoryCalls = 0;

class MarkerPreparedCall final : public PreparedAsyncCall
{
public:
    explicit MarkerPreparedCall(uint64_t marker) : marker(marker) { }

    boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> executeAttempt() override { co_return SharedRuntimeValues::make(marker); }

private:
    uint64_t marker;
};

/// NOLINTBEGIN(readability-convert-member-functions-to-static) mock methods implement the instance operator interface
struct CustomReflectedAsyncOperator
{
    explicit CustomReflectedAsyncOperator(int value) : value(value) { }

    [[nodiscard]] std::string_view getType() const { return "TEST_CUSTOM_PAYLOAD"; }

    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getOutputSchema() const
    {
        return Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("value"), DataType::Type::UINT64}};
    }

    static CustomReflectedAsyncOperator create(AsyncOperatorRegistryArguments args)
    {
        if (args.argumentTypes.size() != 1)
        {
            throw InvalidQuerySyntax("Custom test operator takes exactly one argument");
        }
        ++customFactoryCalls;
        return CustomReflectedAsyncOperator{73};
    }

    [[nodiscard]] CustomReflectedAsyncOperator withInferredArguments(const AsyncArgumentsWithConstants& binding) const
    {
        if (binding.size() != 1)
        {
            throw InvalidQuerySyntax("Custom test operator takes exactly one argument");
        }
        return *this;
    }

    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext&, SharedRuntimeValues) const
    {
        return std::unique_ptr<PreparedAsyncCall>{std::make_unique<MarkerPreparedCall>(static_cast<uint64_t>(value))};
    }

    [[nodiscard]] std::optional<size_t> maxConcurrency() const { return std::nullopt; }

    [[nodiscard]] int getValue() const { return value; }

private:
    int value;
};

/// NOLINTEND(readability-convert-member-functions-to-static)
}
}

namespace NES
{
template <>
struct Reflector<Test::CustomReflectedAsyncOperator>
{
    Reflected operator()(const Test::CustomReflectedAsyncOperator& op, const ReflectionContext& context) const
    {
        return context.reflect(op.getValue());
    }
};

template <>
struct Unreflector<Test::CustomReflectedAsyncOperator>
{
    Test::CustomReflectedAsyncOperator operator()(const Reflected& data, const ReflectionContext& context) const
    {
        return Test::CustomReflectedAsyncOperator{context.unreflect<int>(data)};
    }
};
}

namespace NES
{
namespace
{
using Test::customFactoryCalls;

const auto scalarName = "TEST_OCCURRENCE_SCALAR";
const auto multiName = "TEST_OCCURRENCE_MULTI";

/// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables) test instrumentation counter records factory invocations
size_t scalarFactoryCalls = 0;

/// NOLINTBEGIN(readability-convert-member-functions-to-static) mock methods implement the instance operator interface
struct TestScalarAsyncOperator
{
    size_t marker = 0;
    bool emptyOutputs = false;

    static TestScalarAsyncOperator create(AsyncOperatorRegistryArguments args)
    {
        if (!args.argumentTypes.empty())
        {
            throw InvalidQuerySyntax("Test scalar operator takes no arguments");
        }
        return {.marker = ++scalarFactoryCalls};
    }

    [[nodiscard]] std::string_view getType() const { return scalarName; }

    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getOutputSchema() const
    {
        if (emptyOutputs)
        {
            return {};
        }
        return Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("value"), DataType::Type::UINT64}};
    }

    [[nodiscard]] TestScalarAsyncOperator withInferredArguments(const AsyncArgumentsWithConstants& binding) const
    {
        if (!binding.empty())
        {
            throw InvalidQuerySyntax("Test scalar operator takes no arguments");
        }
        return *this;
    }

    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext&, SharedRuntimeValues) const
    {
        return std::unexpected(AsyncError{.exception = IOError("Unsupported test operator")});
    }

    [[nodiscard]] std::optional<size_t> maxConcurrency() const { return std::nullopt; }
};

struct TestMultiAsyncOperator
{
    DataType resultType;

    static TestMultiAsyncOperator create(AsyncOperatorRegistryArguments args)
    {
        if (args.argumentTypes.size() != 1)
        {
            throw InvalidQuerySyntax("Test multi-output operator takes exactly one argument");
        }
        return {args.argumentTypes.front()};
    }

    [[nodiscard]] std::string_view getType() const { return multiName; }

    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getOutputSchema() const
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("value"), resultType},
            UnqualifiedUnboundField{Identifier::parse("count"), DataType::Type::UINT64}};
    }

    [[nodiscard]] TestMultiAsyncOperator withInferredArguments(const AsyncArgumentsWithConstants& binding) const
    {
        if (binding.size() != 1)
        {
            throw InvalidQuerySyntax("Test multi-output operator takes exactly one argument");
        }
        return {binding.front().first};
    }

    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext&, SharedRuntimeValues) const
    {
        return std::unexpected(AsyncError{.exception = IOError("Unsupported test operator")});
    }

    [[nodiscard]] std::optional<size_t> maxConcurrency() const { return std::nullopt; }
};

/// NOLINTEND(readability-convert-member-functions-to-static)

static_assert(AsyncOperatorConcept<TestScalarAsyncOperator>);
static_assert(AsyncOperatorConcept<TestMultiAsyncOperator>);
static_assert(std::is_aggregate_v<TestScalarAsyncOperator> && !std::is_polymorphic_v<TestScalarAsyncOperator>);

class AsyncOccurrenceTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        ASSERT_TRUE(AsyncOperatorRegistry::instance().addEntry("TEST_CUSTOM_PAYLOAD", &NES::Test::CustomReflectedAsyncOperator::create));
        ASSERT_TRUE(AsyncOperatorRegistry::instance().addEntry(scalarName, &TestScalarAsyncOperator::create));
        ASSERT_TRUE(AsyncOperatorRegistry::instance().addEntry(multiName, &TestMultiAsyncOperator::create));
        ASSERT_TRUE(AsyncOperatorRegistry::instance().addEntry("TEST_SCALAR_ALIAS", &TestScalarAsyncOperator::create));
        ASSERT_TRUE(
            AsyncOperatorUnreflectionRegistry::instance().addEntry(
                scalarName,
                [](const Reflected& data, const ReflectionContext& context) -> AsyncOperator
                { return context.unreflect<TestScalarAsyncOperator>(data); }));
        ASSERT_TRUE(
            AsyncOperatorUnreflectionRegistry::instance().addEntry(
                multiName,
                [](const Reflected& data, const ReflectionContext& context) -> AsyncOperator
                { return context.unreflect<TestMultiAsyncOperator>(data); }));
        ASSERT_TRUE(
            AsyncOperatorUnreflectionRegistry::instance().addEntry(
                "TEST_CUSTOM_PAYLOAD",
                [](const Reflected& data, const ReflectionContext& context) -> AsyncOperator
                { return context.unreflect<NES::Test::CustomReflectedAsyncOperator>(data); }));
    }

    /// Construct a bound source through the current uniform source configuration API.
    /// NOLINTNEXTLINE(misc-include-cleaner) LogicalOperator is declared by public Operators/LogicalOperator.hpp.
    static LogicalOperator source()
    {
        const auto schema = Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("input"), DataTypeProvider::provideDataType(DataType::Type::INT32)}};
        const SourceCatalog catalog;
        const auto descriptor = catalog.getAnonymousSource(
            Identifier::parse("FILE"),
            schema,
            Host{"localhost:8080"},
            {{Identifier::parse("TYPE"), "NATIVE"}},
            {{Identifier::parse("FILE_PATH"), "/dev/null"}});
        if (!descriptor)
        {
            throw TestException("Could not construct async occurrence test source");
        }
        return SourceDescriptorLogicalOperator::create(*descriptor);
    }
};

TEST_F(AsyncOccurrenceTest, PlainStructPayloadSupportsTypedAccessAndValueSemantics)
{
    AsyncOperator original = TestScalarAsyncOperator{.marker = 37};
    const auto typed = original.tryGetAs<TestScalarAsyncOperator>();
    ASSERT_TRUE(typed.has_value());
    const AsyncOperator erasedAgain = *typed;
    const auto copied = original.withInferredArguments(AsyncArgumentsWithConstants{});
    original = TestScalarAsyncOperator{.marker = 99};
    EXPECT_EQ(typed->get().marker, 37);
    EXPECT_EQ(erasedAgain.getAs<TestScalarAsyncOperator>()->marker, 37);
    EXPECT_EQ(copied.getAs<TestScalarAsyncOperator>()->marker, 37);
    EXPECT_EQ(original.getAs<TestScalarAsyncOperator>()->marker, 99);
    EXPECT_FALSE(copied.tryGetAs<TestMultiAsyncOperator>().has_value());
}

TEST_F(AsyncOccurrenceTest, LogicalOperatorRejectsAnEmptyNativeOutputSignature)
{
    const auto child = source();
    const AsyncOperator config = TestScalarAsyncOperator{.marker = 0, .emptyOutputs = true};
    EXPECT_THROW(static_cast<void>(AsyncLogicalOperator::create(child, config, {})), Exception);
}

TEST_F(AsyncOccurrenceTest, ReflectionPreservesConcreteStateWithoutReplayingFactory)
{
    scalarFactoryCalls = 0;
    const auto call = LogicalFunctionProvider::provide("TEST_SCALAR_ALIAS", {}).getAs<AsyncCallLogicalFunction>().get();
    ASSERT_EQ(scalarFactoryCalls, 1);
    TypeInferenceContext inferenceContext{source().getOutputSchema()};
    const auto inferred = call.withChildren({}).withInferredDataType(inferenceContext);
    EXPECT_EQ(inferred.getAs<AsyncCallLogicalFunction>()->getAsyncOperator().getAs<TestScalarAsyncOperator>()->marker, 1);
    const ReflectionContext writer{std::make_shared<SharedPointerSerializationState>()};
    const ReflectionContext reader{std::make_shared<SharedPointerDeserializationState>()};
    const auto restored = reader.unreflect<AsyncCallLogicalFunction>(writer.reflect(call));
    EXPECT_EQ(restored.getAsyncOperator().getAs<TestScalarAsyncOperator>()->marker, 1);
    EXPECT_EQ(restored, call);
    const AsyncOperator custom = TestScalarAsyncOperator{.marker = 73};
    const auto decoded = reader.unreflect<AsyncOperator>(writer.reflect(custom));
    EXPECT_EQ(decoded.getAs<TestScalarAsyncOperator>()->marker, 73);
    EXPECT_EQ(scalarFactoryCalls, 1);
}

TEST_F(AsyncOccurrenceTest, LogicalReflectionKeepsExpressionsOutsideNativeConfig)
{
    customFactoryCalls = 0;
    const std::vector<LogicalFunction> arguments{
        ConstantValueLogicalFunction{DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "7"}};
    const auto call = LogicalFunctionProvider::provide("TEST_CUSTOM_PAYLOAD", arguments).getAs<AsyncCallLogicalFunction>().get();
    ASSERT_EQ(customFactoryCalls, 1);
    const ReflectionContext writer{std::make_shared<SharedPointerSerializationState>()};
    const ReflectionContext reader{std::make_shared<SharedPointerDeserializationState>()};
    const auto restored = reader.unreflect<AsyncCallLogicalFunction>(writer.reflect(call));
    EXPECT_TRUE(std::ranges::equal(restored.getArguments(), arguments, StructuralFunctionEqual{}));
    EXPECT_EQ(restored.getAsyncOperator().getAs<NES::Test::CustomReflectedAsyncOperator>()->getValue(), 73);
    EXPECT_EQ(customFactoryCalls, 1);
}

TEST_F(AsyncOccurrenceTest, CustomReflectionPreservesPrivateNonaggregateState)
{
    static_assert(!std::is_aggregate_v<NES::Test::CustomReflectedAsyncOperator>);
    static_assert(AsyncOperatorConcept<NES::Test::CustomReflectedAsyncOperator>);
    const AsyncOperator original = NES::Test::CustomReflectedAsyncOperator{73};
    const ReflectionContext context;
    const auto wire = context.reflect(original);
    const auto restored = context.unreflect<AsyncOperator>(wire);
    EXPECT_EQ(restored.getAs<NES::Test::CustomReflectedAsyncOperator>()->getValue(), 73);
    const auto concrete = original.getAs<NES::Test::CustomReflectedAsyncOperator>();
    const auto typedWire = context.reflect(concrete);
    const auto typed = context.unreflect<TypedAsyncOperator<NES::Test::CustomReflectedAsyncOperator>>(typedWire);
    EXPECT_EQ(typed->getValue(), 73);
    EXPECT_THROW(static_cast<void>(context.unreflect<TypedAsyncOperator<TestMultiAsyncOperator>>(typedWire)), Exception);
    const auto erasedTyped = restored.getAs<NES::Test::CustomReflectedAsyncOperator>();
    EXPECT_EQ(erasedTyped->getValue(), 73);
    EXPECT_FALSE(restored.tryGetAs<TestMultiAsyncOperator>());
}

TEST_F(AsyncOccurrenceTest, NormalizationTransfersFactoryPayloadInsteadOfConstructingItAgain)
{
    scalarFactoryCalls = 0;
    const auto call = LogicalFunctionProvider::provide("TEST_SCALAR_ALIAS", {});
    const LogicalOperator projection
        = ProjectionLogicalOperator::create(source(), {{Identifier::parse("result"), call}}, ProjectionLogicalOperator::Asterisk{false});
    const auto normalized = AsyncFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {projection}});
    const auto invocation
        = normalized.getRootOperators().front().getAs<ProjectionLogicalOperator>()->getChild().getAs<AsyncLogicalOperator>();
    EXPECT_EQ(invocation->getAsyncOperator().getAs<TestScalarAsyncOperator>()->marker, 1);
    EXPECT_EQ(scalarFactoryCalls, 1);
}

TEST_F(AsyncOccurrenceTest, ProviderCallsHaveDistinctIdentityEvenWithIdenticalArguments)
{
    const auto first = LogicalFunctionProvider::provide(scalarName, {});
    const auto second = LogicalFunctionProvider::provide(scalarName, {});
    EXPECT_NE(first, second);
    EXPECT_EQ(first, first);
    EXPECT_TRUE(StructuralFunctionEqual{}(first, second));
}

TEST_F(AsyncOccurrenceTest, TypeInferenceContextPreservesSharedDagNodesAndDistinctInstances)
{
    const auto schema = source().getOutputSchema();
    const auto sharedCall = LogicalFunctionProvider::provide(scalarName, {});
    const auto separateCall = LogicalFunctionProvider::provide(scalarName, {});
    const LogicalFunction one = ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::UINT64), "1"};
    const LogicalFunction two = ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::UINT64), "2"};

    /// BETWEEN-style DAG: both bounds share the exact same AsyncCallLogicalFunction pointer.
    const LogicalFunction between = AndLogicalFunction{
        GreaterEqualsLogicalFunction{sharedCall, one}, LessEqualsLogicalFunction{sharedCall, two}};
    const auto inferredBetween = between.withInferredDataType(schema);
    const auto inferredLeftCall = inferredBetween.getChildren().at(0).getChildren().at(0);
    const auto inferredRightCall = inferredBetween.getChildren().at(1).getChildren().at(0);
    EXPECT_EQ(inferredLeftCall, inferredRightCall);

    /// Nested pure expression shared across IN-style OR branches.
    const LogicalFunction sharedAdd = AddLogicalFunction{sharedCall, one};
    const LogicalFunction inExpr = OrLogicalFunction{
        EqualsLogicalFunction{sharedAdd, one}, EqualsLogicalFunction{sharedAdd, two}};
    const auto inferredIn = inExpr.withInferredDataType(schema);
    const auto inferredAddLeft = inferredIn.getChildren().at(0).getChildren().at(0);
    const auto inferredAddRight = inferredIn.getChildren().at(1).getChildren().at(0);
    EXPECT_EQ(inferredAddLeft, inferredAddRight);
    EXPECT_EQ(inferredAddLeft.getChildren().at(0), inferredAddRight.getChildren().at(0));

    /// Distinct call nodes remain distinct after type inference.
    const LogicalFunction distinctSum = AddLogicalFunction{sharedCall, separateCall};
    const auto inferredSum = distinctSum.withInferredDataType(schema);
    EXPECT_NE(inferredSum.getChildren().at(0), inferredSum.getChildren().at(1));
    EXPECT_TRUE(StructuralFunctionEqual{}(inferredSum.getChildren().at(0), inferredSum.getChildren().at(1)));

    /// Lifting turns a shared DAG call into one AsyncLogicalOperator and two distinct calls into two.
    const LogicalOperator sharedSelection = SelectionLogicalOperator::create(source(), inExpr);
    const auto liftedShared = AsyncFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {sharedSelection}});
    EXPECT_EQ(getOperatorByType<AsyncLogicalOperator>(liftedShared).size(), 1);

    const LogicalOperator distinctProjection
        = ProjectionLogicalOperator::create(source(), {{Identifier::parse("sum"), distinctSum}}, ProjectionLogicalOperator::Asterisk{false});
    const auto liftedDistinct = AsyncFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {distinctProjection}});
    EXPECT_EQ(getOperatorByType<AsyncLogicalOperator>(liftedDistinct).size(), 2);
}

TEST_F(AsyncOccurrenceTest, ProjectionInferencePreservesSharingAcrossExpressionRoots)
{
    const auto child = source();
    const auto sharedCall = LogicalFunctionProvider::provide(scalarName, {});
    const auto distinctCall = LogicalFunctionProvider::provide(scalarName, {});
    const LogicalOperator projection = ProjectionLogicalOperator::create(
        child,
        {{Identifier::parse("first"), sharedCall}, {Identifier::parse("second"), sharedCall}, {Identifier::parse("third"), distinctCall}},
        ProjectionLogicalOperator::Asterisk{false});
    const auto rewritten = projection.withChildren({child});
    const auto expressions = rewritten.getAs<ProjectionLogicalOperator>()->getProjections();
    ASSERT_EQ(expressions.size(), 3);
    EXPECT_EQ(expressions.at(0).second, expressions.at(1).second);
    EXPECT_NE(expressions.at(0).second, expressions.at(2).second);

    const auto lifted = AsyncFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {rewritten}});
    EXPECT_EQ(getOperatorByType<AsyncLogicalOperator>(lifted).size(), 2);
    const auto outputs = lifted.getRootOperators().front().getAs<ProjectionLogicalOperator>()->getProjections();
    ASSERT_EQ(outputs.size(), 3);
    const auto first = outputs.at(0).second.getAs<FieldAccessLogicalFunction>()->getField().getFullyQualifiedName();
    const auto second = outputs.at(1).second.getAs<FieldAccessLogicalFunction>()->getField().getFullyQualifiedName();
    const auto third = outputs.at(2).second.getAs<FieldAccessLogicalFunction>()->getField().getFullyQualifiedName();
    EXPECT_EQ(first, second);
    EXPECT_NE(first, third);
}

TEST_F(AsyncOccurrenceTest, TypeInferenceContextRetainsSuccessfulChildrenAfterParentFailure)
{
    TypeInferenceContext context{source().getOutputSchema()};
    const auto call = LogicalFunctionProvider::provide(scalarName, {});
    const LogicalFunction text
        = ConstantValueLogicalFunction{DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "not numeric"};
    const LogicalFunction invalid = AddLogicalFunction{call, text};
    EXPECT_THROW(static_cast<void>(context.inferType(invalid)), Exception);
    const auto inferredCall = context.inferType(call);

    const LogicalFunction one = ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "1"};
    const auto valid = context.inferType(LogicalFunction{AddLogicalFunction{call, one}});
    ASSERT_EQ(valid.getChildren().size(), 2);
    EXPECT_EQ(valid.getChildren().front(), inferredCall);
    EXPECT_THROW(static_cast<void>(context.inferType(invalid)), Exception);
    EXPECT_EQ(context.inferType(call), inferredCall);
}

TEST_F(AsyncOccurrenceTest, IsNaNDesugaringPreservesSharedChildIdentity)
{
    const auto floatSchema = Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{Identifier::parse("x"), DataTypeProvider::provideDataType(DataType::Type::FLOAT64)}};
    const SourceCatalog catalog;
    const auto descriptor = catalog.getAnonymousSource(
        Identifier::parse("FILE"),
        floatSchema,
        Host{"localhost:8080"},
        {{Identifier::parse("TYPE"), "NATIVE"}},
        {{Identifier::parse("FILE_PATH"), "/dev/null"}});
    ASSERT_TRUE(descriptor.has_value());
    const LogicalOperator floatSource = SourceDescriptorLogicalOperator::create(*descriptor);

    const LogicalFunction call = LogicalFunctionProvider::provide(scalarName, {});
    const LogicalFunction sumFloat = AddLogicalFunction{call, UnboundFieldAccessLogicalFunction{Identifier::parse("x")}};
    const LogicalFunction isNaN = IsNaNLogicalFunction{sumFloat};

    /// IS NAN on a float desugars to NOT(x = x) where both sides share the same inferred expression and async call.
    const auto inferred = isNaN.withInferredDataType(floatSource.getOutputSchema());
    const auto equalsNode = inferred.getChildren().at(0);
    EXPECT_EQ(equalsNode.getChildren().at(0), equalsNode.getChildren().at(1));

    const LogicalOperator selection = SelectionLogicalOperator::create(floatSource, isNaN);
    const auto lifted = AsyncFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {selection}});
    EXPECT_EQ(getOperatorByType<AsyncLogicalOperator>(lifted).size(), 1);
}

TEST_F(AsyncOccurrenceTest, IntegerIsNaNPreservesOperandEffectsAndNullability)
{
    const auto child = source();
    const LogicalFunction call = LogicalFunctionProvider::provide(scalarName, {});
    const LogicalFunction isNaN = IsNaNLogicalFunction{call};
    const auto inferred = isNaN.withInferredDataType(child.getOutputSchema());
    const auto operands = inferred.getChildren().at(0).getChildren();
    ASSERT_EQ(operands.size(), 2);
    EXPECT_EQ(operands.at(0), operands.at(1));
    ASSERT_TRUE(operands.front().tryGetAs<AsyncCallLogicalFunction>().has_value());
    const LogicalOperator selection = SelectionLogicalOperator::create(child, isNaN);
    const auto lifted = AsyncFunctionLiftingRule{}.apply(LogicalPlan{INVALID_QUERY_ID, {selection}});
    EXPECT_EQ(getOperatorByType<AsyncLogicalOperator>(lifted).size(), 1);

    const LogicalFunction nullable = ConstantValueLogicalFunction{DataType{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE}, "1"};
    const LogicalFunction nullableIsNaN = IsNaNLogicalFunction{nullable};
    const auto nullableInferred = nullableIsNaN.withInferredDataType(child.getOutputSchema());
    EXPECT_TRUE(nullableInferred.getDataType().nullable);
    EXPECT_EQ(nullableInferred.getChildren().at(0).getChildren().size(), 2);
}

TEST_F(AsyncOccurrenceTest, LogicalFunctionReflectionPreservesSharedDagNodes)
{
    const auto sharedCall = LogicalFunctionProvider::provide(scalarName, {});
    const auto separateCall = LogicalFunctionProvider::provide(scalarName, {});
    const LogicalFunction one = ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::UINT64), "1"};
    const LogicalFunction two = ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::UINT64), "2"};

    const LogicalFunction dag = AndLogicalFunction{
        EqualsLogicalFunction{sharedCall, one},
        AndLogicalFunction{EqualsLogicalFunction{sharedCall, two}, EqualsLogicalFunction{separateCall, one}}};

    /// Reflecting or unreflecting without shared pointer state fails fast.
    EXPECT_THROW(static_cast<void>(ReflectionContext{}.reflect(dag)), Exception);

    const ReflectionContext writer{std::make_shared<SharedPointerSerializationState>()};
    const ReflectionContext reader{std::make_shared<SharedPointerDeserializationState>()};
    const auto wire = writer.reflect(dag);
    EXPECT_THROW(static_cast<void>(ReflectionContext{}.unreflect<LogicalFunction>(wire)), Exception);

    const auto restored = reader.unreflect<LogicalFunction>(wire);
    EXPECT_NE(restored, dag);
    EXPECT_TRUE(*restored == *dag);

    const auto restoredSharedLeft = restored.getChildren().at(0).getChildren().at(0);
    const auto restoredSharedRight = restored.getChildren().at(1).getChildren().at(0).getChildren().at(0);
    const auto restoredSeparate = restored.getChildren().at(1).getChildren().at(1).getChildren().at(0);
    EXPECT_EQ(restoredSharedLeft, restoredSharedRight);
    EXPECT_NE(restoredSharedLeft, sharedCall);
    EXPECT_NE(restoredSharedLeft, restoredSeparate);
    EXPECT_TRUE(*restoredSharedLeft == *restoredSeparate);
}

TEST_F(AsyncOccurrenceTest, MultioutputDefinitionRejectsScalarShorthand)
{
    EXPECT_ANY_THROW((LogicalFunctionProvider::provide(multiName, {UnboundFieldAccessLogicalFunction{Identifier::parse("input")}})));
}

TEST_F(AsyncOccurrenceTest, ExplicitOperatorAppendsEveryOutputWithDeclaredTypesAndBindings)
{
    const auto child = source();
    const auto op = AsyncLogicalOperator::create(
        child,
        multiName,
        {UnboundFieldAccessLogicalFunction{Identifier::parse("input")}},
        {Identifier::parse("result"), Identifier::parse("counted")});
    EXPECT_EQ(op.get().getChild(), child);
    EXPECT_EQ(op.get().getOperatorName(), multiName);
    EXPECT_EQ(op->getAsyncOperator().getType(), multiName);
    ASSERT_EQ(op.get().getArguments().size(), 1);
    const auto outputs = op.get().getOutputBindings();
    ASSERT_EQ(outputs.size(), 2);
    /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) outputs has two elements as asserted above
    EXPECT_EQ(outputs[0].binding, Identifier::parse("result"));
    EXPECT_EQ(outputs[1].binding, Identifier::parse("counted"));
    EXPECT_EQ(outputs[0].field.getDataType(), DataTypeProvider::provideDataType(DataType::Type::INT32));
    EXPECT_EQ(outputs[1].field.getDataType(), DataTypeProvider::provideDataType(DataType::Type::UINT64));
    /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    EXPECT_EQ(op.get().getOutputSchema().size(), child.getOutputSchema().size() + 2);
    EXPECT_EQ(op.get().withChildren({child}).getOutputBindings(), outputs);
    EXPECT_EQ(op.get().withInferredSchema().getOutputBindings(), outputs);
    const auto second = AsyncLogicalOperator::create(child, multiName, {UnboundFieldAccessLogicalFunction{Identifier::parse("input")}});
    EXPECT_NE(LogicalOperator{second}, LogicalOperator{op});
}

/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST_F(AsyncOccurrenceTest, MultioutputInvocationExecutesOnceEvenWhenResultsAreDiscardedAboveIt)
{
    const auto child = source();
    const LogicalOperator invocation = AsyncLogicalOperator::create(
        child,
        multiName,
        {UnboundFieldAccessLogicalFunction{Identifier::parse("input")}},
        {Identifier::parse("result"), Identifier::parse("counted")});
    const LogicalOperator discard = ProjectionLogicalOperator::create(
        invocation,
        {{Identifier::parse("input"), UnboundFieldAccessLogicalFunction{Identifier::parse("input")}}},
        ProjectionLogicalOperator::Asterisk{false});
    auto sources = std::make_shared<SourceCatalog>();
    auto sinks = std::make_shared<SinkCatalog>();
    auto workers = std::make_shared<WorkerCatalog>();
    const Host host{"localhost:8080"};
    ASSERT_TRUE(workers->addWorker(host, host.getRawValue(), CapacityKind::Unlimited{}, {}));
    const Schema<UnqualifiedUnboundField, Ordered> schema{
        UnqualifiedUnboundField{Identifier::parse("input"), DataTypeProvider::provideDataType(DataType::Type::INT32)}};
    const auto sink = sinks->getAnonymousSink(schema, Identifier::parse("VOID"), host, {}, {});
    ASSERT_TRUE(sink.has_value());
    const QueryOptimizer optimizer{
        defaultConfiguration<QueryOptimizerConfiguration>(), sources, sinks, workers, std::make_shared<ModelCatalog>()};
    const auto plans = optimizer.optimize(LogicalPlan{INVALID_QUERY_ID, {SinkLogicalOperator::create(discard, *sink)}});
    std::shared_ptr<const AsyncCallGraph> graph;
    for (const auto& local : plans[host])
    {
        for (const auto& op : BFSRange(local.getRootOperators().front()))
        {
            if (const auto sourceOp = op.tryGetAs<SourceDescriptorLogicalOperator>())
            {
                const auto& descriptor = sourceOp.value()->getSourceDescriptor();
                if (descriptor.getSourceType() == Identifier::parse("ASYNC"))
                {
                    ASSERT_FALSE(graph);
                    graph = std::make_shared<AsyncCallGraph>(descriptor.getPluginData().getAs<AsyncSourceConfig>().calls);
                }
            }
        }
    }
    ASSERT_TRUE(graph);
    ASSERT_EQ(graph->getCalls().size(), 1);
    EXPECT_EQ(graph->getCalls().front()->getFunctionType(), multiName);
    ASSERT_EQ(graph->getOutputs().size(), 2);
    /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) graph output count is asserted above
    EXPECT_EQ(graph->getOutputs()[0].result.getProducer(), graph->getOutputs()[1].result.getProducer());
    EXPECT_EQ(graph->getOutputs()[0].field, QualifiedIdentifier::create(Identifier::parse("result")));
    EXPECT_EQ(graph->getOutputs()[1].field, QualifiedIdentifier::create(Identifier::parse("counted")));
    /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    boost::asio::io_context context;
    const auto strand = boost::asio::make_strand(context);
    size_t calls = 0;
    const auto execution = std::make_shared<AsyncGraphExecution>(
        strand,
        graph,
        [](const InputFieldReference&) { return RuntimeValue{int32_t{7}}; },
        [&calls](AsyncCall::Ptr, SharedRuntimeValues arguments) -> boost::asio::awaitable<AsyncResult<SharedRuntimeValues>>
        {
            ++calls;
            co_return SharedRuntimeValues::make(arguments[0].toOwned(), uint64_t{9});
        });
    auto result = boost::asio::co_spawn(
        strand,
        execution->run(),
        boost::asio::use_future);
    context.run();
    const auto values = result.get();
    ASSERT_TRUE(values);
    ASSERT_EQ(values->size(), 2);
    /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) successful result size is asserted above
    EXPECT_EQ((*values)[0].get<int32_t>(), 7);
    EXPECT_EQ((*values)[1].get<uint64_t>(), 9);
    /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    EXPECT_EQ(calls, 1);
}
/// NOLINTEND(misc-include-cleaner)

TEST_F(AsyncOccurrenceTest, RuntimeGraphRoundTripRetainsPrivateConfigWithoutLogicalAstOrFactoryReplay)
{
    customFactoryCalls = 0;
    const auto native = createAsyncOperator("TEST_CUSTOM_PAYLOAD", {DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}});
    ASSERT_EQ(customFactoryCalls, 1);
    const auto child = source();
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) schema lookup returns an optional checked below
    const auto input = child.getOutputSchema()[Identifier::parse("input")];
    ASSERT_TRUE(input.has_value());
    const LogicalOperator invocation
        = AsyncLogicalOperator::create(child, native, {FieldAccessLogicalFunction{*input}}, {Identifier::parse("result")});
    const LogicalOperator projection = ProjectionLogicalOperator::create(
        invocation,
        {{Identifier::parse("input"), UnboundFieldAccessLogicalFunction{Identifier::parse("input")}}},
        ProjectionLogicalOperator::Asterisk{false});

    auto sources = std::make_shared<SourceCatalog>();
    auto sinks = std::make_shared<SinkCatalog>();
    auto workers = std::make_shared<WorkerCatalog>();
    const Host host{"localhost:8080"};
    ASSERT_TRUE(workers->addWorker(host, host.getRawValue(), CapacityKind::Unlimited{}, {}));
    const Schema<UnqualifiedUnboundField, Ordered> schema{
        UnqualifiedUnboundField{Identifier::parse("input"), DataTypeProvider::provideDataType(DataType::Type::INT32)}};
    const auto sink = sinks->getAnonymousSink(schema, Identifier::parse("VOID"), host, {}, {});
    ASSERT_TRUE(sink.has_value());
    const QueryOptimizer optimizer{
        defaultConfiguration<QueryOptimizerConfiguration>(), sources, sinks, workers, std::make_shared<ModelCatalog>()};
    const auto plans = optimizer.optimize(LogicalPlan{INVALID_QUERY_ID, {SinkLogicalOperator::create(projection, *sink)}});

    std::optional<AsyncCallGraph> graph;
    for (const auto& local : plans[host])
    {
        for (const auto& op : BFSRange(local.getRootOperators().front()))
        {
            if (const auto sourceOp = op.tryGetAs<SourceDescriptorLogicalOperator>())
            {
                const auto& descriptor = sourceOp.value()->getSourceDescriptor();
                if (descriptor.getSourceType() == Identifier::parse("ASYNC"))
                {
                    ASSERT_FALSE(graph.has_value());
                    graph = descriptor.getPluginData().getAs<AsyncSourceConfig>().calls;
                }
            }
        }
    }
    ASSERT_TRUE(graph.has_value());
    ASSERT_EQ(graph->getCalls().size(), 1);
    ASSERT_EQ(graph->getCalls().front()->getArguments().size(), 1);
    ASSERT_TRUE(std::holds_alternative<InputFieldReference>(graph->getCalls().front()->getArguments().front()));
    const auto& originalConfig = graph->getCalls().front()->getAsyncOperator();
    EXPECT_EQ(originalConfig.getAs<NES::Test::CustomReflectedAsyncOperator>()->getValue(), 73);
    EXPECT_EQ(customFactoryCalls, 1);

    const ReflectionContext wireContext;
    const auto wire = wireContext.reflect(*graph);
    const ReflectionContext freshContext;
    const auto restored = freshContext.unreflect<AsyncCallGraph>(wire);
    ASSERT_EQ(restored.getCalls().size(), 1);
    const auto& restoredConfig = restored.getCalls().front()->getAsyncOperator();
    EXPECT_EQ(restoredConfig.getAs<NES::Test::CustomReflectedAsyncOperator>()->getValue(), 73);
    EXPECT_EQ(customFactoryCalls, 1);

    boost::asio::io_context ioContext;
    const auto strand = boost::asio::make_strand(ioContext);
    AsyncExecutionContext executionContext;
    auto result = boost::asio::co_spawn(
        strand, invokeOnce(restoredConfig, executionContext, SharedRuntimeValues::make(uint32_t{7})), boost::asio::use_future);
    ioContext.run();
    const auto invocationResult = result.get();
    ASSERT_TRUE(invocationResult);
    ASSERT_EQ(invocationResult->size(), 1);
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) successful result size is asserted above
    EXPECT_EQ((*invocationResult)[0].get<uint64_t>(), 73);
    EXPECT_EQ(customFactoryCalls, 1);
}

TEST_F(AsyncOccurrenceTest, ExplicitOperatorRejectsWrongArityAndCollidingBindings)
{
    const auto child = source();
    const std::vector<LogicalFunction> args{UnboundFieldAccessLogicalFunction{Identifier::parse("input")}};
    EXPECT_ANY_THROW(static_cast<void>(AsyncLogicalOperator::create(child, multiName, {})));
    EXPECT_ANY_THROW(static_cast<void>(AsyncLogicalOperator::create(child, multiName, args, {Identifier::parse("only_one")})));
    EXPECT_ANY_THROW(
        static_cast<void>(
            AsyncLogicalOperator::create(child, multiName, args, {Identifier::parse("input"), Identifier::parse("counted")})));
    EXPECT_ANY_THROW(
        static_cast<void>(AsyncLogicalOperator::create(child, multiName, args, {Identifier::parse("same"), Identifier::parse("same")})));
}
}
}

/// NOLINTEND(bugprone-unchecked-optional-access)
/// NOLINTEND(readability-magic-numbers)
