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
#include <memory>
#include <ranges>
#include <string>
#include <unordered_map>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <QueryOptimizerConfiguration.hpp>

#include <Rules/Static/DecideJoinTypesRule.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinFunctionSplit.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Traits/JoinImplementationTypeTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>

#include <Operators/LogicalOperatorFwd.hpp>
#include <Util/UUID.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>

namespace NES
{
namespace
{

/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class DecideJoinTypesTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("DecideJoinTypesTest.log", LogLevel::LOG_DEBUG); }

    static constexpr uint64_t TUMBLING_WINDOW_SIZE_MS = 1000;

    static Schema<UnqualifiedUnboundField, Ordered> createSchema(const std::string& prefix)
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            {Identifier::parse(prefix + "_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
            {Identifier::parse(prefix + "_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
            {Identifier::parse(prefix + "_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};
    }

    static Windowing::TimeBasedWindowType createTumblingWindow()
    {
        return Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure(TUMBLING_WINDOW_SIZE_MS)}};
    }

    static LogicalSource createLogicalSource(const Identifier& sourceName, const Schema<UnqualifiedUnboundField, Ordered>& schema)
    {
        return LogicalSource{sourceName, schema};
    }

    static SourceDescriptor createSourceDescriptor(const PhysicalSourceId physicalSourceId, const LogicalSource& logicalSource)
    {
        const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("FILE_PATH"), "/dev/null"}};
        return SourceDescriptor::create(
                   physicalSourceId,
                   logicalSource,
                   Identifier::parse("File"),
                   Host{"localhost"},
                   sourceConfig,
                   {{Identifier::parse("type"), "CSV"}},
                   false)
            .value();
    }

    static SinkDescriptor createSinkDescriptor(const Identifier& sinkName, const Schema<UnqualifiedUnboundField, Ordered>& schema)
    {
        const std::unordered_map<Identifier, std::string> sinkConfig{
            {Identifier::parse("FILE_PATH"), "/dev/null"}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}};
        return SinkDescriptor::createNamed(INVALID_SINK_ID, sinkName, Identifier::parse("file"), schema, Host{"localhost"}, sinkConfig, {})
            .value();
    }

    /// Build the expected join output schema for a left/right (prefix "left"/"right") join. For outer joins the fields of the side that may
    /// be unmatched become nullable, mirroring JoinLogicalOperator::inferLocalSchema; the window START/END fields are never nullable.
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
            {Identifier::parse("left_ts"), leftType},
            {Identifier::parse("right_id"), rightType},
            {Identifier::parse("right_value"), rightType},
            {Identifier::parse("right_ts"), rightType},
            {Identifier::parse("START"), u64},
            {Identifier::parse("END"), u64}};
    }

    /// Builds Source(left) JOIN Source(right) ON <joinFunction> -> Sink and returns the join after applying DecideJoinTypesRule.
    /// `createJoinFunction` receives the two source operators to bind its field accesses to.
    template <typename CreateJoinFunction>
    static TypedLogicalOperator<JoinLogicalOperator> applyRuleToJoin(
        CreateJoinFunction createJoinFunction, const JoinLogicalOperator::JoinType joinType, const StreamJoinStrategy joinStrategy)
    {
        const auto leftSourceOp = SourceDescriptorLogicalOperator::create(
            createSourceDescriptor(PhysicalSourceId{1}, createLogicalSource(Identifier::parse("LEFT_TEST"), createSchema("left"))));
        const auto rightSourceOp = SourceDescriptorLogicalOperator::create(
            createSourceDescriptor(PhysicalSourceId{2}, createLogicalSource(Identifier::parse("RIGHT_TEST"), createSchema("right"))));

        auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
            {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
             Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});
        auto joinOp = JoinLogicalOperator::create(
            std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
            createJoinFunction(leftSourceOp, rightSourceOp),
            createTumblingWindow(),
            joinType,
            characteristics.value());

        const bool leftNullable
            = joinType == JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN or joinType == JoinLogicalOperator::JoinType::OUTER_FULL_JOIN;
        const bool rightNullable
            = joinType == JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN or joinType == JoinLogicalOperator::JoinType::OUTER_FULL_JOIN;
        auto sinkDescriptor = createSinkDescriptor(Identifier::parse("test_sink"), createJoinOutputSchema(leftNullable, rightNullable));
        auto sinkOp = SinkLogicalOperator::create(sinkDescriptor).withChildrenUnsafe({joinOp});
        const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp->withInferredSchema()}};

        const DecideJoinTypesRule rule(joinStrategy);
        auto joins = getOperatorByType<JoinLogicalOperator>(rule.apply(plan));
        INVARIANT(joins.size() == 1, "Expected exactly one join");
        return joins[0];
    }

    static JoinImplementation getJoinImplementation(const TypedLogicalOperator<JoinLogicalOperator>& join)
    {
        return join->getTraitSet().get<JoinImplementationTypeTrait>()->implementationType;
    }

    static FieldAccessLogicalFunction fieldOf(const TypedLogicalOperator<SourceDescriptorLogicalOperator>& source, const std::string& name)
    {
        return FieldAccessLogicalFunction{source->getOutputSchema().getFieldByName(Identifier::parse(name)).value()};
    }

    /// left_id = right_id AND right_value >= left_value AND right_ts <= left_ts
    static LogicalFunction equiAndRangeCondition(
        const TypedLogicalOperator<SourceDescriptorLogicalOperator>& left,
        const TypedLogicalOperator<SourceDescriptorLogicalOperator>& right)
    {
        return AndLogicalFunction{
            AndLogicalFunction{
                EqualsLogicalFunction{fieldOf(left, "left_id"), fieldOf(right, "right_id")},
                GreaterEqualsLogicalFunction{fieldOf(right, "right_value"), fieldOf(left, "left_value")}},
            LessEqualsLogicalFunction{fieldOf(right, "right_ts"), fieldOf(left, "left_ts")}};
    }
};

/// A simple Selection → AnonymousSource plan. Verify all operators get CHOICELESS.
TEST_F(DecideJoinTypesTest, NonJoinPlanGetChoicelessTrait)
{
    auto schema = createSchema("src");
    auto logicalSource = createLogicalSource(Identifier::parse("TEST"), schema);
    auto sourceDescriptor = createSourceDescriptor(PhysicalSourceId{1}, logicalSource);
    auto sinkDescriptor = createSinkDescriptor(Identifier::parse("test_sink"), schema);

    const auto sourceOp = SourceDescriptorLogicalOperator::create(sourceDescriptor);

    auto selectionFn = EqualsLogicalFunction{
        FieldAccessLogicalFunction{sourceOp->getOutputSchema().getFieldByName(Identifier::parse("src_id")).value()},
        FieldAccessLogicalFunction{sourceOp->getOutputSchema().getFieldByName(Identifier::parse("src_id")).value()}};

    auto selectionOp = SelectionLogicalOperator::create(sourceOp, selectionFn);
    auto sinkOp = SinkLogicalOperator::create(selectionOp, sinkDescriptor);
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp->withInferredSchema()}};

    DecideJoinTypesRule rule(StreamJoinStrategy::OPTIMIZER_CHOOSES);
    auto result = rule.apply(plan);

    for (const auto& op : BFSRange(result.getRootOperators()[0]))
    {
        ASSERT_TRUE(op.getTraitSet().contains<JoinImplementationTypeTrait>());
        auto trait = op.getTraitSet().get<JoinImplementationTypeTrait>();
        EXPECT_TRUE(trait->implementationType == JoinImplementation::CHOICELESS);
    }
}

/// Build a join with Equals(FieldAccess, FieldAccess). Verify HASH_JOIN trait.
TEST_F(DecideJoinTypesTest, HashJoinConditionProducesHashJoinTrait)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");

    auto leftLogicalSource = createLogicalSource(Identifier::parse("LEFT_TEST"), leftSchema);
    auto leftSourceDescriptor = createSourceDescriptor(PhysicalSourceId{1}, leftLogicalSource);

    auto rightLogicalSource = createLogicalSource(Identifier::parse("RIGHT_TEST"), rightSchema);
    auto rightSourceDescriptor = createSourceDescriptor(PhysicalSourceId{2}, rightLogicalSource);

    const auto leftSourceOp = SourceDescriptorLogicalOperator::create(leftSourceDescriptor);
    const auto rightSourceOp = SourceDescriptorLogicalOperator::create(rightSourceDescriptor);

    auto joinFunction = EqualsLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_id")).value()},
        FieldAccessLogicalFunction{rightSourceOp->getOutputSchema().getFieldByName(Identifier::parse("right_id")).value()}};

    auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
        {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
         Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});

    auto joinOp = JoinLogicalOperator::create(
        std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::INNER_JOIN,
        characteristics.value());

    /// Create a sink schema that includes all fields from both sources plus window boundaries
    const Schema<UnqualifiedUnboundField, Ordered> sinkSchema{
        {Identifier::parse("left_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("START"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("END"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};

    auto sinkDescriptor = createSinkDescriptor(Identifier::parse("test_sink"), sinkSchema);
    auto sinkOp = SinkLogicalOperator::create(joinOp, sinkDescriptor);
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp->withInferredSchema()}};

    const DecideJoinTypesRule rule(StreamJoinStrategy::OPTIMIZER_CHOOSES);
    auto result = rule.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::HASH_JOIN);
}

/// Same join but with NESTED_LOOP_JOIN strategy. Verify NLJ trait.
TEST_F(DecideJoinTypesTest, ForcedNLJStrategyProducesNLJTrait)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");

    auto leftLogicalSource = createLogicalSource(Identifier::parse("LEFT_TEST"), leftSchema);
    auto leftSourceDescriptor = createSourceDescriptor(PhysicalSourceId{1}, leftLogicalSource);

    auto rightLogicalSource = createLogicalSource(Identifier::parse("RIGHT_TEST"), rightSchema);
    auto rightSourceDescriptor = createSourceDescriptor(PhysicalSourceId{2}, rightLogicalSource);

    const auto leftSourceOp = SourceDescriptorLogicalOperator::create(leftSourceDescriptor);
    const auto rightSourceOp = SourceDescriptorLogicalOperator::create(rightSourceDescriptor);

    auto joinFunction = EqualsLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_id")).value()},
        FieldAccessLogicalFunction{rightSourceOp->getOutputSchema().getFieldByName(Identifier::parse("right_id")).value()}};

    auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
        {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
         Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});

    auto joinOp = JoinLogicalOperator::create(
        std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::INNER_JOIN,
        characteristics.value());

    const Schema<UnqualifiedUnboundField, Ordered> sinkSchema{
        {Identifier::parse("left_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("START"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("END"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};

    auto sinkDescriptor = createSinkDescriptor(Identifier::parse("test_sink"), sinkSchema);
    auto sinkOp = SinkLogicalOperator::create(joinOp, sinkDescriptor);
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp->withInferredSchema()}};

    DecideJoinTypesRule rule(StreamJoinStrategy::NESTED_LOOP_JOIN);
    auto result = rule.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::NESTED_LOOP_JOIN);
}

/// Join with a non-field-access leaf in condition + HASH_JOIN strategy. Verify fallback to NLJ.
TEST_F(DecideJoinTypesTest, ForcedHJWithUnsupportedConditionFallsBackToNLJ)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");

    auto leftLogicalSource = createLogicalSource(Identifier::parse("LEFT_TEST"), leftSchema);
    auto leftSourceDescriptor = createSourceDescriptor(PhysicalSourceId{1}, leftLogicalSource);

    auto rightLogicalSource = createLogicalSource(Identifier::parse("RIGHT_TEST"), rightSchema);
    auto rightSourceDescriptor = createSourceDescriptor(PhysicalSourceId{2}, rightLogicalSource);

    const auto leftSourceOp = SourceDescriptorLogicalOperator::create(leftSourceDescriptor);
    const auto rightSourceOp = SourceDescriptorLogicalOperator::create(rightSourceDescriptor);

    /// Use Equals(Add(field, field), FieldAccess) — Add is not a valid hash-join leaf
    auto addFunc = AddLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_id")).value()},
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_value")).value()}};

    auto joinFunction = EqualsLogicalFunction{
        addFunc, FieldAccessLogicalFunction{rightSourceOp->getOutputSchema().getFieldByName(Identifier::parse("right_id")).value()}};

    auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
        {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
         Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});

    auto joinOp = JoinLogicalOperator::create(
        std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::INNER_JOIN,
        characteristics.value());

    const Schema<UnqualifiedUnboundField, Ordered> sinkSchema{
        {Identifier::parse("left_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("START"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("END"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};

    auto sinkDescriptor = createSinkDescriptor(Identifier::parse("test_sink"), sinkSchema);
    auto sinkOp = SinkLogicalOperator::create(joinOp, sinkDescriptor);
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp->withInferredSchema()}};

    DecideJoinTypesRule rule(StreamJoinStrategy::HASH_JOIN);
    auto result = rule.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::NESTED_LOOP_JOIN);
}

/// Equals(field, field) AND Equals(field, field). Verify HASH_JOIN.
TEST_F(DecideJoinTypesTest, ComplexAndConditionProducesHashJoin)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");

    auto leftLogicalSource = createLogicalSource(Identifier::parse("LEFT_TEST"), leftSchema);
    auto leftSourceDescriptor = createSourceDescriptor(PhysicalSourceId{1}, leftLogicalSource);

    auto rightLogicalSource = createLogicalSource(Identifier::parse("RIGHT_TEST"), rightSchema);
    auto rightSourceDescriptor = createSourceDescriptor(PhysicalSourceId{2}, rightLogicalSource);

    const auto leftSourceOp = SourceDescriptorLogicalOperator::create(leftSourceDescriptor);
    const auto rightSourceOp = SourceDescriptorLogicalOperator::create(rightSourceDescriptor);

    auto eq1 = EqualsLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_id")).value()},
        FieldAccessLogicalFunction{rightSourceOp->getOutputSchema().getFieldByName(Identifier::parse("right_id")).value()}};

    auto eq2 = EqualsLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_value")).value()},
        FieldAccessLogicalFunction{rightSourceOp->getOutputSchema().getFieldByName(Identifier::parse("right_value")).value()}};

    auto joinFunction = AndLogicalFunction{eq1, eq2};

    auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
        {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
         Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});

    auto joinOp = JoinLogicalOperator::create(
        std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::INNER_JOIN,
        characteristics.value());

    const Schema<UnqualifiedUnboundField, Ordered> sinkSchema{
        {Identifier::parse("left_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("START"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("END"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};

    auto sinkDescriptor = createSinkDescriptor(Identifier::parse("test_sink"), sinkSchema);
    auto sinkOp = SinkLogicalOperator::create(joinOp, sinkDescriptor);
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp->withInferredSchema()}};

    DecideJoinTypesRule rule(StreamJoinStrategy::OPTIMIZER_CHOOSES);
    auto result = rule.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::HASH_JOIN);
}

/// LEFT outer join with equi-predicate under OPTIMIZER_CHOOSES strategy -> HASH_JOIN trait
TEST_F(DecideJoinTypesTest, LeftOuterJoinWithEquiPredicateGetsHashJoin)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");

    auto leftLogicalSource = createLogicalSource(Identifier::parse("LEFT_TEST"), leftSchema);
    auto leftSourceDescriptor = createSourceDescriptor(PhysicalSourceId{1}, leftLogicalSource);

    auto rightLogicalSource = createLogicalSource(Identifier::parse("RIGHT_TEST"), rightSchema);
    auto rightSourceDescriptor = createSourceDescriptor(PhysicalSourceId{2}, rightLogicalSource);

    const auto leftSourceOp = SourceDescriptorLogicalOperator::create(leftSourceDescriptor);
    const auto rightSourceOp = SourceDescriptorLogicalOperator::create(rightSourceDescriptor);

    auto joinFunction = EqualsLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_id")).value()},
        FieldAccessLogicalFunction{rightSourceOp->getOutputSchema().getFieldByName(Identifier::parse("right_id")).value()}};

    auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
        {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
         Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});

    auto joinOp = JoinLogicalOperator::create(
        std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN,
        characteristics.value());

    auto sinkDescriptor = createSinkDescriptor(Identifier::parse("test_sink"), createJoinOutputSchema(false, true));
    auto sinkOp = SinkLogicalOperator::create(sinkDescriptor).withChildrenUnsafe({joinOp});
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp->withInferredSchema()}};

    DecideJoinTypesRule rule(StreamJoinStrategy::OPTIMIZER_CHOOSES);
    auto result = rule.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::HASH_JOIN);
}

/// RIGHT outer join with equi-predicate under OPTIMIZER_CHOOSES strategy -> HASH_JOIN trait
TEST_F(DecideJoinTypesTest, RightOuterJoinWithEquiPredicateGetsHashJoin)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");

    auto leftLogicalSource = createLogicalSource(Identifier::parse("LEFT_TEST"), leftSchema);
    auto leftSourceDescriptor = createSourceDescriptor(PhysicalSourceId{1}, leftLogicalSource);

    auto rightLogicalSource = createLogicalSource(Identifier::parse("RIGHT_TEST"), rightSchema);
    auto rightSourceDescriptor = createSourceDescriptor(PhysicalSourceId{2}, rightLogicalSource);

    const auto leftSourceOp = SourceDescriptorLogicalOperator::create(leftSourceDescriptor);
    const auto rightSourceOp = SourceDescriptorLogicalOperator::create(rightSourceDescriptor);

    auto joinFunction = EqualsLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_id")).value()},
        FieldAccessLogicalFunction{rightSourceOp->getOutputSchema().getFieldByName(Identifier::parse("right_id")).value()}};

    auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
        {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
         Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});

    auto joinOp = JoinLogicalOperator::create(
        std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN,
        characteristics.value());

    auto sinkDescriptor = createSinkDescriptor(Identifier::parse("test_sink"), createJoinOutputSchema(true, false));
    auto sinkOp = SinkLogicalOperator::create(sinkDescriptor).withChildrenUnsafe({joinOp});
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp->withInferredSchema()}};

    DecideJoinTypesRule rule(StreamJoinStrategy::OPTIMIZER_CHOOSES);
    auto result = rule.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::HASH_JOIN);
}

/// FULL outer join with equi-predicate under OPTIMIZER_CHOOSES strategy -> HASH_JOIN trait
TEST_F(DecideJoinTypesTest, FullOuterJoinWithEquiPredicateGetsHashJoin)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");

    auto leftLogicalSource = createLogicalSource(Identifier::parse("LEFT_TEST"), leftSchema);
    auto leftSourceDescriptor = createSourceDescriptor(PhysicalSourceId{1}, leftLogicalSource);

    auto rightLogicalSource = createLogicalSource(Identifier::parse("RIGHT_TEST"), rightSchema);
    auto rightSourceDescriptor = createSourceDescriptor(PhysicalSourceId{2}, rightLogicalSource);

    const auto leftSourceOp = SourceDescriptorLogicalOperator::create(leftSourceDescriptor);
    const auto rightSourceOp = SourceDescriptorLogicalOperator::create(rightSourceDescriptor);

    auto joinFunction = EqualsLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_id")).value()},
        FieldAccessLogicalFunction{rightSourceOp->getOutputSchema().getFieldByName(Identifier::parse("right_id")).value()}};

    auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
        {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
         Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});

    auto joinOp = JoinLogicalOperator::create(
        std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::OUTER_FULL_JOIN,
        characteristics.value());

    auto sinkDescriptor = createSinkDescriptor(Identifier::parse("test_sink"), createJoinOutputSchema(true, true));
    auto sinkOp = SinkLogicalOperator::create(sinkDescriptor).withChildrenUnsafe({joinOp});
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp->withInferredSchema()}};

    DecideJoinTypesRule rule(StreamJoinStrategy::OPTIMIZER_CHOOSES);
    auto result = rule.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::HASH_JOIN);
}

/// Outer joins under a forced NESTED_LOOP_JOIN strategy still receive the NLJ trait (the strategy is honored verbatim).
TEST_F(DecideJoinTypesTest, OuterJoinGetsHashJoinEvenWithNLJStrategy)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");

    auto leftLogicalSource = createLogicalSource(Identifier::parse("LEFT_TEST"), leftSchema);
    auto leftSourceDescriptor = createSourceDescriptor(PhysicalSourceId{1}, leftLogicalSource);

    auto rightLogicalSource = createLogicalSource(Identifier::parse("RIGHT_TEST"), rightSchema);
    auto rightSourceDescriptor = createSourceDescriptor(PhysicalSourceId{2}, rightLogicalSource);

    const auto leftSourceOp = SourceDescriptorLogicalOperator::create(leftSourceDescriptor);
    const auto rightSourceOp = SourceDescriptorLogicalOperator::create(rightSourceDescriptor);

    auto joinFunction = EqualsLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_id")).value()},
        FieldAccessLogicalFunction{rightSourceOp->getOutputSchema().getFieldByName(Identifier::parse("right_id")).value()}};

    auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
        {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
         Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});

    auto joinOp = JoinLogicalOperator::create(
        std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN,
        characteristics.value());

    auto sinkDescriptor = createSinkDescriptor(Identifier::parse("test_sink"), createJoinOutputSchema(false, true));
    auto sinkOp = SinkLogicalOperator::create(sinkDescriptor).withChildrenUnsafe({joinOp});
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp->withInferredSchema()}};

    DecideJoinTypesRule rule(StreamJoinStrategy::NESTED_LOOP_JOIN);
    auto result = rule.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::NESTED_LOOP_JOIN);
}

/// Outer join with non-equi predicate must be a NLJ, as the hash join does not support non-equi predicates
TEST_F(DecideJoinTypesTest, OuterJoinWithNonEquiPredicateIsRejected)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");

    auto leftLogicalSource = createLogicalSource(Identifier::parse("LEFT_TEST"), leftSchema);
    auto leftSourceDescriptor = createSourceDescriptor(PhysicalSourceId{1}, leftLogicalSource);

    auto rightLogicalSource = createLogicalSource(Identifier::parse("RIGHT_TEST"), rightSchema);
    auto rightSourceDescriptor = createSourceDescriptor(PhysicalSourceId{2}, rightLogicalSource);

    const auto leftSourceOp = SourceDescriptorLogicalOperator::create(leftSourceDescriptor);
    const auto rightSourceOp = SourceDescriptorLogicalOperator::create(rightSourceDescriptor);

    /// Use Add(field, field) as a non-equi leaf — not valid for hash join, but it still contains a FieldAccess so outer-join schema
    /// inference (which requires an explicit equi-join field) does not reject the plan.
    auto addFunc = AddLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_id")).value()},
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_value")).value()}};

    auto joinFunction = EqualsLogicalFunction{
        addFunc, FieldAccessLogicalFunction{rightSourceOp->getOutputSchema().getFieldByName(Identifier::parse("right_id")).value()}};

    auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
        {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
         Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});

    auto joinOp = JoinLogicalOperator::create(
        std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN,
        characteristics.value());

    auto sinkDescriptor = createSinkDescriptor(Identifier::parse("test_sink"), createJoinOutputSchema(false, true));
    auto sinkOp = SinkLogicalOperator::create(sinkDescriptor).withChildrenUnsafe({joinOp});
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp->withInferredSchema()}};

    DecideJoinTypesRule rule(StreamJoinStrategy::OPTIMIZER_CHOOSES);
    auto result = rule.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::NESTED_LOOP_JOIN);
}

/// Equi-key AND range conditions: the hash join hashes on the key and evaluates the ranges as a residual.
TEST_F(DecideJoinTypesTest, EquiKeyWithRangeResidualProducesHashJoin)
{
    for (const auto strategy : {StreamJoinStrategy::OPTIMIZER_CHOOSES, StreamJoinStrategy::HASH_JOIN})
    {
        const auto join = applyRuleToJoin(equiAndRangeCondition, JoinLogicalOperator::JoinType::INNER_JOIN, strategy);
        EXPECT_EQ(getJoinImplementation(join), JoinImplementation::HASH_JOIN);
    }
}

/// A forced NLJ stays a NLJ, even if the condition has an equi-key.
TEST_F(DecideJoinTypesTest, EquiKeyWithRangeResidualAndForcedNLJProducesNLJ)
{
    const auto join
        = applyRuleToJoin(equiAndRangeCondition, JoinLogicalOperator::JoinType::INNER_JOIN, StreamJoinStrategy::NESTED_LOOP_JOIN);
    EXPECT_EQ(getJoinImplementation(join), JoinImplementation::NESTED_LOOP_JOIN);
}

/// Outer joins with a residual use the hash join as well; the probe then decides "matched" per pair including the residual.
TEST_F(DecideJoinTypesTest, OuterJoinsWithEquiKeyAndRangeResidualProduceHashJoin)
{
    for (const auto joinType :
         {JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN,
          JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN,
          JoinLogicalOperator::JoinType::OUTER_FULL_JOIN})
    {
        const auto join = applyRuleToJoin(equiAndRangeCondition, joinType, StreamJoinStrategy::OPTIMIZER_CHOOSES);
        EXPECT_EQ(getJoinImplementation(join), JoinImplementation::HASH_JOIN);
    }
}

/// Only range conditions: there is nothing to hash on.
TEST_F(DecideJoinTypesTest, PureRangeConditionProducesNLJ)
{
    for (const auto strategy : {StreamJoinStrategy::OPTIMIZER_CHOOSES, StreamJoinStrategy::HASH_JOIN})
    {
        const auto join = applyRuleToJoin(
            [](const auto& left, const auto& right) -> LogicalFunction
            {
                return AndLogicalFunction{
                    GreaterEqualsLogicalFunction{fieldOf(right, "right_value"), fieldOf(left, "left_value")},
                    LessEqualsLogicalFunction{fieldOf(right, "right_ts"), fieldOf(left, "left_ts")}};
            },
            JoinLogicalOperator::JoinType::INNER_JOIN,
            strategy);
        EXPECT_EQ(getJoinImplementation(join), JoinImplementation::NESTED_LOOP_JOIN);
    }
}

/// A top-level OR of equalities has no key common to all matches, so it must not be hashed (previously it was hashed on both equalities,
/// i.e. evaluated as an AND).
TEST_F(DecideJoinTypesTest, TopLevelOrOfEqualitiesProducesNLJ)
{
    for (const auto strategy : {StreamJoinStrategy::OPTIMIZER_CHOOSES, StreamJoinStrategy::HASH_JOIN})
    {
        const auto join = applyRuleToJoin(
            [](const auto& left, const auto& right) -> LogicalFunction
            {
                return OrLogicalFunction{
                    EqualsLogicalFunction{fieldOf(left, "left_id"), fieldOf(right, "right_id")},
                    EqualsLogicalFunction{fieldOf(left, "left_value"), fieldOf(right, "right_value")}};
            },
            JoinLogicalOperator::JoinType::INNER_JOIN,
            strategy);
        EXPECT_EQ(getJoinImplementation(join), JoinImplementation::NESTED_LOOP_JOIN);
    }
}

/// An equi-key AND-ed with an OR: the key is hashed and the OR becomes the residual.
TEST_F(DecideJoinTypesTest, EquiKeyAndOrResidualProducesHashJoin)
{
    const auto join = applyRuleToJoin(
        [](const auto& left, const auto& right) -> LogicalFunction
        {
            return AndLogicalFunction{
                EqualsLogicalFunction{fieldOf(left, "left_id"), fieldOf(right, "right_id")},
                OrLogicalFunction{
                    EqualsLogicalFunction{fieldOf(left, "left_value"), fieldOf(right, "right_value")},
                    LessEqualsLogicalFunction{fieldOf(right, "right_ts"), fieldOf(left, "left_ts")}}};
        },
        JoinLogicalOperator::JoinType::INNER_JOIN,
        StreamJoinStrategy::OPTIMIZER_CHOOSES);
    EXPECT_EQ(getJoinImplementation(join), JoinImplementation::HASH_JOIN);
}

/// The split orders each key by join side, regardless of how the equality is written, and keeps the other conjuncts as the residual.
TEST_F(DecideJoinTypesTest, SplitJoinFunctionSeparatesEquiKeysFromResidual)
{
    const auto join = applyRuleToJoin(
        [](const auto& left, const auto& right) -> LogicalFunction
        {
            return AndLogicalFunction{
                AndLogicalFunction{
                    GreaterEqualsLogicalFunction{fieldOf(right, "right_value"), fieldOf(left, "left_value")},
                    EqualsLogicalFunction{fieldOf(right, "right_id"), fieldOf(left, "left_id")}},
                AndLogicalFunction{
                    /// Both fields from the same input: not a join key
                    EqualsLogicalFunction{fieldOf(left, "left_ts"), fieldOf(left, "left_value")},
                    LessEqualsLogicalFunction{fieldOf(right, "right_ts"), fieldOf(left, "left_ts")}}};
        },
        JoinLogicalOperator::JoinType::INNER_JOIN,
        StreamJoinStrategy::OPTIMIZER_CHOOSES);

    const auto [equiKeys, residual] = splitJoinFunction(*join);
    ASSERT_EQ(equiKeys.size(), 1);
    EXPECT_EQ(equiKeys[0].left.getLastName(), Identifier::parse("left_id"));
    EXPECT_EQ(equiKeys[0].right.getLastName(), Identifier::parse("right_id"));
    ASSERT_TRUE(residual.has_value());
    const auto residualComparisons = BFSRange(*residual)
        | std::views::filter([](const LogicalFunction& function) { return not function.tryGetAs<AndLogicalFunction>().has_value(); })
        | std::views::filter([](const LogicalFunction& function) { return not function.getChildren().empty(); })
        | std::ranges::to<std::vector>();
    EXPECT_EQ(residualComparisons.size(), 3);
    EXPECT_TRUE(residual->getDataType().isType(DataType::Type::BOOLEAN));
}

}
}

/// NOLINTEND(bugprone-unchecked-optional-access)
