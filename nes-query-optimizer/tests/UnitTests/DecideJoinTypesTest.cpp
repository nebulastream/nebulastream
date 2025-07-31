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
#include <functional>
#include <memory>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <Phases/DecideJoinTypes.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Traits/JoinImplementationTypeTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>

namespace NES
{
namespace
{

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

    static LogicalSource
    createLogicalSource(SourceCatalog& sourceCatalog, const Identifier& sourceName, const Schema<UnqualifiedUnboundField, Ordered>& schema)
    {
        return sourceCatalog.addLogicalSource(sourceName, schema).value();
    }

    static SourceDescriptor createSourceDescriptor(SourceCatalog& sourceCatalog, const LogicalSource& logicalSource)
    {
        const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("FILE_PATH"), "/dev/null"}};
        const std::unordered_map<Identifier, std::string> parserConfig{{Identifier::parse("TYPE"), "CSV"}};
        return sourceCatalog.addPhysicalSource(logicalSource, Identifier::parse("file"), sourceConfig, parserConfig).value();
    }

    static SinkDescriptor
    createSinkDescriptor(SinkCatalog& sinkCatalog, const Identifier& sinkName, const Schema<UnqualifiedUnboundField, Ordered>& schema)
    {
        const std::unordered_map<Identifier, std::string> sinkConfig{
            {Identifier::parse("FILE_PATH"), "/dev/null"}, {Identifier::parse("INPUT_FORMAT"), "CSV"}};
        return sinkCatalog.addSinkDescriptor(sinkName, schema, Identifier::parse("file"), sinkConfig).value();
    }
};

/// A simple Selection → InlineSource plan. Verify all operators get CHOICELESS.
TEST_F(DecideJoinTypesTest, NonJoinPlanGetChoicelessTrait)
{
    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;

    auto schema = createSchema("src");
    auto logicalSource = createLogicalSource(sourceCatalog, Identifier::parse("TEST"), schema);
    auto sourceDescriptor = createSourceDescriptor(sourceCatalog, logicalSource);
    auto sinkDescriptor = createSinkDescriptor(sinkCatalog, Identifier::parse("test_sink"), schema);

    TypedLogicalOperator<SourceDescriptorLogicalOperator> sourceOp{sourceDescriptor};

    auto selectionFn = EqualsLogicalFunction{
        FieldAccessLogicalFunction{sourceOp->getOutputSchema().getFieldByName(Identifier::parse("src_id")).value()},
        FieldAccessLogicalFunction{sourceOp->getOutputSchema().getFieldByName(Identifier::parse("src_id")).value()}};

    auto selectionOp = TypedLogicalOperator<SelectionLogicalOperator>{selectionFn}.withChildren({sourceOp});
    auto sinkOp = TypedLogicalOperator<SinkLogicalOperator>{sinkDescriptor}.withChildren({selectionOp});
    LogicalPlan plan{sinkOp->withInferredSchema()};

    DecideJoinTypes phase(StreamJoinStrategy::OPTIMIZER_CHOOSES);
    auto result = phase.apply(plan);

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
    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;

    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");

    auto leftLogicalSource = createLogicalSource(sourceCatalog, Identifier::parse("LEFT_TEST"), leftSchema);
    auto leftSourceDescriptor = createSourceDescriptor(sourceCatalog, leftLogicalSource);

    auto rightLogicalSource = createLogicalSource(sourceCatalog, Identifier::parse("RIGHT_TEST"), rightSchema);
    auto rightSourceDescriptor = createSourceDescriptor(sourceCatalog, rightLogicalSource);

    TypedLogicalOperator<SourceDescriptorLogicalOperator> leftSourceOp{leftSourceDescriptor};
    TypedLogicalOperator<SourceDescriptorLogicalOperator> rightSourceOp{rightSourceDescriptor};

    auto joinFunction = EqualsLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_id")).value()},
        FieldAccessLogicalFunction{rightSourceOp->getOutputSchema().getFieldByName(Identifier::parse("right_id")).value()}};

    auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
        {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
         Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});

    auto joinOp = TypedLogicalOperator<JoinLogicalOperator>{
        std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::INNER_JOIN,
        characteristics.value()};

    // Create a sink schema that includes all fields from both sources plus window boundaries
    Schema<UnqualifiedUnboundField, Ordered> sinkSchema{
        {Identifier::parse("left_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("START"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("END"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};

    auto sinkDescriptor = createSinkDescriptor(sinkCatalog, Identifier::parse("test_sink"), sinkSchema);
    auto sinkOp = TypedLogicalOperator<SinkLogicalOperator>{sinkDescriptor}.withChildren({joinOp});
    LogicalPlan plan{sinkOp->withInferredSchema()};

    DecideJoinTypes phase(StreamJoinStrategy::OPTIMIZER_CHOOSES);
    auto result = phase.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::HASH_JOIN);
}

/// Same join but with NESTED_LOOP_JOIN strategy. Verify NLJ trait.
TEST_F(DecideJoinTypesTest, ForcedNLJStrategyProducesNLJTrait)
{
    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;

    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");

    auto leftLogicalSource = createLogicalSource(sourceCatalog, Identifier::parse("LEFT_TEST"), leftSchema);
    auto leftSourceDescriptor = createSourceDescriptor(sourceCatalog, leftLogicalSource);

    auto rightLogicalSource = createLogicalSource(sourceCatalog, Identifier::parse("RIGHT_TEST"), rightSchema);
    auto rightSourceDescriptor = createSourceDescriptor(sourceCatalog, rightLogicalSource);

    TypedLogicalOperator<SourceDescriptorLogicalOperator> leftSourceOp{leftSourceDescriptor};
    TypedLogicalOperator<SourceDescriptorLogicalOperator> rightSourceOp{rightSourceDescriptor};

    auto joinFunction = EqualsLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_id")).value()},
        FieldAccessLogicalFunction{rightSourceOp->getOutputSchema().getFieldByName(Identifier::parse("right_id")).value()}};

    auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
        {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
         Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});

    auto joinOp = TypedLogicalOperator<JoinLogicalOperator>{
        std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::INNER_JOIN,
        characteristics.value()};

    Schema<UnqualifiedUnboundField, Ordered> sinkSchema{
        {Identifier::parse("left_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("START"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("END"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};

    auto sinkDescriptor = createSinkDescriptor(sinkCatalog, Identifier::parse("test_sink"), sinkSchema);
    auto sinkOp = TypedLogicalOperator<SinkLogicalOperator>{sinkDescriptor}.withChildren({joinOp});
    LogicalPlan plan{sinkOp->withInferredSchema()};

    DecideJoinTypes phase(StreamJoinStrategy::NESTED_LOOP_JOIN);
    auto result = phase.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::NESTED_LOOP_JOIN);
}

/// Join with a non-field-access leaf in condition + HASH_JOIN strategy. Verify fallback to NLJ.
TEST_F(DecideJoinTypesTest, ForcedHJWithUnsupportedConditionFallsBackToNLJ)
{
    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;

    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");

    auto leftLogicalSource = createLogicalSource(sourceCatalog, Identifier::parse("LEFT_TEST"), leftSchema);
    auto leftSourceDescriptor = createSourceDescriptor(sourceCatalog, leftLogicalSource);

    auto rightLogicalSource = createLogicalSource(sourceCatalog, Identifier::parse("RIGHT_TEST"), rightSchema);
    auto rightSourceDescriptor = createSourceDescriptor(sourceCatalog, rightLogicalSource);

    TypedLogicalOperator<SourceDescriptorLogicalOperator> leftSourceOp{leftSourceDescriptor};
    TypedLogicalOperator<SourceDescriptorLogicalOperator> rightSourceOp{rightSourceDescriptor};

    /// Use Equals(Add(field, field), FieldAccess) — Add is not a valid hash-join leaf
    auto addFunc = AddLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_id")).value()},
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema().getFieldByName(Identifier::parse("left_value")).value()}};

    auto joinFunction = EqualsLogicalFunction{
        addFunc, FieldAccessLogicalFunction{rightSourceOp->getOutputSchema().getFieldByName(Identifier::parse("right_id")).value()}};

    auto characteristics = JoinLogicalOperator::createJoinTimeCharacteristic(
        {Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
         Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});

    auto joinOp = TypedLogicalOperator<JoinLogicalOperator>{
        std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::INNER_JOIN,
        characteristics.value()};

    Schema<UnqualifiedUnboundField, Ordered> sinkSchema{
        {Identifier::parse("left_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("START"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("END"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};

    auto sinkDescriptor = createSinkDescriptor(sinkCatalog, Identifier::parse("test_sink"), sinkSchema);
    auto sinkOp = TypedLogicalOperator<SinkLogicalOperator>{sinkDescriptor}.withChildren({joinOp});
    LogicalPlan plan{sinkOp->withInferredSchema()};

    DecideJoinTypes phase(StreamJoinStrategy::HASH_JOIN);
    auto result = phase.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::NESTED_LOOP_JOIN);
}

/// Equals(field, field) AND Equals(field, field). Verify HASH_JOIN.
TEST_F(DecideJoinTypesTest, ComplexAndConditionProducesHashJoin)
{
    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;

    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");

    auto leftLogicalSource = createLogicalSource(sourceCatalog, Identifier::parse("LEFT_TEST"), leftSchema);
    auto leftSourceDescriptor = createSourceDescriptor(sourceCatalog, leftLogicalSource);

    auto rightLogicalSource = createLogicalSource(sourceCatalog, Identifier::parse("RIGHT_TEST"), rightSchema);
    auto rightSourceDescriptor = createSourceDescriptor(sourceCatalog, rightLogicalSource);

    TypedLogicalOperator<SourceDescriptorLogicalOperator> leftSourceOp{leftSourceDescriptor};
    TypedLogicalOperator<SourceDescriptorLogicalOperator> rightSourceOp{rightSourceDescriptor};

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

    auto joinOp = TypedLogicalOperator<JoinLogicalOperator>{
        std::array<LogicalOperator, 2>{leftSourceOp, rightSourceOp},
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::INNER_JOIN,
        characteristics.value()};

    Schema<UnqualifiedUnboundField, Ordered> sinkSchema{
        {Identifier::parse("left_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("left_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("right_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("START"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        {Identifier::parse("END"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};

    auto sinkDescriptor = createSinkDescriptor(sinkCatalog, Identifier::parse("test_sink"), sinkSchema);
    auto sinkOp = TypedLogicalOperator<SinkLogicalOperator>{sinkDescriptor}.withChildren({joinOp});
    LogicalPlan plan{sinkOp->withInferredSchema()};

    DecideJoinTypes phase(StreamJoinStrategy::OPTIMIZER_CHOOSES);
    auto result = phase.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::HASH_JOIN);
}

}
}
