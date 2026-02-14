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

#include <memory>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <Phases/DecideJoinTypes.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Traits/ImplementationTypeTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{
namespace
{

class DecideJoinTypesTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("DecideJoinTypesTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override { BaseUnitTest::SetUp(); }

    /// Helper to create a simple source plan with a schema containing an "id" field
    LogicalPlan createSourcePlan(const std::string& sourceType, const Schema& schema)
    {
        return LogicalPlanBuilder::createLogicalPlan(sourceType, schema, {}, {});
    }

    Schema createSchema(const std::string& prefix)
    {
        Schema schema;
        schema.addField(prefix + ".id", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        schema.addField(prefix + ".value", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        schema.addField(prefix + ".ts", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        return schema;
    }

    std::shared_ptr<Windowing::WindowType> createTumblingWindow()
    {
        return std::make_shared<Windowing::TumblingWindow>(
            Windowing::TimeCharacteristic::createIngestionTime(), Windowing::TimeMeasure(1000));
    }
};

/// A simple Selection → InlineSource plan. Verify all operators get CHOICELESS.
TEST_F(DecideJoinTypesTest, NonJoinPlanGetChoicelessTrait)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);
    auto selectionFn = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("src.id")}, LogicalFunction{FieldAccessLogicalFunction("src.id")})};
    plan = LogicalPlanBuilder::addSelection(selectionFn, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

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
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};

    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

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
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};

    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

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
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    /// Use Equals(Add(field, field), FieldAccess) — Add is not a valid hash-join leaf
    auto addFunc = LogicalFunction{AddLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("left.value")})};
    auto joinFunction = LogicalFunction{EqualsLogicalFunction(addFunc, LogicalFunction{FieldAccessLogicalFunction("right.id")})};

    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

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
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto eq1 = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};
    auto eq2 = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.value")}, LogicalFunction{FieldAccessLogicalFunction("right.value")})};
    auto joinFunction = LogicalFunction{AndLogicalFunction(eq1, eq2)};

    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    DecideJoinTypes phase(StreamJoinStrategy::OPTIMIZER_CHOOSES);
    auto result = phase.apply(plan);

    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    auto trait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_TRUE(trait->implementationType == JoinImplementation::HASH_JOIN);
}

/// Verify children of join also get CHOICELESS traits.
TEST_F(DecideJoinTypesTest, TraitPropagationToChildren)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};

    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    DecideJoinTypes phase(StreamJoinStrategy::OPTIMIZER_CHOOSES);
    auto result = phase.apply(plan);

    /// The root (sink) should be CHOICELESS, the join should be HASH_JOIN, and all children of join should be CHOICELESS
    for (const auto& op : BFSRange(result.getRootOperators()[0]))
    {
        ASSERT_TRUE(op.getTraitSet().contains<JoinImplementationTypeTrait>());
        auto trait = op.getTraitSet().get<JoinImplementationTypeTrait>();
        if (op.tryGetAs<JoinLogicalOperator>().has_value())
        {
            EXPECT_TRUE(trait->implementationType == JoinImplementation::HASH_JOIN);
        }
        else
        {
            EXPECT_TRUE(trait->implementationType == JoinImplementation::CHOICELESS);
        }
    }
}

}
}
