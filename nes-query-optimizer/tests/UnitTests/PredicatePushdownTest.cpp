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
#include <memory>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <Phases/PredicatePushdown.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES
{
namespace
{

class PredicatePushdownTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("PredicatePushdownTest.log", LogLevel::LOG_DEBUG); }

    static constexpr uint64_t TUMBLING_WINDOW_SIZE_MS = 1000;

    static LogicalPlan createSourcePlan(const std::string& sourceType, const Schema& schema)
    {
        return LogicalPlanBuilder::createLogicalPlan(sourceType, schema, {}, {});
    }

    static Schema createSchema(const std::string& prefix)
    {
        Schema schema;
        schema.addField(prefix + ".id", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        schema.addField(prefix + ".value", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        schema.addField(prefix + ".ts", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        return schema;
    }

    static std::shared_ptr<Windowing::WindowType> createTumblingWindow()
    {
        return std::make_shared<Windowing::TumblingWindow>(
            Windowing::TimeCharacteristic::createIngestionTime(), Windowing::TimeMeasure(TUMBLING_WINDOW_SIZE_MS));
    }
};

/// Selection is pushed below a Projection when all predicate fields exist in the Projection's input.
TEST_F(PredicatePushdownTest, PushBelowProjection)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);

    /// Project only src.id.
    std::vector<ProjectionLogicalOperator::Projection> projections;
    projections.emplace_back(std::nullopt, LogicalFunction{FieldAccessLogicalFunction("src.id")});
    plan = LogicalPlanBuilder::addProjection(projections, false, plan);

    /// Select on src.value which exists in source but not in projection output.
    auto selectionFn = LogicalFunction{GreaterLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("src.value")}, LogicalFunction{FieldAccessLogicalFunction("src.id")})};
    plan = LogicalPlanBuilder::addSelection(selectionFn, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    PredicatePushdown phase;
    auto result = phase.apply(plan);

    /// After pushdown, the Selection should be below the Projection.
    /// Structure: Sink -> Projection -> Selection -> Source
    auto selections = getOperatorByType<SelectionLogicalOperator>(result);
    ASSERT_EQ(selections.size(), 1);
    auto projectionOps = getOperatorByType<ProjectionLogicalOperator>(result);
    ASSERT_EQ(projectionOps.size(), 1);

    /// The projection's child should be the selection.
    const auto projChild = projectionOps[0]->getChildren()[0];
    EXPECT_TRUE(projChild.tryGetAs<SelectionLogicalOperator>().has_value());
}

/// Selection stays above Projection when predicate references a field not in the Projection's input.
TEST_F(PredicatePushdownTest, NoPushBelowProjectionWhenFieldMissing)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);

    /// Project src.id, renaming to "computed".
    std::vector<ProjectionLogicalOperator::Projection> projections;
    projections.emplace_back(FieldIdentifier("computed"), LogicalFunction{FieldAccessLogicalFunction("src.id")});
    plan = LogicalPlanBuilder::addProjection(projections, false, plan);

    /// Select on "computed" which only exists after the projection.
    auto selectionFn = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("computed")}, LogicalFunction{FieldAccessLogicalFunction("computed")})};
    plan = LogicalPlanBuilder::addSelection(selectionFn, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    PredicatePushdown phase;
    auto result = phase.apply(plan);

    /// Selection should remain above projection.
    /// Structure: Sink -> Selection -> Projection -> Source
    const auto root = result.getRootOperators()[0];
    const auto sinkChild = root.getChildren()[0];
    EXPECT_TRUE(sinkChild.tryGetAs<SelectionLogicalOperator>().has_value());
    const auto selChild = sinkChild.getChildren()[0];
    EXPECT_TRUE(selChild.tryGetAs<ProjectionLogicalOperator>().has_value());
}

/// Left-only predicate is pushed to the left child of a Join.
TEST_F(PredicatePushdownTest, PushBelowJoinToLeftSide)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};
    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);

    /// Add selection on a left-only field.
    auto selectionFn = LogicalFunction{GreaterLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.value")}, LogicalFunction{FieldAccessLogicalFunction("left.id")})};
    plan = LogicalPlanBuilder::addSelection(selectionFn, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    PredicatePushdown phase;
    auto result = phase.apply(plan);

    /// Selection should be pushed to the left child of the join.
    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    const auto leftChild = joins[0]->getChildren()[0];
    EXPECT_TRUE(leftChild.tryGetAs<SelectionLogicalOperator>().has_value());

    /// No selection above the join.
    const auto root = result.getRootOperators()[0];
    const auto sinkChild = root.getChildren()[0];
    EXPECT_TRUE(sinkChild.tryGetAs<JoinLogicalOperator>().has_value());
}

/// Right-only predicate is pushed to the right child of a Join.
TEST_F(PredicatePushdownTest, PushBelowJoinToRightSide)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};
    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);

    /// Add selection on a right-only field.
    auto selectionFn = LogicalFunction{GreaterLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("right.value")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};
    plan = LogicalPlanBuilder::addSelection(selectionFn, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    PredicatePushdown phase;
    auto result = phase.apply(plan);

    /// Selection should be pushed to the right child of the join.
    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    const auto rightChild = joins[0]->getChildren()[1];
    EXPECT_TRUE(rightChild.tryGetAs<SelectionLogicalOperator>().has_value());

    /// No selection above the join.
    const auto root = result.getRootOperators()[0];
    const auto sinkChild = root.getChildren()[0];
    EXPECT_TRUE(sinkChild.tryGetAs<JoinLogicalOperator>().has_value());
}

/// An AND predicate with left-only and right-only conjuncts is split across both join sides.
TEST_F(PredicatePushdownTest, SplitAndAcrossJoin)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};
    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);

    /// left.value > left.id AND right.value > right.id
    auto leftPred = LogicalFunction{GreaterLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.value")}, LogicalFunction{FieldAccessLogicalFunction("left.id")})};
    auto rightPred = LogicalFunction{GreaterLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("right.value")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};
    auto combinedPred = LogicalFunction{AndLogicalFunction(leftPred, rightPred)};
    plan = LogicalPlanBuilder::addSelection(combinedPred, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    PredicatePushdown phase;
    auto result = phase.apply(plan);

    /// Both join children should have selections.
    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    EXPECT_TRUE(joins[0]->getChildren()[0].tryGetAs<SelectionLogicalOperator>().has_value());
    EXPECT_TRUE(joins[0]->getChildren()[1].tryGetAs<SelectionLogicalOperator>().has_value());

    /// No selection above the join.
    const auto root = result.getRootOperators()[0];
    const auto sinkChild = root.getChildren()[0];
    EXPECT_TRUE(sinkChild.tryGetAs<JoinLogicalOperator>().has_value());
}

/// Mixed AND: one conjunct is left-only (pushable), one is cross-side (stays above).
TEST_F(PredicatePushdownTest, MixedAndPartialPush)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};
    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);

    /// left.value > left.id AND left.id = right.id (cross-side)
    auto leftOnlyPred = LogicalFunction{GreaterLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.value")}, LogicalFunction{FieldAccessLogicalFunction("left.id")})};
    auto crossPred = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};
    auto combinedPred = LogicalFunction{AndLogicalFunction(leftOnlyPred, crossPred)};
    plan = LogicalPlanBuilder::addSelection(combinedPred, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    PredicatePushdown phase;
    auto result = phase.apply(plan);

    /// Left child of join should have the left-only selection.
    auto joins = getOperatorByType<JoinLogicalOperator>(result);
    ASSERT_EQ(joins.size(), 1);
    EXPECT_TRUE(joins[0]->getChildren()[0].tryGetAs<SelectionLogicalOperator>().has_value());

    /// There should be a remaining selection above the join for the cross-side predicate.
    const auto root = result.getRootOperators()[0];
    const auto sinkChild = root.getChildren()[0];
    EXPECT_TRUE(sinkChild.tryGetAs<SelectionLogicalOperator>().has_value());
}

/// Selection stays above a source (leaf) operator.
TEST_F(PredicatePushdownTest, NoPushBelowSource)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);

    auto selectionFn = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("src.id")}, LogicalFunction{FieldAccessLogicalFunction("src.id")})};
    plan = LogicalPlanBuilder::addSelection(selectionFn, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    PredicatePushdown phase;
    auto result = phase.apply(plan);

    /// Structure should remain: Sink -> Selection -> Source.
    const auto root = result.getRootOperators()[0];
    const auto sinkChild = root.getChildren()[0];
    EXPECT_TRUE(sinkChild.tryGetAs<SelectionLogicalOperator>().has_value());
}

/// A plan with no selections is returned unchanged.
TEST_F(PredicatePushdownTest, NoPushNeeded)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    PredicatePushdown phase;
    auto result = phase.apply(plan);

    auto selections = getOperatorByType<SelectionLogicalOperator>(result);
    EXPECT_TRUE(selections.empty());
}

}
}
