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
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
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

/// A plan without any Selection should remain unchanged.
TEST_F(PredicatePushdownTest, PlanWithoutSelectionIsUnchanged)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    PredicatePushdown phase;
    auto result = phase.apply(plan);

    /// The plan should have the same structure
    ASSERT_EQ(result.getRootOperators().size(), 1);
    auto root = result.getRootOperators()[0];
    EXPECT_EQ(root.getName(), "Sink");
}

/// Selection above Projection: selection should be pushed below the projection.
/// Plan: Sink -> Selection -> Projection -> Source
/// Expected: Sink -> Projection -> Selection -> Source
TEST_F(PredicatePushdownTest, SelectionPushedBelowProjection)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);

    /// Add a projection that keeps all fields
    std::vector<ProjectionLogicalOperator::Projection> projections;
    projections.emplace_back(std::nullopt, LogicalFunction{FieldAccessLogicalFunction("src.id")});
    projections.emplace_back(std::nullopt, LogicalFunction{FieldAccessLogicalFunction("src.value")});
    plan = LogicalPlanBuilder::addProjection(projections, false, plan);

    /// Add a selection that filters on src.id (which is available before the projection)
    auto selectionFn = LogicalFunction{GreaterLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("src.id")}, LogicalFunction{FieldAccessLogicalFunction("src.id")})};
    plan = LogicalPlanBuilder::addSelection(selectionFn, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    PredicatePushdown phase;
    auto result = phase.apply(plan);

    /// Verify the structure: Sink -> Projection -> Selection -> Source
    auto root = result.getRootOperators()[0];
    EXPECT_EQ(root.getName(), "Sink");

    auto sinkChildren = root.getChildren();
    ASSERT_EQ(sinkChildren.size(), 1);
    EXPECT_EQ(sinkChildren[0].getName(), "Projection") << "Expected Projection as first child of Sink after pushdown";

    auto projChildren = sinkChildren[0].getChildren();
    ASSERT_EQ(projChildren.size(), 1);
    EXPECT_EQ(projChildren[0].getName(), "Selection") << "Expected Selection as child of Projection after pushdown";
}

/// Selection above Join referencing only left side fields: push selection to left child.
/// Plan: Sink -> Selection(left.id > left.id) -> Join -> (Left, Right)
/// Expected: Sink -> Join -> (Selection(left.id > left.id) -> Left, Right)
TEST_F(PredicatePushdownTest, SelectionPushedToLeftSideOfJoin)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};

    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);

    /// Add a selection that references only left-side fields
    auto selectionFn = LogicalFunction{GreaterLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("left.id")})};
    plan = LogicalPlanBuilder::addSelection(selectionFn, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    PredicatePushdown phase;
    auto result = phase.apply(plan);

    /// Verify: Sink -> Join -> (Selection -> Left, Right)
    auto root = result.getRootOperators()[0];
    EXPECT_EQ(root.getName(), "Sink");

    auto sinkChildren = root.getChildren();
    ASSERT_EQ(sinkChildren.size(), 1);
    EXPECT_EQ(sinkChildren[0].getName(), "Join") << "Expected Join directly under Sink after pushdown";

    auto joinChildren = sinkChildren[0].getChildren();
    ASSERT_EQ(joinChildren.size(), 2);
    EXPECT_EQ(joinChildren[0].getName(), "Selection") << "Expected Selection pushed to left side of Join";
}

/// Selection above Join referencing only right side fields: push selection to right child.
TEST_F(PredicatePushdownTest, SelectionPushedToRightSideOfJoin)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};

    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);

    /// Add a selection that references only right-side fields
    auto selectionFn = LogicalFunction{GreaterLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("right.value")}, LogicalFunction{FieldAccessLogicalFunction("right.value")})};
    plan = LogicalPlanBuilder::addSelection(selectionFn, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    PredicatePushdown phase;
    auto result = phase.apply(plan);

    /// Verify: Sink -> Join -> (Left, Selection -> Right)
    auto root = result.getRootOperators()[0];
    auto sinkChildren = root.getChildren();
    ASSERT_EQ(sinkChildren.size(), 1);
    EXPECT_EQ(sinkChildren[0].getName(), "Join");

    auto joinChildren = sinkChildren[0].getChildren();
    ASSERT_EQ(joinChildren.size(), 2);
    EXPECT_NE(joinChildren[0].getName(), "Selection") << "Selection should not be on the left side";
    EXPECT_EQ(joinChildren[1].getName(), "Selection") << "Expected Selection pushed to right side of Join";
}

/// Selection above Join referencing both sides: selection should NOT be pushed down.
TEST_F(PredicatePushdownTest, SelectionReferencingBothSidesStaysAboveJoin)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};

    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);

    /// Add a selection that references fields from both sides
    auto selectionFn = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.value")}, LogicalFunction{FieldAccessLogicalFunction("right.value")})};
    plan = LogicalPlanBuilder::addSelection(selectionFn, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    PredicatePushdown phase;
    auto result = phase.apply(plan);

    /// Verify: Sink -> Selection -> Join (unchanged)
    auto root = result.getRootOperators()[0];
    auto sinkChildren = root.getChildren();
    ASSERT_EQ(sinkChildren.size(), 1);
    EXPECT_EQ(sinkChildren[0].getName(), "Selection") << "Selection referencing both sides should stay above Join";

    auto selectionChildren = sinkChildren[0].getChildren();
    ASSERT_EQ(selectionChildren.size(), 1);
    EXPECT_EQ(selectionChildren[0].getName(), "Join");
}

}
}
