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
#include <unordered_set>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <Phases/ProjectionPruning.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
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

/// Helper: collects all accessed field names from a ProjectionLogicalOperator.
std::unordered_set<std::string> getProjectionFieldNames(const TypedLogicalOperator<ProjectionLogicalOperator>& proj)
{
    std::unordered_set<std::string> fields;
    for (const auto& fieldName : proj->getAccessedFields())
    {
        fields.insert(fieldName);
    }
    return fields;
}

class ProjectionPruningTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ProjectionPruningTest.log", LogLevel::LOG_DEBUG); }

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

/// When a source has 3 fields but only 1 is projected, a pruning projection should be inserted above the source.
TEST_F(ProjectionPruningTest, UnusedFieldsPruned)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);

    /// Project only src.id.
    std::vector<ProjectionLogicalOperator::Projection> projections;
    projections.emplace_back(std::nullopt, LogicalFunction{FieldAccessLogicalFunction("src.id")});
    plan = LogicalPlanBuilder::addProjection(projections, false, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    ProjectionPruning phase;
    auto result = phase.apply(plan);

    /// There should be at least 2 projection operators: the user's projection and the pruning projection.
    auto projectionOps = getOperatorByType<ProjectionLogicalOperator>(result);
    ASSERT_GE(projectionOps.size(), 2u);

    /// The pruning projection (closest to source) should only keep the src.id field.
    /// Find the pruning projection - it's the one whose accessed fields are a subset of the source schema.
    bool foundPruningProjection = false;
    for (const auto& proj : projectionOps)
    {
        auto fields = getProjectionFieldNames(proj);
        if (fields.size() == 1 && fields.contains("src.id"))
        {
            foundPruningProjection = true;
        }
    }
    EXPECT_TRUE(foundPruningProjection);
}

/// A selection referencing field 'value' combined with a projection of 'id' preserves both id and value in the pruning projection.
TEST_F(ProjectionPruningTest, SelectionFieldsPreserved)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);

    /// Select on src.value.
    auto selectionFn = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("src.value")}, LogicalFunction{FieldAccessLogicalFunction("src.value")})};
    plan = LogicalPlanBuilder::addSelection(selectionFn, plan);

    /// Project only src.id.
    std::vector<ProjectionLogicalOperator::Projection> projections;
    projections.emplace_back(std::nullopt, LogicalFunction{FieldAccessLogicalFunction("src.id")});
    plan = LogicalPlanBuilder::addProjection(projections, false, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    ProjectionPruning phase;
    auto result = phase.apply(plan);

    /// The pruning projection should keep both src.id (needed by projection) and src.value (needed by selection).
    auto projectionOps = getOperatorByType<ProjectionLogicalOperator>(result);
    ASSERT_GE(projectionOps.size(), 2u);

    /// Find the pruning projection that has both id and value fields.
    bool foundPruningProjection = false;
    for (const auto& proj : projectionOps)
    {
        auto fields = getProjectionFieldNames(proj);
        if (fields.contains("src.id") && fields.contains("src.value") && fields.size() == 2)
        {
            foundPruningProjection = true;
        }
    }
    EXPECT_TRUE(foundPruningProjection);
}

/// Join key fields are preserved even if not in the final needed fields.
TEST_F(ProjectionPruningTest, JoinKeyFieldsPreserved)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};
    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);

    /// Project only left.value from the join result.
    std::vector<ProjectionLogicalOperator::Projection> projections;
    projections.emplace_back(std::nullopt, LogicalFunction{FieldAccessLogicalFunction("left.value")});
    plan = LogicalPlanBuilder::addProjection(projections, false, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    ProjectionPruning phase;
    auto result = phase.apply(plan);

    /// The left-side pruning projection should include left.id (join key) and left.value (projected field).
    auto projectionOps = getOperatorByType<ProjectionLogicalOperator>(result);
    /// We expect at least 2 pruning projections (one per join side) plus the user projection.
    ASSERT_GE(projectionOps.size(), 2u);

    bool foundLeftPruning = false;
    for (const auto& proj : projectionOps)
    {
        auto fields = getProjectionFieldNames(proj);
        if (fields.contains("left.id") && fields.contains("left.value"))
        {
            foundLeftPruning = true;
        }
    }
    EXPECT_TRUE(foundLeftPruning);
}

/// When all fields are needed, no extra pruning projections are inserted.
TEST_F(ProjectionPruningTest, AllFieldsNeeded)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);

    /// Project all fields.
    std::vector<ProjectionLogicalOperator::Projection> projections;
    projections.emplace_back(std::nullopt, LogicalFunction{FieldAccessLogicalFunction("src.id")});
    projections.emplace_back(std::nullopt, LogicalFunction{FieldAccessLogicalFunction("src.value")});
    projections.emplace_back(std::nullopt, LogicalFunction{FieldAccessLogicalFunction("src.ts")});
    plan = LogicalPlanBuilder::addProjection(projections, false, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    ProjectionPruning phase;
    auto result = phase.apply(plan);

    /// Only the user's projection should exist (no additional pruning projections).
    auto projectionOps = getOperatorByType<ProjectionLogicalOperator>(result);
    ASSERT_EQ(projectionOps.size(), 1u);
}

}
}
