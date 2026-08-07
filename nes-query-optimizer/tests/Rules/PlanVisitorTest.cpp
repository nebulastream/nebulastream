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

#include <Rules/PlanVisitor.hpp>

#include <algorithm>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <OptimizerTestUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class PlanVisitorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("PlanVisitorTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};

namespace
{

using TestVisitor = PlanVisitor<int, int, int>;

TypedLogicalOperator<SelectionLogicalOperator> createSelection(const LogicalOperator& child, const std::string& value)
{
    return SelectionLogicalOperator::create(
        child,
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{child.getOutputSchema()[Identifier::parse("a")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, value}});
}
}

TEST_F(PlanVisitorTest, IdentityKeepsLinearPlanStructureAndComputesDepthTopDown)
{
    /// source(depth 2) < selection(depth 1) < sink(depth 0)
    auto source = utils.createSource("linear", {"a"});
    auto selection = createSelection(source, "0");
    auto sink = utils.createSink(selection, "linear", {"a"});
    auto plan = utils.createPlan(sink);

    std::unordered_map<OperatorId, int> depthByOriginalId;

    TestVisitor visitor{
        [](const LogicalOperator& op, const std::vector<int>& parentDepths) -> TestVisitor::DownResult
        {
            int depth = 0;
            for (auto parentDepth : parentDepths)
            {
                depth = std::max(depth, parentDepth);
            }
            std::unordered_map<LogicalOperator, int> childDepths;
            for (const auto& child : op.getChildren())
            {
                childDepths.emplace(child, depth + 1);
            }
            return {.operatorContext = depth, .downContexts = childDepths};
        },
        [&depthByOriginalId](
            const LogicalOperator& op, std::vector<LogicalOperator> newChildren, int depth, const std::unordered_map<LogicalOperator, int>&)
            -> TestVisitor::UpResult
        {
            depthByOriginalId.emplace(op.getId(), depth);
            return {op.withChildren(std::move(newChildren)), {}};
        }};

    auto result = visitor.apply(plan);

    auto newSink = result.getRootOperators().at(0);
    ASSERT_TRUE(newSink.tryGetAs<SinkLogicalOperator>());
    auto newSelection = newSink.getChildren().at(0);
    ASSERT_TRUE(newSelection.tryGetAs<SelectionLogicalOperator>());
    auto newSource = newSelection.getChildren().at(0);
    ASSERT_TRUE(newSource.tryGetAs<SourceDescriptorLogicalOperator>());

    EXPECT_EQ(depthByOriginalId.at(sink.getId()), 0);
    EXPECT_EQ(depthByOriginalId.at(selection.getId()), 1);
    EXPECT_EQ(depthByOriginalId.at(source.getId()), 2);
}

TEST_F(PlanVisitorTest, IdentityKeepsLinearPlanStructureAndComputesDepthBottomUp)
{
    /// source(depth 2) < selection(depth 1) < sink(depth 0)
    auto source = utils.createSource("linearBottomUp", {"a"});
    auto selection = createSelection(source, "0");
    auto sink = utils.createSink(selection, "linearBottomUp", {"a"});
    auto plan = utils.createPlan(sink);

    std::unordered_map<OperatorId, int> depthByOriginalId;

    TestVisitor visitor{
        [&depthByOriginalId](
            const LogicalOperator& op,
            std::vector<LogicalOperator> newChildren,
            int,
            std::unordered_map<LogicalOperator, int> childContexts) -> TestVisitor::UpResult
        {
            int depth = 0;

            for (const auto& child : newChildren)
            {
                if (!childContexts.contains(child))
                {
                    throw TestException("all children must be in childContext");
                }
            }

            for (const auto& value : childContexts | std::views::values)
            {
                depth = std::max(value + 1, depth);
            }
            depthByOriginalId.emplace(op.getId(), depth);
            return {op.withChildren(std::move(newChildren)), depth};
        }};

    auto result = visitor.apply(plan);

    auto newSink = result.getRootOperators().at(0);
    ASSERT_TRUE(newSink.tryGetAs<SinkLogicalOperator>());
    auto newSelection = newSink.getChildren().at(0);
    ASSERT_TRUE(newSelection.tryGetAs<SelectionLogicalOperator>());
    auto newSource = newSelection.getChildren().at(0);
    ASSERT_TRUE(newSource.tryGetAs<SourceDescriptorLogicalOperator>());

    EXPECT_EQ(depthByOriginalId.at(sink.getId()), 2);
    EXPECT_EQ(depthByOriginalId.at(selection.getId()), 1);
    EXPECT_EQ(depthByOriginalId.at(source.getId()), 0);
}

TEST_F(PlanVisitorTest, SharedSubplanIsVisitedOnceAndSharingIsPreserved)
{
    /// sink < union < (selection1, selection2) < shared source
    /// The source has two parents (selection1 and selection2), so this is a genuine DAG, not a tree.
    auto source = utils.createSource("shared", {"a"});
    auto selection1 = createSelection(source, "0");
    auto selection2 = createSelection(source, "1");
    auto unionOp = UnionLogicalOperator::create(std::vector<LogicalOperator>{selection1, selection2});
    auto sink = utils.createSink(unionOp, "shared", {"a"});
    auto plan = utils.createPlan(sink);

    int sourceVisitCount = 0;
    int sourceParentContextCount = 0;

    TestVisitor visitor{
        [&](const LogicalOperator& op, const std::vector<int>& parentContexts) -> TestVisitor::DownResult
        {
            if (op.tryGetAs<SourceDescriptorLogicalOperator>())
            {
                ++sourceVisitCount;
                sourceParentContextCount = static_cast<int>(parentContexts.size());
            }
            std::unordered_map<LogicalOperator, int> childContexts;
            for (const auto& child : op.getChildren())
            {
                childContexts.emplace(child, 0);
            }
            return {.operatorContext = 0, .downContexts = childContexts};
        },
        [](const LogicalOperator& op, std::vector<LogicalOperator> newChildren, int, const std::unordered_map<LogicalOperator, int>&)
            -> TestVisitor::UpResult { return {op.withChildren(std::move(newChildren)), {}}; }};

    auto result = visitor.apply(plan);

    /// fnDown must only run once for the shared source, but must see contributions from both parents.
    EXPECT_EQ(sourceVisitCount, 1);
    EXPECT_EQ(sourceParentContextCount, 2);

    auto newSink = result.getRootOperators().at(0);
    auto newUnion = newSink.getChildren().at(0);
    ASSERT_TRUE(newUnion.tryGetAs<UnionLogicalOperator>());
    auto newSelection1 = newUnion.getChildren().at(0);
    auto newSelection2 = newUnion.getChildren().at(1);
    ASSERT_TRUE(newSelection1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(newSelection2.tryGetAs<SelectionLogicalOperator>());

    auto newSource1 = newSelection1.getChildren().at(0);
    auto newSource2 = newSelection2.getChildren().at(0);
    ASSERT_TRUE(newSource1.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_TRUE(newSource2.tryGetAs<SourceDescriptorLogicalOperator>());

    /// The rebuilt source must be the very same instance under both parents: fnUp is expected
    /// to run once per shared node and the result is reused, not duplicated.
    EXPECT_EQ(newSource1, newSource2);
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
