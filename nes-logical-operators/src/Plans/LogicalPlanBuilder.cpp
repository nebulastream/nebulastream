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

#include <Plans/LogicalPlanBuilder.hpp>

#include <array>
#include <memory>
#include <ranges>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/RenameLogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <ErrorHandling.hpp>
#include "Functions/UnboundFieldAccessLogicalFunction.hpp"

namespace NES
{
LogicalPlan LogicalPlanBuilder::createLogicalPlan(std::string logicalSourceName)
{
    NES_TRACE("LogicalPlanBuilder: create query plan for input source  {}", logicalSourceName);
    const DescriptorConfig::Config sourceDescriptorConfig{};
    return LogicalPlan(SourceNameLogicalOperator(logicalSourceName));
}

LogicalPlan LogicalPlanBuilder::addProjection(
    std::vector<ProjectionLogicalOperator::Projection> projections, bool asterisk, const LogicalPlan& queryPlan)
{
    NES_TRACE("LogicalPlanBuilder: add projection operator to query plan");
    return promoteOperatorToRoot(
        queryPlan, ProjectionLogicalOperator(std::move(projections), ProjectionLogicalOperator::Asterisk(asterisk)));
}

LogicalPlan LogicalPlanBuilder::addSelection(LogicalFunction selectionFunction, const LogicalPlan& queryPlan)
{
    NES_TRACE("LogicalPlanBuilder: add selection operator to query plan");
    if (selectionFunction.tryGet<RenameLogicalFunction>())
    {
        throw UnsupportedQuery("Selection predicate cannot have a FieldRenameFunction");
    }
    return promoteOperatorToRoot(queryPlan, SelectionLogicalOperator(std::move(selectionFunction)));
}

LogicalPlan LogicalPlanBuilder::addWindowAggregation(
    LogicalPlan queryPlan,
    const std::shared_ptr<Windowing::WindowType>& windowType,
    std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation> windowAggs,
    std::vector<UnboundFieldAccessLogicalFunction> onKeys,
    Windowing::TimeCharacteristic timeCharacteristic)
{
    PRECONDITION(not queryPlan.getRootOperators().empty(), "invalid query plan, as the root operator is empty");

    if (auto* timeBasedWindowType = dynamic_cast<Windowing::TimeBasedWindowType*>(windowType.get()); timeBasedWindowType == nullptr)
    {
        throw NotImplemented("Only TimeBasedWindowType is supported for now");
    }

    queryPlan = checkAndAddWatermarkAssigner(queryPlan, timeCharacteristic);

    auto inputSchema = queryPlan.getRootOperators().front().getOutputSchema();
    auto keysWithNames = onKeys
        | std::views::transform(
                             [](const UnboundFieldAccessLogicalFunction& key)
                             {
                                 return std::make_pair<UnboundFieldAccessLogicalFunction, std::optional<Identifier>>(
                                     UnboundFieldAccessLogicalFunction{key}, std::make_optional(*(key.getFieldName().end() - 1)));
                             })
        | std::ranges::to<std::vector>();
    return promoteOperatorToRoot(
        queryPlan,
        WindowedAggregationLogicalOperator(std::move(keysWithNames), std::move(windowAggs), windowType, std::move(timeCharacteristic)));
}

LogicalPlan LogicalPlanBuilder::addUnion(LogicalPlan leftLogicalPlan, LogicalPlan rightLogicalPlan)
{
    NES_TRACE("LogicalPlanBuilder: unionWith the subQuery to current query plan");
    leftLogicalPlan = addBinaryOperatorAndUpdateSource(UnionLogicalOperator(), leftLogicalPlan, std::move(rightLogicalPlan));
    return leftLogicalPlan;
}

LogicalPlan LogicalPlanBuilder::addJoin(
    LogicalPlan leftLogicalPlan,
    LogicalPlan rightLogicalPlan,
    const LogicalFunction& joinFunction,
    std::shared_ptr<Windowing::WindowType> windowType,
    JoinLogicalOperator::JoinType joinType,
    Windowing::TimeCharacteristic leftCharacteristic,
    Windowing::TimeCharacteristic rightCharacteristic)
{
    NES_TRACE("LogicalPlanBuilder: Iterate over all ExpressionNode to check join field.");
    std::unordered_set<LogicalFunction> visitedFunctions;
    /// We are iterating over all binary functions and check if each side's leaf is a constant value, as we are supposedly not supporting this
    /// I am not sure why this is the case, but I will keep it for now. IMHO, the whole LogicalPlanBuilder should be refactored to be more readable and
    /// also to be more maintainable. TODO #506
    for (const LogicalFunction& itr : BFSRange(joinFunction))
    {
        if (itr.getChildren().size() == 2)
        {
            auto leftVisitingOp = itr.getChildren()[0];
            if (leftVisitingOp.getChildren().size() == 1)
            {
                if (visitedFunctions.find(leftVisitingOp) == visitedFunctions.end())
                {
                    visitedFunctions.insert(leftVisitingOp);
                    auto leftChild = leftVisitingOp.getChildren().at(0);
                    auto rightChild = leftVisitingOp.getChildren().at(1);
                    /// ensure that the child nodes are not binary
                    if ((leftChild.getChildren().size() == 1) && (rightChild.getChildren().size() == 1))
                    {
                        if (leftChild.tryGet<ConstantValueLogicalFunction>() || rightChild.tryGet<ConstantValueLogicalFunction>())
                        {
                            throw InvalidQuerySyntax("One of the join keys does only consist of a constant function. Use WHERE instead.");
                        }
                        auto leftKeyFieldAccess = leftChild.get<FieldAccessLogicalFunction>();
                        auto rightKeyFieldAccess = rightChild.get<FieldAccessLogicalFunction>();
                    }
                }
            }
        }
    }

    /// check if query contain watermark assigner, and add if missing (as default behaviour)
    leftLogicalPlan = checkAndAddWatermarkAssigner(leftLogicalPlan, leftCharacteristic);
    rightLogicalPlan = checkAndAddWatermarkAssigner(rightLogicalPlan, rightCharacteristic);
    auto joinTimeCharacteristicOpt
        = JoinLogicalOperator::createJoinTimeCharacteristic({std::move(leftCharacteristic), std::move(rightCharacteristic)});
    PRECONDITION(joinTimeCharacteristicOpt.has_value(), "Join time characteristics must be either both bound or unbound");


    INVARIANT(!rightLogicalPlan.getRootOperators().empty(), "RootOperators of rightLogicalPlan are empty");
    auto rootOperatorRhs = rightLogicalPlan.getRootOperators().front();
    auto leftJoinType = leftLogicalPlan.getRootOperators().front().getOutputSchema();
    auto rightLogicalPlanJoinType = rootOperatorRhs.getOutputSchema();


    NES_TRACE("LogicalPlanBuilder: add join operator to query plan");
    leftLogicalPlan = addBinaryOperatorAndUpdateSource(
        JoinLogicalOperator(joinFunction, std::move(windowType), joinType, std::move(joinTimeCharacteristicOpt).value()),
        leftLogicalPlan,
        rightLogicalPlan);
    return leftLogicalPlan;
}

LogicalPlan LogicalPlanBuilder::addSink(std::string sinkName, const LogicalPlan& queryPlan)
{
    return promoteOperatorToRoot(queryPlan, SinkLogicalOperator(std::move(sinkName)));
}

LogicalPlan LogicalPlanBuilder::checkAndAddWatermarkAssigner(LogicalPlan queryPlan, const Windowing::TimeCharacteristic& timeCharacteristic)
{
    NES_TRACE("LogicalPlanBuilder: checkAndAddWatermarkAssigner for a (sub)query plan");
    return std::visit(
        [&queryPlan](const auto& unboundBound)
        {
            return std::visit(
                Overloaded{
                    [&queryPlan](const Windowing::IngestionTimeCharacteristic&)
                    { return promoteOperatorToRoot(queryPlan, IngestionTimeWatermarkAssignerLogicalOperator()); },
                    [&queryPlan](const auto& eventTime)
                    {
                        return promoteOperatorToRoot(queryPlan, EventTimeWatermarkAssignerLogicalOperator(eventTime.field, eventTime.unit));
                    },
                },
                unboundBound);
        },
        timeCharacteristic);
}

LogicalPlan LogicalPlanBuilder::addBinaryOperatorAndUpdateSource(
    const LogicalOperator& operatorNode, const LogicalPlan& leftLogicalPlan, const LogicalPlan& rightLogicalPlan)
{
    auto newRootOperators = std::ranges::views::join(std::array{leftLogicalPlan.getRootOperators(), rightLogicalPlan.getRootOperators()});
    return promoteOperatorToRoot(leftLogicalPlan.withRootOperators(newRootOperators | std::ranges::to<std::vector>()), operatorNode);
}
}
