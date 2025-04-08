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

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Functions/LogicalFunctions/EqualsLogicalFunction.hpp>
#include <Functions/RenameLogicalFunction.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/MapLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
LogicalPlan LogicalPlanBuilder::createLogicalPlan(std::string logicalSourceName)
{
    NES_TRACE("LogicalPlanBuilder: create query plan for input source  {}", logicalSourceName);
    NES::Configurations::DescriptorConfig::Config SourceDescriptorConfig{};
    auto queryPlan = LogicalPlan(SourceNameLogicalOperator(logicalSourceName));
    return queryPlan;
}

LogicalPlan
LogicalPlanBuilder::addProjection(std::vector<LogicalFunction> functions, LogicalPlan queryPlan)
{
    NES_TRACE("LogicalPlanBuilder: add projection operator to query plan");
    queryPlan.promoteOperatorToRoot(ProjectionLogicalOperator(functions));
    return queryPlan;
}

LogicalPlan
LogicalPlanBuilder::addSelection(LogicalFunction selectionFunction, LogicalPlan queryPlan)
{
    NES_TRACE("LogicalPlanBuilder: add selection operator to query plan");
    if (selectionFunction.tryGet<RenameLogicalFunction>())
    {
        throw UnsupportedQuery("Selection predicate cannot have a FieldRenameFunction");
    }
    queryPlan.promoteOperatorToRoot(SelectionLogicalOperator(selectionFunction));
    return queryPlan;
}

LogicalPlan
LogicalPlanBuilder::addMap(LogicalFunction mapFunction, LogicalPlan queryPlan)
{
    NES_TRACE("LogicalPlanBuilder: add map operator to query plan");
    if (mapFunction.tryGet<RenameLogicalFunction>())
    {
        throw UnsupportedQuery("Map function cannot have a FieldRenameFunction");
    }
    queryPlan.promoteOperatorToRoot(MapLogicalOperator(mapFunction.get<FieldAssignmentLogicalFunction>()));
    return queryPlan;
}

LogicalPlan LogicalPlanBuilder::addWindowAggregation(
    LogicalPlan queryPlan,
    std::shared_ptr<Windowing::WindowType> windowType,
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggs,
    std::vector<FieldAccessLogicalFunction> onKeys)
{
    PRECONDITION(not queryPlan.getRootOperators().empty(), "invalid query plan, as the root operator is empty");

    auto timeBasedWindowType = dynamic_cast<Windowing::TimeBasedWindowType*>(windowType.get());
    if (timeBasedWindowType)
    {
        switch (timeBasedWindowType->getTimeCharacteristic().getType())
        {
            case Windowing::TimeCharacteristic::Type::IngestionTime:
                queryPlan.promoteOperatorToRoot(IngestionTimeWatermarkAssignerLogicalOperator());
                break;
            case Windowing::TimeCharacteristic::Type::EventTime:
                queryPlan.promoteOperatorToRoot(EventTimeWatermarkAssignerLogicalOperator(
                        FieldAccessLogicalFunction(timeBasedWindowType->getTimeCharacteristic().getField().getName()),
                        timeBasedWindowType->getTimeCharacteristic().getTimeUnit()));
                break;
        }
    }
    else
    {
        throw NotImplemented("Only TimeBasedWindowType is supported for now");
    }

    auto inputSchema = queryPlan.getRootOperators()[0].getOutputSchema();
    queryPlan.promoteOperatorToRoot(WindowedAggregationLogicalOperator(onKeys,  windowAggs, windowType));
    return queryPlan;
}

LogicalPlan LogicalPlanBuilder::addUnion(LogicalPlan leftLogicalPlan, LogicalPlan rightLogicalPlan)
{
    NES_TRACE("LogicalPlanBuilder: unionWith the subQuery to current query plan");
    leftLogicalPlan = addBinaryOperatorAndUpdateSource(UnionLogicalOperator(), leftLogicalPlan, rightLogicalPlan);
    return leftLogicalPlan;
}

LogicalPlan LogicalPlanBuilder::addJoin(
    LogicalPlan leftLogicalPlan,
    LogicalPlan rightLogicalPlan,
    LogicalFunction joinFunction,
    std::shared_ptr<Windowing::WindowType> windowType,
    JoinLogicalOperator::JoinType joinType = JoinLogicalOperator::JoinType::CARTESIAN_PRODUCT)
{
    NES_TRACE("LogicalPlanBuilder: Iterate over all ExpressionNode to check join field.");
    std::unordered_set<LogicalFunction> visitedFunctions;
    /// We are iterating over all binary functions and check if each side's leaf is a constant value, as we are supposedly not supporting this
    /// I am not sure why this is the case, but I will keep it for now. IMHO, the whole LogicalPlanBuilder should be refactored to be more readable and
    /// also to be more maintainable. TODO #506
    for (LogicalFunction itr : BFSRange<LogicalFunction>(joinFunction))
    {
        // Check if 'itr' is a BinaryLogicalFunction.
        if (itr.getChildren().size() == 2)
        {
            if (itr.getChildren()[0].getChildren().size() != 2)
            {
                if (visitedFunctions.find(itr.getChildren()[0]) == visitedFunctions.end())
                {
                    visitedFunctions.insert(itr.getChildren()[0]);
                    if ((itr.getChildren()[0].getChildren()[0].getChildren().size() != 2) && (itr.getChildren()[0].getChildren()[1].getChildren().size() != 2))
                        {
                        if (itr.getChildren()[0].getChildren()[0].tryGet<ConstantValueLogicalFunction>() ||
                            itr.getChildren()[0].getChildren()[1].tryGet<ConstantValueLogicalFunction>())
                        {
                            throw InvalidQuerySyntax("One of the join keys does only consist of a constant function. Use WHERE instead.");
                        }
                        auto leftKeyFieldAccess = itr.getChildren()[0].getChildren()[0].get<FieldAccessLogicalFunction>();
                        auto rightKeyFieldAccess = itr.getChildren()[0].getChildren()[1].get<FieldAccessLogicalFunction>();
                    }
                }
            }
        }
    }


    INVARIANT(!rightLogicalPlan.getRootOperators().empty(), "RootOperators of rightLogicalPlan are empty");
    auto& rootOperatorRhs = rightLogicalPlan.getRootOperators()[0];
    auto leftJoinType = leftLogicalPlan.getRootOperators()[0].getOutputSchema();
    auto rightLogicalPlanJoinType = rootOperatorRhs.getOutputSchema();

    /// check if query contain watermark assigner, and add if missing (as default behaviour)
    leftLogicalPlan = checkAndAddWatermarkAssignment(leftLogicalPlan, windowType);
    rightLogicalPlan = checkAndAddWatermarkAssignment(rightLogicalPlan, windowType);

    NES_TRACE("LogicalPlanBuilder: add join operator to query plan");
    ///TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    ///TODO(Ventura?>Steffen) can we know this at this query submission time?
    leftLogicalPlan = addBinaryOperatorAndUpdateSource(JoinLogicalOperator(joinFunction, std::move(windowType), 1, 1, joinType), leftLogicalPlan, rightLogicalPlan);
    return leftLogicalPlan;
}

LogicalPlan LogicalPlanBuilder::addSink(std::string sinkName, LogicalPlan queryPlan, WorkerId)
{
    queryPlan.promoteOperatorToRoot(SinkLogicalOperator(sinkName));
    return queryPlan;
}

LogicalPlan LogicalPlanBuilder::checkAndAddWatermarkAssignment(
    LogicalPlan queryPlan, const std::shared_ptr<Windowing::WindowType> windowType)
{
    NES_TRACE("LogicalPlanBuilder: checkAndAddWatermarkAssignment for a (sub)query plan");
    auto timeBasedWindowType = Util::as<Windowing::TimeBasedWindowType>(windowType);

    if (queryPlan.getOperatorByType<IngestionTimeWatermarkAssignerLogicalOperator>().empty() and
        queryPlan.getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>().empty())
    {
        if (timeBasedWindowType->getTimeCharacteristic().getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
        {
            queryPlan.promoteOperatorToRoot(IngestionTimeWatermarkAssignerLogicalOperator());
            return queryPlan;
        }
        if (timeBasedWindowType->getTimeCharacteristic().getType() == Windowing::TimeCharacteristic::Type::EventTime)
        {
            auto logicalFunction = FieldAccessLogicalFunction(timeBasedWindowType->getTimeCharacteristic().getField().getName());
            auto assigner = EventTimeWatermarkAssignerLogicalOperator(logicalFunction, timeBasedWindowType->getTimeCharacteristic().getTimeUnit());
            queryPlan.promoteOperatorToRoot(assigner);
            return queryPlan;
        }
    }
    return queryPlan;
}

LogicalPlan LogicalPlanBuilder::addBinaryOperatorAndUpdateSource(
    LogicalOperator operatorNode, LogicalPlan leftLogicalPlan, LogicalPlan rightLogicalPlan)
{
    leftLogicalPlan.addRootOperator(rightLogicalPlan.getRootOperators()[0]);
    leftLogicalPlan.promoteOperatorToRoot(std::move(operatorNode));
    NES_TRACE("LogicalPlanBuilder: addBinaryOperatorAndUpdateSource: update the source names");
    return leftLogicalPlan;
}
}
