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
#include <Plans/Operator.hpp>
#include <Plans/QueryPlanBuilder.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
QueryPlan QueryPlanBuilder::createQueryPlan(std::string logicalSourceName)
{
    NES_TRACE("QueryPlanBuilder: create query plan for input source  {}", logicalSourceName);
    NES::Configurations::DescriptorConfig::Config SourceDescriptorConfig{};
    auto queryPlan = QueryPlan(SourceNameLogicalOperator(logicalSourceName));
    return queryPlan;
}

QueryPlan
QueryPlanBuilder::addProjection(std::vector<LogicalFunction> functions, QueryPlan queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add projection operator to query plan");
    queryPlan.promoteOperatorToRoot(ProjectionLogicalOperator(std::move(functions)));
    return queryPlan;
}

QueryPlan
QueryPlanBuilder::addSelection(LogicalFunction selectionFunction, QueryPlan queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add selection operator to query plan");
    if (!selectionFunction.tryGet<RenameLogicalFunction>())
    {
        throw UnsupportedQuery("Selection predicate cannot have a FieldRenameFunction");
    }
    queryPlan.promoteOperatorToRoot(SelectionLogicalOperator(std::move(selectionFunction)));
    return queryPlan;
}

QueryPlan
QueryPlanBuilder::addMap(LogicalFunction mapFunction, QueryPlan queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add map operator to query plan");
    if (!mapFunction.tryGet<RenameLogicalFunction>())
    {
        throw UnsupportedQuery("Map function cannot have a FieldRenameFunction");
    }
    queryPlan.promoteOperatorToRoot(MapLogicalOperator(mapFunction));
    return queryPlan;
}

QueryPlan QueryPlanBuilder::addWindowAggregation(
    QueryPlan queryPlan,
    std::unique_ptr<Windowing::WindowType> windowType,
    std::vector<std::unique_ptr<WindowAggregationLogicalFunction>> windowAggs,
    std::vector<std::unique_ptr<FieldAccessLogicalFunction>> onKeys)
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

    auto inputSchema = dynamic_cast<const LogicalOperator*>(queryPlan.getRootOperators()[0])->getOutputSchema();
    queryPlan.promoteOperatorToRoot(WindowedAggregationLogicalOperator(std::move(onKeys),  std::move(windowAggs), std::move(windowType)));
    return queryPlan;
}

QueryPlan QueryPlanBuilder::addUnion(QueryPlan leftQueryPlan, QueryPlan rightQueryPlan)
{
    NES_TRACE("QueryPlanBuilder: unionWith the subQuery to current query plan");
    leftQueryPlan = addBinaryOperatorAndUpdateSource(UnionLogicalOperator(), leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlan QueryPlanBuilder::addJoin(
    QueryPlan leftQueryPlan,
    QueryPlan rightQueryPlan,
    LogicalFunction joinFunction,
    std::unique_ptr<Windowing::WindowType> windowType,
    JoinLogicalOperator::JoinType joinType = JoinLogicalOperator::JoinType::CARTESIAN_PRODUCT)
{
    NES_TRACE("QueryPlanBuilder: Iterate over all ExpressionNode to check join field.");
    std::unordered_set<LogicalFunction*> visitedFunctions;
    /// We are iterating over all binary functions and check if each side's leaf is a constant value, as we are supposedly not supporting this
    /// I am not sure why this is the case, but I will keep it for now. IMHO, the whole QueryPlanBuilder should be refactored to be more readable and
    /// also to be more maintainable. TODO #506
    for (LogicalFunction* itr : BFSRange<LogicalFunction>(joinFunction.get()))
    {
        // Check if 'itr' is a BinaryLogicalFunction.
        if (auto* binaryFunc = dynamic_cast<BinaryLogicalFunction*>(itr))
        {
            if (!dynamic_cast<BinaryLogicalFunction*>(&binaryFunc->getLeftChild()))
            {
                if (visitedFunctions.find(binaryFunc) == visitedFunctions.end())
                {
                    visitedFunctions.insert(binaryFunc);
                    if (!dynamic_cast<BinaryLogicalFunction*>(&binaryFunc->getLeftChild()) &&
                        !dynamic_cast<BinaryLogicalFunction*>(&binaryFunc->getRightChild()))
                    {
                        if (dynamic_cast<ConstantValueLogicalFunction*>(&binaryFunc->getLeftChild()) ||
                            dynamic_cast<ConstantValueLogicalFunction*>(&binaryFunc->getRightChild()))
                        {
                            throw InvalidQuerySyntax("One of the join keys does only consist of a constant function. Use WHERE instead.");
                        }
                        auto leftKeyFieldAccess = asFieldAccessLogicalFunction(binaryFunc->getLeftChild().clone(), "leftSide");
                        auto rightKeyFieldAccess = asFieldAccessLogicalFunction(binaryFunc->getRightChild().clone(), "rightSide");
                    }
                }
            }
        }
    }


    INVARIANT(!rightQueryPlan.getRootOperators().empty(), "RootOperators of rightQueryPlan are empty");
    auto& rootOperatorRhs = rightQueryPlan.getRootOperators()[0];
    auto leftJoinType = dynamic_cast<const Operator*>(leftQueryPlan.getRootOperators()[0])->getOutputSchema();
    auto rightQueryPlanJoinType = dynamic_cast<Operator*>(rootOperatorRhs)->getOutputSchema();

    /// check if query contain watermark assigner, and add if missing (as default behaviour)
    leftQueryPlan = checkAndAddWatermarkAssignment(leftQueryPlan, windowType);
    rightQueryPlan = checkAndAddWatermarkAssignment(rightQueryPlan, windowType);

    NES_TRACE("QueryPlanBuilder: add join operator to query plan");
    ///TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    ///TODO(Ventura?>Steffen) can we know this at this query submission time?
    leftQueryPlan = addBinaryOperatorAndUpdateSource(JoinLogicalOperator(joinFunction, std::move(windowType), 1, 1, joinType), leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlan QueryPlanBuilder::addSink(std::string sinkName, QueryPlan queryPlan, WorkerId)
{
    queryPlan.promoteOperatorToRoot(SinkLogicalOperator(std::move(sinkName)));
    return queryPlan;
}

/*
std::shared_ptr<QueryPlan> QueryPlanBuilder::assignWatermark(
    std::shared_ptr<QueryPlan> queryPlan, std::shared_ptr<Windowing::WatermarkStrategyDescriptor> const& watermarkStrategyDescriptor)
{
    const std::shared_ptr<Operator> op
        = std::make_shared<WatermarkAssignerLogicalOperator>(watermarkStrategyDescriptor);
    queryPlan->promoteOperatorToRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::checkAndAddWatermarkAssignment(
    std::shared_ptr<QueryPlan> queryPlan, const std::shared_ptr<Windowing::WindowType> windowType)
{
    NES_TRACE("QueryPlanBuilder: checkAndAddWatermarkAssignment for a (sub)query plan");
    auto timeBasedWindowType = Util::as<Windowing::TimeBasedWindowType>(windowType);

    if (queryPlan->getOperatorByType<WatermarkAssignerLogicalOperator>().empty())
    {
        if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
        {
            return assignWatermark(queryPlan, Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
        }
        else if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::EventTime)
        {
            return assignWatermark(
                queryPlan,
                Windowing::EventTimeWatermarkStrategyDescriptor::create(
                    std::make_shared<FieldAccessLogicalFunction>(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()),
                    timeBasedWindowType->getTimeCharacteristic()->getTimeUnit()));
        }
    }
    return queryPlan;
}
*/

QueryPlan QueryPlanBuilder::addBinaryOperatorAndUpdateSource(
    Operator operatorNode, QueryPlan leftQueryPlan, QueryPlan rightQueryPlan)
{
    leftQueryPlan.addRootOperator(rightQueryPlan.getRootOperators()[0]);
    leftQueryPlan.promoteOperatorToRoot(std::move(operatorNode));
    NES_TRACE("QueryPlanBuilder: addBinaryOperatorAndUpdateSource: update the source names");
    return leftQueryPlan;
}
}
