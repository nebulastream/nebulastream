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
#include <LogicalFunctions/FieldAccessFunction.hpp>
#include <LogicalOperators/Sinks/SinkOperator.hpp>
#include <LogicalPlans/PlanBuilder.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctions/ConstantValueFunction.hpp>
#include <LogicalFunctions/RenameFunction.hpp>
#include <LogicalOperators/EventTimeWatermarkAssignerOperator.hpp>
#include <LogicalOperators/IngestionTimeWatermarkAssignerOperator.hpp>
#include <LogicalOperators/MapOperator.hpp>
#include <LogicalOperators/ProjectionOperator.hpp>
#include <LogicalOperators/SelectionOperator.hpp>
#include <LogicalOperators/UnionOperator.hpp>
#include <LogicalOperators/Sources/SourceNameOperator.hpp>
#include <LogicalOperators/Windows/WindowedAggregationOperator.hpp>

namespace NES::Logical
{
Plan PlanBuilder::createPlan(std::string logicalSourceName)
{
    NES_TRACE("PlanBuilder: create query plan for input source  {}", logicalSourceName);
    NES::Configurations::DescriptorConfig::Config SourceDescriptorConfig{};
    auto queryPlan = Plan(SourceNameOperator(logicalSourceName));
    return queryPlan;
}

Plan PlanBuilder::addProjection(std::vector<Function> functions, Plan queryPlan)
{
    NES_TRACE("PlanBuilder: add projection operator to query plan");
    queryPlan.promoteOperatorToRoot(ProjectionOperator(std::move(functions)));
    return queryPlan;
}

Plan PlanBuilder::addSelection(Function selectionFunction, Plan queryPlan)
{
    NES_TRACE("PlanBuilder: add selection operator to query plan");
    if (selectionFunction.tryGet<RenameFunction>())
    {
        throw UnsupportedQuery("Selection predicate cannot have a FieldRenameFunction");
    }
    queryPlan.promoteOperatorToRoot(SelectionOperator(std::move(selectionFunction)));
    return queryPlan;
}

Plan PlanBuilder::addMap(Function mapFunction, Plan queryPlan)
{
    NES_TRACE("PlanBuilder: add map operator to query plan");
    if (mapFunction.tryGet<RenameFunction>())
    {
        throw UnsupportedQuery("Map function cannot have a FieldRenameFunction");
    }
    queryPlan.promoteOperatorToRoot(MapOperator(mapFunction.get<FieldAssignmentFunction>()));
    return queryPlan;
}

Plan PlanBuilder::addWindowAggregation(
    Plan queryPlan,
    std::shared_ptr<Windowing::WindowType> windowType,
    std::vector<std::shared_ptr<WindowAggregationFunction>> windowAggs,
    std::vector<FieldAccessFunction> onKeys)
{
    PRECONDITION(not queryPlan.rootOperators.empty(), "invalid query plan, as the root operator is empty");

    if (auto timeBasedWindowType = dynamic_cast<Windowing::TimeBasedWindowType*>(windowType.get()))
    {
        switch (timeBasedWindowType->getTimeCharacteristic().getType())
        {
            case Windowing::TimeCharacteristic::Type::IngestionTime:
                queryPlan.promoteOperatorToRoot(IngestionTimeWatermarkAssignerOperator());
                break;
            case Windowing::TimeCharacteristic::Type::EventTime:
                queryPlan.promoteOperatorToRoot(EventTimeWatermarkAssignerOperator(
                    FieldAccessFunction(timeBasedWindowType->getTimeCharacteristic().field->getName()),
                    timeBasedWindowType->getTimeCharacteristic().getTimeUnit()));
                break;
        }
    }
    else
    {
        throw NotImplemented("Only TimeBasedWindowType is supported for now");
    }

    auto inputSchema = queryPlan.rootOperators[0].getOutputSchema();
    queryPlan.promoteOperatorToRoot(WindowedAggregationOperator(onKeys, windowAggs, windowType));
    return queryPlan;
}

Plan PlanBuilder::addUnion(Plan leftPlan, Plan rightPlan)
{
    NES_TRACE("PlanBuilder: unionWith the subQuery to current query plan");
    leftPlan = addBinaryOperatorAndUpdateSource(UnionOperator(), leftPlan, rightPlan);
    return leftPlan;
}

Plan PlanBuilder::addJoin(
    Plan leftPlan,
    Plan rightPlan,
    Function joinFunction,
    std::shared_ptr<Windowing::WindowType> windowType,
    JoinOperator::JoinType joinType = JoinOperator::JoinType::CARTESIAN_PRODUCT)
{
    NES_TRACE("PlanBuilder: Iterate over all ExpressionNode to check join field.");
    std::unordered_set<Function> visitedFunctions;
    /// We are iterating over all binary functions and check if each side's leaf is a constant value, as we are supposedly not supporting this
    /// I am not sure why this is the case, but I will keep it for now. IMHO, the whole PlanBuilder should be refactored to be more readable and
    /// also to be more maintainable. TODO #506
    for (Function itr : BFSRange(joinFunction))
    {
        // Check if 'itr' is a BinaryFunction.
        if (itr.getChildren().size() == 2)
        {
            auto leftVisitingOp = itr.getChildren()[0];
            if (leftVisitingOp.getChildren().size() == 1)
            {
                if (visitedFunctions.find(leftVisitingOp) == visitedFunctions.end())
                {
                    visitedFunctions.insert(leftVisitingOp);
                    auto leftChild = leftVisitingOp.getChildren()[0];
                    auto rightChild = leftVisitingOp.getChildren()[1];
                    /// ensure that the child nodes are not binary
                    if ((leftChild.getChildren().size() == 1) && (rightChild.getChildren().size() == 1))
                    {
                        if (leftChild.tryGet<ConstantValueFunction>() || rightChild.tryGet<ConstantValueFunction>())
                        {
                            throw InvalidQuerySyntax("One of the join keys does only consist of a constant function. Use WHERE instead.");
                        }
                        auto leftKeyFieldAccess = leftChild.get<FieldAccessFunction>();
                        auto rightKeyFieldAccess = rightChild.get<FieldAccessFunction>();
                    }
                }
            }
        }
    }


    INVARIANT(!rightPlan.rootOperators.empty(), "RootOperators of rightPlan are empty");
    auto rootOperatorRhs = rightPlan.rootOperators[0];
    auto leftJoinType = leftPlan.rootOperators[0].getOutputSchema();
    auto rightPlanJoinType = rootOperatorRhs.getOutputSchema();

    /// check if query contain watermark assigner, and add if missing (as default behaviour)
    leftPlan = checkAndAddWatermarkAssigner(leftPlan, windowType);
    rightPlan = checkAndAddWatermarkAssigner(rightPlan, windowType);

    NES_TRACE("PlanBuilder: add join operator to query plan");
    ///TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    ///TODO(Ventura?>Steffen) can we know this at this query submission time?
    leftPlan = addBinaryOperatorAndUpdateSource(
        JoinOperator(joinFunction, std::move(windowType), joinType), leftPlan, rightPlan);
    return leftPlan;
}

Plan PlanBuilder::addSink(std::string sinkName, Plan queryPlan, WorkerId)
{
    queryPlan.promoteOperatorToRoot(SinkOperator(std::move(sinkName)));
    return queryPlan;
}

Plan PlanBuilder::checkAndAddWatermarkAssigner(Plan queryPlan, const std::shared_ptr<Windowing::WindowType> windowType)
{
    NES_TRACE("PlanBuilder: checkAndAddWatermarkAssigner for a (sub)query plan");
    auto timeBasedWindowType = Util::as<Windowing::TimeBasedWindowType>(windowType);

    if (queryPlan.getOperatorByType<IngestionTimeWatermarkAssignerOperator>().empty()
        and queryPlan.getOperatorByType<EventTimeWatermarkAssignerOperator>().empty())
    {
        if (timeBasedWindowType->getTimeCharacteristic().getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
        {
            queryPlan.promoteOperatorToRoot(IngestionTimeWatermarkAssignerOperator());
            return queryPlan;
        }
        if (timeBasedWindowType->getTimeCharacteristic().getType() == Windowing::TimeCharacteristic::Type::EventTime)
        {
            auto logicalFunction = FieldAccessFunction(timeBasedWindowType->getTimeCharacteristic().field->getName());
            auto assigner
                = EventTimeWatermarkAssignerOperator(logicalFunction, timeBasedWindowType->getTimeCharacteristic().getTimeUnit());
            queryPlan.promoteOperatorToRoot(assigner);
            return queryPlan;
        }
    }
    return queryPlan;
}

Plan PlanBuilder::addBinaryOperatorAndUpdateSource(
    Operator operatorNode, Plan leftPlan, Plan rightPlan)
{
    leftPlan.rootOperators.push_back(rightPlan.rootOperators[0]);
    leftPlan.promoteOperatorToRoot(std::move(operatorNode));
    NES_TRACE("PlanBuilder: addBinaryOperatorAndUpdateSource: update the source names");
    return leftPlan;
}
}
