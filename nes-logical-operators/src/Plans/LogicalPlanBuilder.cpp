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
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/RenameLogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
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
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <ErrorHandling.hpp>
#include <LogicalInferModelNameOperator.hpp>

namespace NES
{
LogicalPlan LogicalPlanBuilder::createLogicalPlan(std::string logicalSourceName)
{
    NES_TRACE("LogicalPlanBuilder: create query plan for input source  {}", logicalSourceName);
    const NES::Configurations::DescriptorConfig::Config sourceDescriptorConfig{};
    auto queryPlan = LogicalPlan(SourceNameLogicalOperator(logicalSourceName));
    return queryPlan;
}

LogicalPlan LogicalPlanBuilder::addProjection(std::vector<LogicalFunction> functions, const LogicalPlan& queryPlan)
{
    NES_TRACE("LogicalPlanBuilder: add projection operator to query plan");
    return promoteOperatorToRoot(queryPlan, ProjectionLogicalOperator(std::move(functions)));
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

LogicalPlan LogicalPlanBuilder::addMap(const LogicalFunction& mapFunction, const LogicalPlan& queryPlan)
{
    NES_TRACE("LogicalPlanBuilder: add map operator to query plan");
    if (mapFunction.tryGet<RenameLogicalFunction>())
    {
        throw UnsupportedQuery("Map function cannot have a FieldRenameFunction");
    }
    return promoteOperatorToRoot(queryPlan, MapLogicalOperator(mapFunction.get<FieldAssignmentLogicalFunction>()));
}

LogicalPlan LogicalPlanBuilder::addWindowAggregation(
    LogicalPlan queryPlan,
    const std::shared_ptr<Windowing::WindowType>& windowType,
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggs,
    std::vector<FieldAccessLogicalFunction> onKeys)
{
    PRECONDITION(not queryPlan.rootOperators.empty(), "invalid query plan, as the root operator is empty");

    if (auto* timeBasedWindowType = dynamic_cast<Windowing::TimeBasedWindowType*>(windowType.get()))
    {
        switch (timeBasedWindowType->getTimeCharacteristic().getType())
        {
            case Windowing::TimeCharacteristic::Type::IngestionTime:
                queryPlan = promoteOperatorToRoot(queryPlan, IngestionTimeWatermarkAssignerLogicalOperator());
                break;
            case Windowing::TimeCharacteristic::Type::EventTime:
                queryPlan = promoteOperatorToRoot(
                    queryPlan,
                    EventTimeWatermarkAssignerLogicalOperator(
                        FieldAccessLogicalFunction(timeBasedWindowType->getTimeCharacteristic().field.name),
                        timeBasedWindowType->getTimeCharacteristic().getTimeUnit()));
                break;
        }
    }
    else
    {
        throw NotImplemented("Only TimeBasedWindowType is supported for now");
    }

    auto inputSchema = queryPlan.rootOperators[0].getOutputSchema();
    return promoteOperatorToRoot(queryPlan, WindowedAggregationLogicalOperator(std::move(onKeys), std::move(windowAggs), windowType));
}

LogicalPlan LogicalPlanBuilder::addUnion(LogicalPlan leftLogicalPlan, LogicalPlan rightLogicalPlan)
{
    NES_TRACE("LogicalPlanBuilder: unionWith the subQuery to current query plan");
    leftLogicalPlan = addBinaryOperatorAndUpdateSource(UnionLogicalOperator(), leftLogicalPlan, std::move(rightLogicalPlan));
    return leftLogicalPlan;
}

LogicalPlan
LogicalPlanBuilder::addInferModel(const std::string& model, const std::vector<LogicalFunction>& inputFields, LogicalPlan queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add map inferModel to query plan");
    return promoteOperatorToRoot(queryPlan, LogicalOperator(InferModel::LogicalInferModelNameOperator(model, inputFields)));
}


LogicalPlan LogicalPlanBuilder::addJoin(
    LogicalPlan leftLogicalPlan,
    LogicalPlan rightLogicalPlan,
    const LogicalFunction& joinFunction,
    std::shared_ptr<Windowing::WindowType> windowType,
    JoinLogicalOperator::JoinType joinType)
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
                    auto leftChild = leftVisitingOp.getChildren()[0];
                    auto rightChild = leftVisitingOp.getChildren()[1];
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


    INVARIANT(!rightLogicalPlan.rootOperators.empty(), "RootOperators of rightLogicalPlan are empty");
    auto rootOperatorRhs = rightLogicalPlan.rootOperators[0];
    auto leftJoinType = leftLogicalPlan.rootOperators[0].getOutputSchema();
    auto rightLogicalPlanJoinType = rootOperatorRhs.getOutputSchema();

    /// check if query contain watermark assigner, and add if missing (as default behaviour)
    leftLogicalPlan = checkAndAddWatermarkAssigner(leftLogicalPlan, windowType);
    rightLogicalPlan = checkAndAddWatermarkAssigner(rightLogicalPlan, windowType);

    NES_TRACE("LogicalPlanBuilder: add join operator to query plan");
    leftLogicalPlan = addBinaryOperatorAndUpdateSource(
        JoinLogicalOperator(joinFunction, std::move(windowType), joinType), leftLogicalPlan, rightLogicalPlan);
    return leftLogicalPlan;
}

LogicalPlan LogicalPlanBuilder::addSink(std::string sinkName, const LogicalPlan& queryPlan)
{
    return promoteOperatorToRoot(queryPlan, SinkLogicalOperator(std::move(sinkName)));
}

LogicalPlan
LogicalPlanBuilder::checkAndAddWatermarkAssigner(LogicalPlan queryPlan, const std::shared_ptr<Windowing::WindowType>& windowType)
{
    NES_TRACE("LogicalPlanBuilder: checkAndAddWatermarkAssigner for a (sub)query plan");
    auto timeBasedWindowType = Util::as<Windowing::TimeBasedWindowType>(windowType);

    if (getOperatorByType<IngestionTimeWatermarkAssignerLogicalOperator>(queryPlan).empty()
        and getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(queryPlan).empty())
    {
        if (timeBasedWindowType->getTimeCharacteristic().getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
        {
            return promoteOperatorToRoot(queryPlan, IngestionTimeWatermarkAssignerLogicalOperator());
        }
        if (timeBasedWindowType->getTimeCharacteristic().getType() == Windowing::TimeCharacteristic::Type::EventTime)
        {
            auto logicalFunction = FieldAccessLogicalFunction(timeBasedWindowType->getTimeCharacteristic().field.name);
            auto assigner
                = EventTimeWatermarkAssignerLogicalOperator(logicalFunction, timeBasedWindowType->getTimeCharacteristic().getTimeUnit());
            return promoteOperatorToRoot(queryPlan, assigner);
        }
    }
    return queryPlan;
}

LogicalPlan LogicalPlanBuilder::addBinaryOperatorAndUpdateSource(
    const LogicalOperator& operatorNode, LogicalPlan leftLogicalPlan, LogicalPlan rightLogicalPlan)
{
    leftLogicalPlan.rootOperators.push_back(rightLogicalPlan.rootOperators[0]);
    return promoteOperatorToRoot(leftLogicalPlan, operatorNode);
}
}
