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
#include <API/AttributeField.hpp>
#include <ErrorHandling.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/EqualsBinaryLogicalFunction.hpp>
#include <Functions/RenameLogicalFunction.hpp>
#include <Operators/LogicalOperators/LimitLogicalOperator.hpp>
#include <Operators/LogicalOperators/MapLogicalOperator.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperator.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperator.hpp>
#include <Operators/LogicalOperators/RenameSourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/SelectionLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/WindowLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/QueryPlanBuilder.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
std::shared_ptr<QueryPlan> QueryPlanBuilder::createQueryPlan(std::string logicalSourceName)
{
    NES_TRACE("QueryPlanBuilder: create query plan for input source  {}", logicalSourceName);
    Configurations::DescriptorConfig::Config SourceDescriptorConfig{};
    auto sourceOperator = std::make_shared<SourceNameLogicalOperator>(logicalSourceName, getNextOperatorId());
    auto queryPlan = QueryPlan::create(sourceOperator);
    queryPlan->setSourceConsumed(logicalSourceName);
    return queryPlan;
}

std::shared_ptr<QueryPlan>
QueryPlanBuilder::addProjection(const std::vector<std::shared_ptr<LogicalFunction>>& functions, std::shared_ptr<QueryPlan> queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add projection operator to query plan");
    const std::shared_ptr<Operator> op = std::make_shared<ProjectionLogicalOperator>(functions, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addRename(const std::string& newSourceName, std::shared_ptr<QueryPlan> queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add rename operator to query plan");
    auto op = std::make_shared<RenameSourceLogicalOperator>(newSourceName, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan>
QueryPlanBuilder::addSelection(std::shared_ptr<LogicalFunction> const& selectionFunction, std::shared_ptr<QueryPlan> queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add selection operator to query plan");
    if (!selectionFunction->getFunctionByType<RenameLogicalFunction>().empty())
    {
        throw UnsupportedQuery("Selection predicate cannot have a FieldRenameFunction");
    }
    const std::shared_ptr<Operator> op = std::make_shared<SelectionLogicalOperator>(selectionFunction, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addLimit(const uint64_t limit, std::shared_ptr<QueryPlan> queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add limit operator to query plan");
    const std::shared_ptr<Operator> op = std::make_shared<LimitLogicalOperator>(limit, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan>
QueryPlanBuilder::addMap(std::shared_ptr<FieldAssignmentBinaryLogicalFunction> const& mapFunction, std::shared_ptr<QueryPlan> queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add map operator to query plan");
    if (!mapFunction->getFunctionByType<RenameLogicalFunction>().empty())
    {
        throw UnsupportedQuery("Map function cannot have a FieldRenameFunction");
    }
    const std::shared_ptr<Operator> op = std::make_shared<MapLogicalOperator>(mapFunction, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addWindowAggregation(
    std::shared_ptr<QueryPlan> queryPlan,
    const std::shared_ptr<Windowing::WindowType>& windowType,
    const std::vector<std::shared_ptr<Windowing::WindowAggregationDescriptor>>& windowAggs,
    const std::vector<std::shared_ptr<FieldAccessLogicalFunction>>& onKeys)
{
    PRECONDITION(not queryPlan->getRootOperators().empty(), "invalid query plan, as the root operator is empty");

    if (const auto timeBasedWindowType = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(windowType))
    {
        /// check if query contain watermark assigner, and add if missing (as default behaviour)
        if (not NES::Util::instanceOf<WatermarkAssignerLogicalOperator>(queryPlan->getRootOperators()[0]))
            NES_DEBUG("add default watermark strategy as non is provided");

        switch (timeBasedWindowType->getTimeCharacteristic()->getType())
        {
            case Windowing::TimeCharacteristic::Type::IngestionTime:
                queryPlan->appendOperatorAsNewRoot(std::make_shared<WatermarkAssignerLogicalOperator>(
                    Windowing::IngestionTimeWatermarkStrategyDescriptor::create(), getNextOperatorId()));
                break;
            case Windowing::TimeCharacteristic::Type::EventTime:
                queryPlan->appendOperatorAsNewRoot(std::make_shared<WatermarkAssignerLogicalOperator>(
                    Windowing::EventTimeWatermarkStrategyDescriptor::create(
                        FieldAccessLogicalFunction::create(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()),
                        timeBasedWindowType->getTimeCharacteristic()->getTimeUnit()),
                    getNextOperatorId()));
                break;
        }
    }
    else
    {
        throw NotImplemented("Only TimeBasedWindowType is supported for now");
    }


    auto inputSchema = queryPlan->getRootOperators()[0]->getOutputSchema();
    const auto windowDefinition = Windowing::LogicalWindowDescriptor::create(onKeys, windowAggs, windowType);
    const auto windowOperator = std::make_shared<WindowLogicalOperator>(windowDefinition, getNextOperatorId());

    queryPlan->appendOperatorAsNewRoot(windowOperator);
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addUnion(std::shared_ptr<QueryPlan> leftQueryPlan, std::shared_ptr<QueryPlan> rightQueryPlan)
{
    NES_TRACE("QueryPlanBuilder: unionWith the subQuery to current query plan");
    const std::shared_ptr<Operator> op = std::make_shared<UnionLogicalOperator>(getNextOperatorId());
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addJoin(
    std::shared_ptr<QueryPlan> leftQueryPlan,
    std::shared_ptr<QueryPlan> rightQueryPlan,
    std::shared_ptr<LogicalFunction> joinFunction,
    const std::shared_ptr<Windowing::WindowType>& windowType,
    Join::LogicalJoinDescriptor::JoinType joinType = Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT)
{
    NES_TRACE("QueryPlanBuilder: Iterate over all ExpressionNode to check join field.");
    std::unordered_set<std::shared_ptr<BinaryLogicalFunction>> visitedFunctions;
    /// We are iterating over all binary functions and check if each side's leaf is a constant value, as we are supposedly not supporting this
    /// I am not sure why this is the case, but I will keep it for now. IMHO, the whole QueryPlanBuilder should be refactored to be more readable and
    /// also to be more maintainable. TODO #506
    for (auto itr : BFSRange<LogicalFunction>(joinFunction))
    {
        if (NES::Util::instanceOf<BinaryLogicalFunction>(itr)
            and not NES::Util::instanceOf<BinaryLogicalFunction>(NES::Util::as<BinaryLogicalFunction>(itr)->getLeftChild()))
        {
            auto visitingOp = NES::Util::as<BinaryLogicalFunction>(itr);
            if (not visitedFunctions.contains(visitingOp))
            {
                visitedFunctions.insert(visitingOp);
                auto onLeftKey = visitingOp->getLeftChild();
                auto onRightKey = visitingOp->getRightChild();
                /// ensure that the child nodes are not binary
                if (!NES::Util::instanceOf<BinaryLogicalFunction>(onLeftKey) && !NES::Util::instanceOf<BinaryLogicalFunction>(onRightKey))
                {
                    if (NES::Util::instanceOf<ConstantValueLogicalFunction>(onLeftKey)
                        || NES::Util::instanceOf<ConstantValueLogicalFunction>(onRightKey))
                    {
                        throw InvalidQuerySyntax("One of the join keys does only consist of a constant function. Use WHERE instead.");
                    }
                    auto leftKeyFieldAccess = asFieldAccessLogicalFunction(onLeftKey, "leftSide");
                    auto rightQueryPlanKeyFieldAccess = asFieldAccessLogicalFunction(onRightKey, "rightSide");
                }
            }
        }
    }

    INVARIANT(rightQueryPlan, "invalid rightQueryPlan query plan, it is a nullptr");
    INVARIANT(!rightQueryPlan->getRootOperators().empty(), "RootOperators of rightQueryPlan are empty");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = leftQueryPlan->getRootOperators()[0]->getOutputSchema();
    auto rightQueryPlanJoinType = rootOperatorRhs->getOutputSchema();

    /// check if query contain watermark assigner, and add if missing (as default behaviour)
    leftQueryPlan = checkAndAddWatermarkAssignment(leftQueryPlan, windowType);
    rightQueryPlan = checkAndAddWatermarkAssignment(rightQueryPlan, windowType);

    ///TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    ///TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::LogicalJoinDescriptor::create(joinFunction, windowType, 1, 1, joinType);

    NES_TRACE("QueryPlanBuilder: add join operator to query plan");
    auto op = std::make_shared<JoinLogicalOperator>(joinDefinition, getNextOperatorId());
    NES_INFO("Created join {}", *op, Util::as<JoinLogicalOperator>(op)->getJoinDefinition()->getWindowType()->toString());
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addSink(std::string sinkName, std::shared_ptr<QueryPlan> queryPlan, WorkerId workerId)
{
    auto sinkOperator = std::make_shared<SinkLogicalOperator>(std::move(sinkName), getNextOperatorId());
    if (workerId != INVALID_WORKER_NODE_ID)
    {
        sinkOperator->addProperty(Optimizer::PINNED_WORKER_ID, workerId);
    }
    const std::shared_ptr<Operator> op = sinkOperator;
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::assignWatermark(
    std::shared_ptr<QueryPlan> queryPlan, std::shared_ptr<Windowing::WatermarkStrategyDescriptor> const& watermarkStrategyDescriptor)
{
    const std::shared_ptr<Operator> op
        = std::make_shared<WatermarkAssignerLogicalOperator>(watermarkStrategyDescriptor, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
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
                    FieldAccessLogicalFunction::create(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()),
                    timeBasedWindowType->getTimeCharacteristic()->getTimeUnit()));
        }
    }
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addBinaryOperatorAndUpdateSource(
    std::shared_ptr<Operator> operatorNode, std::shared_ptr<QueryPlan> leftQueryPlan, std::shared_ptr<QueryPlan> rightQueryPlan)
{
    leftQueryPlan->addRootOperator(rightQueryPlan->getRootOperators()[0]);
    leftQueryPlan->appendOperatorAsNewRoot(operatorNode);
    NES_TRACE("QueryPlanBuilder: addBinaryOperatorAndUpdateSource: update the source names");
    auto newSourceName = Util::updateSourceName(leftQueryPlan->getSourceConsumed(), rightQueryPlan->getSourceConsumed());
    leftQueryPlan->setSourceConsumed(newSourceName);
    return leftQueryPlan;
}

std::shared_ptr<FieldAccessLogicalFunction>
QueryPlanBuilder::asFieldAccessLogicalFunction(const std::shared_ptr<LogicalFunction>& function, std::string side)
{
    if (!NES::Util::instanceOf<FieldAccessLogicalFunction>(function))
    {
        throw UnsupportedQuery("Window key ({}) has to be an FieldAccessFunction but it was a  {}", side, *function);
    }
    return NES::Util::as<FieldAccessLogicalFunction>(function);
}
}
