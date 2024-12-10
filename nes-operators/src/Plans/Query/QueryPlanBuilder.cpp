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
#include <utility>
#include <vector>

#include <API/AttributeField.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Functions/NodeFunctionFieldRename.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinOperator.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/RenameSourceOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Types/WindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <ErrorHandling.hpp>
#include <Operators/LogicalOperators/LogicalSortBufferOperator.hpp>

namespace NES
{
QueryPlanPtr QueryPlanBuilder::createQueryPlan(std::string logicalSourceName)
{
    NES_TRACE("QueryPlanBuilder: create query plan for input source  {}", logicalSourceName);
    Configurations::DescriptorConfig::Config SourceDescriptorConfig{};
    auto sourceOperator = std::make_shared<SourceNameLogicalOperator>(logicalSourceName, getNextOperatorId());
    auto queryPlanPtr = QueryPlan::create(sourceOperator);
    queryPlanPtr->setSourceConsumed(logicalSourceName);
    return queryPlanPtr;
}

QueryPlanPtr QueryPlanBuilder::addProjection(const std::vector<NodeFunctionPtr>& functions, QueryPlanPtr queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add projection operator to query plan");
    OperatorPtr const op = std::make_shared<LogicalProjectionOperator>(functions, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addRename(std::string const& newSourceName, QueryPlanPtr queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add rename operator to query plan");
    auto op = std::make_shared<RenameSourceOperator>(newSourceName, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addSelection(NodeFunctionPtr const& selectionFunction, QueryPlanPtr queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add selection operator to query plan");
    if (!selectionFunction->getNodesByType<NodeFunctionFieldRename>().empty())
    {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Selection predicate cannot have a FieldRenameFunction");
    }
    OperatorPtr const op = std::make_shared<LogicalSelectionOperator>(selectionFunction, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addSortBuffer(std::string const& sortFieldIdentifier, std::string const& sortOrder, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add sort buffer operator to query plan");
    const auto op = std::make_shared<NES::LogicalSortBufferOperator>(sortFieldIdentifier, sortOrder, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addLimit(const uint64_t limit, QueryPlanPtr queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add limit operator to query plan");
    OperatorPtr const op = std::make_shared<LogicalLimitOperator>(limit, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addMap(NodeFunctionFieldAssignmentPtr const& mapFunction, QueryPlanPtr queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add map operator to query plan");
    if (!mapFunction->getNodesByType<NodeFunctionFieldRename>().empty())
    {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Map function cannot have a FieldRenameFunction");
    }
    OperatorPtr const op = std::make_shared<LogicalMapOperator>(mapFunction, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addWindowAggregation(
    QueryPlanPtr queryPlan,
    const std::shared_ptr<Windowing::WindowType>& windowType,
    const std::vector<Windowing::WindowAggregationDescriptorPtr>& windowAggs,
    const std::vector<std::shared_ptr<NodeFunctionFieldAccess>>& onKeys)
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
                        NodeFunctionFieldAccess::create(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()),
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
    const auto windowOperator = std::make_shared<LogicalWindowOperator>(windowDefinition, getNextOperatorId());

    queryPlan->appendOperatorAsNewRoot(windowOperator);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addUnion(QueryPlanPtr leftQueryPlan, QueryPlanPtr rightQueryPlan)
{
    NES_TRACE("QueryPlanBuilder: unionWith the subQuery to current query plan");
    OperatorPtr const op = std::make_shared<LogicalUnionOperator>(getNextOperatorId());
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addJoin(
    QueryPlanPtr leftQueryPlan,
    QueryPlanPtr rightQueryPlan,
    NodeFunctionPtr joinFunction,
    const std::shared_ptr<Windowing::WindowType>& windowType,
    Join::LogicalJoinDescriptor::JoinType joinType = Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT)
{
    NES_TRACE("QueryPlanBuilder: Iterate over all ExpressionNode to check join field.");
    std::unordered_set<std::shared_ptr<NodeFunctionBinary>> visitedFunctions;
    auto bfsIterator = BreadthFirstNodeIterator(joinFunction);
    /// We are iterating over all binary functions and check if each side's leaf is a constant value, as we are supposedly not supporting this
    /// I am not sure why this is the case, but I will keep it for now. IMHO, the whole QueryPlanBuilder should be refactored to be more readable and
    /// also to be more maintainable. TODO #506
    for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr)
    {
        if (NES::Util::instanceOf<NodeFunctionBinary>(*itr)
            and not NES::Util::instanceOf<NodeFunctionBinary>(NES::Util::as<NodeFunctionBinary>(*itr)->getLeft()))
        {
            auto visitingOp = NES::Util::as<NodeFunctionBinary>(*itr);
            if (not visitedFunctions.contains(visitingOp))
            {
                visitedFunctions.insert(visitingOp);
                auto onLeftKey = visitingOp->getLeft();
                auto onRightKey = visitingOp->getRight();
                /// ensure that the child nodes are not binary
                if (!NES::Util::instanceOf<NodeFunctionBinary>(onLeftKey) && !NES::Util::instanceOf<NodeFunctionBinary>(onRightKey))
                {
                    if (NES::Util::instanceOf<NodeFunctionConstantValue>(onLeftKey)
                        || NES::Util::instanceOf<NodeFunctionConstantValue>(onRightKey))
                    {
                        throw InvalidQuerySyntax("One of the join keys does only consist of a constant function. Use WHERE instead.");
                    }
                    auto leftKeyFieldAccess = asNodeFunctionFieldAccess(onLeftKey, "leftSide");
                    auto rightQueryPlanKeyFieldAccess = asNodeFunctionFieldAccess(onRightKey, "rightSide");
                }
            }
        }
    }

    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid rightQueryPlan query plan");
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
    auto op = std::make_shared<LogicalJoinOperator>(joinDefinition, getNextOperatorId());
    NES_INFO("Created join {}", *op, Util::as<LogicalJoinOperator>(op)->getJoinDefinition()->getWindowType()->toString());
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addBatchJoin(
    QueryPlanPtr leftQueryPlan, QueryPlanPtr rightQueryPlan, NodeFunctionPtr onProbeKey, NodeFunctionPtr onBuildKey)
{
    NES_TRACE("Query: joinWith the subQuery to current query");
    auto probeKeyFieldAccess = asNodeFunctionFieldAccess(onProbeKey, "onProbeKey");
    auto buildKeyFieldAccess = asNodeFunctionFieldAccess(onBuildKey, "onBuildKey");

    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid rightQueryPlan query plan");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = leftQueryPlan->getRootOperators()[0]->getOutputSchema();
    auto rightQueryPlanJoinType = rootOperatorRhs->getOutputSchema();

    auto joinDefinition = Join::Experimental::LogicalBatchJoinDescriptor::create(buildKeyFieldAccess, probeKeyFieldAccess, 1, 1);

    auto op = std::make_shared<Experimental::LogicalBatchJoinOperator>(joinDefinition, getNextOperatorId());
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addSink(std::string sinkName, QueryPlanPtr queryPlan, WorkerId workerId)
{
    auto sinkOperator = std::make_shared<SinkLogicalOperator>(std::move(sinkName), getNextOperatorId());
    if (workerId != INVALID_WORKER_NODE_ID)
    {
        sinkOperator->addProperty(Optimizer::PINNED_WORKER_ID, workerId);
    }
    OperatorPtr const op = sinkOperator;
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr
QueryPlanBuilder::assignWatermark(QueryPlanPtr queryPlan, Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor)
{
    OperatorPtr const op = std::make_shared<WatermarkAssignerLogicalOperator>(watermarkStrategyDescriptor, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::checkAndAddWatermarkAssignment(QueryPlanPtr queryPlan, const Windowing::WindowTypePtr windowType)
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
                    NodeFunctionFieldAccess::create(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()),
                    timeBasedWindowType->getTimeCharacteristic()->getTimeUnit()));
        }
    }
    return queryPlan;
}

QueryPlanPtr
QueryPlanBuilder::addBinaryOperatorAndUpdateSource(OperatorPtr operatorNode, QueryPlanPtr leftQueryPlan, QueryPlanPtr rightQueryPlan)
{
    leftQueryPlan->addRootOperator(rightQueryPlan->getRootOperators()[0]);
    leftQueryPlan->appendOperatorAsNewRoot(operatorNode);
    NES_TRACE("QueryPlanBuilder: addBinaryOperatorAndUpdateSource: update the source names");
    auto newSourceName = Util::updateSourceName(leftQueryPlan->getSourceConsumed(), rightQueryPlan->getSourceConsumed());
    leftQueryPlan->setSourceConsumed(newSourceName);
    return leftQueryPlan;
}

std::shared_ptr<NodeFunctionFieldAccess> QueryPlanBuilder::asNodeFunctionFieldAccess(const NodeFunctionPtr& function, std::string side)
{
    if (!NES::Util::instanceOf<NodeFunctionFieldAccess>(function))
    {
        NES_ERROR("QueryPlanBuilder: window key ({}) has to be an FieldAccessFunction but it was a  {}", side, *function);
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: window key has to be an FieldAccessFunction");
    }
    return NES::Util::as<NodeFunctionFieldAccess>(function);
}
}
