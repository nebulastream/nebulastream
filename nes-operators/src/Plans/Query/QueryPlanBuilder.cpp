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
#include <utility>
#include <API/AttributeField.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Functions/NodeFunctionFieldRename.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES
{

QueryPlanPtr QueryPlanBuilder::createQueryPlan(std::string logicalSourceName)
{
    NES_DEBUG("QueryPlanBuilder: create query plan for input source  {}", logicalSourceName);
    Configurations::DescriptorConfig::Config SourceDescriptorConfig{};
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(logicalSourceName); /// using default id and originId
    auto queryPlanPtr = QueryPlan::create(sourceOperator);
    queryPlanPtr->setSourceConsumed(logicalSourceName);
    return queryPlanPtr;
}

QueryPlanPtr QueryPlanBuilder::addProjection(const std::vector<NodeFunctionPtr>& functions, QueryPlanPtr queryPlan)
{
    NES_DEBUG("QueryPlanBuilder: add projection operator to query plan");
    OperatorPtr op = LogicalOperatorFactory::createProjectionOperator(functions);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addRename(std::string const& newSourceName, QueryPlanPtr queryPlan)
{
    NES_DEBUG("QueryPlanBuilder: add rename operator to query plan");
    auto op = LogicalOperatorFactory::createRenameSourceOperator(newSourceName);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addFilter(NodeFunctionPtr const& filterFunction, QueryPlanPtr queryPlan)
{
    NES_DEBUG("QueryPlanBuilder: add filter operator to query plan");
    if (!filterFunction->getNodesByType<NodeFunctionFieldRename>().empty())
    {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Filter predicate cannot have a FieldRenameFunction");
    }
    OperatorPtr op = LogicalOperatorFactory::createFilterOperator(filterFunction);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addLimit(const uint64_t limit, QueryPlanPtr queryPlan)
{
    NES_DEBUG("QueryPlanBuilder: add limit operator to query plan");
    OperatorPtr op = LogicalOperatorFactory::createLimitOperator(limit);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addMap(NodeFunctionFieldAssignmentPtr const& mapFunction, QueryPlanPtr queryPlan)
{
    NES_DEBUG("QueryPlanBuilder: add map operator to query plan");
    if (!mapFunction->getNodesByType<NodeFunctionFieldRename>().empty())
    {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Map function cannot have a FieldRenameFunction");
    }
    OperatorPtr op = LogicalOperatorFactory::createMapOperator(mapFunction);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addUnion(QueryPlanPtr leftQueryPlan, QueryPlanPtr rightQueryPlan)
{
    NES_DEBUG("QueryPlanBuilder: unionWith the subQuery to current query plan");
    OperatorPtr op = LogicalOperatorFactory::createUnionOperator();
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addJoin(
    QueryPlanPtr leftQueryPlan,
    QueryPlanPtr rightQueryPlan,
    NodeFunctionPtr joinFunction,
    const Windowing::WindowTypePtr& windowType,
    Join::LogicalJoinDescriptor::JoinType joinType = Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT)
{
    NES_DEBUG("QueryPlanBuilder: joinWith the subQuery to current query");

    NES_DEBUG("QueryPlanBuilder: Iterate over all NodeFunction to check join field.");
    std::unordered_set<std::shared_ptr<NodeFunctionBinary>> visitedFunctions;
    auto bfsIterator = BreadthFirstNodeIterator(joinFunction);
    for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr)
    {
        if (NES::Util::instanceOf<NodeFunctionBinary>(*itr))
        {
            auto visitingOp = NES::Util::as<NodeFunctionBinary>(*itr);
            if (visitedFunctions.contains(visitingOp))
            {
                /// skip rest of the steps as the node found in already visited node list
                continue;
            }
            else
            {
                visitedFunctions.insert(visitingOp);
                auto onLeftKey = NES::Util::as<NodeFunctionBinary>(*itr)->getLeft();
                auto onRightKey = NES::Util::as<NodeFunctionBinary>(*itr)->getRight();
                NES_DEBUG("QueryPlanBuilder: Check if Functions are FieldFunctions.");
                auto leftKeyFieldAccess = checkFunction(onLeftKey, "leftSide");
                auto rightQueryPlanKeyFieldAccess = checkFunction(onRightKey, "rightSide");
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

    NES_DEBUG("QueryPlanBuilder: add join operator to query plan");
    auto op = LogicalOperatorFactory::createJoinOperator(joinDefinition);
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addBatchJoin(
    QueryPlanPtr leftQueryPlan, QueryPlanPtr rightQueryPlan, NodeFunctionPtr onProbeKey, NodeFunctionPtr onBuildKey)
{
    NES_DEBUG("Query: joinWith the subQuery to current query");
    auto probeKeyFieldAccess = checkFunction(onProbeKey, "onProbeKey");
    auto buildKeyFieldAccess = checkFunction(onBuildKey, "onBuildKey");

    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid rightQueryPlan query plan");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = leftQueryPlan->getRootOperators()[0]->getOutputSchema();
    auto rightQueryPlanJoinType = rootOperatorRhs->getOutputSchema();

    /// todo here again we wan't to extend to distributed joins:
    ///TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    ///TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::Experimental::LogicalBatchJoinDescriptor::create(buildKeyFieldAccess, probeKeyFieldAccess, 1, 1);

    auto op = LogicalOperatorFactory::createBatchJoinOperator(joinDefinition);
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addSink(QueryPlanPtr queryPlan, SinkDescriptorPtr sinkDescriptor, WorkerId workerId)
{
    OperatorPtr op = LogicalOperatorFactory::createSinkOperator(sinkDescriptor, workerId);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr
QueryPlanBuilder::assignWatermark(QueryPlanPtr queryPlan, Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor)
{
    OperatorPtr op = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::checkAndAddWatermarkAssignment(QueryPlanPtr queryPlan, const Windowing::WindowTypePtr windowType)
{
    NES_DEBUG("QueryPlanBuilder: checkAndAddWatermarkAssignment for a (sub)query plan");
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
                    Windowing::TimeMeasure(0),
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
    NES_DEBUG("QueryPlanBuilder: addBinaryOperatorAndUpdateSource: update the source names");
    auto newSourceName = Util::updateSourceName(leftQueryPlan->getSourceConsumed(), rightQueryPlan->getSourceConsumed());
    leftQueryPlan->setSourceConsumed(newSourceName);
    return leftQueryPlan;
}

std::shared_ptr<NodeFunctionFieldAccess> QueryPlanBuilder::checkFunction(NodeFunctionPtr function, std::string side)
{
    if (!NES::Util::instanceOf<NodeFunctionFieldAccess>(function))
    {
        NES_ERROR("QueryPlanBuilder: window key ({}) has to be an FieldAccessFunction but it was a  {}", side, function->toString());
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: window key has to be an FieldAccessFunction");
    }
    return NES::Util::as<NodeFunctionFieldAccess>(function);
}
}
