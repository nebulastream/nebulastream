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

#include <API/AttributeField.hpp>
#include <API/Query.hpp>
#include <API/WindowedQuery.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <iostream>
#include <utility>

namespace NES {

QueryPlanPtr QueryPlanBuilder::createQueryPlan(std::string sourceName) {
    NES_DEBUG("QueryPlanBuilder: create query plan for input source " << sourceName);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(sourceName));
    auto queryPlanPtr = QueryPlan::create(sourceOperator);
    queryPlanPtr->setSourceConsumed(sourceName);
    return queryPlanPtr;
}

QueryPlanPtr QueryPlanBuilder::addProjection(std::vector<NES::ExpressionNodePtr> expressionPtr, NES::QueryPlanPtr queryPlanPtr) {
    NES_DEBUG("QueryPlanBuilder: add projection operator to query plan");
    OperatorNodePtr op = LogicalOperatorFactory::createProjectionOperator(expressionPtr);
    queryPlanPtr->appendOperatorAsNewRoot(op);
    return queryPlanPtr;
}

QueryPlanPtr QueryPlanBuilder::addRename(std::string const& newSourceName, NES::QueryPlanPtr queryPlanPtr) {
    NES_DEBUG("QueryPlanBuilder: add rename operator to query plan");
    auto op = LogicalOperatorFactory::createRenameSourceOperator(newSourceName);
    queryPlanPtr->appendOperatorAsNewRoot(op);
    return queryPlanPtr;
}

QueryPlanPtr QueryPlanBuilder::addFilter(NES::ExpressionNodePtr const& filterExpression, NES::QueryPlanPtr queryPlanPtr) {
    NES_DEBUG("QueryPlanBuilder: add filter operator to query plan");
    if (!filterExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Filter predicate cannot have a FieldRenameExpression");
    }
    OperatorNodePtr op = LogicalOperatorFactory::createFilterOperator(filterExpression);
    queryPlanPtr->appendOperatorAsNewRoot(op);
    return queryPlanPtr;
}

QueryPlanPtr QueryPlanBuilder::addMap(NES::FieldAssignmentExpressionNodePtr const& mapExpressionPtr, NES::QueryPlanPtr queryPlanPtr) {
    NES_DEBUG("QueryPlanBuilder: add map operator to query plan");
    if (!mapExpressionPtr->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Map expression cannot have a FieldRenameExpression");
    }
    OperatorNodePtr op = LogicalOperatorFactory::createMapOperator(mapExpressionPtr);
    queryPlanPtr->appendOperatorAsNewRoot(op);
    return queryPlanPtr;
}

QueryPlanPtr QueryPlanBuilder::addUnionOperator(NES::QueryPlanPtr queryPlanPtr, NES::QueryPlanPtr rightQueryPlanQueryPlanPtr) {
    NES_DEBUG("QueryPlanBuilder: unionWith the subQuery to current query plan");
    OperatorNodePtr op = LogicalOperatorFactory::createUnionOperator();
    queryPlanPtr = addBinaryOperatorAndUpdateSource(op, queryPlanPtr, rightQueryPlanQueryPlanPtr);
    return queryPlanPtr;
}

QueryPlanPtr QueryPlanBuilder::addJoinOperator(NES::QueryPlanPtr queryPlanPtr,
                                               NES::QueryPlanPtr rightQueryPlanQueryPlanPtr,
                                               ExpressionItem onLeftKey,
                                               ExpressionItem onRightQueryPlanKey,
                                               const Windowing::WindowTypePtr& windowType,
                                               Join::LogicalJoinDefinition::JoinType joinType) {
    NES_DEBUG("Query: joinWith the subQuery to current query");

    auto leftKeyFieldAccess = checkExpression(onLeftKey.getExpressionNode(), "leftSide");
    auto rightQueryPlanKeyFieldAccess = checkExpression(onRightQueryPlanKey.getExpressionNode(), "leftSide");

    //we use a on time trigger as default that triggers on each change of the watermark
    auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();
    //    auto triggerPolicy = OnTimeTriggerPolicyDescription::create(1000);

    //we use a lazy NL join because this is currently the only one that is implemented
    auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();

    // we use a complete window type as we currently do not have a distributed join
    auto distrType = Windowing::DistributionCharacteristic::createCompleteWindowType();

    NES_ASSERT(rightQueryPlanQueryPlanPtr && !rightQueryPlanQueryPlanPtr->getRootOperators().empty(), "invalid rightQueryPlan query plan");
    auto rootOperatorRhs = rightQueryPlanQueryPlanPtr->getRootOperators()[0];
    auto leftJoinType = queryPlanPtr->getRootOperators()[0]->getOutputSchema();
    auto rightQueryPlanJoinType = rootOperatorRhs->getOutputSchema();

    // check if query contain watermark assigner, and add if missing (as default behaviour)
    queryPlanPtr = checkAndAddWatermarkAssignment(queryPlanPtr, windowType);
    rightQueryPlanQueryPlanPtr = checkAndAddWatermarkAssignment(rightQueryPlanQueryPlanPtr, windowType);

    //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    //TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::LogicalJoinDefinition::create(leftKeyFieldAccess,
                                                        rightQueryPlanKeyFieldAccess,
                                                        windowType,
                                                        distrType,
                                                        triggerPolicy,
                                                        triggerAction,
                                                        1,
                                                        1,
                                                        joinType);

    NES_DEBUG("QueryPlanBuilder: add join operator to query plan");
    auto op = LogicalOperatorFactory::createJoinOperator(joinDefinition);
    queryPlanPtr = addBinaryOperatorAndUpdateSource(op, queryPlanPtr, rightQueryPlanQueryPlanPtr);
    return queryPlanPtr;
}

NES::QueryPlanPtr QueryPlanBuilder::addBatchJoinOperator(NES::QueryPlanPtr queryPlanPtr,
                                                         NES::QueryPlanPtr rightQueryPlanPtr,
                                                         ExpressionItem onProbeKey,
                                                         ExpressionItem onBuildKey) {
    NES_DEBUG("Query: joinWith the subQuery to current query");
    auto probeKeyFieldAccess = checkExpression(onProbeKey.getExpressionNode(), "onProbeKey");
    auto buildKeyFieldAccess = checkExpression(onBuildKey.getExpressionNode(), "onBuildKey");

    NES_ASSERT(rightQueryPlanPtr && !rightQueryPlanPtr->getRootOperators().empty(), "invalid rightQueryPlan query plan");
    auto rootOperatorRhs = rightQueryPlanPtr->getRootOperators()[0];
    auto leftJoinType = queryPlanPtr->getRootOperators()[0]->getOutputSchema();
    auto rightQueryPlanJoinType = rootOperatorRhs->getOutputSchema();

    // todo here again we wan't to extend to distributed joins:
    //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    //TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::Experimental::LogicalBatchJoinDefinition::create(buildKeyFieldAccess, probeKeyFieldAccess, 1, 1);

    auto op = LogicalOperatorFactory::createBatchJoinOperator(joinDefinition);
    queryPlanPtr = addBinaryOperatorAndUpdateSource(op, queryPlanPtr, rightQueryPlanPtr);
    return queryPlanPtr;
}

NES::QueryPlanPtr QueryPlanBuilder::addSink(NES::QueryPlanPtr queryPlanPtr, NES::SinkDescriptorPtr sinkDescriptorPtr) {
    OperatorNodePtr op = LogicalOperatorFactory::createSinkOperator(sinkDescriptorPtr);
    queryPlanPtr->appendOperatorAsNewRoot(op);
    return queryPlanPtr;
}

NES::QueryPlanPtr
QueryPlanBuilder::assignWatermark(NES::QueryPlanPtr queryPlanPtr,
                                  NES::Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor) {
    OperatorNodePtr op = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor);
    queryPlanPtr->appendOperatorAsNewRoot(op);
    return queryPlanPtr;
}

NES::QueryPlanPtr QueryPlanBuilder::checkAndAddWatermarkAssignment(NES::QueryPlanPtr queryPlanPtr,
                                                                   const NES::Windowing::WindowTypePtr windowTypePtr) {
    NES_DEBUG("QueryPlanBuilder: checkAndAddWatermarkAssignment for a (sub)query plan");
    auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowTypePtr);

    if (queryPlanPtr->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
            return assignWatermark(queryPlanPtr, Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
        } else if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::EventTime) {
            return assignWatermark(queryPlanPtr,
                                   Windowing::EventTimeWatermarkStrategyDescriptor::create(
                                       Attribute(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()),
                                       API::Milliseconds(0),
                                       timeBasedWindowType->getTimeCharacteristic()->getTimeUnit()));
        }
    }
    return queryPlanPtr;
}

NES::QueryPlanPtr QueryPlanBuilder::addBinaryOperatorAndUpdateSource(NES::OperatorNodePtr operatorNodePtr,
                                                                     NES::QueryPlanPtr queryPlanPtr,
                                                                     NES::QueryPlanPtr rightQueryPlanPtr) {
    queryPlanPtr->addRootOperator(rightQueryPlanPtr->getRootOperators()[0]);
    queryPlanPtr->appendOperatorAsNewRoot(operatorNodePtr);
    NES_DEBUG("QueryPlanBuilder: addBinaryOperatorAndUpdateSource: update the source names");
    auto newSourceName = Util::updateSourceName(queryPlanPtr->getSourceConsumed(), rightQueryPlanPtr->getSourceConsumed());
    queryPlanPtr->setSourceConsumed(newSourceName);
    return queryPlanPtr;
}

std::shared_ptr<FieldAccessExpressionNode> QueryPlanBuilder::checkExpression(NES::ExpressionNodePtr expressionPtr,
                                                                             std::string side) {
    if (!expressionPtr->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key"
                  << "(" << side << ")"
                  << "has to be an FieldAccessExpression but it was a " + expressionPtr->toString());
        NES_THROW_RUNTIME_ERROR("Query: window key has to be an FieldAccessExpression");
    }
    return expressionPtr->as<FieldAccessExpressionNode>();
}
}