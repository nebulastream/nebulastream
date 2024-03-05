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
#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/Expressions/FieldRenameExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDefinition.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Actions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Actions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/DistributionCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/TriggerPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <Operators/LogicalOperators/Windows/Types/TimeBasedWindowType.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <utility>

namespace NES {

QueryPlanPtr QueryPlanBuilder::createQueryPlan(std::string sourceName) {
    NES_DEBUG("QueryPlanBuilder: create query plan for input source  {}", sourceName);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(sourceName));
    auto queryPlanPtr = QueryPlan::create(sourceOperator);
    queryPlanPtr->setSourceConsumed(sourceName);
    return queryPlanPtr;
}

QueryPlanPtr QueryPlanBuilder::addProjection(std::vector<NES::ExpressionNodePtr> expressions, NES::QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add projection operator to query plan");
    OperatorNodePtr op = LogicalOperatorFactory::createProjectionOperator(expressions);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addRename(std::string const& newSourceName, NES::QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add rename operator to query plan");
    auto op = LogicalOperatorFactory::createRenameSourceOperator(newSourceName);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addFilter(NES::ExpressionNodePtr const& filterExpression, NES::QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add filter operator to query plan");
    if (!filterExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Filter predicate cannot have a FieldRenameExpression");
    }
    OperatorNodePtr op = LogicalOperatorFactory::createFilterOperator(filterExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addLimit(const uint64_t limit, NES::QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add limit operator to query plan");
    OperatorNodePtr op = LogicalOperatorFactory::createLimitOperator(limit);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

NES::QueryPlanPtr QueryPlanBuilder::addMapUDF(Catalogs::UDF::UDFDescriptorPtr const& descriptor, NES::QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add map java udf operator to query plan");
    auto op = LogicalOperatorFactory::createMapUDFLogicalOperator(descriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

NES::QueryPlanPtr QueryPlanBuilder::addFlatMapUDF(Catalogs::UDF::UDFDescriptorPtr const& descriptor,
                                                  NES::QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add flat map java udf operator to query plan");
    auto op = LogicalOperatorFactory::createFlatMapUDFLogicalOperator(descriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addMap(NES::FieldAssignmentExpressionNodePtr const& mapExpression, NES::QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add map operator to query plan");
    if (!mapExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Map expression cannot have a FieldRenameExpression");
    }
    OperatorNodePtr op = LogicalOperatorFactory::createMapOperator(mapExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addUnion(NES::QueryPlanPtr leftQueryPlan, NES::QueryPlanPtr rightQueryPlan) {
    NES_DEBUG("QueryPlanBuilder: unionWith the subQuery to current query plan");
    OperatorNodePtr op = LogicalOperatorFactory::createUnionOperator();
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addJoin(NES::QueryPlanPtr leftQueryPlan,
                                       NES::QueryPlanPtr rightQueryPlan,
                                       ExpressionNodePtr onLeftKey,
                                       ExpressionNodePtr onRightKey,
                                       const Windowing::WindowTypePtr& windowType,
                                       Join::LogicalJoinDefinition::JoinType joinType) {
    NES_DEBUG("Query: joinWith the subQuery to current query");

    auto leftKeyFieldAccess = checkExpression(onLeftKey, "leftSide");
    auto rightQueryPlanKeyFieldAccess = checkExpression(onRightKey, "leftSide");

    //we use a on time trigger as default that triggers on each change of the watermark
    auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();
    //    auto triggerPolicy = OnTimeTriggerPolicyDescription::create(1000);

    //we use a lazy NL join because this is currently the only one that is implemented
    auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();

    // we use a complete window type as we currently do not have a distributed join
    auto distrType = Windowing::DistributionCharacteristic::createCompleteWindowType();

    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid rightQueryPlan query plan");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = leftQueryPlan->getRootOperators()[0]->getOutputSchema();
    auto rightQueryPlanJoinType = rootOperatorRhs->getOutputSchema();

    // check if query contain watermark assigner, and add if missing (as default behaviour)
    leftQueryPlan = checkAndAddWatermarkAssignment(leftQueryPlan, windowType);
    rightQueryPlan = checkAndAddWatermarkAssignment(rightQueryPlan, windowType, true);

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
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

NES::QueryPlanPtr QueryPlanBuilder::addBatchJoin(NES::QueryPlanPtr leftQueryPlan,
                                                 NES::QueryPlanPtr rightQueryPlan,
                                                 ExpressionNodePtr onProbeKey,
                                                 ExpressionNodePtr onBuildKey) {
    NES_DEBUG("Query: joinWith the subQuery to current query");
    auto probeKeyFieldAccess = checkExpression(onProbeKey, "onProbeKey");
    auto buildKeyFieldAccess = checkExpression(onBuildKey, "onBuildKey");

    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid rightQueryPlan query plan");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = leftQueryPlan->getRootOperators()[0]->getOutputSchema();
    auto rightQueryPlanJoinType = rootOperatorRhs->getOutputSchema();

    // todo here again we wan't to extend to distributed joins:
    //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    //TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::Experimental::LogicalBatchJoinDefinition::create(buildKeyFieldAccess, probeKeyFieldAccess, 1, 1);

    auto op = LogicalOperatorFactory::createBatchJoinOperator(joinDefinition);
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

NES::QueryPlanPtr QueryPlanBuilder::addSink(NES::QueryPlanPtr queryPlan, NES::SinkDescriptorPtr sinkDescriptor) {
    OperatorNodePtr op = LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

NES::QueryPlanPtr
QueryPlanBuilder::assignWatermark(NES::QueryPlanPtr queryPlan,
                                  NES::Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor) {
    OperatorNodePtr op = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

NES::QueryPlanPtr QueryPlanBuilder::checkAndAddWatermarkAssignment(NES::QueryPlanPtr queryPlan,
                                                                   const NES::Windowing::WindowTypePtr windowType,
                                                                   bool other) {
    NES_DEBUG("QueryPlanBuilder: checkAndAddWatermarkAssignment for a (sub)query plan");
    auto timeBasedWindowType = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(windowType);
    auto timeChar =
        other && timeBasedWindowType->hasOther() ? timeBasedWindowType->getOther() : timeBasedWindowType->getTimeCharacteristic();

    if (queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (timeChar->getType() == Windowing::TimeCharacteristic::Type::IngestionTime) {
            return assignWatermark(queryPlan, Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
        } else if (timeChar->getType() == Windowing::TimeCharacteristic::Type::EventTime) {
            return assignWatermark(queryPlan,
                                   Windowing::EventTimeWatermarkStrategyDescriptor::create(
                                       FieldAccessExpressionNode::create(timeChar->getField()->getName()),
                                       Windowing::TimeMeasure(0),
                                       timeChar->getTimeUnit()));
        }
    }
    return queryPlan;
}

NES::QueryPlanPtr QueryPlanBuilder::addBinaryOperatorAndUpdateSource(NES::OperatorNodePtr operatorNode,
                                                                     NES::QueryPlanPtr leftQueryPlan,
                                                                     NES::QueryPlanPtr rightQueryPlan) {
    leftQueryPlan->addRootOperator(rightQueryPlan->getRootOperators()[0]);
    leftQueryPlan->appendOperatorAsNewRoot(operatorNode);
    NES_DEBUG("QueryPlanBuilder: addBinaryOperatorAndUpdateSource: update the source names");
    auto newSourceName = NES::Util::updateSourceName(leftQueryPlan->getSourceConsumed(), rightQueryPlan->getSourceConsumed());
    leftQueryPlan->setSourceConsumed(newSourceName);
    return leftQueryPlan;
}

std::shared_ptr<FieldAccessExpressionNode> QueryPlanBuilder::checkExpression(NES::ExpressionNodePtr expression,
                                                                             std::string side) {
    if (!expression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key ({}) has to be an FieldAccessExpression but it was a  {}", side, expression->toString());
        NES_THROW_RUNTIME_ERROR("Query: window key has to be an FieldAccessExpression");
    }
    return expression->as<FieldAccessExpressionNode>();
}
}// namespace NES