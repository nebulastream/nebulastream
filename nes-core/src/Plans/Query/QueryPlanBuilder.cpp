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
#include "Plans/Query/QueryPlanBuilder.h"
#include "API/AttributeField.hpp"
#include "API/Query.hpp"
#include "API/WindowedQuery.hpp"
#include "Nodes/Expressions/FieldAssignmentExpressionNode.hpp"
#include "Nodes/Expressions/FieldRenameExpressionNode.hpp"
#include "Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp"
#include "Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp"
#include "Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp"
#include "Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp"
#include "Windowing/DistributionCharacteristic.hpp"
#include "Windowing/LogicalWindowDefinition.hpp"
#include "Windowing/TimeCharacteristic.hpp"
#include "Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp"
#include "Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp"
#include "Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp"
#include "Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp"
#include "Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp"
#include "Windowing/WindowTypes/TimeBasedWindowType.hpp"
#include "Windowing/WindowTypes/WindowType.hpp"
#include <iostream>
#include <utility>

using namespace NES;
using namespace NES::Join;
using namespace NES::Windowing;

QueryPlanPtr QueryPlanBuilder::createQueryPlan(std::string sourceName) {
    NES_DEBUG("Query: create query plan for input source " << sourceName);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(sourceName));
    auto queryPlan = QueryPlan::create(sourceOperator);
    queryPlan->setSourceConsumed(sourceName);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addProjectNode(std::vector<NES::ExpressionNodePtr> expressions, NES::QueryPlanPtr qplan) {
    NES_DEBUG("QueryPlanBuilder: add projection operator to query plan");
    OperatorNodePtr op = LogicalOperatorFactory::createProjectionOperator(expressions);
    qplan->appendOperatorAsNewRoot(op);
    return qplan;
}

QueryPlanPtr QueryPlanBuilder::addFilterNode(NES::ExpressionNodePtr const& filterExpression, NES::QueryPlanPtr qplan) {
    NES_DEBUG("QueryPlanBuilder: add filter operator to query plan");
    if (!filterExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Filter predicate cannot have a FieldRenameExpression");
    }
    OperatorNodePtr op = LogicalOperatorFactory::createFilterOperator(filterExpression);
    qplan->appendOperatorAsNewRoot(op);

    return qplan;
}

QueryPlanPtr QueryPlanBuilder::addMapNode(NES::FieldAssignmentExpressionNodePtr const& mapExpression, NES::QueryPlanPtr qplan) {
    NES_DEBUG("QueryPlanBuilder: add map operator to query plan");
    if (!mapExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Map expression cannot have a FieldRenameExpression");
    }
    OperatorNodePtr op = LogicalOperatorFactory::createMapOperator(mapExpression);
    qplan->appendOperatorAsNewRoot(op);

    return qplan;
}

QueryPlanPtr QueryPlanBuilder::addUnionOperatorNode(NES::QueryPlanPtr left, NES::QueryPlanPtr right, NES::QueryPlanPtr currentPlan) {
    NES_DEBUG("QueryPlanBuilder: unionWith the subQuery to current query plan");
    OperatorNodePtr op = LogicalOperatorFactory::createUnionOperator();
    currentPlan = addBinaryOperatorAndUpdateSource(op, left, right, currentPlan);

    return currentPlan;
}

QueryPlanPtr QueryPlanBuilder::addJoinOperatorNode(NES::QueryPlanPtr left, NES::QueryPlanPtr right, NES::QueryPlanPtr currentPlan,
     ExpressionItem onLeftKey,
     ExpressionItem onRightKey,
     const Windowing::WindowTypePtr& windowType,
     LogicalJoinDefinition::JoinType joinType) {
    NES_DEBUG("Query: joinWith the subQuery to current query");

    auto leftKeyExpression = onLeftKey.getExpressionNode();
    if (!leftKeyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + leftKeyExpression->toString());
        NES_THROW_RUNTIME_ERROR("Query: window key has to be an FieldAccessExpression");
    }
    auto rightKeyExpression = onRightKey.getExpressionNode();
    if (!rightKeyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + rightKeyExpression->toString());
        NES_THROW_RUNTIME_ERROR("Query: window key has to be an FieldAccessExpression");
    }
    auto leftKeyFieldAccess = leftKeyExpression->as<FieldAccessExpressionNode>();
    auto rightKeyFieldAccess = rightKeyExpression->as<FieldAccessExpressionNode>();

    //we use a on time trigger as default that triggers on each change of the watermark
    auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();
    //    auto triggerPolicy = OnTimeTriggerPolicyDescription::create(1000);

    //we use a lazy NL join because this is currently the only one that is implemented
    auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();

    // we use a complete window type as we currently do not have a distributed join
    auto distrType = Windowing::DistributionCharacteristic::createCompleteWindowType();

    NES_ASSERT(right && !right->getRootOperators().empty(), "invalid right query plan");
    auto rootOperatorRhs = right->getRootOperators()[0];
    auto leftJoinType = left->getRootOperators()[0]->getOutputSchema();
    auto rightJoinType = rootOperatorRhs->getOutputSchema();

    // check if query contain watermark assigner, and add if missing (as default behaviour)
    left = checkAndAddWatermarkAssignment(left, windowType);
    right = checkAndAddWatermarkAssignment(right, windowType);

    //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    //TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = LogicalJoinDefinition::create(leftKeyFieldAccess,
                                                              rightKeyFieldAccess,
                                                              windowType,
                                                              distrType,
                                                              triggerPolicy,
                                                              triggerAction,
                                                              1,
                                                              1,
                                                              joinType);

    NES_DEBUG("QueryPlanBuilder: add join operator to query plan");
    auto op = LogicalOperatorFactory::createJoinOperator(joinDefinition);
    currentPlan = addBinaryOperatorAndUpdateSource(op, left, right, currentPlan);

    return currentPlan;
}

NES::QueryPlanPtr QueryPlanBuilder::checkAndAddWatermarkAssignment(NES::QueryPlanPtr qplan,
                                                                   const NES::Windowing::WindowTypePtr windowType) {
    NES_DEBUG("QueryPlanBuilder: checkAndAddWatermarkAssignment for a (sub)query plan");
    auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowType);

    if (qplan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
            qplan->appendOperatorAsNewRoot(LogicalOperatorFactory::createWatermarkAssignerOperator(
                Windowing::IngestionTimeWatermarkStrategyDescriptor::create()));
        } else if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::EventTime) {
            qplan->appendOperatorAsNewRoot(
                LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::EventTimeWatermarkStrategyDescriptor::create(
                    Attribute(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()),
                    API::Milliseconds(0),
                    timeBasedWindowType->getTimeCharacteristic()->getTimeUnit())));
        }
    }

    return qplan;
}

NES::QueryPlanPtr QueryPlanBuilder::addBinaryOperatorAndUpdateSource(NES::OperatorNodePtr op,
                                                                     NES::QueryPlanPtr left,
                                                                     NES::QueryPlanPtr right,
                                                                     NES::QueryPlanPtr currentPlan) {
    if(currentPlan){
        NES_DEBUG("QueryPlanBuilder: addBinaryOperatorAndUpdateSource: if current query plan is not empty");
        currentPlan->addRootOperator(left->getRootOperators()[0]);
        currentPlan->addRootOperator(right->getRootOperators()[0]);
        currentPlan->appendOperatorAsNewRoot(op);
    }else {
        NES_DEBUG("QueryPlanBuilder: addBinaryOperatorAndUpdateSource: if current query plan is empty");
        currentPlan = left;
        currentPlan->addRootOperator(right->getRootOperators()[0]);
        currentPlan->appendOperatorAsNewRoot(op);
    }

    NES_DEBUG("QueryPlanBuilder: addBinaryOperatorAndUpdateSource: update the source names");
    auto sourceNameLeft = left->getSourceConsumed();
    auto sourceNameRight = right->getSourceConsumed();
    auto newSourceName = Util::updateSourceName(sourceNameLeft, sourceNameRight);
    newSourceName = Util::updateSourceName(newSourceName, right->getSourceConsumed());
    currentPlan->setSourceConsumed(newSourceName);

    return currentPlan;
}
