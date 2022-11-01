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
#include <Parsers/QueryPlanBuilder.h>
#include <API/AttributeField.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Query.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <iostream>
#include <numeric>

#include <API/WindowedQuery.hpp>
#include <API/Windowing.hpp>
#include <utility>

using namespace NES;
using namespace NES::Join;
using namespace NES::Windowing;

QueryPlanPtr QueryPlanBuilder::createQueryPlan(std::string sourceName) {
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(sourceName));
    auto queryPlan = QueryPlan::create(sourceOperator);
    queryPlan->setSourceConsumed(sourceName);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addUnionOperatorNode(NES::QueryPlanPtr left, NES::QueryPlanPtr right, NES::QueryPlanPtr currentPlan) {
    OperatorNodePtr op = LogicalOperatorFactory::createUnionOperator();
    currentPlan->addRootOperator(right->getRootOperators()[0]);
    currentPlan->addRootOperator(left->getRootOperators()[0]);
    currentPlan->appendOperatorAsNewRoot(op);
    //update source name
    auto newSourceName =
        Util::updateSourceName(currentPlan->getSourceConsumed(), left->getSourceConsumed());
    newSourceName = Util::updateSourceName(newSourceName, right->getSourceConsumed());
    currentPlan->setSourceConsumed(newSourceName);
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
    if (left->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
            left->appendOperatorAsNewRoot(LogicalOperatorFactory::createWatermarkAssignerOperator(
                Windowing::IngestionTimeWatermarkStrategyDescriptor::create()));
        } else if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::EventTime) {
            left->appendOperatorAsNewRoot(
                LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::EventTimeWatermarkStrategyDescriptor::create(
                    Attribute(windowType->getTimeCharacteristic()->getField()->getName()),
                    API::Milliseconds(0),
                    windowType->getTimeCharacteristic()->getTimeUnit())));
        }
    }

    if (right->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
            auto op = LogicalOperatorFactory::createWatermarkAssignerOperator(
                Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
            right->appendOperatorAsNewRoot(op);
        } else if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::EventTime) {
            auto op =
                LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::EventTimeWatermarkStrategyDescriptor::create(
                    Attribute(windowType->getTimeCharacteristic()->getField()->getName()),
                    API::Milliseconds(0),
                    windowType->getTimeCharacteristic()->getTimeUnit()));
            right->appendOperatorAsNewRoot(op);
        }
    }

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

    auto op = LogicalOperatorFactory::createJoinOperator(joinDefinition);


    currentPlan->addRootOperator(right->getRootOperators()[0]);
    currentPlan->addRootOperator(left->getRootOperators()[0]);
    currentPlan->appendOperatorAsNewRoot(op);
    //update source name
    auto sourceNameLeft = left->getSourceConsumed();
    auto sourceNameRight = right->getSourceConsumed();
    auto newSourceName = Util::updateSourceName(sourceNameLeft, sourceNameRight);
    newSourceName = Util::updateSourceName(currentPlan->getSourceConsumed(),newSourceName);
    currentPlan->setSourceConsumed(newSourceName);
    return currentPlan;
}
