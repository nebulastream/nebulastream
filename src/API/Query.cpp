/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <API/Expressions/Expressions.hpp>
#include <API/Query.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/WatermarkStrategy.hpp>

#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnRecordTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <cstdarg>
#include <iostream>

#include <API/WindowedQuery.hpp>
#include <utility>

namespace NES {

JoinOperatorBuilder::Join Query::joinWith(const Query& subQueryRhs) { return JoinOperatorBuilder::Join(subQueryRhs, *this); }

namespace JoinOperatorBuilder {

JoinWhere Join::where(const ExpressionItem& onLeftKey) const { return JoinWhere(subQueryRhs, originalQuery, onLeftKey); }

Join::Join(const Query& subQueryRhs, Query& originalQuery) : subQueryRhs(subQueryRhs), originalQuery(originalQuery) {}

JoinCondition JoinWhere::equalsTo(const ExpressionItem& onRightKey) const {
    return JoinCondition(subQueryRhs, originalQuery, onLeftKey, onRightKey);
}

JoinWhere::JoinWhere(const Query& subQueryRhs, Query& originalQuery, const ExpressionItem& onLeftKey)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery), onLeftKey(onLeftKey) {}

Query& JoinCondition::window(const Windowing::WindowTypePtr& windowType) const {
    return originalQuery.joinWith(subQueryRhs, onLeftKey, onRightKey, windowType);//call original joinWith() function
}

JoinCondition::JoinCondition(const Query& subQueryRhs,
                             Query& originalQuery,
                             const ExpressionItem& onLeftKey,
                             const ExpressionItem& onRightKey)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery), onLeftKey(onLeftKey), onRightKey(onRightKey) {}

}// namespace JoinOperatorBuilder

Query::Query(QueryPlanPtr queryPlan) : queryPlan(std::move(queryPlan)) {}

Query::Query(const Query& query) = default;

Query Query::from(const std::string& sourceStreamName) {
    NES_DEBUG("Query: create query for input stream " << sourceStreamName);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create(sourceStreamName));
    auto queryPlan = QueryPlan::create(sourceOperator);
    return Query(queryPlan);
}

Query& Query::as(const std::string& newStreamName) {
    auto renameOperator = LogicalOperatorFactory::createRenameStreamOperator(newStreamName);
    queryPlan->appendOperatorAsNewRoot(renameOperator);
    return *this;
}

Query& Query::unionWith(Query* subQuery) {
    NES_DEBUG("Query: unionWith the subQuery to current query");
    OperatorNodePtr op = LogicalOperatorFactory::createUnionOperator();
    queryPlan->addRootOperator(subQuery->getQueryPlan()->getRootOperators()[0]);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::joinWith(const Query& subQueryRhs,
                       ExpressionItem onLeftKey,
                       ExpressionItem onRightKey,
                       const Windowing::WindowTypePtr& windowType) {
    NES_DEBUG("Query: joinWith the subQuery to current query");

    auto subQuery = const_cast<Query&>(subQueryRhs);

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
    auto triggerPolicy = OnWatermarkChangeTriggerPolicyDescription::create();
    //    auto triggerPolicy = OnTimeTriggerPolicyDescription::create(1000);

    //we use a lazy NL join because this is currently the only one that is implemented
    auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();

    // we use a complete window type as we currently do not have a distributed join
    auto distrType = Windowing::DistributionCharacteristic::createCompleteWindowType();

    auto rightQueryPlan = subQuery.getQueryPlan();
    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid right query plan");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = getQueryPlan()->getRootOperators()[0]->getOutputSchema();
    auto rightJoinType = rootOperatorRhs->getOutputSchema();

    // check if query contain watermark assigner, and add if missing (as default behaviour)
    if (queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (windowType->getTimeCharacteristic()->getType() == TimeCharacteristic::IngestionTime) {
            queryPlan->appendOperatorAsNewRoot(
                LogicalOperatorFactory::createWatermarkAssignerOperator(IngestionTimeWatermarkStrategyDescriptor::create()));
        } else if (windowType->getTimeCharacteristic()->getType() == TimeCharacteristic::EventTime) {
            queryPlan->appendOperatorAsNewRoot(
                LogicalOperatorFactory::createWatermarkAssignerOperator(EventTimeWatermarkStrategyDescriptor::create(
                    Attribute(windowType->getTimeCharacteristic()->getField()->getName()),
                    Milliseconds(0),
                    windowType->getTimeCharacteristic()->getTimeUnit())));
        }
    }

    if (rightQueryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (windowType->getTimeCharacteristic()->getType() == TimeCharacteristic::IngestionTime) {
            auto op = LogicalOperatorFactory::createWatermarkAssignerOperator(IngestionTimeWatermarkStrategyDescriptor::create());
            rightQueryPlan->appendOperatorAsNewRoot(op);
        } else if (windowType->getTimeCharacteristic()->getType() == TimeCharacteristic::EventTime) {
            auto op = LogicalOperatorFactory::createWatermarkAssignerOperator(EventTimeWatermarkStrategyDescriptor::create(
                Attribute(windowType->getTimeCharacteristic()->getField()->getName()),
                Milliseconds(0),
                windowType->getTimeCharacteristic()->getTimeUnit()));
            rightQueryPlan->appendOperatorAsNewRoot(op);
        }
    }

    //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    //TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::LogicalJoinDefinition::create(leftKeyFieldAccess,
                                                              rightKeyFieldAccess,
                                                              windowType,
                                                              distrType,
                                                              triggerPolicy,
                                                              triggerAction,
                                                              1,
                                                              1);

    auto op = LogicalOperatorFactory::createJoinOperator(joinDefinition);
    queryPlan->addRootOperator(rightQueryPlan->getRootOperators()[0]);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::filter(const ExpressionNodePtr& filterExpression) {
    NES_DEBUG("Query: add filter operator to query");
    if (!filterExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("Query: Filter predicate cannot have a FieldRenameExpression");
    }
    OperatorNodePtr op = LogicalOperatorFactory::createFilterOperator(filterExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::map(const FieldAssignmentExpressionNodePtr& mapExpression) {
    NES_DEBUG("Query: add map operator to query");
    if (!mapExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("Query: Map expression cannot have a FieldRenameExpression");
    }
    OperatorNodePtr op = LogicalOperatorFactory::createMapOperator(mapExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::iter(const uint64_t minIteration, const uint64_t maxIteration) {
    NES_DEBUG("Pattern: enter iteration function with (min, max)" << minIteration << "," << maxIteration);
    OperatorNodePtr op = LogicalOperatorFactory::createCEPIterationOperator(minIteration, maxIteration);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::sink(const SinkDescriptorPtr sinkDescriptor) {
    NES_DEBUG("Query: add sink operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::assignWatermark(const Windowing::WatermarkStrategyDescriptorPtr& watermarkStrategyDescriptor) {
    NES_DEBUG("Query: add assignWatermark operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

QueryPlanPtr Query::getQueryPlan() { return queryPlan; }

}// namespace NES
