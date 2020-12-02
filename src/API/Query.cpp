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

#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <iostream>

namespace NES {

Query::Query(QueryPlanPtr queryPlan) : queryPlan(queryPlan) {}

Query::Query(const Query& query) : queryPlan(query.queryPlan) {}

Query Query::from(const std::string sourceStreamName) {
    NES_DEBUG("Query: create query for input stream " << sourceStreamName);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(
        LogicalStreamSourceDescriptor::create(sourceStreamName, UtilityFunctions::getNextOperatorId()));
    auto queryPlan = QueryPlan::create(sourceOperator);
    return Query(queryPlan);
}

Query& Query::merge(Query* subQuery) {
    NES_DEBUG("Query: merge the subQuery to current query");
    OperatorNodePtr op = LogicalOperatorFactory::createMergeOperator();
    queryPlan->addRootOperator(subQuery->getQueryPlan()->getRootOperators()[0]);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::join(Query* subQuery, ExpressionItem onKey, const Windowing::WindowTypePtr windowType) {
    NES_DEBUG("Query: join the subQuery to current query");

    auto keyExpression = onKey.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();

    //we use a on time trigger as default that triggers every 1 second
    auto triggerPolicy = OnTimeTriggerPolicyDescription::create(defaultTriggerTimeInMs);

    //we use a lazy NL join because this is currently the only one that is implemented
    auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();

    // we use a complete window type as we currently do not have a distributed join
    auto distrType = Windowing::DistributionCharacteristic::createCompleteWindowType();
    //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    auto joinDefinition =
        Join::LogicalJoinDefinition::create(fieldAccess, windowType, distrType, triggerPolicy, triggerAction, 1, 1);

    // check if query contain watermark assigner, and add if missing (as default behaviour)
    if (queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (windowType->getTimeCharacteristic()->getType() == TimeCharacteristic::IngestionTime) {
            queryPlan->appendOperatorAsNewRoot(
                LogicalOperatorFactory::createWatermarkAssignerOperator(IngestionTimeWatermarkStrategyDescriptor::create()));
        } else if (windowType->getTimeCharacteristic()->getType() == TimeCharacteristic::EventTime) {
            queryPlan->appendOperatorAsNewRoot(
                LogicalOperatorFactory::createWatermarkAssignerOperator(EventTimeWatermarkStrategyDescriptor::create(
                    Attribute(windowType->getTimeCharacteristic()->getField()->name), Milliseconds(0))));
        }
    }

    OperatorNodePtr op = LogicalOperatorFactory::createJoinOperator(joinDefinition);
    queryPlan->addRootOperator(subQuery->getQueryPlan()->getRootOperators()[0]);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::window(const Windowing::WindowTypePtr windowType, const Windowing::WindowAggregationPtr aggregation) {
    NES_DEBUG("Query: add window operator");
    auto triggerPolicy = OnTimeTriggerPolicyDescription::create(defaultTriggerTimeInMs);
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    //numberOfInputEdges = 1, this will in a later rule be replaced with the number of children of the window
    auto windowDefinition = LogicalWindowDefinition::create(
        aggregation, windowType, DistributionCharacteristic::createCompleteWindowType(), 1, triggerPolicy, triggerAction);
    auto windowOperator = LogicalOperatorFactory::createWindowOperator(windowDefinition);

    // check if query contain watermark assigner, and add if missing (as default behaviour)
    if (queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (windowType->getTimeCharacteristic()->getType() == TimeCharacteristic::IngestionTime) {
            queryPlan->appendOperatorAsNewRoot(
                LogicalOperatorFactory::createWatermarkAssignerOperator(IngestionTimeWatermarkStrategyDescriptor::create()));
        } else if (windowType->getTimeCharacteristic()->getType() == TimeCharacteristic::EventTime) {
            queryPlan->appendOperatorAsNewRoot(
                LogicalOperatorFactory::createWatermarkAssignerOperator(EventTimeWatermarkStrategyDescriptor::create(
                    Attribute(windowType->getTimeCharacteristic()->getField()->name), Milliseconds(0))));
        }
    }

    queryPlan->appendOperatorAsNewRoot(windowOperator);
    return *this;
}

Query& Query::windowByKey(ExpressionItem onKey, const Windowing::WindowTypePtr windowType,
                          const Windowing::WindowAggregationPtr aggregation) {
    NES_DEBUG("Query: add keyed window operator");
    auto keyExpression = onKey.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    auto triggerPolicy = OnTimeTriggerPolicyDescription::create(defaultTriggerTimeInMs);
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    //numberOfInputEdges = 1, this will in a later rule be replaced with the number of children of the window
    auto windowDefinition = Windowing::LogicalWindowDefinition::create(
        fieldAccess, aggregation, windowType, Windowing::DistributionCharacteristic::createCompleteWindowType(), 1, triggerPolicy,
        triggerAction);
    auto windowOperator = LogicalOperatorFactory::createWindowOperator(windowDefinition);

    // check if query contain watermark assigner, and add if missing (as default behaviour)
    if (queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (windowType->getTimeCharacteristic()->getType() == TimeCharacteristic::IngestionTime) {
            queryPlan->appendOperatorAsNewRoot(
                LogicalOperatorFactory::createWatermarkAssignerOperator(IngestionTimeWatermarkStrategyDescriptor::create()));
        } else if (windowType->getTimeCharacteristic()->getType() == TimeCharacteristic::EventTime) {
            queryPlan->appendOperatorAsNewRoot(
                LogicalOperatorFactory::createWatermarkAssignerOperator(EventTimeWatermarkStrategyDescriptor::create(
                    Attribute(windowType->getTimeCharacteristic()->getField()->name), Milliseconds(0))));
        }
    }

    queryPlan->appendOperatorAsNewRoot(windowOperator);
    return *this;
}

Query& Query::filter(const ExpressionNodePtr filterExpression) {
    NES_DEBUG("Query: add filter operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createFilterOperator(filterExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::map(const FieldAssignmentExpressionNodePtr mapExpression) {
    NES_DEBUG("Query: add map operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createMapOperator(mapExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::sink(const SinkDescriptorPtr sinkDescriptor) {
    NES_DEBUG("Query: add sink operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

QueryPlanPtr Query::getQueryPlan() { return queryPlan; }
Query& Query::assignWatermark(const Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor) {
    NES_DEBUG("Query: add assignWatermark operator to query");

    OperatorNodePtr op = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

}// namespace NES
