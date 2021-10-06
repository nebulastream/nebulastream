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
#include <API/AttributeField.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Query.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
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

#include <API/WindowedQuery.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnRecordTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <cstdarg>
#include <iostream>
#include <utility>

namespace NES {

WindowOperatorBuilder::WindowedQuery Query::window(const Windowing::WindowTypePtr& windowType) {
    return WindowOperatorBuilder::WindowedQuery(*this, windowType);
}

namespace WindowOperatorBuilder {
WindowedQuery::WindowedQuery(Query& originalQuery, Windowing::WindowTypePtr windowType)
    : originalQuery(originalQuery), windowType(std::move(windowType)) {}

//KeyedWindowedQuery keyBy(ExpressionItem onKey);
KeyedWindowedQuery WindowedQuery::byKey(const ExpressionItem& onKey) const {
    return KeyedWindowedQuery(originalQuery, windowType, onKey);
}

Query& WindowedQuery::apply(const Windowing::WindowAggregationPtr& aggregation) {
    return originalQuery.window(windowType, aggregation);
}

KeyedWindowedQuery::KeyedWindowedQuery(Query& originalQuery, Windowing::WindowTypePtr windowType, const ExpressionItem& onKey)
    : originalQuery(originalQuery), windowType(std::move(windowType)), onKey(onKey) {}

Query& KeyedWindowedQuery::apply(Windowing::WindowAggregationPtr aggregation) {
    return originalQuery.windowByKey(onKey, windowType, std::move(aggregation));
}

}//namespace WindowOperatorBuilder

Query& Query::window(const Windowing::WindowTypePtr& windowType, const Windowing::WindowAggregationPtr& aggregation) {
    NES_DEBUG("Query: add window operator");
    //we use a on time trigger as default that triggers on each change of the watermark
    auto triggerPolicy = OnWatermarkChangeTriggerPolicyDescription::create();
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    //numberOfInputEdges = 1, this will in a later rule be replaced with the number of children of the window

    uint64_t allowedLateness = 0;
    if (!queryPlan->getRootOperators()[0]->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        NES_DEBUG("add default watermark strategy as non is provided");
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
    } else {
        NES_DEBUG("add existing watermark strategy for window");
        auto assigner = queryPlan->getRootOperators()[0]->as<WatermarkAssignerLogicalOperatorNode>();
        if (auto eventTimeWatermarkStrategyDescriptor =
                std::dynamic_pointer_cast<Windowing::EventTimeWatermarkStrategyDescriptor>(
                    assigner->getWatermarkStrategyDescriptor())) {
            allowedLateness = eventTimeWatermarkStrategyDescriptor->getAllowedLateness().getTime();
        } else if (auto ingestionTimeWatermarkDescriptior =
                       std::dynamic_pointer_cast<Windowing::IngestionTimeWatermarkStrategyDescriptor>(
                           assigner->getWatermarkStrategyDescriptor())) {
            NES_WARNING("Note: ingestion time does not support allowed lateness yet");
        } else {
            NES_ERROR("cannot create watermark strategy from descriptor");
        }
    }

    auto inputSchema = getQueryPlan()->getRootOperators()[0]->getOutputSchema();

    auto windowDefinition = LogicalWindowDefinition::create(aggregation,
                                                            windowType,
                                                            DistributionCharacteristic::createCompleteWindowType(),
                                                            1,
                                                            triggerPolicy,
                                                            triggerAction,
                                                            allowedLateness);
    auto windowOperator = LogicalOperatorFactory::createWindowOperator(windowDefinition);

    queryPlan->appendOperatorAsNewRoot(windowOperator);
    return *this;
}

Query& Query::windowByKey(ExpressionItem onKey,
                          const Windowing::WindowTypePtr& windowType,
                          const Windowing::WindowAggregationPtr& aggregation) {
    NES_DEBUG("Query: add keyed window operator");
    auto keyExpression = onKey.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();

    //we use a on time trigger as default that triggers on each change of the watermark
    auto triggerPolicy = OnWatermarkChangeTriggerPolicyDescription::create();

    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    //numberOfInputEdges = 1, this will in a later rule be replaced with the number of children of the window

    uint64_t allowedLateness = 0;
    // check if query contain watermark assigner, and add if missing (as default behaviour)
    if (!queryPlan->getRootOperators()[0]->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        NES_DEBUG("add default watermark strategy as non is provided");
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
    } else {
        NES_DEBUG("add existing watermark strategy for window");
        auto assigner = queryPlan->getRootOperators()[0]->as<WatermarkAssignerLogicalOperatorNode>();
        if (auto eventTimeWatermarkStrategyDescriptor =
                std::dynamic_pointer_cast<Windowing::EventTimeWatermarkStrategyDescriptor>(
                    assigner->getWatermarkStrategyDescriptor())) {
            allowedLateness = eventTimeWatermarkStrategyDescriptor->getAllowedLateness().getTime();
        } else if (auto ingestionTimeWatermarkDescriptior =
                       std::dynamic_pointer_cast<Windowing::IngestionTimeWatermarkStrategyDescriptor>(
                           assigner->getWatermarkStrategyDescriptor())) {
            NES_WARNING("Note: ingestion time does not support allowed lateness yet");
        } else {
            NES_ERROR("cannot create watermark strategy from descriptor");
        }
    }

    auto inputSchema = getQueryPlan()->getRootOperators()[0]->getOutputSchema();

    auto windowDefinition =
        Windowing::LogicalWindowDefinition::create(fieldAccess,
                                                   aggregation,
                                                   windowType,
                                                   Windowing::DistributionCharacteristic::createCompleteWindowType(),
                                                   1,
                                                   triggerPolicy,
                                                   triggerAction,
                                                   allowedLateness);
    auto windowOperator = LogicalOperatorFactory::createWindowOperator(windowDefinition);

    queryPlan->appendOperatorAsNewRoot(windowOperator);
    return *this;
}

}// namespace NES
