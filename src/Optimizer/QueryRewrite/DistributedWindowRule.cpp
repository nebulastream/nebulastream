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
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <algorithm>

#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/SliceAggregationTriggerActionDescriptor.hpp>

namespace NES {

DistributeWindowRule::DistributeWindowRule() {}

DistributeWindowRulePtr DistributeWindowRule::create() {
    return std::make_shared<DistributeWindowRule>(DistributeWindowRule());
}

QueryPlanPtr DistributeWindowRule::apply(QueryPlanPtr queryPlan) {
    NES_INFO("DistributeWindowRule: Apply DistributeWindowRule.");
    NES_DEBUG("DistributeWindowRule::apply: plan before replace " << queryPlan->toString());
    auto windowOps = queryPlan->getOperatorByType<WindowLogicalOperatorNode>();
    if (!windowOps.empty()) {
        NES_DEBUG("DistributeWindowRule::apply: found " << windowOps.size() << " window operators");
        for (auto& windowOp : windowOps) {
            NES_DEBUG("DistributeWindowRule::apply: window operator " << windowOp << " << windowOp->toString()");
            if (windowOp->getChildren().size() < CHILD_NODE_THRESHOLD) {
                createCentralWindowOperator(windowOp);
            } else {
                createDistributedWindowOperator(windowOp);
            }
        }
    } else {
        NES_DEBUG("DistributeWindowRule::apply: no window operator in query");
    }

    NES_DEBUG("DistributeWindowRule::apply: plan after replace " << queryPlan->toString());

    return queryPlan;
}

void DistributeWindowRule::createCentralWindowOperator(WindowOperatorNodePtr windowOp) {
    NES_DEBUG("DistributeWindowRule::apply: introduce centralized window operator for window " << windowOp << " << windowOp->toString()");
    auto newWindowOp = LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowOp->getWindowDefinition());
    newWindowOp->setInputSchema(windowOp->getInputSchema());
    newWindowOp->setOutputSchema(windowOp->getOutputSchema());
    NES_DEBUG("DistributeWindowRule::apply: newNode=" << newWindowOp->toString() << " old node=" << windowOp->toString());
    windowOp->replace(newWindowOp);
}

void DistributeWindowRule::createDistributedWindowOperator(WindowOperatorNodePtr logicalWindowOperator) {

    if (!logicalWindowOperator->getWindowDefinition()->isKeyed()) {
        NES_THROW_RUNTIME_ERROR("DistributeWindowRule: distributed windowing is currently only supported on keyed streams.");
    }

    // To distribute the window operator we replace the current window operator with 1 WindowComputationOperator (performs the final aggregate)
    // and n SliceCreationOperators.
    // To this end, we have to a the window definitions in the following way:
    // The SliceCreation consumes input and outputs data in the schema: {startTs, endTs, keyField, value}
    // The WindowComputation consumes that schema and outputs data in: {startTs, endTs, keyField, outputAggField}
    // First we prepare the final WindowComputation operator:

    NES_DEBUG("DistributeWindowRule::apply: introduce distributed window operator for window " << logicalWindowOperator << " << logicalWindowOperator->toString()");
    auto windowDefinition = logicalWindowOperator->getWindowDefinition();
    auto triggerPolicy = windowDefinition->getTriggerPolicy();
    auto triggerActionComplete = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    auto windowType = windowDefinition->getWindowType();
    auto windowAggregation = windowDefinition->getWindowAggregation();
    auto keyField = windowDefinition->getOnKey();

    // For the final window computation we have to change copy aggregation function and manipulate the fields we want to aggregate.
    auto windowComputationAggregation = windowAggregation->copy();
    windowComputationAggregation->on()->as<FieldAccessExpressionNode>()->setFieldName("value");

    auto windowComputationDefinition = Windowing::LogicalWindowDefinition::create(keyField,
                                                                                  windowComputationAggregation,
                                                                                  windowType,
                                                                                  Windowing::DistributionCharacteristic::createCombiningWindowType(),
                                                                                  logicalWindowOperator->getChildren().size(),
                                                                                  triggerPolicy,
                                                                                  triggerActionComplete);

    auto windowComputationOperator = LogicalOperatorFactory::createWindowComputationSpecializedOperator(windowComputationDefinition);

    //replace logical window op with window computation operator
    NES_DEBUG("DistributeWindowRule::apply: newNode=" << windowComputationOperator->toString() << " old node=" << logicalWindowOperator->toString());
    if (!logicalWindowOperator->replace(windowComputationOperator)) {
        NES_FATAL_ERROR("DistributeWindowRule:: replacement of window operator failed.");
    };

    //add a slicer to each child
    auto windowChildren = windowComputationOperator->getChildren();
    for (auto& child : windowChildren) {
        NES_DEBUG("DistributeWindowRule::apply: process child " << child->toString());

        // For the SliceCreation operator we have to change copy aggregation function and manipulate the fields we want to aggregate.
        auto sliceCreationWindowAggregation = windowAggregation->copy();
        sliceCreationWindowAggregation->as()->as<FieldAccessExpressionNode>()->setFieldName("value");
        auto triggerActionSlicing = Windowing::SliceAggregationTriggerActionDescriptor::create();

        auto sliceCreationWindowDefinition = Windowing::LogicalWindowDefinition::create(keyField,
                                                                                        sliceCreationWindowAggregation,
                                                                                        windowType,
                                                                                        Windowing::DistributionCharacteristic::createSlicingWindowType(), 1,
                                                                                        triggerPolicy,
                                                                                        triggerActionSlicing);
        auto sliceOp = LogicalOperatorFactory::createSliceCreationSpecializedOperator(sliceCreationWindowDefinition);
        child->insertBetweenThisAndParentNodes(sliceOp);
    }
}

}// namespace NES
