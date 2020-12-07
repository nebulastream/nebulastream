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
#include <Windowing/WindowActions/SliceCombinerTriggerActionDescriptor.hpp>
namespace NES {

DistributeWindowRule::DistributeWindowRule() {}

DistributeWindowRulePtr DistributeWindowRule::create() { return std::make_shared<DistributeWindowRule>(DistributeWindowRule()); }

QueryPlanPtr DistributeWindowRule::apply(QueryPlanPtr queryPlan) {
    NES_INFO("DistributeWindowRule: Apply DistributeWindowRule.");
    NES_DEBUG("DistributeWindowRule::apply: plan before replace " << queryPlan->toString());
    auto windowOps = queryPlan->getOperatorByType<WindowLogicalOperatorNode>();
    if (!windowOps.empty()) {
        /**
         * @end
         */
        NES_DEBUG("DistributeWindowRule::apply: found " << windowOps.size() << " window operators");
        for (auto& windowOp : windowOps) {
            NES_DEBUG("DistributeWindowRule::apply: window operator " << windowOp << " << windowOp->toString()");

            /**
             * @brief FOR DEBUG ONLY TODO CHANGE BACK
             */
            createDistributedWindowOperator(windowOp);
            //            if (windowOp->getChildren().size() < CHILD_NODE_THRESHOLD) {
            //                createCentralWindowOperator(windowOp);
            //            } else {
            //                createDistributedWindowOperator(windowOp);
            //            }
        }
    } else {
        NES_DEBUG("DistributeWindowRule::apply: no window operator in query");
    }

    NES_DEBUG("DistributeWindowRule::apply: plan after replace " << queryPlan->toString());

    return queryPlan;
}

void DistributeWindowRule::createCentralWindowOperator(WindowOperatorNodePtr windowOp) {
    NES_DEBUG("DistributeWindowRule::apply: introduce centralized window operator for window " << windowOp
                                                                                               << " << windowOp->toString()");
    auto newWindowOp = LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowOp->getWindowDefinition());
    newWindowOp->setInputSchema(windowOp->getInputSchema());
    newWindowOp->setOutputSchema(windowOp->getOutputSchema());
    NES_DEBUG("DistributeWindowRule::apply: newNode=" << newWindowOp->toString() << " old node=" << windowOp->toString());
    windowOp->replace(newWindowOp);
}

void DistributeWindowRule::createDistributedWindowOperator(WindowOperatorNodePtr logicalWindowOperator) {
    // To distribute the window operator we replace the current window operator with 1 WindowComputationOperator (performs the final aggregate)
    // and n SliceCreationOperators.
    // To this end, we have to a the window definitions in the following way:
    // The SliceCreation consumes input and outputs data in the schema: {startTs, endTs, keyField, value}
    // The WindowComputation consumes that schema and outputs data in: {startTs, endTs, keyField, outputAggField}
    // First we prepare the final WindowComputation operator:

    //if window has more than 4 edges, we introduce a combiner

    NES_DEBUG("DistributeWindowRule::apply: introduce distributed window operator for window "
              << logicalWindowOperator << " << logicalWindowOperator->toString()");
    auto windowDefinition = logicalWindowOperator->getWindowDefinition();
    auto triggerPolicy = windowDefinition->getTriggerPolicy();
    auto triggerActionComplete = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    auto windowType = windowDefinition->getWindowType();
    auto windowAggregation = windowDefinition->getWindowAggregation();
    auto keyField = windowDefinition->getOnKey();

    // For the final window computation we have to change copy aggregation function and manipulate the fields we want to aggregate.
    auto windowComputationAggregation = windowAggregation->copy();
    //    windowComputationAggregation->on()->as<FieldAccessExpressionNode>()->setFieldName("value");

    //TODO: @Ankit we have to change this depending on how you do the placement
    uint64_t numberOfEdges = logicalWindowOperator->getChildren().size();
    if (logicalWindowOperator->getChildren().size() > CHILD_NODE_THRESHOLD_COMBINER) {
        numberOfEdges = 1;
    }

    Windowing::LogicalWindowDefinitionPtr windowDef;
    if (logicalWindowOperator->getWindowDefinition()->isKeyed()) {
        windowDef = Windowing::LogicalWindowDefinition::create(keyField, windowComputationAggregation, windowType,
                                                               Windowing::DistributionCharacteristic::createCombiningWindowType(),
                                                               numberOfEdges, triggerPolicy,
                                                               triggerActionComplete);

    } else {
        windowDef = Windowing::LogicalWindowDefinition::create(
            windowComputationAggregation, windowType, Windowing::DistributionCharacteristic::createCombiningWindowType(),
            numberOfEdges, triggerPolicy, triggerActionComplete);
    }
    NES_DEBUG("DistributeWindowRule::apply: created logical window definition for computation operator" << windowDef->toString());

    auto windowComputationOperator = LogicalOperatorFactory::createWindowComputationSpecializedOperator(windowDef);

    //replace logical window op with window computation operator
    NES_DEBUG("DistributeWindowRule::apply: newNode=" << windowComputationOperator->toString()
                                                      << " old node=" << logicalWindowOperator->toString());
    if (!logicalWindowOperator->replace(windowComputationOperator)) {
        NES_FATAL_ERROR("DistributeWindowRule:: replacement of window operator failed.");
    }


    auto windowChildren = windowComputationOperator->getChildren();

    //add merger
    if (windowComputationOperator->getChildren().size()
        >= CHILD_NODE_THRESHOLD_COMBINER) {//TODO change back to CHILD_NODE_THRESHOLD_COMBINER
        auto sliceCombinerWindowAggregation = windowAggregation->copy();
        //        sliceCombinerWindowAggregation->on()->as<FieldAccessExpressionNode>()->setFieldName("value");
        auto triggerActionSlicing = Windowing::SliceCombinerTriggerActionDescriptor::create();

        Windowing::LogicalWindowDefinitionPtr windowDef;
        if (logicalWindowOperator->getWindowDefinition()->isKeyed()) {
            windowDef = Windowing::LogicalWindowDefinition::create(
                keyField, sliceCombinerWindowAggregation, windowType,
                Windowing::DistributionCharacteristic::createMergingWindowType(), windowComputationOperator->getChildren().size(),
                triggerPolicy, triggerActionComplete);

        } else {
            windowDef = Windowing::LogicalWindowDefinition::create(
                sliceCombinerWindowAggregation, windowType, Windowing::DistributionCharacteristic::createMergingWindowType(),
                windowComputationOperator->getChildren().size(), triggerPolicy, triggerActionComplete);
        }
        NES_DEBUG("DistributeWindowRule::apply: created logical window definition for slice merger operator"
                  << windowDef->toString());
        auto sliceOp = LogicalOperatorFactory::createSliceMergingSpecializedOperator(windowDef);
        windowComputationOperator->insertBetweenThisAndChildNodes(sliceOp);
        windowChildren = sliceOp->getChildren();
    }


    for (auto& child : windowChildren) {
        NES_DEBUG("DistributeWindowRule::apply: process child " << child->toString());

        // For the SliceCreation operator we have to change copy aggregation function and manipulate the fields we want to aggregate.
        auto sliceCreationWindowAggregation = windowAggregation->copy();
        //        sliceCreationWindowAggregation->as()->as<FieldAccessExpressionNode>()->setFieldName("value");//TODO: I am not sure if this is correct
        auto triggerActionSlicing = Windowing::SliceAggregationTriggerActionDescriptor::create();

        Windowing::LogicalWindowDefinitionPtr windowDef;
        if (logicalWindowOperator->getWindowDefinition()->isKeyed()) {
            windowDef = Windowing::LogicalWindowDefinition::create(
                keyField, sliceCreationWindowAggregation, windowType,
                Windowing::DistributionCharacteristic::createSlicingWindowType(), 1, triggerPolicy, triggerActionSlicing);
        } else {
            windowDef = Windowing::LogicalWindowDefinition::create(
                sliceCreationWindowAggregation, windowType, Windowing::DistributionCharacteristic::createSlicingWindowType(), 1,
                triggerPolicy, triggerActionSlicing);
        }
        NES_DEBUG("DistributeWindowRule::apply: created logical window definition for slice operator" << windowDef->toString());
        auto sliceOp = LogicalOperatorFactory::createSliceCreationSpecializedOperator(windowDef);
        child->insertBetweenThisAndParentNodes(sliceOp);
    }
}

}// namespace NES
