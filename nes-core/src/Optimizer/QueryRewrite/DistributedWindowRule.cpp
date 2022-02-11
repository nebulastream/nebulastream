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

#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/SliceAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>

namespace NES::Optimizer {

DistributeWindowRule::DistributeWindowRule(uint64_t windowDistributionChildrenThreshold,
                                           uint64_t windowDistributionCombinerThreshold)
    : windowDistributionChildrenThreshold(windowDistributionChildrenThreshold),
      windowDistributionCombinerThreshold(windowDistributionCombinerThreshold){};

DistributeWindowRulePtr DistributeWindowRule::create(uint64_t windowDistributionChildrenThreshold,
                                                     uint64_t windowDistributionCombinerThreshold) {
    return std::make_shared<DistributeWindowRule>(
        DistributeWindowRule(windowDistributionChildrenThreshold, windowDistributionCombinerThreshold));
}

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
            NES_DEBUG("DistributeWindowRule::apply: window operator " << windowOp->toString());

            if (windowOp->getChildren().size() < windowDistributionChildrenThreshold) {
                createCentralWindowOperator(windowOp);
                NES_DEBUG("DistributeWindowRule::apply: central op " << queryPlan->toString());
            } else {
                createDistributedWindowOperator(windowOp, queryPlan);
            }
        }
    } else {
        NES_DEBUG("DistributeWindowRule::apply: no window operator in query");
    }
    NES_DEBUG("DistributeWindowRule::apply: plan after replace " << queryPlan->toString());
    return queryPlan;
}

void DistributeWindowRule::createCentralWindowOperator(const WindowOperatorNodePtr& windowOp) {
    NES_DEBUG("DistributeWindowRule::apply: introduce centralized window operator for window " << windowOp << " "
                                                                                               << windowOp->toString());
    windowOp->getWindowDefinition()->setOriginId(windowOp->getId());
    auto newWindowOp = LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowOp->getWindowDefinition());
    newWindowOp->setInputSchema(windowOp->getInputSchema());
    newWindowOp->setOutputSchema(windowOp->getOutputSchema());
    NES_DEBUG("DistributeWindowRule::apply: newNode=" << newWindowOp->toString() << " old node=" << windowOp->toString());
    windowOp->replace(newWindowOp);
}

void DistributeWindowRule::createDistributedWindowOperator(const WindowOperatorNodePtr& logicalWindowOperator,
                                                           const QueryPlanPtr& queryPlan) {
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
    auto keyField = windowDefinition->getKeys()[0];
    auto allowedLateness = windowDefinition->getAllowedLateness();
    // For the final window computation we have to change copy aggregation function and manipulate the fields we want to aggregate.
    auto windowComputationAggregation = windowAggregation[0]->copy();
    //    windowComputationAggregation->on()->as<FieldAccessExpressionNode>()->setFieldName("value");

    //TODO: @Ankit we have to change this depending on how you do the placement
    uint64_t numberOfEdgesForFinalComputation = logicalWindowOperator->getChildren().size();
    if (logicalWindowOperator->getChildren().size() >= windowDistributionCombinerThreshold) {
        numberOfEdgesForFinalComputation = 1;
    }
    uint64_t numberOfEdgesForMerger = logicalWindowOperator->getChildren().size();

    Windowing::LogicalWindowDefinitionPtr windowDef;
    if (logicalWindowOperator->getWindowDefinition()->isKeyed()) {
        windowDef = Windowing::LogicalWindowDefinition::create({keyField},
                                                               {windowComputationAggregation},
                                                               windowType,
                                                               Windowing::DistributionCharacteristic::createCombiningWindowType(),
                                                               numberOfEdgesForFinalComputation,
                                                               triggerPolicy,
                                                               triggerActionComplete,
                                                               allowedLateness);

    } else {
        windowDef = Windowing::LogicalWindowDefinition::create({windowComputationAggregation},
                                                               windowType,
                                                               Windowing::DistributionCharacteristic::createCombiningWindowType(),
                                                               numberOfEdgesForFinalComputation,
                                                               triggerPolicy,
                                                               triggerActionComplete,
                                                               allowedLateness);
    }
    NES_DEBUG("DistributeWindowRule::apply: created logical window definition for computation operator" << windowDef->toString());

    auto windowComputationOperator = LogicalOperatorFactory::createWindowComputationSpecializedOperator(windowDef);
    windowDef->setOriginId(windowComputationOperator->getId());

    //replace logical window op with window computation operator
    NES_DEBUG("DistributeWindowRule::apply: newNode=" << windowComputationOperator->toString()
                                                      << " old node=" << logicalWindowOperator->toString());
    if (!logicalWindowOperator->replace(windowComputationOperator)) {
        NES_FATAL_ERROR("DistributeWindowRule:: replacement of window operator failed.");
    }

    auto windowChildren = windowComputationOperator->getChildren();

    auto assignerOp = queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>();
    UnaryOperatorNodePtr finalComputationAssigner = windowComputationOperator;
    NES_ASSERT(assignerOp.size() > 1, "at least one assigner has to be there");

    //add merger
    UnaryOperatorNodePtr mergerAssigner;
    if (finalComputationAssigner->getChildren().size() >= windowDistributionCombinerThreshold) {
        auto sliceCombinerWindowAggregation = windowAggregation[0]->copy();

        if (logicalWindowOperator->getWindowDefinition()->isKeyed()) {
            windowDef =
                Windowing::LogicalWindowDefinition::create({keyField},
                                                           {sliceCombinerWindowAggregation},
                                                           windowType,
                                                           Windowing::DistributionCharacteristic::createMergingWindowType(),
                                                           numberOfEdgesForMerger,
                                                           triggerPolicy,
                                                           Windowing::SliceAggregationTriggerActionDescriptor::create(),
                                                           allowedLateness);

        } else {
            windowDef =
                Windowing::LogicalWindowDefinition::create({sliceCombinerWindowAggregation},
                                                           windowType,
                                                           Windowing::DistributionCharacteristic::createMergingWindowType(),
                                                           numberOfEdgesForMerger,
                                                           triggerPolicy,
                                                           Windowing::SliceAggregationTriggerActionDescriptor::create(),
                                                           allowedLateness);
        }
        NES_DEBUG("DistributeWindowRule::apply: created logical window definition for slice merger operator"
                  << windowDef->toString());
        auto sliceOp = LogicalOperatorFactory::createSliceMergingSpecializedOperator(windowDef);
        windowDef->setOriginId(sliceOp->getId());
        finalComputationAssigner->insertBetweenThisAndChildNodes(sliceOp);

        mergerAssigner = sliceOp;
        windowChildren = mergerAssigner->getChildren();
    }

    //adding slicer
    for (auto& child : windowChildren) {
        NES_DEBUG("DistributeWindowRule::apply: process child " << child->toString());

        // For the SliceCreation operator we have to change copy aggregation function and manipulate the fields we want to aggregate.
        auto sliceCreationWindowAggregation = windowAggregation[0]->copy();
        auto triggerActionSlicing = Windowing::SliceAggregationTriggerActionDescriptor::create();

        if (logicalWindowOperator->getWindowDefinition()->isKeyed()) {
            windowDef =
                Windowing::LogicalWindowDefinition::create({keyField},
                                                           {sliceCreationWindowAggregation},
                                                           windowType,
                                                           Windowing::DistributionCharacteristic::createSlicingWindowType(),
                                                           1,
                                                           triggerPolicy,
                                                           triggerActionSlicing,
                                                           allowedLateness);
        } else {
            windowDef =
                Windowing::LogicalWindowDefinition::create({sliceCreationWindowAggregation},
                                                           windowType,
                                                           Windowing::DistributionCharacteristic::createSlicingWindowType(),
                                                           1,
                                                           triggerPolicy,
                                                           triggerActionSlicing,
                                                           allowedLateness);
        }
        NES_DEBUG("DistributeWindowRule::apply: created logical window definition for slice operator" << windowDef->toString());
        auto sliceOp = LogicalOperatorFactory::createSliceCreationSpecializedOperator(windowDef);
        windowDef->setOriginId(sliceOp->getId());
        child->insertBetweenThisAndParentNodes(sliceOp);
    }
}

}// namespace NES::Optimizer
