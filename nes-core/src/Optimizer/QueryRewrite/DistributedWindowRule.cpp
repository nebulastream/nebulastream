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

#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
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

DistributeWindowRule::DistributeWindowRule(Configurations::OptimizerConfiguration configuration)
    : performDistributedWindowOptimization(configuration.performDistributedWindowOptimization),
      windowDistributionChildrenThreshold(configuration.distributedWindowChildThreshold),
      windowDistributionCombinerThreshold(configuration.distributedWindowCombinerThreshold) {
    if (performDistributedWindowOptimization) {
        NES_DEBUG("Create DistributeWindowRule with distributedWindowChildThreshold: " << windowDistributionChildrenThreshold
                                                                                       << " distributedWindowCombinerThreshold: "
                                                                                       << windowDistributionCombinerThreshold);
    } else {
        NES_DEBUG("Disable DistributeWindowRule");
    }
};

DistributeWindowRulePtr DistributeWindowRule::create(Configurations::OptimizerConfiguration configuration) {
    return std::make_shared<DistributeWindowRule>(DistributeWindowRule(configuration));
}

QueryPlanPtr DistributeWindowRule::apply(QueryPlanPtr queryPlan) {
    NES_INFO("DistributeWindowRule: Apply DistributeWindowRule.");
    NES_INFO("DistributeWindowRule::apply: plan before replace \n" << queryPlan->toString());
    if (!performDistributedWindowOptimization) {
        return queryPlan;
    }
    auto windowOps = queryPlan->getOperatorByType<WindowLogicalOperatorNode>();
    if (!windowOps.empty()) {
        /**
         * @end
         */
        NES_DEBUG("DistributeWindowRule::apply: found " << windowOps.size() << " window operators");
        for (auto& windowOp : windowOps) {
            NES_DEBUG("DistributeWindowRule::apply: window operator " << windowOp->toString());

            if (windowOp->getChildren().size() >= windowDistributionChildrenThreshold
                && windowOp->getWindowDefinition()->getWindowAggregation().size() == 1) {
                createDistributedWindowOperator(windowOp, queryPlan);
            } else {
                createCentralWindowOperator(windowOp);
                NES_DEBUG("DistributeWindowRule::apply: central op " << queryPlan->toString());
            }
        }
    } else {
        NES_DEBUG("DistributeWindowRule::apply: no window operator in query");
    }
    NES_DEBUG("DistributeWindowRule::apply: plan after replace \n" << queryPlan->toString());
    return queryPlan;
}

void DistributeWindowRule::createCentralWindowOperator(const WindowOperatorNodePtr& windowOp) {
    NES_DEBUG("DistributeWindowRule::apply: introduce centralized window operator for window " << windowOp << " "
                                                                                               << windowOp->toString());
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
    auto keyField = windowDefinition->getKeys();
    auto allowedLateness = windowDefinition->getAllowedLateness();
    // For the final window computation we have to change copy aggregation function and manipulate the fields we want to aggregate.
    auto windowComputationAggregation = windowAggregation[0]->copy();
    //    windowComputationAggregation->on()->as<FieldAccessExpressionNode>()->setFieldName("value");

    Windowing::LogicalWindowDefinitionPtr windowDef;
    if (logicalWindowOperator->getWindowDefinition()->isKeyed()) {
        windowDef = Windowing::LogicalWindowDefinition::create(keyField,
                                                               {windowComputationAggregation},
                                                               windowType,
                                                               Windowing::DistributionCharacteristic::createCombiningWindowType(),
                                                               triggerPolicy,
                                                               triggerActionComplete,
                                                               allowedLateness);

    } else {
        windowDef = Windowing::LogicalWindowDefinition::create({windowComputationAggregation},
                                                               windowType,
                                                               Windowing::DistributionCharacteristic::createCombiningWindowType(),
                                                               triggerPolicy,
                                                               triggerActionComplete,
                                                               allowedLateness);
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

    auto assignerOp = queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>();
    UnaryOperatorNodePtr finalComputationAssigner = windowComputationOperator;
    NES_ASSERT(assignerOp.size() > 1, "at least one assigner has to be there");

    NES_DEBUG("DistributedWindowRule: Plan before " << queryPlan->toString());

    //add merger
    UnaryOperatorNodePtr mergerAssigner;
    if (finalComputationAssigner->getChildren().size() >= windowDistributionCombinerThreshold) {
        auto sliceCombinerWindowAggregation = windowAggregation[0]->copy();

        if (logicalWindowOperator->getWindowDefinition()->isKeyed()) {
            windowDef =
                Windowing::LogicalWindowDefinition::create(keyField,
                                                           {sliceCombinerWindowAggregation},
                                                           windowType,
                                                           Windowing::DistributionCharacteristic::createMergingWindowType(),
                                                           triggerPolicy,
                                                           Windowing::SliceAggregationTriggerActionDescriptor::create(),
                                                           allowedLateness);

        } else {
            windowDef =
                Windowing::LogicalWindowDefinition::create({sliceCombinerWindowAggregation},
                                                           windowType,
                                                           Windowing::DistributionCharacteristic::createMergingWindowType(),
                                                           triggerPolicy,
                                                           Windowing::SliceAggregationTriggerActionDescriptor::create(),
                                                           allowedLateness);
        }
        NES_DEBUG("DistributeWindowRule::apply: created logical window definition for slice merger operator"
                  << windowDef->toString());

        uint64_t replicationFac = (finalComputationAssigner->getChildren().size() / 2) + 1;
        NES_DEBUG("DistributedWindowRule: Replicating slice merger with " << replicationFac);
        auto childrenChunks = Util::partition(finalComputationAssigner->getChildren(), replicationFac);
        finalComputationAssigner->removeChildren();

        for (auto newChildren : childrenChunks) {
            auto repSliceOp = LogicalOperatorFactory::createSliceMergingSpecializedOperator(windowDef);

            repSliceOp->removeChildren();
            for (auto newChild : newChildren) {
                repSliceOp->addChild(newChild);
                NES_DEBUG("DistributedWindowRule: Adding child " << newChild);
            }
            finalComputationAssigner->addChild(repSliceOp);
            addSlicer(repSliceOp->getChildren(), logicalWindowOperator);
        }
    }
    NES_DEBUG("DistributedWindowRule: Plan before " << queryPlan->toString());
}

void DistributeWindowRule::addSlicer(std::vector<NodePtr> windowChildren, const WindowOperatorNodePtr& logicalWindowOperator) {
    auto oldWindowDef = logicalWindowOperator->getWindowDefinition();
    auto triggerPolicy = oldWindowDef->getTriggerPolicy();
    auto triggerActionComplete = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    auto windowType = oldWindowDef->getWindowType();
    auto windowAggregation = oldWindowDef->getWindowAggregation();
    auto keyField = oldWindowDef->getKeys();
    auto allowedLateness = oldWindowDef->getAllowedLateness();

    //adding slicer
    Windowing::LogicalWindowDefinitionPtr windowDef;
    for (auto& child : windowChildren) {
        NES_DEBUG("DistributeWindowRule::apply: process child " << child->toString());

        // For the SliceCreation operator we have to change copy aggregation function and manipulate the fields we want to aggregate.
        auto triggerActionSlicing = Windowing::SliceAggregationTriggerActionDescriptor::create();
        auto sliceCreationWindowAggregation = windowAggregation[0]->copy();

        if (logicalWindowOperator->getWindowDefinition()->isKeyed()) {
            windowDef =
                Windowing::LogicalWindowDefinition::create({keyField},
                                                           {sliceCreationWindowAggregation},
                                                           windowType,
                                                           Windowing::DistributionCharacteristic::createSlicingWindowType(),
                                                           triggerPolicy,
                                                           triggerActionSlicing,
                                                           allowedLateness);
        } else {
            windowDef =
                Windowing::LogicalWindowDefinition::create({sliceCreationWindowAggregation},
                                                           windowType,
                                                           Windowing::DistributionCharacteristic::createSlicingWindowType(),
                                                           triggerPolicy,
                                                           triggerActionSlicing,
                                                           allowedLateness);
        }
        NES_DEBUG("DistributeWindowRule::apply: created logical window definition for slice operator" << windowDef->toString());
        auto sliceOp = LogicalOperatorFactory::createSliceCreationSpecializedOperator(windowDef);
        child->insertBetweenThisAndParentNodes(sliceOp);
    }
}

}// namespace NES::Optimizer
