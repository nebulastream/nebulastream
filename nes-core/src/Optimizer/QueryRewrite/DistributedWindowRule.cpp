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
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/SliceAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <iterator>
#include <vector>

namespace NES::Optimizer {

DistributeWindowRule::DistributeWindowRule(Configurations::OptimizerConfiguration configuration, TopologyPtr topology)
    : performDistributedWindowOptimization(configuration.performDistributedWindowOptimization),
      windowDistributionChildrenThreshold(configuration.distributedWindowChildThreshold),
      windowDistributionCombinerThreshold(configuration.distributedWindowCombinerThreshold), topology(topology) {
    if (performDistributedWindowOptimization) {
        NES_DEBUG("Create DistributeWindowRule with distributedWindowChildThreshold: " << windowDistributionChildrenThreshold
                                                                                       << " distributedWindowCombinerThreshold: "
                                                                                       << windowDistributionCombinerThreshold);
    } else {
        NES_DEBUG("Disable DistributeWindowRule");
    }
};

DistributeWindowRulePtr DistributeWindowRule::create(Configurations::OptimizerConfiguration configuration, TopologyPtr topology) {
    NES_ASSERT(topology != nullptr, "DistributedWindowRule: Topology is null");
    return std::make_shared<DistributeWindowRule>(DistributeWindowRule(configuration, topology));
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

            if (windowOp->getChildren().size() >= windowDistributionChildrenThreshold) {
                createDistributedWindowOperator(windowOp, queryPlan);
            } else {
                createCentralWindowOperator(windowOp);
                NES_DEBUG("DistributeWindowRule::apply: central op \n" << queryPlan->toString());
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

void DistributeWindowRule::createDistributedWindowOperator(const WindowOperatorNodePtr& windowOp, const QueryPlanPtr& queryPlan) {
    NES_DEBUG("DistributeWindowRule::apply: introduce new distributed window operator for window " << windowOp << " "
                                                                                                   << windowOp->toString());
    auto parents = windowOp->getParents();
    auto mergerNodes = getMergerNodes(windowOp, windowDistributionCombinerThreshold);
    windowOp->removeChildren();
    windowOp->removeAllParent();

    for (auto parent : parents) {
        parent->removeChildren();

        for (auto mergerPair : mergerNodes) {
            auto nodeId = mergerPair.first;
            auto newWindowOp = LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowOp->getWindowDefinition());
            newWindowOp->setInputSchema(windowOp->getInputSchema());
            newWindowOp->setOutputSchema(windowOp->getOutputSchema());
            newWindowOp->addProperty(NES::Optimizer::PINNED_NODE_ID, nodeId);
            NES_DEBUG("DistributeWindowRule::apply: newNode=" << newWindowOp->toString() << " old node=" << windowOp->toString());

            auto children = mergerPair.second;
            for (auto source : children) {
                parent->addChild(newWindowOp);
                newWindowOp->addChild(source);
            }
        }
    }
    NES_DEBUG("DistributedWindowRule: Plan after \n" << queryPlan->toString());
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

std::unordered_map<uint64_t, std::vector<WatermarkAssignerLogicalOperatorNodePtr>>
DistributeWindowRule::getMergerNodes(OperatorNodePtr operatorNode, uint64_t combinerThreshold) {
    std::unordered_map<uint64_t, std::vector<std::pair<TopologyNodePtr, WatermarkAssignerLogicalOperatorNodePtr>>> nodePlacement;
    for (auto child : operatorNode->getAndFlattenAllChildren(true)) {
        if (child->as_if<OperatorNode>()->hasProperty(NES::Optimizer::PINNED_NODE_ID)) {
            auto nodeId = std::any_cast<uint64_t>(child->as_if<OperatorNode>()->getProperty(NES::Optimizer::PINNED_NODE_ID));
            //auto operatorId = child->as_if<OperatorNode>()->getId();
            TopologyNodePtr node = topology->findNodeWithId(nodeId);
            for (auto& parent : node->getParents()) {
                auto parentId = std::any_cast<uint64_t>(parent->as_if<TopologyNode>()->getId());

                // get the watermark parent
                WatermarkAssignerLogicalOperatorNodePtr watermark;
                for (auto ancestor : child->getAndFlattenAllAncestors()) {
                    if (ancestor->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
                        watermark = ancestor->as_if<WatermarkAssignerLogicalOperatorNode>();
                        break;
                    }
                }
                NES_ASSERT(watermark != nullptr, "DistributedWindowRule: Window source does not contain a watermark");

                auto newPair = std::make_pair(node, watermark);
                if (nodePlacement.contains(parentId)) {
                    nodePlacement[parentId].emplace_back(newPair);
                } else {
                    nodePlacement[parentId] =
                        std::vector<std::pair<TopologyNodePtr, WatermarkAssignerLogicalOperatorNodePtr>>{newPair};
                }
            }
        }
    }
    auto sinkNodes = operatorNode->getAllRootNodes();
    std::vector<std::pair<TopologyNodePtr, WatermarkAssignerLogicalOperatorNodePtr>> rootOperators;
    auto rootId = topology->getRoot()->getId();

    if (nodePlacement.contains(rootId)) {
        rootOperators = nodePlacement[rootId];
    } else {
        nodePlacement[rootId] = rootOperators;
    }

    // add windows under the threshold to the root
    std::unordered_map<uint64_t, std::vector<WatermarkAssignerLogicalOperatorNodePtr>> output;
    for (auto plcmnt : nodePlacement) {
        if (plcmnt.second.size() < combinerThreshold) {
            // resolve the placement
            for (auto pairs : plcmnt.second) {
                output[pairs.first->getId()] = std::vector<WatermarkAssignerLogicalOperatorNodePtr>{pairs.second};
            }
        } else {
            // add to output
            if (plcmnt.second.size() > 1) {
                auto addedNodes = std::vector<WatermarkAssignerLogicalOperatorNodePtr>{};
                for (auto pairs : plcmnt.second) {
                    addedNodes.emplace_back(pairs.second);
                }
                output[plcmnt.first] = addedNodes;
            } else {
                // place at the root of topology if there is no shared parent
                if (output.contains(rootId)) {
                    output[rootId].emplace_back(plcmnt.second[0].second);
                } else {
                    output[rootId] = std::vector<WatermarkAssignerLogicalOperatorNodePtr>{plcmnt.second[0].second};
                }
            }
        }
    }
    return output;
}

}// namespace NES::Optimizer
