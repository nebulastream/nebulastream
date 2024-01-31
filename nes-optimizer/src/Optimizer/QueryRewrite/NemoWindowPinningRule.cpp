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

#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperatorNode.hpp>
#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
#include <Optimizer/QueryRewrite/NemoWindowPinningRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <vector>

namespace NES::Optimizer {

NemoWindowPinningRule::NemoWindowPinningRule(Configurations::OptimizerConfiguration configuration, TopologyPtr topology)
    : DistributedWindowRule(configuration),
      performDistributedWindowOptimization(configuration.performDistributedWindowOptimization),
      windowDistributionChildrenThreshold(configuration.distributedWindowChildThreshold),
      windowDistributionCombinerThreshold(configuration.distributedWindowCombinerThreshold), topology(topology),
      enableNemoPlacement(configuration.enableNemoPlacement) {
    if (performDistributedWindowOptimization) {
        NES_DEBUG("Create NemoWindowPinningRule with distributedWindowChildThreshold: {} distributedWindowCombinerThreshold: {}",
                  windowDistributionChildrenThreshold,
                  windowDistributionCombinerThreshold);
    } else {
        NES_DEBUG("Disable NemoWindowPinningRule");
    }
};

NemoWindowPinningRulePtr NemoWindowPinningRule::create(Configurations::OptimizerConfiguration configuration,
                                                       TopologyPtr topology) {
    NES_ASSERT(topology != nullptr, "DistributedWindowRule: Topology is null");
    return std::make_shared<NemoWindowPinningRule>(NemoWindowPinningRule(configuration, topology));
}

QueryPlanPtr NemoWindowPinningRule::apply(QueryPlanPtr queryPlan) {
    NES_DEBUG("NemoWindowPinningRule: Apply NemoWindowPinningRule.");
    NES_DEBUG("NemoWindowPinningRule::apply: plan before replace\n{}", queryPlan->toString());
    NES_DEBUG("NemoWindowPinningRule::apply: plan after replace\n{}", queryPlan->toString());
    return queryPlan;
}

std::unordered_map<uint64_t, std::vector<WatermarkAssignerLogicalOperatorNodePtr>>
NemoWindowPinningRule::getMergerNodes(OperatorNodePtr operatorNode, uint64_t sharedParentThreshold) {
    std::unordered_map<uint64_t, std::vector<std::pair<TopologyNodePtr, WatermarkAssignerLogicalOperatorNodePtr>>> nodePlacement;
    //iterate over all children of the operator
    for (auto child : operatorNode->getAndFlattenAllChildren(true)) {
        if (child->as_if<OperatorNode>()->hasProperty(NES::Optimizer::PINNED_WORKER_ID)) {
            auto nodeId = std::any_cast<uint64_t>(child->as_if<OperatorNode>()->getProperty(NES::Optimizer::PINNED_WORKER_ID));
            TopologyNodePtr node = topology->getCopyOfTopologyNodeWithId(nodeId);
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
                //identify shared parent and add to result
                if (nodePlacement.contains(parentId)) {
                    nodePlacement[parentId].emplace_back(newPair);
                } else {
                    nodePlacement[parentId] =
                        std::vector<std::pair<TopologyNodePtr, WatermarkAssignerLogicalOperatorNodePtr>>{newPair};
                }
            }
        }
    }
    std::vector<std::pair<TopologyNodePtr, WatermarkAssignerLogicalOperatorNodePtr>> rootOperators;
    auto rootId = topology->getRootTopologyNodeId();

    //get the root operators
    if (nodePlacement.contains(rootId)) {
        rootOperators = nodePlacement[rootId];
    } else {
        nodePlacement[rootId] = rootOperators;
    }

    // add windows under the threshold to the root
    std::unordered_map<uint64_t, std::vector<WatermarkAssignerLogicalOperatorNodePtr>> output;
    for (auto plcmnt : nodePlacement) {
        if (plcmnt.second.size() <= sharedParentThreshold) {
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

void NemoWindowPinningRule::createDistributedWindowOperator(const WindowOperatorNodePtr& logicalWindowOperator,
                                                            const QueryPlanPtr& queryPlan) {
    // To distribute the window operator we replace the current window operator with 1 WindowComputationOperator (performs the final aggregate)
    // and n SliceCreationOperators.
    // To this end, we have to a the window definitions in the following way:
    // The SliceCreation consumes input and outputs data in the schema: {startTs, endTs, keyField, value}
    // The WindowComputation consumes that schema and outputs data in: {startTs, endTs, keyField, outputAggField}
    // First we prepare the final WindowComputation operator:

    //if window has more than 4 edges, we introduce a combiner

    NES_DEBUG("NemoWindowPinningRule::apply: introduce distributed window operator for window {}",
              logicalWindowOperator->toString());
    auto windowDefinition = logicalWindowOperator->getWindowDefinition();
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
                                                               allowedLateness);

    } else {
        windowDef = Windowing::LogicalWindowDefinition::create({windowComputationAggregation},
                                                               windowType,
                                                               allowedLateness);
    }
    NES_DEBUG("NemoWindowPinningRule::apply: created logical window definition for computation operator{}",
              windowDef->toString());

    auto assignerOp = queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>();
    NES_ASSERT(assignerOp.size() > 1, "at least one assigner has to be there");
}

}// namespace NES::Optimizer
