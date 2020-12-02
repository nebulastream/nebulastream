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

#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/L0QueryMergerRule.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/GlobalQueryMetaData.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

L0QueryMergerRule::L0QueryMergerRule() : processedNodes() {}

L0QueryMergerRulePtr L0QueryMergerRule::create() { return std::make_shared<L0QueryMergerRule>(L0QueryMergerRule()); }

bool L0QueryMergerRule::apply(const GlobalQueryPlanPtr& globalQueryPlan) {

    NES_INFO("L0QueryMergerRule: Applying L0 Merging rule to the Global Query Plan");

    std::vector<GlobalQueryMetaDataPtr> allGQMs = globalQueryPlan->getAllGlobalQueryMetaData();
    if (allGQMs.size() == 1) {
        NES_DEBUG(
            "L0QueryMergerRule: Found only a single query metadata in the global query plan. Skipping the L0 Query Merging.");
        return true;
    }

    std::vector<GlobalQueryMetaDataPtr> allGQMToDeploy = globalQueryPlan->getGlobalQueryMetaDataToDeploy();

    NES_DEBUG("L0QueryMergerRule: Iterating over all sink GQNs in the Global Query Plan");
    for (auto& targetGQM : allGQMToDeploy) {

        for (auto& hostGQM : allGQMs) {
            if (targetGQM->getGlobalQueryId() == hostGQM->getGlobalQueryId()){
                continue;
            }

            auto hostQueryPlan = hostGQM->getQueryPlan();
            auto targetQueryPlan = targetGQM->getQueryPlan();

            if(checkIfGQNCanMerge()){

                break;
            }
        }
    }
    return true;
}

bool L0QueryMergerRule::checkIfGQNCanMerge(const GlobalQueryNodePtr& targetGQNode, const GlobalQueryNodePtr& hostGQNode,
                                           std::map<GlobalQueryNodePtr, GlobalQueryNodePtr>& targetGQNToHostGQNMap) {

    NES_DEBUG("L0QueryMergerRule: Checking if the target GQN: " << targetGQNode->toString() << " and host GQN: "
                                                                << hostGQNode->toString() << " can be merged");
    processedNodes.push_back(targetGQNode);
    processedNodes.push_back(hostGQNode);
    if (targetGQNode->getId() == 0 && hostGQNode->getId() == 0) {
        NES_DEBUG("L0QueryMergerRule: Skip checking the root of global query plan");
        return true;
    }
    NES_DEBUG("L0QueryMergerRule: Get target and host GQNs operator sets");
    std::vector<OperatorNodePtr> targetOperators = targetGQNode->getOperators();
    std::vector<OperatorNodePtr> hostOperators = hostGQNode->getOperators();
    if (targetOperators.empty() && hostOperators.empty()) {
        NES_WARNING("L0QueryMergerRule: Both target and host operator lists are empty");
        return false;
    }
    if (targetOperators.size() != hostOperators.size()) {
        NES_WARNING("L0QueryMergerRule: target and host operator lists have different size");
        return false;
    }
    if (targetOperators[0]->instanceOf<SinkLogicalOperatorNode>() != hostOperators[0]->instanceOf<SinkLogicalOperatorNode>()) {
        NES_WARNING("L0QueryMergerRule: Matching Sink operator to another operator type.");
        return false;
    }
    if (!targetOperators[0]->instanceOf<SinkLogicalOperatorNode>() && !hostOperators[0]->instanceOf<SinkLogicalOperatorNode>()) {
        NES_DEBUG("L0QueryMergerRule: Comparing target and host operators to find equality");
        for (auto& targetOperator : targetOperators) {
            bool found = false;
            for (auto& hostOperator : hostOperators) {
                if (targetOperator->equal(hostOperator)) {
                    found = true;
                    NES_TRACE("L0QueryMergerRule: Found an equal target and host operator");
                    break;
                }
            }
            if (!found) {
                NES_WARNING("L0QueryMergerRule: unable to find a match for the target operators");
                return false;
            }
        }

        NES_DEBUG("L0QueryMergerRule: Adding matched host GQN to the map");
        targetGQNToHostGQNMap[targetGQNode] = hostGQNode;
    }
    //Check if all target parents have a matching host parent
    for (auto& targetParentGQN : targetGQNode->getParents()) {
        //Check if the node was processed previously
        auto foundExistingMatchForTargetGQN =
            std::find(processedNodes.begin(), processedNodes.end(), targetParentGQN->as<GlobalQueryNode>());
        if (foundExistingMatchForTargetGQN != processedNodes.end()) {
            NES_TRACE("L0QueryMergerRule: The target GQN " << targetParentGQN->toString() << " was processed already. Skipping.");
            continue;
        }
        bool found = false;
        for (auto& hostParentGQN : hostGQNode->getParents()) {

            //Check if the node was processed previously
            auto foundExistingMatchForHostParentGQN =
                std::find(processedNodes.begin(), processedNodes.end(), hostParentGQN->as<GlobalQueryNode>());
            if (foundExistingMatchForHostParentGQN != processedNodes.end()) {
                NES_TRACE("L0QueryMergerRule: The target GQN " << hostParentGQN->toString()
                                                               << " was processed already. Skipping.");
                continue;
            }

            if (checkIfGQNCanMerge(targetParentGQN->as<GlobalQueryNode>(), hostParentGQN->as<GlobalQueryNode>(),
                                   targetGQNToHostGQNMap)) {
                found = true;
                break;
            }
        }
        if (!found) {
            NES_WARNING("L0QueryMergerRule: unable to find any match for the target parent GQN: " << targetParentGQN->toString());
            return false;
        }
    }
    //Check if all target children have a matching host child
    for (auto& targetChildGQN : targetGQNode->getChildren()) {
        //Check if the node was processed previously
        auto foundExistingMatchForTargetGQN =
            std::find(processedNodes.begin(), processedNodes.end(), targetChildGQN->as<GlobalQueryNode>());
        if (foundExistingMatchForTargetGQN != processedNodes.end()) {
            NES_TRACE("L0QueryMergerRule: The target GQN " << targetChildGQN->toString() << " was processed already. Skipping.");
            continue;
        }
        bool found = false;
        for (auto& hostChildGQN : hostGQNode->getChildren()) {
            //Check if the node was processed previously
            auto foundExistingMatchForHostChildGQN =
                std::find(processedNodes.begin(), processedNodes.end(), hostChildGQN->as<GlobalQueryNode>());
            if (foundExistingMatchForHostChildGQN != processedNodes.end()) {
                NES_TRACE("L0QueryMergerRule: The target GQN " << hostChildGQN->toString()
                                                               << " was processed already. Skipping.");
                continue;
            }
            if (checkIfGQNCanMerge(targetChildGQN->as<GlobalQueryNode>(), hostChildGQN->as<GlobalQueryNode>(),
                                   targetGQNToHostGQNMap)) {
                found = true;
                break;
            }
        }
        if (!found) {
            NES_WARNING("L0QueryMergerRule: unable to find any match for the target child GQN: " << targetChildGQN->toString());
            return false;
        }
    }
    NES_DEBUG("L0QueryMergerRule: Adding matched host GQN to the map");
    return true;
}

bool L0QueryMergerRule::areGQNodesEqual(const std::vector<NodePtr>& targetGQNs, const std::vector<NodePtr>& hostGQNs) {

    NES_DEBUG("L0QueryMergerRule: Checking if the two set of GQNs are equal or not");
    for (auto& targetGQN : targetGQNs) {
        bool found = false;
        for (auto& hostGQN : hostGQNs) {
            if (targetGQN->as<GlobalQueryNode>()->getId() == hostGQN->as<GlobalQueryNode>()->getId()) {
                found = true;
                break;
            }
        }
        if (!found) {
            NES_DEBUG("L0QueryMergerRule: GQN sets are not equal");
            return false;
        }
    }
    NES_DEBUG("L0QueryMergerRule: GQN sets are equal");
    return true;
}

}// namespace NES