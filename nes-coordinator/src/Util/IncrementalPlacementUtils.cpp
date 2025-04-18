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
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/IncrementalPlacementUtils.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <any>

namespace NES::Experimental {

std::pair<std::set<OperatorId>, std::set<OperatorId>>
findUpstreamAndDownstreamPinnedOperators(const SharedQueryPlanPtr& sharedQueryPlan,
                                         Optimizer::ExecutionNodeWLock lockedUpstreamNode,
                                         Optimizer::ExecutionNodeWLock lockedDownstreamNode,
                                         const TopologyPtr& topology) {
    NES_ERROR("Phase 1");
    auto sharedQueryPlanId = sharedQueryPlan->getId();
    auto queryPlanForSharedQuery = sharedQueryPlan->getQueryPlan();
    //find the pairs of source and sink operators that were using the removed link
    auto upstreamDownstreamOperatorPairs =
        findNetworkOperatorsForLink(sharedQueryPlanId, lockedUpstreamNode, lockedDownstreamNode);
    for (auto& [upstreamOperator, downstreamOperator] : upstreamDownstreamOperatorPairs) {
        //replace the system generated operators with their non system up- or downstream operators
        auto upstreamLogicalOperatorId =
            std::any_cast<OperatorId>(upstreamOperator->getProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID));
        upstreamOperator = queryPlanForSharedQuery->getOperatorWithOperatorId(upstreamLogicalOperatorId)->as<LogicalOperator>();
        auto downstreamLogicalOperatorId =
            std::any_cast<OperatorId>(downstreamOperator->getProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID));
        downstreamOperator =
            queryPlanForSharedQuery->getOperatorWithOperatorId(downstreamLogicalOperatorId)->as<LogicalOperator>();
    }

    std::set<OperatorId> upstreamPinned;
    std::set<OperatorId> downstreamPinned;
    std::set<OperatorId> toRemove;
    NES_ERROR("Phase 2");
    for (const auto& [upstreamOperator, downstreamOperator] : upstreamDownstreamOperatorPairs) {
        const auto upstreamSharedQueryOperater = queryPlanForSharedQuery->getOperatorWithOperatorId(upstreamOperator->getId());
        const auto upstreamWorkerId =
            std::any_cast<WorkerId>(upstreamSharedQueryOperater->getProperty(Optimizer::PINNED_WORKER_ID));
        const auto downstreamSharedQueryOperator =
            queryPlanForSharedQuery->getOperatorWithOperatorId(downstreamOperator->getId());
        const auto downstreamWorkerId =
            std::any_cast<WorkerId>(downstreamSharedQueryOperator->getProperty(Optimizer::PINNED_WORKER_ID));

        //assuming that we can always pin this operator will hold as long as only leave nodes are changing their parent
        //in case a node with children changes its parent, this method might not discover some possible paths because it ignores the children
        //to handle that case, a new reachable set needs to be calculated from the children in case the first one fails
        upstreamPinned.insert(upstreamOperator->getId());

        //find all toplogy nodes that are reachable from the pinned upstream operator node
        std::set<WorkerId> reachable;
        NES_ERROR("findAllDownstreamNodes");
        topology->findAllDownstreamNodes(upstreamWorkerId, reachable, {downstreamWorkerId});

        //check if the old downstream was found, then only forward operators need to be inserted between the old up and downstream
        if (reachable.contains(downstreamWorkerId)) {
            //only one target node as been supplied, so the vector of found targets can contain one item at most
            downstreamPinned.insert(downstreamOperator->getId());
        } else {
            //because the old downstream was not found (list of found target nodes is empty), another path has to be found

            //at this point all reachable downstream nodes in the new topology are found for a specific operator
            //now find the closest downstream operator hosted on one of these reachable nodes
            std::queue<LogicalOperatorPtr> queryPlanBFSQueue;
            std::set<OperatorId> visitedOperators;

            //populate queue with the non system parents of the upstream operator
            auto startOperatorInSharedQueryPlan = queryPlanForSharedQuery->getOperatorWithOperatorId(upstreamOperator->getId());
            for (const auto& parent : startOperatorInSharedQueryPlan->getParents()) {
                queryPlanBFSQueue.push(parent->as<LogicalOperator>());
            }
            NES_ERROR("entered while loop");
            while (!queryPlanBFSQueue.empty()) {
                const auto currentOperator = queryPlanBFSQueue.front();
                queryPlanBFSQueue.pop();
                if (visitedOperators.contains(currentOperator->getId())) {
                    continue;
                }
                const auto currentWorkerId = std::any_cast<WorkerId>(currentOperator->getProperty(Optimizer::PINNED_WORKER_ID));
                visitedOperators.insert(currentOperator->getId());
                upstreamPinned.erase(currentOperator->getId());
                if (reachable.contains(currentWorkerId)) {
                    downstreamPinned.insert(currentOperator->getId());
                } else {
                    //if the operator is reachable from another one of its upstream operators but not from this one,
                    //we need to remove it from the pinned set and keep looking for a further downstream operator
                    //which is reachable by this node as well and pin that one.
                    downstreamPinned.erase(currentOperator->getId());
                    for (const auto& parent : currentOperator->getParents()) {
                        queryPlanBFSQueue.push(parent->as<LogicalOperator>());
                    }
                    toRemove.insert(currentOperator->getId());
                    for (const auto& child : currentOperator->getChildren()) {
                        auto nonSystemChild = child->as<LogicalOperator>();
                        if (!toRemove.contains(nonSystemChild->getId())) {
                            upstreamPinned.insert(nonSystemChild->getId());
                        }
                    }
                }
            }
            NES_ERROR("left while loop");
        }
    }
    NES_ERROR("FInished");
    return {upstreamPinned, downstreamPinned};
}

std::vector<std::pair<LogicalOperatorPtr, LogicalOperatorPtr>>
findNetworkOperatorsForLink(const SharedQueryId& sharedQueryPlanId,
                            Optimizer::ExecutionNodeWLock lockedUpstreamNode,
                            Optimizer::ExecutionNodeWLock lockedDownstreamNode) {
    const auto& upstreamSubPlans = lockedUpstreamNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
    std::unordered_map<Network::NesPartition, LogicalOperatorPtr> upstreamSinkMap;
    auto downstreamWorkerId = lockedDownstreamNode->operator*()->getId();
    for (const auto& subPlan : upstreamSubPlans) {
        for (const auto& sinkOperator : subPlan->getSinkOperators()) {
            auto upstreamNetworkSinkDescriptor =
                std::dynamic_pointer_cast<Network::NetworkSinkDescriptor>(sinkOperator->getSinkDescriptor());
            if (upstreamNetworkSinkDescriptor
                && upstreamNetworkSinkDescriptor->getNodeLocation().getNodeId() == downstreamWorkerId) {
                upstreamSinkMap.insert({upstreamNetworkSinkDescriptor->getNesPartition(), sinkOperator});
            }
        }
    }

    const auto& downstreamSubPlans = lockedDownstreamNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
    auto upstreamWorkerId = lockedUpstreamNode->operator*()->getId();
    std::vector<std::pair<LogicalOperatorPtr, LogicalOperatorPtr>> pairs;
    for (const auto& subPlan : downstreamSubPlans) {
        for (const auto& sourceOperator : subPlan->getSourceOperators()) {
            auto downNetworkSourceDescriptor =
                std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(sourceOperator->getSourceDescriptor());
            if (downNetworkSourceDescriptor && downNetworkSourceDescriptor->getNodeLocation().getNodeId() == upstreamWorkerId) {
                pairs.emplace_back(upstreamSinkMap.at(downNetworkSourceDescriptor->getNesPartition()), sourceOperator);
            }
        }
    }
    return pairs;
}
}// namespace NES::Experimental
