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
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <RequestProcessor/RequestTypes/TopologyChangeRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <queue>
namespace NES::RequestProcessor::Experimental {

TopologyChangeRequestPtr TopologyChangeRequest::create() {
    return std::shared_ptr<TopologyChangeRequest>();
}

//todo: for testing, remove
TopologyChangeRequest::TopologyChangeRequest(uint8_t maxRetries,
                                             TopologyPtr topology,
                                             GlobalQueryPlanPtr globalQueryPlan,
                                             Optimizer::GlobalExecutionPlanPtr globalExecutionPlan)
    : AbstractUniRequest({}, maxRetries), topology(topology), globalQueryPlan(globalQueryPlan), globalExecutionPlan(globalExecutionPlan) {}

std::vector<AbstractRequestPtr>
TopologyChangeRequest::executeRequestLogic(const StorageHandlerPtr& storageHandle) {
    topology = storageHandle->getTopologyHandle(requestId);
//    auto queryCatalogService = storageHandle->getQueryCatalogServiceHandle(requestId);
//    auto sourceCatalog = storageHandle->getSourceCatalogHandle(requestId);
    globalQueryPlan = storageHandle->getGlobalQueryPlanHandle(requestId);
//    auto globalQueryPlanUpdatePhase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology, queryCatalogService, );
globalExecutionPlan = storageHandle->getGlobalExecutionPlanHandle(requestId);
    return {};
}

void TopologyChangeRequest::processRemoveTopologyLinkRequest(WorkerId upstreamNodeId, WorkerId downstreamNodeId) {
    auto upstreamExecutionNode = globalExecutionPlan->getExecutionNodeById(upstreamNodeId);
    auto downstreamExecutionNode = globalExecutionPlan->getExecutionNodeById(downstreamNodeId);
    //If any of the two execution nodes do not exist then skip rest of the operation
    if (!upstreamExecutionNode || !downstreamExecutionNode) {
        NES_INFO("Removing topology link {}->{} has no effect on the running queries", upstreamNodeId, downstreamNodeId);
        return;
    }

    auto upstreamSharedQueryIds = upstreamExecutionNode->getPlacedSharedQueryPlanIds();
    auto downstreamSharedQueryIds = downstreamExecutionNode->getPlacedSharedQueryPlanIds();
    //If any of the two execution nodes do not have any shared query plan placed then skip rest of the operation
    if (upstreamSharedQueryIds.empty() || downstreamSharedQueryIds.empty()) {
        NES_INFO("Removing topology link {}->{} has no effect on the running queries", upstreamNodeId, downstreamNodeId);
        return;
    }

    //compute intersection among the shared query plans placed on two nodes
    std::set<SharedQueryId> impactedSharedQueryIds;
    std::set_intersection(upstreamSharedQueryIds.begin(),
                          upstreamSharedQueryIds.end(),
                          downstreamSharedQueryIds.begin(),
                          downstreamSharedQueryIds.end(),
                          std::inserter(impactedSharedQueryIds, impactedSharedQueryIds.begin()));

    //If no common shared query plan was found to be placed on two nodes then skip rest of the operation
    if (impactedSharedQueryIds.empty()) {
        NES_INFO("Found no shared query plan that was using the removed link");
        return;
    }

    //todo: replace
    //Iterate over each shared query plan id and identify the operators that need to be replaced
    for (auto impactedSharedQueryId : impactedSharedQueryIds) {
        markOperatorsForReOperatorPlacement(impactedSharedQueryId, upstreamExecutionNode, downstreamExecutionNode);
    }
}

void TopologyChangeRequest::processRemoveTopologyNodeRequest(WorkerId removedNodeId) {

    //1. If the removed execution nodes do not exist then remove skip rest of the operation
    auto removedExecutionNode = globalExecutionPlan->getExecutionNodeById(removedNodeId);
    if (!removedExecutionNode) {
        NES_INFO("Removing node {} has no effect on the running queries as there are no queries "
                 "placed on the node.",
                 removedNodeId);
        return;
    }

    //2. If the removed execution nodes does not have any shared query plan placed then skip rest of the operation
    auto impactedSharedQueryIds = removedExecutionNode->getPlacedSharedQueryPlanIds();
    if (impactedSharedQueryIds.empty()) {
        NES_INFO("Removing node {} has no effect on the running queries as there are no queries placed "
                 "on the node.",
                 removedNodeId);
        return;
    }

    //3. Get the topology node with removed node id
    TopologyNodePtr removedTopologyNode = topology->getCopyOfTopologyNodeWithId(removedNodeId);

    //4. Fetch upstream and downstream topology nodes connected via the removed topology node
    auto downstreamTopologyNodes = removedTopologyNode->getParents();
    auto upstreamTopologyNodes = removedTopologyNode->getChildren();

    //5. If the topology node either do not have upstream or downstream node then fail the request
    if (upstreamTopologyNodes.empty() || downstreamTopologyNodes.empty()) {
        //FIXME: how to handle this case? If the node to remove has physical source then we may need to kill the
        // whole query.
        NES_NOT_IMPLEMENTED();
    }

    //todo: capy block and place function above
    //6. Iterate over all upstream and downstream topology node pairs and try to mark operators for re-placement
    for (auto const& upstreamTopologyNode : upstreamTopologyNodes) {
        for (auto const& downstreamTopologyNode : downstreamTopologyNodes) {

            //6.1. Iterate over impacted shared query plan ids to identify the shared query plans placed on the
            // upstream and downstream execution nodes
            for (auto const& impactedSharedQueryId : impactedSharedQueryIds) {

                auto upstreamExecutionNode =
                    globalExecutionPlan->getExecutionNodeById(upstreamTopologyNode->as<TopologyNode>()->getId());
                auto downstreamExecutionNode =
                    globalExecutionPlan->getExecutionNodeById(downstreamTopologyNode->as<TopologyNode>()->getId());

                //6.2. If there exists no upstream or downstream execution nodes than skip rest of the operation
                if (!upstreamExecutionNode || !downstreamExecutionNode) {
                    continue;
                }

                //6.3. Only process the upstream and downstream execution node pairs when both have shared query plans
                // with the impacted shared query id
                if (upstreamExecutionNode->hasRegisteredDecomposedQueryPlans(impactedSharedQueryId)
                    && downstreamExecutionNode->hasRegisteredDecomposedQueryPlans(impactedSharedQueryId)) {
                    markOperatorsForReOperatorPlacement(impactedSharedQueryId, upstreamExecutionNode, downstreamExecutionNode);
                }
            }
        }
    }
}

void TopologyChangeRequest::markOperatorsForReOperatorPlacement(SharedQueryId sharedQueryPlanId,
                                                                     const Optimizer::ExecutionNodePtr& upstreamExecutionNode,
                                                                     const Optimizer::ExecutionNodePtr& downstreamExecutionNode) {

    //    //1. Iterate over all upstream sub query plans and extract the operator id of most upstream non-system
    //    // generated (anything except Network Sink or Source) operator.
    //    std::set<OperatorId> upstreamOperatorIds;
    //    getUpstreamPinnedOperatorIds(sharedQueryPlanId, upstreamExecutionNode, upstreamOperatorIds);
    //
    //    //2. Iterate over all sub query plans in the downstream execution node and extract the operator ids of most upstream non-system
    //    // generated (anything except Network Sink or Source) operator.
    //    std::set<OperatorId> downstreamOperatorIds;
    //    getDownstreamPinnedOperatorIds(sharedQueryPlanId, downstreamExecutionNode, downstreamOperatorIds);

    //Note to self - do we need to consider the possibility that a sub query plan that is placed on the given upstream node does not
    // communicate with the given downstream node?

    //3. Fetch the shared query plan
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryPlanId);

    auto [upstreamOperatorIds, downstreamOperatorIds] =
        findAffectedTopologySubGraph(sharedQueryPlanId, upstreamExecutionNode, downstreamExecutionNode);
    //4. Mark the operators for re-operator placement
    sharedQueryPlan->performReOperatorPlacement(upstreamOperatorIds, downstreamOperatorIds);
}

std::pair<LogicalOperatorNodePtr, WorkerId>
TopologyChangeRequest::findUpstreamNonSystemOperators(const LogicalOperatorNodePtr& downstreamOperator,
                                                           WorkerId downstreamWorkerId,
                                                           SharedQueryId sharedQueryId) {
    //find the upstream non system generated operators of the sink
    std::queue<std::pair<LogicalOperatorNodePtr, WorkerId>> queue;
    queue.emplace(downstreamOperator, downstreamWorkerId);
    std::set<OperatorId> visited;
    std::vector<LogicalOperatorNodePtr> upstreamAffectedOperators;

    while (!queue.empty()) {
        const auto& [currentOperator, workerId] = queue.front();
        queue.pop();
        auto currentId = currentOperator->getId();
        if (visited.contains(currentId)) {
            continue;
        }
        visited.insert(currentId);
        auto sinkOperator = currentOperator->as_if<SinkLogicalOperatorNode>();
        if (sinkOperator) {
            auto networkSinkDescriptor =
                std::dynamic_pointer_cast<Network::NetworkSinkDescriptor>(sinkOperator->getSinkDescriptor());
            if (networkSinkDescriptor) {
                for (const auto& child : currentOperator->getChildren()) {
                    queue.emplace(child->as<LogicalOperatorNode>(), workerId);
                }
                continue;
            }
            return {currentOperator->as<LogicalOperatorNode>(), workerId};
        }

        auto sourceOperator = currentOperator->as_if<SourceLogicalOperatorNode>();
        if (sourceOperator) {
            auto networkSourceDescriptor =
                std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(sourceOperator->getSourceDescriptor());
            if (networkSourceDescriptor) {
                auto upstreamSinkAndWorker = findUpstreamNetworkSinkAndWorkerId(sharedQueryId, workerId, networkSourceDescriptor);
                queue.push(upstreamSinkAndWorker);
                continue;
            }
            return {currentOperator->as<LogicalOperatorNode>(), workerId};
        }

        //operator is not system generated, do not insert children into queue and record it as upstream user generated operator
        return {currentOperator->as<LogicalOperatorNode>(), workerId};
    }
    //todo: throw a specific exception instead of failing assertion
    NES_ASSERT(false, "No upstream operator found for the operator with the id " << downstreamOperator->getId());
    return {};
}

std::pair<LogicalOperatorNodePtr, WorkerId>
TopologyChangeRequest::findDownstreamNonSystemOperators(const LogicalOperatorNodePtr& upstreamOperator,
                                                             WorkerId upstreamWorkerId,
                                                             SharedQueryId sharedQueryId) {
    //find the upstream non system generated operators of the sink
    std::queue<std::pair<LogicalOperatorNodePtr, WorkerId>> queue;
    queue.emplace(upstreamOperator, upstreamWorkerId);
    std::set<OperatorId> visited;
    std::vector<LogicalOperatorNodePtr> upstreamAffectedOperators;

    while (!queue.empty()) {
        const auto& [currentOperator, workerId] = queue.front();
        queue.pop();
        auto currentId = currentOperator->getId();
        if (visited.contains(currentId)) {
            continue;
        }
        visited.insert(currentId);
        auto sinkOperator = currentOperator->as_if<SinkLogicalOperatorNode>();
        if (sinkOperator) {
            auto networkSinkDescriptor =
                std::dynamic_pointer_cast<Network::NetworkSinkDescriptor>(sinkOperator->getSinkDescriptor());
            if (networkSinkDescriptor) {
                auto downstreamSourceAndWorker =
                    findDownstreamNetworkSourceAndWorkerId(sharedQueryId, workerId, networkSinkDescriptor);
                queue.push(downstreamSourceAndWorker);
                continue;
            }
            return {currentOperator->as<LogicalOperatorNode>(), workerId};
        }

        auto sourceOperator = currentOperator->as_if<SourceLogicalOperatorNode>();
        if (sourceOperator) {
            auto networkSourceDescriptor =
                std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(sourceOperator->getSourceDescriptor());
            if (networkSourceDescriptor) {
                for (const auto& parent : currentOperator->getParents()) {
                    queue.emplace(parent->as<LogicalOperatorNode>(), workerId);
                }
                continue;
            }
            return {currentOperator->as<LogicalOperatorNode>(), workerId};
        }

        //operator is not system generated, do not insert children into queue and record it as upstream user generated operator
        return {currentOperator->as<LogicalOperatorNode>(), workerId};
    }
    //todo: throw a specific exception instead of failing assertion
    NES_ASSERT(false, "No upstream operator found for the operator with the id " << upstreamOperator->getId());
    return {};
}

std::pair<std::set<OperatorId>, std::set<OperatorId>>
TopologyChangeRequest::findAffectedTopologySubGraph(const SharedQueryId& sharedQueryPlanId,
                                                         const Optimizer::ExecutionNodePtr& upstreamNode,
                                                         const Optimizer::ExecutionNodePtr& downstreamNode) {

    auto upstreamDownstreamOperatorPairs = findNetworkOperatorsForLink(sharedQueryPlanId, upstreamNode, downstreamNode);
    std::vector<std::pair<std::pair<LogicalOperatorNodePtr, WorkerId>, std::pair<LogicalOperatorNodePtr, WorkerId>>>
        upstreamDownstremOperatorAndNodePairs;
    for (auto& [upstreamOperator, downstreamOperator] : upstreamDownstreamOperatorPairs) {
        auto upstreamPair = findUpstreamNonSystemOperators(upstreamOperator, upstreamNode->getId(), sharedQueryPlanId);
        auto downstreamPair = findDownstreamNonSystemOperators(downstreamOperator, downstreamNode->getId(), sharedQueryPlanId);
        upstreamDownstremOperatorAndNodePairs.emplace_back(upstreamPair, downstreamPair);
    }

    std::set<OperatorId> upstreamPinned;
    std::set<OperatorId> downstreamPinned;
    std::set<OperatorId> toReplace;
    for (const auto& [upstreamPair, downstreamPair] : upstreamDownstremOperatorAndNodePairs) {
        auto& [upstreamOperator, upstreamWorkerId] = upstreamPair;
        auto& [downstreamOperator, downstreamWorkerId] = downstreamPair;

        //assuming that we can always pin this operator will hold as long as only leave nodes are changing their parent
        //in case a node with children changes its parent, this method might not discover some possible paths because it ignores the children
        //to handle that case, a new reachable set needs to be calculated from the children in case the first one fails
        upstreamPinned.insert(upstreamOperator->getId());

        std::set<WorkerId> reachable;
        std::queue<TopologyNodePtr> queue;
        //todo: proper locking how?
        queue.push(topology->lockTopologyNode(upstreamWorkerId)->operator*());

        bool oldDownstreamFound = false;
        //find reachable nodes
        while (!oldDownstreamFound && !queue.empty()) {
            const auto currentNode = queue.front();
            queue.pop();

            for (const auto& parent : currentNode->getParents()) {
                const auto topologyNode = parent->as<TopologyNode>();
                const auto topologyNodeId = topologyNode->getId();

                //check if we have visited this node before
                if (reachable.contains(topologyNodeId)) {
                    continue;
                }

                //check if the node is the old upstream, then we only need to insert forward operators between these nodes
                reachable.insert(topologyNodeId);
                if (topologyNodeId == downstreamWorkerId) {
                    downstreamPinned.insert(downstreamOperator->getId());
                    oldDownstreamFound = true;
                    break;
                }
                queue.push(parent->as<TopologyNode>());
            }
        }

        if (!oldDownstreamFound) {
            //at this point all reachable downstream nodes in the new topology are found, now find old path
            std::queue<std::pair<LogicalOperatorNodePtr, WorkerId>> queryPlanBFSQueue;
            std::set<OperatorId> visitedOperators;

            //populate queue with the non system  parents of the upstream operator
            for (const auto& parent : upstreamOperator->getParents()) {
                auto downstreamNonSystemPair =
                    findDownstreamNonSystemOperators(parent->as<LogicalOperatorNode>(), upstreamWorkerId, sharedQueryPlanId);
                queryPlanBFSQueue.push(downstreamNonSystemPair);
            }

            while (!queryPlanBFSQueue.empty()) {
                const auto [currentOperator, currentWorkerId] = queryPlanBFSQueue.front();
                queryPlanBFSQueue.pop();
                if (visitedOperators.contains(currentOperator->getId())) {
                    continue;
                }
                visitedOperators.insert(currentOperator->getId());
                upstreamPinned.erase(currentOperator->getId());
                if (reachable.contains(currentWorkerId)) {
                    downstreamPinned.insert(currentOperator->getId());
                } else {
                    //if the operator was inserted in a previous iteration but is not reachable now, we need to remove it again
                    downstreamPinned.erase(currentOperator->getId());
                    for (const auto& parent : currentOperator->getParents()) {
                        auto downstreamNonSystemPair = findDownstreamNonSystemOperators(parent->as<LogicalOperatorNode>(),
                                                                                        currentWorkerId,
                                                                                        sharedQueryPlanId);
                        queryPlanBFSQueue.push(downstreamNonSystemPair);
                    }
                    toReplace.insert(currentOperator->getId());
                    for (const auto& child : currentOperator->getChildren()) {
                        auto [nonSystemChild, nonSystemChildHost] =
                            findUpstreamNonSystemOperators(child->as<LogicalOperatorNode>(), currentWorkerId, sharedQueryPlanId);
                        if (!toReplace.contains(nonSystemChild->getId())) {
                            upstreamPinned.insert(nonSystemChild->getId());
                        }
                    }
                }
            }
        }
    }

    //return {{upstreamPinned.begin(), upstreamPinned.end()}, {downstreamPinned.begin(), downstreamPinned.end()}};
    return {upstreamPinned, downstreamPinned};
}

std::vector<std::pair<LogicalOperatorNodePtr, LogicalOperatorNodePtr>>
TopologyChangeRequest::findNetworkOperatorsForLink(const SharedQueryId& sharedQueryPlanId,
                                                        const Optimizer::ExecutionNodePtr& upstreamNode,
                                                        const Optimizer::ExecutionNodePtr& downstreamNode) {
    const auto& upstreamSubPlans = upstreamNode->getAllDecomposedQueryPlans(sharedQueryPlanId);
    std::unordered_map<Network::NesPartition, LogicalOperatorNodePtr> upstreamSinkMap;
    auto downstreamWorkerId = downstreamNode->getId();
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

    const auto& downstreamSubPlans = downstreamNode->getAllDecomposedQueryPlans(sharedQueryPlanId);
    auto upstreamWorkerId = upstreamNode->getId();
    std::vector<std::pair<LogicalOperatorNodePtr, LogicalOperatorNodePtr>> pairs;
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

std::pair<SinkLogicalOperatorNodePtr, WorkerId> TopologyChangeRequest::findUpstreamNetworkSinkAndWorkerId(
    const SharedQueryId& sharedQueryPlanId,
    const WorkerId workerId,
    const Network::NetworkSourceDescriptorPtr& networkSourceDescriptor) {
    auto childNodeId = networkSourceDescriptor->getNodeLocation().getNodeId();
    auto subPlans = globalExecutionPlan->getExecutionNodeById(childNodeId)->getAllDecomposedQueryPlans(sharedQueryPlanId);
    for (const auto& plan : subPlans) {
        for (const auto& sinkOperator : plan->getSinkOperators()) {
            auto upstreamNetworkSinkDescriptor =
                std::dynamic_pointer_cast<Network::NetworkSinkDescriptor>(sinkOperator->getSinkDescriptor());
            if (upstreamNetworkSinkDescriptor && upstreamNetworkSinkDescriptor->getNodeLocation().getNodeId() == workerId
                && upstreamNetworkSinkDescriptor->getNesPartition() == networkSourceDescriptor->getNesPartition()) {
                return {sinkOperator, childNodeId};
            }
        }
    }
    throw std::exception();
}

std::pair<SourceLogicalOperatorNodePtr, WorkerId> TopologyChangeRequest::findDownstreamNetworkSourceAndWorkerId(
    const SharedQueryId& sharedQueryPlanId,
    const WorkerId workerId,
    const Network::NetworkSinkDescriptorPtr& networkSinkDescriptor) {
    auto parentNodeId = networkSinkDescriptor->getNodeLocation().getNodeId();
    auto subPlans = globalExecutionPlan->getExecutionNodeById(parentNodeId)->getAllDecomposedQueryPlans(sharedQueryPlanId);
    for (const auto& plan : subPlans) {
        for (const auto& sourceOperator : plan->getSourceOperators()) {
            auto downNetworkSourceDescriptor =
                std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(sourceOperator->getSourceDescriptor());
            if (downNetworkSourceDescriptor && downNetworkSourceDescriptor->getNodeLocation().getNodeId() == workerId
                && downNetworkSourceDescriptor->getNesPartition() == networkSinkDescriptor->getNesPartition()) {
                return {sourceOperator, parentNodeId};
            }
        }
    }
    throw std::exception();
}
void TopologyChangeRequest::preRollbackHandle(std::exception_ptr, const StorageHandlerPtr& ) {}
std::vector<AbstractRequestPtr> TopologyChangeRequest::rollBack(std::exception_ptr , const StorageHandlerPtr& ) {
    return std::vector<AbstractRequestPtr>();
}
void TopologyChangeRequest::postRollbackHandle(std::exception_ptr , const StorageHandlerPtr& ) {}
void TopologyChangeRequest::postExecution(const StorageHandlerPtr&) {}
};// namespace NES::RequestProcessor::Experimental
