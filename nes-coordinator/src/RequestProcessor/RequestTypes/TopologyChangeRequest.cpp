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
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <RequestProcessor/RequestTypes/TopologyChangeRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <queue>
namespace NES::RequestProcessor::Experimental {

TopologyChangeRequestPtr TopologyChangeRequest::create(std::vector<std::pair<WorkerId, WorkerId>> removedLinks,
                                                       std::vector<std::pair<WorkerId, WorkerId>> addedLinks,
                                                       uint8_t maxRetries) {
    return std::make_shared<TopologyChangeRequest>(removedLinks, addedLinks, maxRetries);
}

TopologyChangeRequest::TopologyChangeRequest(std::vector<std::pair<WorkerId, WorkerId>> removedLinks,
                                             std::vector<std::pair<WorkerId, WorkerId>> addedLinks,
                                             uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::Topology,
                          ResourceType::GlobalQueryPlan,
                          ResourceType::GlobalExecutionPlan,
                          ResourceType::SourceCatalog,
                          ResourceType::UdfCatalog,
                          ResourceType::CoordinatorConfiguration,
                          ResourceType::QueryCatalogService},
                         maxRetries),
      removedLinks(removedLinks), addedLinks(addedLinks) {
    NES_ASSERT(!(removedLinks.empty() && addedLinks.empty()), "Could not find any removed or added links");
}

std::vector<AbstractRequestPtr>
TopologyChangeRequest::executeRequestLogic(const StorageHandlerPtr& storageHandle) {
    topology = storageHandle->getTopologyHandle(requestId);
    globalQueryPlan = storageHandle->getGlobalQueryPlanHandle(requestId);
    globalExecutionPlan = storageHandle->getGlobalExecutionPlanHandle(requestId);
    sourceCatalog = storageHandle->getSourceCatalogHandle(requestId);
    udfCatalog = storageHandle->getUDFCatalogHandle(requestId);
    coordinatorConfiguration = storageHandle->getCoordinatorConfiguration(requestId);
    queryCatalogService = storageHandle->getQueryCatalogServiceHandle(requestId);

    //no function yet to process multiple removed links
    if (removedLinks.size() > 1) {
        NES_NOT_IMPLEMENTED();
    }

    //make modifications to topology
    for (const auto& [removedUp, removedDown] : removedLinks) {
        topology->removeTopologyNodeAsChild(removedDown, removedUp);
    }
    for (const auto& [addedUp, addedDown] : addedLinks) {
        topology->addTopologyNodeAsChild(addedDown, addedUp);
    }

    //identigy operators to be replaced
    auto [upstreamId, downstreamId] = removedLinks.front();
    processRemoveTopologyLinkRequest(upstreamId, downstreamId);

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
        //todo: how do we turn this back again
        //queryCatalogService->checkAndMarkForMigration(impactedSharedQueryId, QueryState::MIGRATING);
        markOperatorsForReOperatorPlacement(impactedSharedQueryId, upstreamExecutionNode, downstreamExecutionNode);
    }
}

//todo: do we need this?
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
    sharedQueryPlan->setStatus(SharedQueryPlanStatus::MIGRATING);

    auto [upstreamOperatorIds, downstreamOperatorIds] =
        findAffectedTopologySubGraph(sharedQueryPlan, upstreamExecutionNode, downstreamExecutionNode, topology, globalExecutionPlan);
    //4. Mark the operators for re-operator placement
    sharedQueryPlan->performReOperatorPlacement(upstreamOperatorIds, downstreamOperatorIds);

    //ammendment phase
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto amendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                          topology,
                                                                          typeInferencePhase,
                                                                          coordinatorConfiguration);
    amendmentPhase->execute(sharedQueryPlan);

    //todo: set state to migrating?
    //todo: do this for all the affected subplans? which of this is the amendment phase already doing?
    //    crd->getQueryCatalogService()->checkAndMarkForMigration(sharedQueryId, oldSubplanId, QueryState::MIGRATING);
    //    //todo: make sure the state on the query plan is set in the right phase
    //    crd->getGlobalExecutionPlan()
    //        ->getExecutionNodeById(oldWorker->getWorkerId())
    //        ->getQuerySubPlans(sharedQueryId)
    //        .front()
    //        ->setQueryState(QueryState::MARKED_FOR_MIGRATION);

    //todo: instead of the above we probably only need to set the entry itself as migrating

    //deployment phase
    //todo: make member var
    auto queryDeploymentPhase = QueryDeploymentPhase::create(globalExecutionPlan, queryCatalogService, coordinatorConfiguration);
    queryDeploymentPhase->execute(sharedQueryPlan);

    //todo: do we still have to modify the status here?

    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();

    //todo: do we still need the workaround found in the processor service?
}

LogicalOperatorNodePtr TopologyChangeRequest::findUpstreamNonSystemOperators(const LogicalOperatorNodePtr& downstreamOperator,
                                                                             WorkerId downstreamWorkerId,
                                                                             SharedQueryId sharedQueryId,
                                                                             const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan) {
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
            return currentOperator->as<LogicalOperatorNode>();
        }

        auto sourceOperator = currentOperator->as_if<SourceLogicalOperatorNode>();
        if (sourceOperator) {
            auto networkSourceDescriptor =
                std::dynamic_pointer_cast<Network::NetworkSourceDescriptor>(sourceOperator->getSourceDescriptor());
            if (networkSourceDescriptor) {
                auto upstreamSinkAndWorker =
                    findUpstreamNetworkSinkAndWorkerId(sharedQueryId, workerId, networkSourceDescriptor, globalExecutionPlan);
                queue.push(upstreamSinkAndWorker);
                continue;
            }
            return currentOperator->as<LogicalOperatorNode>();
        }

        //operator is not system generated, do not insert children into queue and record it as upstream user generated operator
        return currentOperator->as<LogicalOperatorNode>();
    }
    //todo: throw a specific exception instead of failing assertion
    NES_ASSERT(false, "No upstream operator found for the operator with the id " << downstreamOperator->getId());
    return {};
}

LogicalOperatorNodePtr
TopologyChangeRequest::findDownstreamNonSystemOperators(const LogicalOperatorNodePtr& upstreamOperator,
                                                        WorkerId upstreamWorkerId,
                                                        SharedQueryId sharedQueryId,
                                                        const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan) {
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
                    findDownstreamNetworkSourceAndWorkerId(sharedQueryId, workerId, networkSinkDescriptor, globalExecutionPlan);
                queue.push(downstreamSourceAndWorker);
                continue;
            }
            return currentOperator->as<LogicalOperatorNode>();
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
            return currentOperator->as<LogicalOperatorNode>();
        }

        //operator is not system generated, do not insert children into queue and record it as upstream user generated operator
        return currentOperator->as<LogicalOperatorNode>();
    }
    //todo: throw a specific exception instead of failing assertion
    NES_ASSERT(false, "No upstream operator found for the operator with the id " << upstreamOperator->getId());
    return {};
}

std::pair<std::set<OperatorId>, std::set<OperatorId>>
TopologyChangeRequest::findAffectedTopologySubGraph(const SharedQueryPlanPtr& sharedQueryPlan,
                                                    const Optimizer::ExecutionNodePtr& upstreamNode,
                                                    const Optimizer::ExecutionNodePtr& downstreamNode,
                                                    const TopologyPtr& topology,
                                                    const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan) {
    auto sharedQueryPlanId = sharedQueryPlan->getId();
    //find the pairs of source and sink operators that were using the removed link
    auto upstreamDownstreamOperatorPairs = findNetworkOperatorsForLink(sharedQueryPlanId, upstreamNode, downstreamNode);
    for (auto& [upstreamOperator, downstreamOperator] : upstreamDownstreamOperatorPairs) {
        //replace the system generated operators with their non system up- or downstream operators
        upstreamOperator =
            findUpstreamNonSystemOperators(upstreamOperator, upstreamNode->getId(), sharedQueryPlanId, globalExecutionPlan);
        downstreamOperator =
            findDownstreamNonSystemOperators(downstreamOperator, downstreamNode->getId(), sharedQueryPlanId, globalExecutionPlan);
    }

    auto queryPlanForSharedQuery = sharedQueryPlan->getQueryPlan();
    std::set<OperatorId> upstreamPinned;
    std::set<OperatorId> downstreamPinned;
    std::set<OperatorId> toRemove;

    for (const auto& [upstreamOperator, downstreamOperator] : upstreamDownstreamOperatorPairs) {
        const auto upstreamSharedQueryOperater = queryPlanForSharedQuery->getOperatorWithId(upstreamOperator->getId());
        const auto upstreamWorkerId =
            std::any_cast<OperatorId>(upstreamSharedQueryOperater->getProperty(Optimizer::PINNED_WORKER_ID));
        const auto downstreamSharedQueryOperator = queryPlanForSharedQuery->getOperatorWithId(downstreamOperator->getId());
        const auto downstreamWorkerId =
            std::any_cast<OperatorId>(downstreamSharedQueryOperator->getProperty(Optimizer::PINNED_WORKER_ID));

        //assuming that we can always pin this operator will hold as long as only leave nodes are changing their parent
        //in case a node with children changes its parent, this method might not discover some possible paths because it ignores the children
        //to handle that case, a new reachable set needs to be calculated from the children in case the first one fails
        upstreamPinned.insert(upstreamOperator->getId());

        //find all toplogy nodes that are reachable from the pinned upstream operator node
        std::set<WorkerId> reachable;
        auto foundTargetNodes = topology->findAllDownstreamNodes(upstreamWorkerId, reachable, {downstreamWorkerId});

        //check if the old downstream was found, then only forward operators need to be inserted between the old up and downstream
        if (!foundTargetNodes.empty()) {
            //only one target node as been supplied, so the vector of found targets can contain one item at most
            downstreamPinned.insert(downstreamOperator->getId());
        } else {
            //because the old downstream was not found (list of found target nodes is empty), another path has to be found

            //at this point all reachable downstream nodes in the new topology are found for a specific operator
            //now find the closest downstream operator hosted on one of these reachable nodes
            std::queue<LogicalOperatorNodePtr> queryPlanBFSQueue;
            std::set<OperatorId> visitedOperators;

            //populate queue with the non system parents of the upstream operator
            auto startOperatorInSharedQueryPlan = queryPlanForSharedQuery->getOperatorWithId(upstreamOperator->getId());
            for (const auto& parent : startOperatorInSharedQueryPlan->getParents()) {
                queryPlanBFSQueue.push(parent->as<LogicalOperatorNode>());
            }

            while (!queryPlanBFSQueue.empty()) {
                const auto currentOperator = queryPlanBFSQueue.front();
                queryPlanBFSQueue.pop();
                if (visitedOperators.contains(currentOperator->getId())) {
                    continue;
                }
                const auto currentWorkerId = std::any_cast<OperatorId>(currentOperator->getProperty(Optimizer::PINNED_WORKER_ID));
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
                        queryPlanBFSQueue.push(parent->as<LogicalOperatorNode>());
                    }
                    toRemove.insert(currentOperator->getId());
                    for (const auto& child : currentOperator->getChildren()) {
                        auto nonSystemChild = child->as<LogicalOperatorNode>();
                        if (!toRemove.contains(nonSystemChild->getId())) {
                            upstreamPinned.insert(nonSystemChild->getId());
                        }
                    }
                }
            }
        }
    }

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

std::pair<SinkLogicalOperatorNodePtr, WorkerId>
TopologyChangeRequest::findUpstreamNetworkSinkAndWorkerId(const SharedQueryId& sharedQueryPlanId,
                                                          const WorkerId workerId,
                                                          const Network::NetworkSourceDescriptorPtr& networkSourceDescriptor,
                                                          const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan) {
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

std::pair<SourceLogicalOperatorNodePtr, WorkerId>
TopologyChangeRequest::findDownstreamNetworkSourceAndWorkerId(const SharedQueryId& sharedQueryPlanId,
                                                              const WorkerId workerId,
                                                              const Network::NetworkSinkDescriptorPtr& networkSinkDescriptor,
                                                              const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan) {
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
void TopologyChangeRequest::preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}
std::vector<AbstractRequestPtr> TopologyChangeRequest::rollBack(std::exception_ptr, const StorageHandlerPtr&) {
    return std::vector<AbstractRequestPtr>();
}
void TopologyChangeRequest::postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}
void TopologyChangeRequest::postExecution(const StorageHandlerPtr&) {}
};// namespace NES::RequestProcessor::Experimental
