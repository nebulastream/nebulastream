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
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/DeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <RequestProcessor/RequestTypes/TopologyNodeRelocationRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/IncrementalPlacementUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TopologyLinkInformation.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <queue>

namespace NES::RequestProcessor::Experimental {

TopologyNodeRelocationRequestPtr TopologyNodeRelocationRequest::create(const std::vector<TopologyLinkInformation>& removedLinks,
                                                                       const std::vector<TopologyLinkInformation>& addedLinks,
                                                                       uint8_t maxRetries) {
    return std::make_shared<TopologyNodeRelocationRequest>(removedLinks, addedLinks, maxRetries);
}

TopologyNodeRelocationRequest::TopologyNodeRelocationRequest(const std::vector<TopologyLinkInformation>& removedLinks,
                                                             const std::vector<TopologyLinkInformation>& addedLinks,
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

std::vector<AbstractRequestPtr> TopologyNodeRelocationRequest::executeRequestLogic(const StorageHandlerPtr& storageHandle) {
    topology = storageHandle->getTopologyHandle(requestId);
    globalQueryPlan = storageHandle->getGlobalQueryPlanHandle(requestId);
    globalExecutionPlan = storageHandle->getGlobalExecutionPlanHandle(requestId);
    sourceCatalog = storageHandle->getSourceCatalogHandle(requestId);
    udfCatalog = storageHandle->getUDFCatalogHandle(requestId);
    coordinatorConfiguration = storageHandle->getCoordinatorConfiguration(requestId);
    queryCatalog = storageHandle->getQueryCatalogHandle(requestId);

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

    if (!removedLinks.empty()) {
        //identify operators to be replaced
        auto [upstreamId, downstreamId] = removedLinks.front();
        auto impactedSharedQueryIds = identifyImpactedSharedQueries(upstreamId, downstreamId);

        // Step3. Perform operator placement amendment for impacted SQPs and deploy
        for (const auto& impactedSharedQueryId : impactedSharedQueryIds) {

            //Amendment phase
            auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
            auto placementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                           topology,
                                                                                           typeInferencePhase,
                                                                                           coordinatorConfiguration);
            auto impactedSharedQuery = globalQueryPlan->getSharedQueryPlan(impactedSharedQueryId);
            auto deploymentContexts = placementAmendmentPhase->execute(impactedSharedQuery);

            //deployment phase
            auto deploymentPhase = DeploymentPhase::create(queryCatalog);
            deploymentPhase->execute(deploymentContexts, RequestType::AddQuery);

            // Iterate over deployment context and update execution plan
            for (const auto& deploymentContext : deploymentContexts) {
                auto executionNodeId = deploymentContext->getWorkerId();
                auto sharedQueryId = deploymentContext->getSharedQueryId();
                auto decomposedQueryPlanId = deploymentContext->getDecomposedQueryPlanId();
                auto decomposedQueryPlanVersion = deploymentContext->getDecomposedQueryPlanVersion();
                auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
                switch (decomposedQueryPlanState) {
                    case QueryState::MARKED_FOR_REDEPLOYMENT:
                    case QueryState::MARKED_FOR_DEPLOYMENT: {
                        globalExecutionPlan->updateDecomposedQueryPlanState(executionNodeId,
                                                                            sharedQueryId,
                                                                            decomposedQueryPlanId,
                                                                            decomposedQueryPlanVersion,
                                                                            QueryState::DEPLOYED);
                    }
                    case QueryState::MARKED_FOR_MIGRATION: {
                        globalExecutionPlan->updateDecomposedQueryPlanState(executionNodeId,
                                                                            sharedQueryId,
                                                                            decomposedQueryPlanId,
                                                                            decomposedQueryPlanVersion,
                                                                            QueryState::STOPPED);
                        globalExecutionPlan->removeDecomposedQueryPlan(executionNodeId,
                                                                       sharedQueryId,
                                                                       decomposedQueryPlanId,
                                                                       decomposedQueryPlanVersion);
                    }
                    default:
                        NES_WARNING("Unhandled Deployment context with status: {}",
                                    magic_enum::enum_name(QueryState::MARKED_FOR_MIGRATION));
                }
            }
        }
        globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    }
    responsePromise.set_value(std::make_shared<TopologyNodeRelocationRequestResponse>(true));
    return {};
}

std::set<WorkerId> TopologyNodeRelocationRequest::identifyImpactedSharedQueries(WorkerId upstreamNodeId,
                                                                                WorkerId downstreamNodeId) {

    // Step1. Identify the impacted SQPs
    auto upstreamExecutionNode = globalExecutionPlan->getLockedExecutionNode(upstreamNodeId);
    auto downstreamExecutionNode = globalExecutionPlan->getLockedExecutionNode(downstreamNodeId);
    //If any of the two execution nodes do not exist then skip rest of the operation
    if (!upstreamExecutionNode || !downstreamExecutionNode) {
        NES_INFO("Removing topology link {}->{} has no effect on the running queries", upstreamNodeId, downstreamNodeId);
        return {};
    }

    auto upstreamSharedQueryIds = upstreamExecutionNode->operator*()->getPlacedSharedQueryPlanIds();
    auto downstreamSharedQueryIds = downstreamExecutionNode->operator*()->getPlacedSharedQueryPlanIds();
    //If any of the two execution nodes do not have any shared query plan placed then skip rest of the operation
    if (upstreamSharedQueryIds.empty() || downstreamSharedQueryIds.empty()) {
        NES_INFO("Removing topology link {}->{} has no effect on the running queries", upstreamNodeId, downstreamNodeId);
        return {};
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
        return {};
    }

    //Iterate over each shared query plan id and identify the operators that need to be replaced
    for (auto impactedSharedQueryId : impactedSharedQueryIds) {
        // Step2. Mark operators for re-placements

        //Fetch the shared query plan and update its status
        auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(impactedSharedQueryId);
        sharedQueryPlan->setStatus(SharedQueryPlanStatus::MIGRATING);

        queryCatalog->updateSharedQueryStatus(impactedSharedQueryId, QueryState::MIGRATING, "");

        //find the pinned operators for the changelog
        auto [upstreamOperatorIds, downstreamOperatorIds] =
            NES::Experimental::findUpstreamAndDownstreamPinnedOperators(sharedQueryPlan,
                                                                        upstreamExecutionNode,
                                                                        downstreamExecutionNode,
                                                                        topology);
        //perform re-operator placement on the query plan
        sharedQueryPlan->performReOperatorPlacement(upstreamOperatorIds, downstreamOperatorIds);
    }

    return impactedSharedQueryIds;
}

//todo #4493: call from this when all links to and from a node are removed
//void TopologyNodeRelocationRequest::processRemoveTopologyNodeRequest(WorkerId removedNodeId) {
//
//    //1. If the removed execution nodes do not exist then remove skip rest of the operation
//    auto removedExecutionNode = globalExecutionPlan->getExecutionNodeById(removedNodeId);
//    if (!removedExecutionNode) {
//        NES_INFO("Removing node {} has no effect on the running queries as there are no queries "
//                 "placed on the node.",
//                 removedNodeId);
//        return;
//    }
//
//    //2. If the removed execution nodes does not have any shared query plan placed then skip rest of the operation
//    auto impactedSharedQueryIds = removedExecutionNode->getPlacedSharedQueryPlanIds();
//    if (impactedSharedQueryIds.empty()) {
//        NES_INFO("Removing node {} has no effect on the running queries as there are no queries placed "
//                 "on the node.",
//                 removedNodeId);
//        return;
//    }
//
//    //3. Get the topology node with removed node id
//    TopologyNodePtr removedTopologyNode = topology->getCopyOfTopologyNodeWithId(removedNodeId);
//
//    //4. Fetch upstream and downstream topology nodes connected via the removed topology node
//    auto downstreamTopologyNodes = removedTopologyNode->getParents();
//    auto upstreamTopologyNodes = removedTopologyNode->getChildren();
//
//    //5. If the topology node either do not have upstream or downstream node then fail the request
//    if (upstreamTopologyNodes.empty() || downstreamTopologyNodes.empty()) {
//        //FIXME: how to handle this case? If the node to remove has physical source then we may need to kill the
//        // whole query.
//        NES_NOT_IMPLEMENTED();
//    }
//
//    //todo: capy block and place function above
//    //6. Iterate over all upstream and downstream topology node pairs and try to mark operators for re-placement
//    for (auto const& upstreamTopologyNode : upstreamTopologyNodes) {
//        for (auto const& downstreamTopologyNode : downstreamTopologyNodes) {
//
//            //6.1. Iterate over impacted shared query plan ids to identify the shared query plans placed on the
//            // upstream and downstream execution nodes
//            for (auto const& impactedSharedQueryId : impactedSharedQueryIds) {
//
//                auto upstreamExecutionNode =
//                    globalExecutionPlan->getExecutionNodeById(upstreamTopologyNode->as<TopologyNode>()->getId());
//                auto downstreamExecutionNode =
//                    globalExecutionPlan->getExecutionNodeById(downstreamTopologyNode->as<TopologyNode>()->getId());
//
//                //6.2. If there exists no upstream or downstream execution nodes than skip rest of the operation
//                if (!upstreamExecutionNode || !downstreamExecutionNode) {
//                    continue;
//                }
//
//                //6.3. Only process the upstream and downstream execution node pairs when both have shared query plans
//                // with the impacted shared query id
//                if (upstreamExecutionNode->hasQuerySubPlans(impactedSharedQueryId)
//                    && downstreamExecutionNode->hasQuerySubPlans(impactedSharedQueryId)) {
//                    markOperatorsForReOperatorPlacement(impactedSharedQueryId, upstreamExecutionNode, downstreamExecutionNode);
//                }
//            }
//        }
//    }
//}

//todo #4494: implement all the following functions
void TopologyNodeRelocationRequest::preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}

std::vector<AbstractRequestPtr> TopologyNodeRelocationRequest::rollBack(std::exception_ptr, const StorageHandlerPtr&) {
    return {};
}

void TopologyNodeRelocationRequest::postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}

};// namespace NES::RequestProcessor::Experimental
