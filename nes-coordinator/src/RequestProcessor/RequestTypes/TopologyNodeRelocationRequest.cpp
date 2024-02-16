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
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <RequestProcessor/RequestTypes/TopologyNodeRelocationRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/IncrementalPlacementUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TopologyLinkInformation.hpp>
#include <queue>

namespace NES::RequestProcessor::Experimental {

TopologyNodeRelocationRequestPtr TopologyNodeRelocationRequest::create(const std::vector<TopologyLinkInformation>& removedLinks,
                                                                       const std::vector<TopologyLinkInformation>& addedLinks,
                                                                       const std::vector<TopologyLinkInformation>& expectedRemovedLinks,
                                                                       const std::vector<TopologyLinkInformation>& expectedAddedLinks,
                                                                       Timestamp expectedTime,
                                                                       uint8_t maxRetries) {
    return std::make_shared<TopologyNodeRelocationRequest>(removedLinks, addedLinks, expectedRemovedLinks, expectedAddedLinks, expectedTime, maxRetries);
}

TopologyNodeRelocationRequest::TopologyNodeRelocationRequest(const std::vector<TopologyLinkInformation>& removedLinks,
                                                             const std::vector<TopologyLinkInformation>& addedLinks,
                                                             const std::vector<TopologyLinkInformation>& expectedRemovedLinks,
                                                             const std::vector<TopologyLinkInformation>& expectedAddedLinks,
                                                             Timestamp expectedTime,
                                                             uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::Topology,
                          ResourceType::GlobalQueryPlan,
                          ResourceType::GlobalExecutionPlan,
                          ResourceType::SourceCatalog,
                          ResourceType::UdfCatalog,
                          ResourceType::CoordinatorConfiguration,
                          ResourceType::QueryCatalogService},
                         maxRetries),
      removedLinks(removedLinks), addedLinks(addedLinks), expectedRemovedLinks(expectedRemovedLinks), expectedAddedLinks(expectedAddedLinks), expectedTime(expectedTime) {
    NES_ASSERT(!(removedLinks.empty() && addedLinks.empty()), "Could not find any removed or added links");
}

std::vector<AbstractRequestPtr> TopologyNodeRelocationRequest::executeRequestLogic(const StorageHandlerPtr& storageHandle) {
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

    //todo: make if else condition here checking if proactive deployment is in progress

    std::optional<std::pair<::NES::Experimental::TopologyPrediction::TopologyDelta, std::vector<SharedQueryPlanPtr>>> nextPrediction = std::nullopt;
    if (coordinatorConfiguration->enableProactiveDeployment.getValue()) {
        nextPrediction = topology->getNextPrediction();
    }
    if (nextPrediction) {
        topology->removeNextPrediction();
        auto [delta, impactedPlans] = nextPrediction.value();
        //todo: we currently assume perfect predicitons
        //todo: match prediction for expected and if not, remove it
        //todo: to do that we need a map on node ids and not only timestamps
        NES_ASSERT(delta.getAdded() == addedLinks, "Prediction does not match");
        NES_ASSERT(delta.getRemoved() == removedLinks, "Prediction does not match");
        //deployment phase
        auto queryDeploymentPhase = QueryDeploymentPhase::create(globalExecutionPlan, queryCatalogService, coordinatorConfiguration);
        //todo: only execute partial deployment here
        for (auto sharedQueryPlan : impactedPlans) {
            queryDeploymentPhase->execute(sharedQueryPlan, PROACTIVE_PHASE_2);
            globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
        }
    } else {
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
            processRemoveTopologyLinkRequest(upstreamId, downstreamId);
        }
    }


    if (!expectedRemovedLinks.empty() && coordinatorConfiguration->enableProactiveDeployment) {
        NES_ASSERT(!expectedAddedLinks.empty(), "We currently expect exactly one parent for moving devices");
        NES_ASSERT(expectedTime != 0, "Invalid expected timestamp");
        NES_ASSERT(expectedAddedLinks.size() == 1, "Too many expected added links, multiple parents not supported");
        NES_ASSERT(expectedRemovedLinks.size() == 1, "Too many expected removed links, multiple parents not supported");

        //make modifications to topology
        for (const auto& [removedUp, removedDown] : expectedRemovedLinks) {
            topology->removeTopologyNodeAsChild(removedDown, removedUp);
        }
        for (const auto& [addedUp, addedDown] : expectedAddedLinks) {
            topology->addTopologyNodeAsChild(addedDown, addedUp);
        }
        auto [upstreamId, downstreamId] = expectedRemovedLinks.front();
        auto impactedPlans = proactiveDeployment(upstreamId, downstreamId);
        topology->insertPrediction(expectedRemovedLinks, expectedAddedLinks, impactedPlans, expectedTime);
    }

    responsePromise.set_value(std::make_shared<TopologyNodeRelocationRequestResponse>(true));
    return {};
}

std::vector<SharedQueryPlanPtr> TopologyNodeRelocationRequest::proactiveDeployment(WorkerId upstreamNodeId, WorkerId downstreamNodeId) {
    NES_INFO("Proactive deployment: start")
    auto upstreamExecutionNode = globalExecutionPlan->getExecutionNodeById(upstreamNodeId);
    auto downstreamExecutionNode = globalExecutionPlan->getExecutionNodeById(downstreamNodeId);
    //If any of the two execution nodes do not exist then skip rest of the operation
    if (!upstreamExecutionNode || !downstreamExecutionNode) {
        NES_INFO("Removing topology link {}->{} has no effect on the running queries", upstreamNodeId, downstreamNodeId);
        return {};
    }

    NES_INFO("Proactive Deployment: find affected queries")
    auto upstreamSharedQueryIds = upstreamExecutionNode->getPlacedSharedQueryPlanIds();
    auto downstreamSharedQueryIds = downstreamExecutionNode->getPlacedSharedQueryPlanIds();
    //If any of the two execution nodes do not have any shared query plan placed then skip rest of the operation
    if (upstreamSharedQueryIds.empty() || downstreamSharedQueryIds.empty()) {
        NES_INFO("Removing topology link {}->{} has no effect on the running queries", upstreamNodeId, downstreamNodeId);
        return {};
    }

    NES_INFO("Proactive Deployment: find intersection of queries")
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

    NES_INFO("TopologyNodeRelocatinRequest: perform re-placement")
    //Iterate over each shared query plan id and identify the operators that need to be replaced
    std::vector<SharedQueryPlanPtr> impactedPlans;
    for (auto impactedSharedQueryId : impactedSharedQueryIds) {
        markOperatorsForProactivePlacement(impactedSharedQueryId, upstreamExecutionNode, downstreamExecutionNode);
        impactedPlans.push_back(globalQueryPlan->getSharedQueryPlan(impactedSharedQueryId));
    }
    return impactedPlans;
    NES_INFO("TopologyNodeRelocatinRequest: done")
}

void TopologyNodeRelocationRequest::processRemoveTopologyLinkRequest(WorkerId upstreamNodeId, WorkerId downstreamNodeId) {
    NES_INFO("TopologyNodeRelocatinRequest: start")
    auto upstreamExecutionNode = globalExecutionPlan->getExecutionNodeById(upstreamNodeId);
    auto downstreamExecutionNode = globalExecutionPlan->getExecutionNodeById(downstreamNodeId);
    //If any of the two execution nodes do not exist then skip rest of the operation
    if (!upstreamExecutionNode || !downstreamExecutionNode) {
        NES_INFO("Removing topology link {}->{} has no effect on the running queries", upstreamNodeId, downstreamNodeId);
        return;
    }

    NES_INFO("TopologyNodeRelocatinRequest: find affected queries")
    auto upstreamSharedQueryIds = upstreamExecutionNode->getPlacedSharedQueryPlanIds();
    auto downstreamSharedQueryIds = downstreamExecutionNode->getPlacedSharedQueryPlanIds();
    //If any of the two execution nodes do not have any shared query plan placed then skip rest of the operation
    if (upstreamSharedQueryIds.empty() || downstreamSharedQueryIds.empty()) {
        NES_INFO("Removing topology link {}->{} has no effect on the running queries", upstreamNodeId, downstreamNodeId);
        return;
    }

    NES_INFO("TopologyNodeRelocatinRequest: find intersection of queries")
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

    NES_INFO("TopologyNodeRelocatinRequest: perform re-placement")
    //Iterate over each shared query plan id and identify the operators that need to be replaced
    for (auto impactedSharedQueryId : impactedSharedQueryIds) {
        markOperatorsForReOperatorPlacement(impactedSharedQueryId, upstreamExecutionNode, downstreamExecutionNode);
    }
    NES_INFO("TopologyNodeRelocatinRequest: done")
}

//todo: looks like we actually just need to pass the deployment type to this function and then we're good and can delete this one
void TopologyNodeRelocationRequest::markOperatorsForProactivePlacement(
    SharedQueryId sharedQueryPlanId,
    const Optimizer::ExecutionNodePtr& upstreamExecutionNode,
    const Optimizer::ExecutionNodePtr& downstreamExecutionNode) {
    //Fetch the shared query plan and update its status
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryPlanId);
    sharedQueryPlan->setStatus(SharedQueryPlanStatus::MIGRATING);

    NES_INFO("TopologyNodeRelocationRequest: find up and downstream operators")
    std::set<OperatorId> upstreamOperatorIds, downstreamOperatorIds;
    if (coordinatorConfiguration->enableQueryReconfiguration) {
        //find the pinned operators for the changelog
        auto upAndDownStreamPinned =
            NES::Experimental::findUpstreamAndDownstreamPinnedOperators(sharedQueryPlan,
                                                                        upstreamExecutionNode,
                                                                        downstreamExecutionNode,
                                                                        topology);
        upstreamOperatorIds = upAndDownStreamPinned.first;
        downstreamOperatorIds = upAndDownStreamPinned.second;
    } else {
        auto queryPlan = sharedQueryPlan->getQueryPlan();
        NES_DEBUG("QueryPlacementAmendmentPhase: Perform query placement for query plan\n{}", queryPlan->toString());

        //1. Fetch all upstream pinned operators
        std::set<LogicalOperatorNodePtr> pinnedUpstreamOperators;
        for (const auto& leafOperator : queryPlan->getLeafOperators()) {
            pinnedUpstreamOperators.insert(leafOperator->as<LogicalOperatorNode>());
        };

        //2. Fetch all downstream pinned operators
        std::set<LogicalOperatorNodePtr> pinnedDownStreamOperators;
        for (const auto& rootOperator : queryPlan->getRootOperators()) {
            pinnedDownStreamOperators.insert(rootOperator->as<LogicalOperatorNode>());
        };

        for (auto upstreamOperator : pinnedUpstreamOperators) {
            upstreamOperatorIds.insert(upstreamOperator->getId());
        }
        for (auto downsStreamOperator : pinnedDownStreamOperators) {
            downstreamOperatorIds.insert(downsStreamOperator->getId());
        }
    }

    NES_INFO("TopologyNodeRelocationRequest: mark operators for new placement")
    //perform re-operator placement on the query plan
    sharedQueryPlan->performReOperatorPlacement(upstreamOperatorIds, downstreamOperatorIds);

    NES_INFO("TopologyNodeRelocationRequest: calculate new placement")
    //ammendment phase
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto amendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                          topology,
                                                                          typeInferencePhase,
                                                                          coordinatorConfiguration);
    amendmentPhase->execute(sharedQueryPlan);

    //deployment phase
    auto queryDeploymentPhase = QueryDeploymentPhase::create(globalExecutionPlan, queryCatalogService, coordinatorConfiguration);
    //todo: only execute partial deployment here
    queryDeploymentPhase->execute(sharedQueryPlan, PROACTIVE_PHASE_1);

    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
}

void TopologyNodeRelocationRequest::markOperatorsForReOperatorPlacement(
    SharedQueryId sharedQueryPlanId,
    const Optimizer::ExecutionNodePtr& upstreamExecutionNode,
    const Optimizer::ExecutionNodePtr& downstreamExecutionNode) {
    //Fetch the shared query plan and update its status
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryPlanId);
    sharedQueryPlan->setStatus(SharedQueryPlanStatus::MIGRATING);

    NES_INFO("TopologyNodeRelocationRequest: find up and downstream operators")
    std::set<OperatorId> upstreamOperatorIds, downstreamOperatorIds;
    if (coordinatorConfiguration->enableQueryReconfiguration) {
        //find the pinned operators for the changelog
        auto upAndDownStreamPinned =
            NES::Experimental::findUpstreamAndDownstreamPinnedOperators(sharedQueryPlan,
                                                                        upstreamExecutionNode,
                                                                        downstreamExecutionNode,
                                                                        topology);
        upstreamOperatorIds = upAndDownStreamPinned.first;
        downstreamOperatorIds = upAndDownStreamPinned.second;
    } else {
        auto queryPlan = sharedQueryPlan->getQueryPlan();
        NES_DEBUG("QueryPlacementAmendmentPhase: Perform query placement for query plan\n{}", queryPlan->toString());

        //1. Fetch all upstream pinned operators
        std::set<LogicalOperatorNodePtr> pinnedUpstreamOperators;
        for (const auto& leafOperator : queryPlan->getLeafOperators()) {
            pinnedUpstreamOperators.insert(leafOperator->as<LogicalOperatorNode>());
        };

        //2. Fetch all downstream pinned operators
        std::set<LogicalOperatorNodePtr> pinnedDownStreamOperators;
        for (const auto& rootOperator : queryPlan->getRootOperators()) {
            pinnedDownStreamOperators.insert(rootOperator->as<LogicalOperatorNode>());
        };

        for (auto upstreamOperator : pinnedUpstreamOperators) {
            upstreamOperatorIds.insert(upstreamOperator->getId());
        }
        for (auto downsStreamOperator : pinnedDownStreamOperators) {
            downstreamOperatorIds.insert(downsStreamOperator->getId());
        }
    }

    NES_INFO("TopologyNodeRelocationRequest: mark operators for new placement")
    //perform re-operator placement on the query plan
    sharedQueryPlan->performReOperatorPlacement(upstreamOperatorIds, downstreamOperatorIds);

    NES_INFO("TopologyNodeRelocationRequest: calculate new placement")
    //ammendment phase
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto amendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                          topology,
                                                                          typeInferencePhase,
                                                                          coordinatorConfiguration);
    amendmentPhase->execute(sharedQueryPlan);

    //deployment phase
    auto queryDeploymentPhase = QueryDeploymentPhase::create(globalExecutionPlan, queryCatalogService, coordinatorConfiguration);
    queryDeploymentPhase->execute(sharedQueryPlan);

    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
}

//todo #4494: implement all the following functions
void TopologyNodeRelocationRequest::preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}

std::vector<AbstractRequestPtr> TopologyNodeRelocationRequest::rollBack(std::exception_ptr, const StorageHandlerPtr&) {
    return {};
}

void TopologyNodeRelocationRequest::postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}

};// namespace NES::RequestProcessor::Experimental
