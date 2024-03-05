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

#include <Catalogs/Exceptions/QueryNotFoundException.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Optimizer/Exceptions/SharedQueryPlanNotFoundException.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddLinkEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddNodeEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddQueryEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveLinkEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveNodeEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveQueryEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/IncrementalPlacementUtils.hpp>

namespace NES::RequestProcessor {

ISQPRequest::ISQPRequest(const z3::ContextPtr& z3Context, std::vector<ISQPEventPtr> events, uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::QueryCatalogService,
                          ResourceType::GlobalExecutionPlan,
                          ResourceType::Topology,
                          ResourceType::GlobalQueryPlan,
                          ResourceType::UdfCatalog,
                          ResourceType::SourceCatalog,
                          ResourceType::CoordinatorConfiguration},
                         maxRetries),
      z3Context(z3Context), events(events) {}

ISQPRequestPtr ISQPRequest::create(const z3::ContextPtr& z3Context, std::vector<ISQPEventPtr> events, uint8_t maxRetries) {
    return std::make_shared<ISQPRequest>(z3Context, events, maxRetries);
}

std::vector<AbstractRequestPtr> ISQPRequest::executeRequestLogic(const NES::RequestProcessor::StorageHandlerPtr& storageHandle) {

    auto startTime =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    topology = storageHandle->getTopologyHandle(requestId);
    globalQueryPlan = storageHandle->getGlobalQueryPlanHandle(requestId);
    globalExecutionPlan = storageHandle->getGlobalExecutionPlanHandle(requestId);
    queryCatalog = storageHandle->getQueryCatalogHandle(requestId);
    udfCatalog = storageHandle->getUDFCatalogHandle(requestId);
    sourceCatalog = storageHandle->getSourceCatalogHandle(requestId);
    coordinatorConfiguration = storageHandle->getCoordinatorConfiguration(requestId);

    // Apply all topology events
    for (const auto& event : events) {
        if (event->instanceOf<ISQPRemoveNodeEvent>()) {
            auto removeNodeEvent = event->as<ISQPRemoveNodeEvent>();
            topology->unregisterWorker(removeNodeEvent->getWorkerId());
        } else if (event->instanceOf<ISQPRemoveLinkEvent>()) {
            auto removeLinkEvent = event->as<ISQPRemoveLinkEvent>();
            topology->removeTopologyNodeAsChild(removeLinkEvent->getParentNodeId(), removeLinkEvent->getChildNodeId());
        } else if (event->instanceOf<ISQPAddLinkEvent>()) {
            auto addLinkEvent = event->as<ISQPAddLinkEvent>();
            topology->addTopologyNodeAsChild(addLinkEvent->getParentNodeId(), addLinkEvent->getChildNodeId());
            event->response.set_value(std::make_shared<ISQPAddLinkResponse>(true));
        } else if (event->instanceOf<ISQPAddNodeEvent>()) {
            auto addNodeEvent = event->as<ISQPAddNodeEvent>();
            topology->registerWorker(addNodeEvent->getWorkerId(),
                                     addNodeEvent->getIpAddress(),
                                     addNodeEvent->getGrpcPort(),
                                     addNodeEvent->getDataPort(),
                                     addNodeEvent->getResources(),
                                     addNodeEvent->getProperties(),
                                     0,
                                     0);
            event->response.set_value(std::make_shared<ISQPAddLinkResponse>(true));
        }
    }

    // Identify affected operator placements
    for (const auto& event : events) {
        if (event->instanceOf<ISQPRemoveNodeEvent>()) {
            handleRemoveNodeRequest(event->as<ISQPRemoveNodeEvent>());
            event->response.set_value(std::make_shared<ISQPRemoveNodeResponse>(true));
        } else if (event->instanceOf<ISQPRemoveLinkEvent>()) {
            handleRemoveLinkRequest(event->as<ISQPRemoveLinkEvent>());
            event->response.set_value(std::make_shared<ISQPRemoveLinkResponse>(true));
        } else if (event->instanceOf<ISQPAddQueryEvent>()) {
            auto queryId = handleAddQueryRequest(event->as<ISQPAddQueryEvent>());
            event->response.set_value(std::make_shared<ISQPAddQueryResponse>(queryId));
        } else if (event->instanceOf<ISQPRemoveQueryEvent>()) {
            handleRemoveQueryRequest(event->as<ISQPRemoveQueryEvent>());
            event->response.set_value(std::make_shared<ISQPRemoveQueryResponse>(true));
        }
    }

    // Fetch affected SQPs and call in parallel operator placement amendment phase
    auto sharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Initiate placement requests
    auto placementAmendmentThreadCount = coordinatorConfiguration->optimizer.placementAmendmentThreadCount;

    for (const auto& sharedQueryPlan : sharedQueryPlans) {
        amendmentInstanceQueue.enqueue(PlacementAmemderInstance::create(sharedQueryPlan,
                                                                        globalExecutionPlan,
                                                                        topology,
                                                                        typeInferencePhase,
                                                                        coordinatorConfiguration,
                                                                        queryCatalog));
    }

    // Initiate amendment runners
    for (size_t i = 0; i < placementAmendmentThreadCount; ++i) {
        amendmentRunners.emplace_back(std::thread([this]() {
            runPlacementAmenderInstance();
        }));
    }

    // Wait for all amendment runners to finish processing
    for (auto& amendmentRunner : amendmentRunners) {
        if (amendmentRunner.joinable()) {
            amendmentRunner.join();
        }
    }

    auto endTime =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    responsePromise.set_value(std::make_shared<ISQPRequestResponse>(startTime, endTime, true));
    return {};
}

void ISQPRequest::runPlacementAmenderInstance() {
    while (true) {
        PlacementAmemderInstancePtr placementAmemderInstance;
        if (!amendmentInstanceQueue.try_dequeue(placementAmemderInstance)) {
            return;
        }
        placementAmemderInstance->execute();
    }
}

void ISQPRequest::handleRemoveLinkRequest(NES::RequestProcessor::ISQPRemoveLinkEventPtr removeLinkEvent) {

    auto upstreamNodeId = removeLinkEvent->getChildNodeId();
    auto downstreamNodeId = removeLinkEvent->getParentNodeId();

    // Step1. Identify the impacted SQPs
    auto upstreamExecutionNode = globalExecutionPlan->getLockedExecutionNode(upstreamNodeId);
    auto downstreamExecutionNode = globalExecutionPlan->getLockedExecutionNode(downstreamNodeId);
    //If any of the two execution nodes do not exist then skip rest of the operation
    if (!upstreamExecutionNode || !downstreamExecutionNode) {
        NES_INFO("Removing topology link {}->{} has no effect on the running queries", upstreamNodeId, downstreamNodeId);
        return;
    }

    auto upstreamSharedQueryIds = upstreamExecutionNode->operator*()->getPlacedSharedQueryPlanIds();
    auto downstreamSharedQueryIds = downstreamExecutionNode->operator*()->getPlacedSharedQueryPlanIds();
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
}

void ISQPRequest::handleRemoveNodeRequest(NES::RequestProcessor::ISQPRemoveNodeEventPtr removeNodeEvent) {

    auto removedNodeId = removeNodeEvent->getWorkerId();

    //1. If the removed execution nodes does not have any shared query plan placed then skip rest of the operation
    auto impactedSharedQueryIds = globalExecutionPlan->getPlacedSharedQueryIds(removedNodeId);
    if (impactedSharedQueryIds.empty()) {
        NES_INFO("Removing node {} has no effect on the running queries as there are no queries placed "
                 "on the node.",
                 removedNodeId);
        return;
    }

    //2. Fetch upstream and downstream topology nodes connected via the removed topology node
    auto downstreamTopologyNodes = removeNodeEvent->getDownstreamWorkerIds();
    auto upstreamTopologyNodes = removeNodeEvent->getUpstreamWorkerIds();

    //3. If the topology node either do not have upstream or downstream node then fail the request
    if (upstreamTopologyNodes.empty() || downstreamTopologyNodes.empty()) {
        //FIXME: how to handle this case? If the node to remove has physical source then we may need to kill the
        // whole query.
        NES_NOT_IMPLEMENTED();
    }

    //todo: capy block and place function above
    //4. Iterate over all upstream and downstream topology node pairs and try to mark operators for re-placement
    for (auto const& upstreamTopologyNode : upstreamTopologyNodes) {
        for (auto const& downstreamTopologyNode : downstreamTopologyNodes) {

            //4.1. Iterate over impacted shared query plan ids to identify the shared query plans placed on the
            // upstream and downstream execution nodes
            for (auto const& impactedSharedQueryId : impactedSharedQueryIds) {

                auto upstreamExecutionNode = globalExecutionPlan->getLockedExecutionNode(upstreamTopologyNode);
                auto downstreamExecutionNode = globalExecutionPlan->getLockedExecutionNode(downstreamTopologyNode);

                //4.2. If there exists no upstream or downstream execution nodes than skip rest of the operation
                if (!upstreamExecutionNode || !downstreamExecutionNode) {
                    continue;
                }

                //4.3. Only process the upstream and downstream execution node pairs when both have shared query plans
                // with the impacted shared query id
                if (upstreamExecutionNode->operator*()->hasRegisteredDecomposedQueryPlans(impactedSharedQueryId)
                    && downstreamExecutionNode->operator*()->hasRegisteredDecomposedQueryPlans(impactedSharedQueryId)) {

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
            }
        }
    }
}

QueryId ISQPRequest::handleAddQueryRequest(NES::RequestProcessor::ISQPAddQueryEventPtr addQueryEvent) {

    auto queryPlan = addQueryEvent->getQueryPlan();
    auto queryPlacementStrategy = addQueryEvent->getPlacementStrategy();

    // Set unique identifier and additional properties to the query
    auto queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    queryPlan->setPlacementStrategy(queryPlacementStrategy);

    // Create a new entry in the query catalog
    queryCatalog->createQueryCatalogEntry("", queryPlan, queryPlacementStrategy, QueryState::REGISTERED);

    //1. Add the initial version of the query to the query catalog
    queryCatalog->addUpdatedQueryPlan(queryId, "Input Query Plan", queryPlan);

    //2. Set query status as Optimizing
    queryCatalog->updateQueryStatus(queryId, QueryState::OPTIMIZING, "");

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryRewritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificQueryRewritePhase =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, coordinatorConfiguration->optimizer);
    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(z3Context, coordinatorConfiguration->optimizer.queryMergerRule);
    auto queryMergerPhase = Optimizer::QueryMergerPhase::create(z3Context, coordinatorConfiguration->optimizer);

    //3. Execute type inference phase
    NES_DEBUG("Performing Query type inference phase for query:  {}", queryId);
    queryPlan = typeInferencePhase->execute(queryPlan);

    //5. Perform query re-write
    NES_DEBUG("Performing Query rewrite phase for query:  {}", queryId);
    queryPlan = queryRewritePhase->execute(queryPlan);

    //6. Add the updated query plan to the query catalog
    queryCatalog->addUpdatedQueryPlan(queryId, "Query Rewrite Phase", queryPlan);

    //7. Execute type inference phase on rewritten query plan
    queryPlan = typeInferencePhase->execute(queryPlan);

    //9. Perform signature inference phase for sharing identification among query plans
    signatureInferencePhase->execute(queryPlan);

    //10. Perform topology specific rewrites to the query plan
    queryPlan = topologySpecificQueryRewritePhase->execute(queryPlan);

    //11. Add the updated query plan to the query catalog
    queryCatalog->addUpdatedQueryPlan(queryId, "Topology Specific Query Rewrite Phase", queryPlan);

    //12. Perform type inference over re-written query plan
    queryPlan = typeInferencePhase->execute(queryPlan);

    //15. Add the updated query plan to the query catalog
    queryCatalog->addUpdatedQueryPlan(queryId, "Executed Query Plan", queryPlan);

    //16. Add the updated query plan to the global query plan
    NES_DEBUG("Performing Query type inference phase for query:  {}", queryId);
    globalQueryPlan->addQueryPlan(queryPlan);

    //17. Perform query merging for newly added query plan
    NES_DEBUG("Applying Query Merger Rules as Query Merging is enabled.");
    queryMergerPhase->execute(globalQueryPlan);

    //18. Get the shared query plan id for the added query
    auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
    if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
        throw Exceptions::SharedQueryPlanNotFoundException(
            "Could not find shared query id in global query plan. Shared query id is invalid.",
            sharedQueryId);
    }

    //19. Get the shared query plan for the added query
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
    if (!sharedQueryPlan) {
        throw Exceptions::SharedQueryPlanNotFoundException("Could not obtain shared query plan by shared query id.",
                                                           sharedQueryId);
    }

    if (sharedQueryPlan->getStatus() == SharedQueryPlanStatus::CREATED) {
        queryCatalog->createSharedQueryCatalogEntry(sharedQueryId, {queryId}, QueryState::OPTIMIZING);
    } else {
        queryCatalog->updateSharedQueryStatus(sharedQueryId, QueryState::OPTIMIZING, "");
    }
    //Link both catalogs
    queryCatalog->linkSharedQuery(queryId, sharedQueryId);
    return queryId;
}

void ISQPRequest::handleRemoveQueryRequest(NES::RequestProcessor::ISQPRemoveQueryEventPtr removeQueryEvent) {

    auto queryId = removeQueryEvent->getQueryId();
    if (queryId == INVALID_SHARED_QUERY_ID) {
        throw Exceptions::QueryNotFoundException("Cannot stop query with invalid query id " + std::to_string(queryId)
                                                 + ". Please enter a valid query id.");
    }

    //mark query for hard stop
    queryCatalog->updateQueryStatus(queryId, QueryState::MARKED_FOR_HARD_STOP, "Query Stop Requested");

    auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
    if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
        throw Exceptions::QueryNotFoundException("Could not find a a valid shared query plan for query with id "
                                                 + std::to_string(queryId) + " in the global query plan");
    }
    // remove single query from global query plan
    globalQueryPlan->removeQuery(queryId, RequestType::StopQuery);
}

std::vector<AbstractRequestPtr> ISQPRequest::rollBack(std::exception_ptr, const StorageHandlerPtr&) {
    return std::vector<AbstractRequestPtr>();
}

void ISQPRequest::preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}

void ISQPRequest::postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}

PlacementAmemderInstancePtr PlacementAmemderInstance::create(NES::SharedQueryPlanPtr sharedQueryPlan,
                                                             Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                                                             NES::TopologyPtr topology,
                                                             Optimizer::TypeInferencePhasePtr typeInferencePhase,
                                                             Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                                             Catalogs::Query::QueryCatalogPtr queryCatalog) {
    return std::make_unique<PlacementAmemderInstance>(sharedQueryPlan,
                                                      globalExecutionPlan,
                                                      topology,
                                                      typeInferencePhase,
                                                      coordinatorConfiguration,
                                                      queryCatalog);
}

PlacementAmemderInstance::PlacementAmemderInstance(SharedQueryPlanPtr sharedQueryPlan,
                                                   Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                                                   TopologyPtr topology,
                                                   Optimizer::TypeInferencePhasePtr typeInferencePhase,
                                                   Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                                   Catalogs::Query::QueryCatalogPtr queryCatalog)
    : sharedQueryPlan(sharedQueryPlan), globalExecutionPlan(globalExecutionPlan), topology(topology),
      typeInferencePhase(typeInferencePhase), coordinatorConfiguration(coordinatorConfiguration), queryCatalog(queryCatalog){};

bool PlacementAmemderInstance::execute() {
    auto queryPlacementPhaseInstance = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                       topology,
                                                                                       typeInferencePhase,
                                                                                       coordinatorConfiguration);

    auto sharedQueryId = sharedQueryPlan->getId();
    auto deploymentContexts = queryPlacementPhaseInstance->execute(sharedQueryPlan);

    // Iterate over deployment context and update execution plan
    for (const auto& deploymentContext : deploymentContexts) {
        auto executionNodeId = deploymentContext->getWorkerId();
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
                                                                    QueryState::RUNNING);
                break;
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
                break;
            }
            default: NES_WARNING("Unhandled Deployment context with status: {}", magic_enum::enum_name(decomposedQueryPlanState));
        }
    }
    sharedQueryPlan->setStatus(SharedQueryPlanStatus::DEPLOYED);
    //FIXME: figure out how to change the status if the query is to be removed
    queryCatalog->updateSharedQueryStatus(sharedQueryId, QueryState::RUNNING, "");
    return true;
}
}// namespace NES::RequestProcessor
