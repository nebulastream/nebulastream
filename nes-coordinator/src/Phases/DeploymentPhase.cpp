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
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Phases/DeploymentPhase.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/Logger/Logger.hpp>

#define ASYNC_DEPLOYMENT

namespace NES {

DeploymentPhasePtr DeploymentPhase::create(const Catalogs::Query::QueryCatalogPtr& queryCatalog) {
    return std::make_shared<DeploymentPhase>(queryCatalog);
}

DeploymentPhase::DeploymentPhase(const Catalogs::Query::QueryCatalogPtr& queryCatalog)
    : workerRPCClient(WorkerRPCClient::create()), queryCatalog(queryCatalog) {
    NES_INFO("DeploymentPhase()");
}

void DeploymentPhase::execute(const std::set<Optimizer::DeploymentContextPtr>& deploymentContexts, RequestType requestType) {
    if (!deploymentContexts.empty()) {
        NES_ERROR("Register or unregister {} decomposed queries.", deploymentContexts.size());
        registerOrStopDecomposedQueryPlan(deploymentContexts, requestType);

        NES_ERROR("Start or stop {} decomposed queries.", deploymentContexts.size());
        startOrUnregisterDecomposedQueryPlan(deploymentContexts, requestType);
        NES_ERROR("Deployment phase finished.");
    }
}

void DeploymentPhase::execute(const std::set<Optimizer::ReconfigurationMarkerUnit>& reconfigurationMarkerUnits,
                              const ReconfigurationMarkerPtr& reconfigurationMarker) {
    if (!reconfigurationMarkerUnits.empty()) {
        std::vector<RpcAsyncRequest> asyncRequests;
        for (const auto& reconfigurationMarkerUnit : reconfigurationMarkerUnits) {
            auto queueForReconfigurationMarker = std::make_shared<CompletionQueue>();
            workerRPCClient->addReconfigurationMarker(reconfigurationMarkerUnit.address,
                                                      reconfigurationMarkerUnit.sharedQueryId,
                                                      reconfigurationMarkerUnit.decomposedQueryId,
                                                      reconfigurationMarker,
                                                      queueForReconfigurationMarker);
            asyncRequests.emplace_back(RpcAsyncRequest{queueForReconfigurationMarker, RpcClientMode::Reconfiguration});
        }
        workerRPCClient->checkAsyncResult(asyncRequests);
    }
}

void DeploymentPhase::registerOrStopDecomposedQueryPlan(const std::set<Optimizer::DeploymentContextPtr>& deploymentContexts,
                                                        RequestType requestType) {

    // TODO as part of issue #4699, we will solve this better
    QueryState sharedQueryState = QueryState::REGISTERED;
    SharedQueryId sharedQueryId = INVALID_SHARED_QUERY_ID;
#ifdef ASYNC_DEPLOYMENT
    NES_ERROR("Async deployment enabled")
    std::vector<RpcAsyncRequest> asyncRequests;
#endif

    auto count = 1;
    for (const auto& deploymentContext : deploymentContexts) {
        auto queueForDeploymentContext = std::make_shared<CompletionQueue>();
        auto grpcAddress = deploymentContext->getGrpcAddress();
        sharedQueryId = deploymentContext->getSharedQueryId();
        auto decomposedQueryId = deploymentContext->getDecomposedQueryId();
        auto decomposedQueryPlanVersion = deploymentContext->getDecomposedQueryPlanVersion();
        auto workerId = deploymentContext->getWorkerId();
        auto decomposedQueryPlan = deploymentContext->getDecomposedQueryPlan();
        auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
        NES_ERROR("Deployment context {} of {} for in status {} with decomposed query {}, request type {}",
                  count++,
                  deploymentContexts.size(),
                  magic_enum::enum_name(decomposedQueryPlanState),
                  decomposedQueryId,
                  magic_enum::enum_name(requestType));
        switch (decomposedQueryPlanState) {
            case QueryState::MARKED_FOR_DEPLOYMENT: {
                if (sharedQueryState != QueryState::FAILED && sharedQueryState != QueryState::STOPPED
                    && sharedQueryState != QueryState::MIGRATING) {
                    sharedQueryState = QueryState::RUNNING;
                }
                // Register the decomposed query plan
#ifdef ASYNC_DEPLOYMENT
                workerRPCClient->registerDecomposedQueryAsync(grpcAddress, decomposedQueryPlan, queueForDeploymentContext);
#else
                workerRPCClient->registerDecomposedQuery(grpcAddress, decomposedQueryPlan);
#endif
                // Update decomposed query plan status
                queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                              decomposedQueryId,
                                                              decomposedQueryPlanVersion,
                                                              decomposedQueryPlanState,
                                                              workerId);
#ifdef ASYNC_DEPLOYMENT
                asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Register});
#endif

                break;
            }
            case QueryState::MARKED_FOR_REDEPLOYMENT: {
                if (sharedQueryState != QueryState::FAILED && sharedQueryState != QueryState::STOPPED) {
                    sharedQueryState = QueryState::MIGRATING;
                }
                // Update decomposed query plan status
                queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                              decomposedQueryId,
                                                              decomposedQueryPlanVersion,
                                                              decomposedQueryPlanState,
                                                              workerId);
                break;
            }
            case QueryState::MARKED_FOR_MIGRATION: {
                if (requestType == RequestType::AddQuery) {
                    /* todo #5133: In the new redeployment logic migrated plans get undeployed via reconfiguration markers they
                     * receive from their upstream worker nodes. We therefore do not have to send a direct message from the
                     * coordinator.
                     * But the logic below that updates the query catalog will have to be reactivated (or moved) when the query
                     * catalog is adjusted to the new redeployment logic
                     */
                    continue;
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryId,
                                                                  decomposedQueryPlanVersion,
                                                                  decomposedQueryPlanState,
                                                                  workerId);
                } else if (requestType == RequestType::StopQuery) {
                    sharedQueryState = QueryState::STOPPED;
#ifdef ASYNC_DEPLOYMENT
                                        workerRPCClient->stopDecomposedQueryAsync(grpcAddress,
                                                              sharedQueryId,
                                                              decomposedQueryId,
                                                              Runtime::QueryTerminationType::HardStop,
                                                              queueForDeploymentContext);
#else
                    workerRPCClient->stopDecomposedQuery(grpcAddress,
                                                              sharedQueryId,
                                                              decomposedQueryId,
                                                              Runtime::QueryTerminationType::HardStop);
#endif

                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::MARKED_FOR_HARD_STOP,
                                                                  workerId);
#ifdef ASYNC_DEPLOYMENT
                    asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Stop});
#endif
                } else if (requestType == RequestType::RestartQuery) {
                    sharedQueryState = QueryState::STOPPED;
                    //TODO:  remove stop call (is handled by marker now
#ifdef ASYNC_DEPLOYMENT
                    workerRPCClient->stopDecomposedQueryAsync(grpcAddress,
                                                              sharedQueryId,
                                                              decomposedQueryId,
                                                              Runtime::QueryTerminationType::Graceful,
                                                              queueForDeploymentContext);
#else
                    workerRPCClient->stopDecomposedQuery(grpcAddress,
                                                              sharedQueryId,
                                                              decomposedQueryId,
                                                              Runtime::QueryTerminationType::Graceful);
#endif
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::MARKED_FOR_SOFT_STOP,
                                                                  workerId);
#ifdef ASYNC_DEPLOYMENT
                    asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Stop});
#endif
                } else if (requestType == RequestType::FailQuery) {
                    sharedQueryState = QueryState::FAILED;
                    //TODO: fail is always performed synchronously

                    // Fail the decomposed query plan
#ifdef ASYNC_DEPLOYMENT
                    workerRPCClient->stopDecomposedQueryAsync(grpcAddress,
                                                              sharedQueryId,
                                                              decomposedQueryId,
                                                              Runtime::QueryTerminationType::Failure,
                                                              queueForDeploymentContext);
#else
                    workerRPCClient->stopDecomposedQuery(grpcAddress,
                                                              sharedQueryId,
                                                              decomposedQueryId,
                                                              Runtime::QueryTerminationType::Failure);
#endif

                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::MARKED_FOR_FAILURE,
                                                                  workerId);

#ifdef ASYNC_DEPLOYMENT
                    asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Stop});
#endif
                } else {
//                    continue;
//                    NES_ERROR("Unhandled request type {} for decomposed query plan in status MARKED_FOR_MIGRATION",
//                              magic_enum::enum_name(requestType));
                }
                break;
            }
            default: {
                NES_WARNING("Can not handle decomposed query plan in the state {}",
                            magic_enum::enum_name(decomposedQueryPlanState));
            }
        }
    }
#ifdef ASYNC_DEPLOYMENT
    workerRPCClient->checkAsyncResult(asyncRequests);
#endif
    queryCatalog->updateSharedQueryStatus(sharedQueryId, sharedQueryState, "");
}

void DeploymentPhase::startOrUnregisterDecomposedQueryPlan(const std::set<Optimizer::DeploymentContextPtr>& deploymentContexts,
                                                           NES::RequestType requestType) {
    std::vector<RpcAsyncRequest> asyncRequests;
    for (const auto& deploymentContext : deploymentContexts) {
        auto queueForDeploymentContext = std::make_shared<CompletionQueue>();
        auto grpcAddress = deploymentContext->getGrpcAddress();
        auto sharedQueryId = deploymentContext->getSharedQueryId();
        auto decomposedQueryId = deploymentContext->getDecomposedQueryId();
        auto decomposedQueryPlanVersion = deploymentContext->getDecomposedQueryPlanVersion();
        auto workerId = deploymentContext->getWorkerId();
        auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
        switch (decomposedQueryPlanState) {
            case QueryState::MARKED_FOR_DEPLOYMENT:
            case QueryState::MARKED_FOR_REDEPLOYMENT: {
                workerRPCClient->startDecomposedQueryAsync(grpcAddress,
                                                           sharedQueryId,
                                                           decomposedQueryId,
                                                           queueForDeploymentContext);
                // Update decomposed query plan status
                queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                              decomposedQueryId,
                                                              decomposedQueryPlanVersion,
                                                              QueryState::RUNNING,
                                                              workerId);
                asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Start});
                break;
            }
            case QueryState::MARKED_FOR_MIGRATION: {
                if (requestType == RequestType::AddQuery) {
                    /* todo #5133: In the new redeployment logic migrated plans get undeployed via reconfiguration markers they
                     * receive from their upstream worker nodes. We therefore do not have to send a direct message from the
                     * coordinator.
                     * But the logic below that updates the query catalog will have to be reactivated (or moved) when the query
                     * catalog is adjusted to the new redeployment logic
                     */
                    continue;
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::MIGRATING,
                                                                  workerId);
                } else if (requestType == RequestType::StopQuery || requestType == RequestType::RestartQuery) {
                    // Unregister the decomposed query plan
                    workerRPCClient->unregisterDecomposedQueryAsync(grpcAddress,
                                                                    sharedQueryId,
                                                                    decomposedQueryId,
                                                                    queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::STOPPED,
                                                                  workerId);
                    asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Unregister});
                } else if (requestType == RequestType::FailQuery) {
                    // Unregister the decomposed query plan
                    workerRPCClient->unregisterDecomposedQueryAsync(grpcAddress,
                                                                    sharedQueryId,
                                                                    decomposedQueryId,
                                                                    queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::FAILED,
                                                                  workerId);
                    asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Unregister});
                } else {
                    NES_ERROR("Unhandled request type {} for decomposed query plan in status MARKED_FOR_MIGRATION",
                              magic_enum::enum_name(requestType));
                }
                break;
            }
            default: {
                NES_WARNING("Can not handle decomposed query plan in the state {}",
                            magic_enum::enum_name(decomposedQueryPlanState));
            }
        }
    }
    workerRPCClient->checkAsyncResult(asyncRequests);
}

}// namespace NES
