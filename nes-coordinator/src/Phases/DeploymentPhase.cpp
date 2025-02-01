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
    std::vector<RpcAsyncRequest> asyncRequests;
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
        NES_ERROR("Deployment context {} of {} for in status {} with decomposed query {}",
                  count++,
                  magic_enum::enum_name(decomposedQueryPlanState),
                  deploymentContexts.size(),
                  decomposedQueryId);
        switch (decomposedQueryPlanState) {
            case QueryState::MARKED_FOR_DEPLOYMENT: {
                if (sharedQueryState != QueryState::FAILED && sharedQueryState != QueryState::STOPPED
                    && sharedQueryState != QueryState::MIGRATING) {
                    sharedQueryState = QueryState::RUNNING;
                }
                // Register the decomposed query plan
                workerRPCClient->registerDecomposedQueryAsync(grpcAddress, decomposedQueryPlan, queueForDeploymentContext);
                // Update decomposed query plan status
                queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                              decomposedQueryId,
                                                              decomposedQueryPlanVersion,
                                                              decomposedQueryPlanState,
                                                              workerId);
                asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Register});
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
                    workerRPCClient->stopDecomposedQueryAsync(grpcAddress,
                                                              sharedQueryId,
                                                              decomposedQueryId,
                                                              Runtime::QueryTerminationType::HardStop,
                                                              queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::MARKED_FOR_HARD_STOP,
                                                                  workerId);
                    asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Stop});
                } else if (requestType == RequestType::RestartQuery) {
                    sharedQueryState = QueryState::STOPPED;
                    workerRPCClient->stopDecomposedQueryAsync(grpcAddress,
                                                              sharedQueryId,
                                                              decomposedQueryId,
                                                              Runtime::QueryTerminationType::Graceful,
                                                              queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::MARKED_FOR_SOFT_STOP,
                                                                  workerId);
                    asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Stop});
                } else if (requestType == RequestType::FailQuery) {
                    sharedQueryState = QueryState::FAILED;
                    // Fail the decomposed query plan
                    workerRPCClient->stopDecomposedQueryAsync(grpcAddress,
                                                              sharedQueryId,
                                                              decomposedQueryId,
                                                              Runtime::QueryTerminationType::Failure,
                                                              queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::MARKED_FOR_FAILURE,
                                                                  workerId);
                    asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Stop});
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
