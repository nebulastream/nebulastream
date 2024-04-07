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

    NES_INFO("Register or unregister {} decomposed queries.", deploymentContexts.size());
    registerOrStopDecomposedQueryPlan(deploymentContexts, requestType);

    NES_INFO("Start or stop {} decomposed queries.", deploymentContexts.size());
    startOrUnregisterDecomposedQueryPlan(deploymentContexts, requestType);
}

void DeploymentPhase::registerOrStopDecomposedQueryPlan(const std::set<Optimizer::DeploymentContextPtr>& deploymentContexts,
                                                        RequestType requestType) {

    // TODO as part of issue #4699, we will solve this better
    QueryState sharedQueryState = QueryState::REGISTERED;
    SharedQueryId sharedQueryId;
    std::vector<RpcAsyncRequest> asyncRequests;
    for (const auto& deploymentContext : deploymentContexts) {
        auto queueForDeploymentContext = std::make_shared<CompletionQueue>();
        auto grpcAddress = deploymentContext->getGrpcAddress();
        sharedQueryId = deploymentContext->getSharedQueryId();
        auto decomposedQueryPlanId = deploymentContext->getDecomposedQueryPlanId();
        auto decomposedQueryPlanVersion = deploymentContext->getDecomposedQueryPlanVersion();
        auto workerId = deploymentContext->getWorkerId();
        auto decomposedQueryPlan = deploymentContext->getDecomposedQueryPlan();
        auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
        switch (decomposedQueryPlanState) {
            case QueryState::MARKED_FOR_DEPLOYMENT: {
                if (sharedQueryState != QueryState::FAILED && sharedQueryState != QueryState::STOPPED
                    && sharedQueryState != QueryState::MIGRATING) {
                    sharedQueryState = QueryState::RUNNING;
                }
            }
            case QueryState::MARKED_FOR_REDEPLOYMENT: {
                if (sharedQueryState != QueryState::FAILED && sharedQueryState != QueryState::STOPPED
                    && decomposedQueryPlanState == QueryState::MARKED_FOR_REDEPLOYMENT) {
                    sharedQueryState = QueryState::MIGRATING;
                }
                // Register the decomposed query plan
                workerRPCClient->registerQueryAsync(grpcAddress, decomposedQueryPlan, queueForDeploymentContext);
                // Update decomposed query plan status
                queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                              decomposedQueryPlanId,
                                                              decomposedQueryPlanVersion,
                                                              decomposedQueryPlanState,
                                                              workerId);
                asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Register});
                break;
            }
            case QueryState::MARKED_FOR_MIGRATION: {
                if (requestType == RequestType::AddQuery) {
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryPlanId,
                                                                  decomposedQueryPlanVersion,
                                                                  decomposedQueryPlanState,
                                                                  workerId);
                } else if (requestType == RequestType::StopQuery) {
                    sharedQueryState = QueryState::STOPPED;
                    workerRPCClient->stopQueryAsync(grpcAddress,
                                                    sharedQueryId,
                                                    decomposedQueryPlanId,
                                                    Runtime::QueryTerminationType::HardStop,
                                                    queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryPlanId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::MARKED_FOR_HARD_STOP,
                                                                  workerId);
                    asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Stop});
                } else if (requestType == RequestType::FailQuery) {
                    sharedQueryState = QueryState::FAILED;
                    // Fail the decomposed query plan
                    workerRPCClient->stopQueryAsync(grpcAddress,
                                                    sharedQueryId,
                                                    decomposedQueryPlanId,
                                                    Runtime::QueryTerminationType::Failure,
                                                    queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryPlanId,
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
        auto decomposedQueryPlanId = deploymentContext->getDecomposedQueryPlanId();
        auto decomposedQueryPlanVersion = deploymentContext->getDecomposedQueryPlanVersion();
        auto workerId = deploymentContext->getWorkerId();
        auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
        switch (decomposedQueryPlanState) {
            case QueryState::MARKED_FOR_DEPLOYMENT:
            case QueryState::MARKED_FOR_REDEPLOYMENT: {
                workerRPCClient->startQueryAsync(grpcAddress, sharedQueryId, decomposedQueryPlanId, queueForDeploymentContext);
                // Update decomposed query plan status
                queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                              decomposedQueryPlanId,
                                                              decomposedQueryPlanVersion,
                                                              QueryState::RUNNING,
                                                              workerId);
                asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Start});
                break;
            }
            case QueryState::MARKED_FOR_MIGRATION: {
                if (requestType == RequestType::AddQuery) {
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryPlanId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::MIGRATING,
                                                                  workerId);
                } else if (requestType == RequestType::StopQuery) {
                    // Unregister the decomposed query plan
                    workerRPCClient->unregisterQueryAsync(grpcAddress, sharedQueryId, queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryPlanId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::STOPPED,
                                                                  workerId);
                    asyncRequests.emplace_back(RpcAsyncRequest{queueForDeploymentContext, RpcClientMode::Unregister});
                } else if (requestType == RequestType::FailQuery) {
                    // Unregister the decomposed query plan
                    workerRPCClient->unregisterQueryAsync(grpcAddress, sharedQueryId, queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryPlanId,
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
