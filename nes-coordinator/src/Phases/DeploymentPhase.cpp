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
    registerUnregisterDecomposedQueryPlan(deploymentContexts, requestType);

    NES_INFO("Start or stop {} decomposed queries.", deploymentContexts.size());
    startStopDecomposedQueryPlan(deploymentContexts, requestType);
}

void DeploymentPhase::registerUnregisterDecomposedQueryPlan(const std::set<Optimizer::DeploymentContextPtr>& deploymentContexts,
                                                            RequestType requestType) {
    std::map<CompletionQueuePtr, uint64_t> asyncRequests;
    for (const auto& deploymentContext : deploymentContexts) {
        auto queueForDeploymentContext = std::make_shared<CompletionQueue>();
        auto grpcAddress = deploymentContext->getGrpcAddress();
        auto sharedQueryId = deploymentContext->getSharedQueryId();
        auto decomposedQueryPlanId = deploymentContext->getDecomposedQueryPlanId();
        auto decomposedQueryPlanVersion = deploymentContext->getDecomposedQueryPlanVersion();
        auto workerId = deploymentContext->getWorkerId();
        auto decomposedQueryPlan = deploymentContext->getDecomposedQueryPlan();
        auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
        switch (decomposedQueryPlanState) {
            case QueryState::MARKED_FOR_DEPLOYMENT:
            case QueryState::MARKED_FOR_REDEPLOYMENT: {
                // Register the decomposed query plan
                workerRPCClient->registerQueryAsync(grpcAddress, decomposedQueryPlan, queueForDeploymentContext);
                // Update decomposed query plan status
                queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                              decomposedQueryPlanId,
                                                              decomposedQueryPlanVersion,
                                                              decomposedQueryPlanState,
                                                              workerId);
                break;
            }
            case QueryState::MARKED_FOR_MIGRATION: {
                if (requestType == RequestType::AddQuery) {
                    // Register the decomposed query plan
                    workerRPCClient->registerQueryAsync(grpcAddress, decomposedQueryPlan, queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryPlanId,
                                                                  decomposedQueryPlanVersion,
                                                                  decomposedQueryPlanState,
                                                                  workerId);
                } else if (requestType == RequestType::StopQuery) {
                    // Unregister the decomposed query plan
                    workerRPCClient->unregisterQueryAsync(grpcAddress, sharedQueryId, queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryPlanId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::MARKED_FOR_HARD_STOP,
                                                                  workerId);
                } else if (requestType == RequestType::FailQuery) {
                    // Unregister the decomposed query plan
                    workerRPCClient->unregisterQueryAsync(grpcAddress, sharedQueryId, queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryPlanId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::MARKED_FOR_FAILURE,
                                                                  workerId);
                } else {
                    NES_ERROR("Unhandled request type {} for decomposed query plan in status MARKED_FOR_MIGRATION",
                              magic_enum::enum_name(requestType));
                }
                break;
            }
            default: {
                NES_WARNING("Can not hande decomposed query plan in the state {}",
                            magic_enum::enum_name(decomposedQueryPlanState));
            }
        }
        asyncRequests[queueForDeploymentContext] = 1;
    }
    workerRPCClient->checkAsyncResult(asyncRequests, RpcClientModes::Register);
}

void DeploymentPhase::startStopDecomposedQueryPlan(const std::set<Optimizer::DeploymentContextPtr>& deploymentContexts,
                                                   NES::RequestType requestType) {
    std::map<CompletionQueuePtr, uint64_t> asyncRequests;
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
                workerRPCClient->startQueryAsync(grpcAddress, sharedQueryId, queueForDeploymentContext);
                // Update decomposed query plan status
                queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                              decomposedQueryPlanId,
                                                              decomposedQueryPlanVersion,
                                                              QueryState::DEPLOYED,
                                                              workerId);
                break;
            }
            case QueryState::MARKED_FOR_MIGRATION: {
                if (requestType == RequestType::AddQuery) {
                    workerRPCClient->startQueryAsync(grpcAddress, sharedQueryId, queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryPlanId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::MIGRATING,
                                                                  workerId);
                } else if (requestType == RequestType::StopQuery) {
                    workerRPCClient->stopQueryAsync(grpcAddress,
                                                    sharedQueryId,
                                                    Runtime::QueryTerminationType::HardStop,
                                                    queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryPlanId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::STOPPED,
                                                                  workerId);
                } else if (requestType == RequestType::FailQuery) {
                    workerRPCClient->stopQueryAsync(grpcAddress,
                                                    sharedQueryId,
                                                    Runtime::QueryTerminationType::Failure,
                                                    queueForDeploymentContext);
                    // Update decomposed query plan status
                    queryCatalog->updateDecomposedQueryPlanStatus(sharedQueryId,
                                                                  decomposedQueryPlanId,
                                                                  decomposedQueryPlanVersion,
                                                                  QueryState::FAILED,
                                                                  workerId);
                } else {
                    NES_ERROR("Unhandled request type {} for decomposed query plan in status MARKED_FOR_MIGRATION",
                              magic_enum::enum_name(requestType));
                }
                break;
            }
            default: {
                NES_WARNING("Can not hande decomposed query plan in the state {}",
                            magic_enum::enum_name(decomposedQueryPlanState));
            }
        }
        asyncRequests[queueForDeploymentContext] = 1;
    }
    workerRPCClient->checkAsyncResult(asyncRequests, RpcClientModes::Start);
}

}// namespace NES
