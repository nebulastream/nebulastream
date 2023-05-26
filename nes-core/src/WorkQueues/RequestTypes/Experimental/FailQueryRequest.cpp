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
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Util/RequestType.hpp>
#include <WorkQueues/RequestTypes/Experimental/FailQueryRequest.hpp>
#include <WorkQueues/StorageHandles/ResourceType.hpp>
#include <WorkQueues/StorageHandles/StorageHandler.hpp>
#include <utility>

namespace NES::Experimental {
FailQueryRequest::FailQueryRequest(NES::QueryId queryId,
                                   NES::QuerySubPlanId failedSubPlanId,
                                   uint8_t maxRetries,
                                   NES::WorkerRPCClientPtr workerRpcClient)
    : AbstractRequest({ResourceType::GlobalQueryPlan,
                       ResourceType::QueryCatalogService,
                       ResourceType::Topology,
                       ResourceType::GlobalExecutionPlan},
                      maxRetries),
      queryId(queryId), querySubPlanId(failedSubPlanId), workerRpcClient(std::move(workerRpcClient)) {}

void FailQueryRequest::preRollbackHandle(std::exception, NES::StorageHandler&) {}

void FailQueryRequest::rollBack(std::exception&, StorageHandler&) {}

void FailQueryRequest::postRollbackHandle(std::exception ex, NES::StorageHandler& storageHandler) {
    (void) ex;
    (void) storageHandler;

    //todo #3727: perform the below error handling when the specific request type has been implemented
    /*
    try {
        auto undeploymentException = dynamic_cast<QueryUndeploymentException&>(ex);

        //undeploy queries
        globalExecutionPlan = storageHandler.getGlobalExecutionPlanHandle();
        topology = storageHandler.getTopologyHandle();
        auto queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
        queryUndeploymentPhase->execute(queryId, SharedQueryPlanStatus::Failed);
        auto sharedQueryId = undeploymentException.getSharedQueryid();

        for (auto& queryId : globalQueryPlan->getSharedQueryPlan(sharedQueryId)->getQueryIds()) {
            queryCatalogService->updateQueryStatus(queryId, QueryStatus::FAILED, "Failed");
        }
    } catch (std::bad_cast& e) {}
     */
}

void FailQueryRequest::postExecution(NES::StorageHandler&) {}

void NES::Experimental::FailQueryRequest::executeRequestLogic(NES::StorageHandler& storageHandle) {
    globalQueryPlan = storageHandle.getGlobalQueryPlanHandle();
    auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
    if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
        throw QueryNotFoundException("Could not find a query with the id " + std::to_string(queryId)
                                     + " in the global query plan");
    }

    queryCatalogService = storageHandle.getQueryCatalogServiceHandle();

    queryCatalogService->checkAndMarkForFailure(sharedQueryId, querySubPlanId);

    globalQueryPlan->removeQuery(queryId, RequestType::Fail);

    //undeploy queries
    globalExecutionPlan = storageHandle.getGlobalExecutionPlanHandle();
    topology = storageHandle.getTopologyHandle();
    auto queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);

    try {
        queryUndeploymentPhase->execute(queryId, SharedQueryPlanStatus::Failed);
    } catch (NES::Exceptions::RuntimeException& e) {
        throw QueryUndeploymentException("failed to undeploy query with id " + std::to_string(queryId));
    }

    //update global query plan
    for (auto& id : globalQueryPlan->getSharedQueryPlan(sharedQueryId)->getQueryIds()) {
        queryCatalogService->updateQueryStatus(id, QueryStatus::FAILED, "Failed");
    }
}
}// namespace NES::Experimental
