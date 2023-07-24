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
#include <Exceptions/InvalidQueryStateException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Exceptions/RuntimeException.hpp>
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
FailQueryRequest::FailQueryRequest(const RequestId requestId,
                                   const NES::QueryId queryId,
                                   const NES::QuerySubPlanId failedSubPlanId,
                                   const uint8_t maxRetries,
                                   NES::WorkerRPCClientPtr workerRpcClient)
    : AbstractRequest(requestId,
                      {ResourceType::GlobalQueryPlan,
                       ResourceType::QueryCatalogService,
                       ResourceType::Topology,
                       ResourceType::GlobalExecutionPlan},
                      maxRetries),
      queryId(queryId), querySubPlanId(failedSubPlanId), workerRpcClient(std::move(workerRpcClient)) {}

void FailQueryRequest::preRollbackHandle(const RequestExecutionException&, NES::StorageHandler&) {}

void FailQueryRequest::rollBack(const RequestExecutionException&, StorageHandler&) {}

void FailQueryRequest::postRollbackHandle(const RequestExecutionException&, NES::StorageHandler&) {

    //todo #3727: perform error handling
}

void FailQueryRequest::postExecution(NES::StorageHandler& storageHandler) { storageHandler.releaseResources(queryId); }

void NES::Experimental::FailQueryRequest::executeRequestLogic(NES::StorageHandler& storageHandle) {
    globalQueryPlan = storageHandle.getGlobalQueryPlanHandle(requestId);
    globalExecutionPlan = storageHandle.getGlobalExecutionPlanHandle(requestId);
    queryCatalogService = storageHandle.getQueryCatalogServiceHandle(requestId);
    topology = storageHandle.getTopologyHandle(requestId);
    auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
    if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
        throw Exceptions::QueryNotFoundException("Could not find a query with the id " + std::to_string(queryId)
                                                 + " in the global query plan");
    }

    queryCatalogService->checkAndMarkForFailure(sharedQueryId, querySubPlanId);

    globalQueryPlan->removeQuery(queryId, RequestType::FailQuery);

    //undeploy queries
    try {
        auto queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
        queryUndeploymentPhase->execute(queryId, SharedQueryPlanStatus::Failed);
    } catch (NES::Exceptions::RuntimeException& e) {
        throw Exceptions::QueryUndeploymentException(sharedQueryId,
                                                     "failed to undeploy query with id " + std::to_string(queryId));
    }

    //update global query plan
    for (auto& id : globalQueryPlan->getSharedQueryPlan(sharedQueryId)->getQueryIds()) {
        queryCatalogService->updateQueryStatus(id, QueryState::FAILED, "Failed");
    }
}
}// namespace NES::Experimental
