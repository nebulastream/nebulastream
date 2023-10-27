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

#include <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include <Catalogs/Exceptions/QueryNotFoundException.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Exceptions/RuntimeException.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <RequestProcessor/RequestTypes/FailQueryRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/RequestType.hpp>

namespace NES::RequestProcessor::Experimental {

FailQueryRequest::FailQueryRequest(const NES::QueryId queryId,
                                   const NES::QuerySubPlanId failedSubPlanId,
                                   const uint8_t maxRetries)
    : AbstractRequest({ResourceType::GlobalQueryPlan,
                       ResourceType::QueryCatalogService,
                       ResourceType::Topology,
                       ResourceType::GlobalExecutionPlan},
                      maxRetries),
      queryId(queryId), querySubPlanId(failedSubPlanId) {}

FailQueryRequestPtr FailQueryRequest::create(NES::QueryId queryId, NES::QuerySubPlanId failedSubPlanId, uint8_t maxRetries) {
    return std::make_shared<FailQueryRequest>(queryId, failedSubPlanId, maxRetries);
}

void FailQueryRequest::preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}

std::vector<AbstractRequestPtr> FailQueryRequest::rollBack(std::exception_ptr ex, const StorageHandlerPtr&) {
    //make sure the promise is set before returning in case a the caller is waiting on it
    trySetExceptionInPromise(ex);
    return {};
}

void FailQueryRequest::postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {
    //todo #3727: perform error handling
}

void FailQueryRequest::postExecution(const StorageHandlerPtr& storageHandler) { storageHandler->releaseResources(requestId); }

std::vector<AbstractRequestPtr> FailQueryRequest::executeRequestLogic(const StorageHandlerPtr& storageHandle) {
    globalQueryPlan = storageHandle->getGlobalQueryPlanHandle(requestId);
    globalExecutionPlan = storageHandle->getGlobalExecutionPlanHandle(requestId);
    queryCatalogService = storageHandle->getQueryCatalogServiceHandle(requestId);
    topology = storageHandle->getTopologyHandle(requestId);
    auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
    if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
        throw Exceptions::QueryNotFoundException("Could not find a query with the id " + std::to_string(queryId)
                                                 + " in the global query plan");
    }

    //respond to the calling service which is the shared query id to the query being undeployed
    responsePromise.set_value(std::make_shared<FailQueryResponse>(sharedQueryId));

    //todo 4255: allow requests to skip to the front of the line
    queryCatalogService->checkAndMarkForFailure(sharedQueryId, querySubPlanId);

    globalQueryPlan->removeQuery(queryId, RequestType::FailQuery);

    //undeploy queries
    try {
        auto queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan);
        queryUndeploymentPhase->execute(sharedQueryId, SharedQueryPlanStatus::Failed);
    } catch (NES::Exceptions::RuntimeException& e) {
        throw Exceptions::QueryUndeploymentException(sharedQueryId,
                                                     "Failed to undeploy query with id " + std::to_string(queryId));
    }

    //update global query plan
    for (auto& id : globalQueryPlan->getSharedQueryPlan(sharedQueryId)->getQueryIds()) {
        queryCatalogService->updateQueryStatus(id, QueryState::FAILED, "Failed");
    }

    //no follow up requests
    return {};
    //todo #3727: catch exceptions for error handling
}
}// namespace NES::RequestProcessor::Experimental
