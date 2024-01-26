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
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/DeploymentPhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <RequestProcessor/RequestTypes/FailQueryRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/RequestType.hpp>

namespace NES::RequestProcessor::Experimental {

FailQueryRequest::FailQueryRequest(const NES::QueryId queryId,
                                   const NES::DecomposedQueryPlanId failedSubPlanId,
                                   const uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::GlobalQueryPlan,
                          ResourceType::QueryCatalogService,
                          ResourceType::Topology,
                          ResourceType::UdfCatalog,
                          ResourceType::SourceCatalog,
                          ResourceType::GlobalExecutionPlan,
                          ResourceType::CoordinatorConfiguration},
                         maxRetries),
      queryId(queryId), querySubPlanId(failedSubPlanId) {}

FailQueryRequestPtr
FailQueryRequest::create(NES::QueryId queryId, NES::DecomposedQueryPlanId failedSubPlanId, uint8_t maxRetries) {
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
    coordinatorConfiguration = storageHandle->getCoordinatorConfiguration(requestId);
    auto sourceCatalog = storageHandle->getSourceCatalogHandle(requestId);
    auto udfCatalog = storageHandle->getUDFCatalogHandle(requestId);
    typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
    if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
        throw Exceptions::QueryNotFoundException("Could not find a query with the id " + std::to_string(queryId)
                                                 + " in the global query plan");
    }

    //todo 4255: allow requests to skip to the front of the line
    queryCatalogService->checkAndMarkForFailure(sharedQueryId, querySubPlanId);

    globalQueryPlan->removeQuery(sharedQueryId, RequestType::FailQuery);

    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
    auto deploymentContexts = queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    //undeploy queries
    try {
        auto deploymentPhase = DeploymentPhase::create(queryCatalogService);
        deploymentPhase->execute(deploymentContexts, RequestType::FailQuery);
    } catch (NES::Exceptions::RuntimeException& e) {
        throw Exceptions::QueryUndeploymentException(sharedQueryId,
                                                     "Failed to undeploy query with id " + std::to_string(queryId));
    }

    //update global query plan
    for (auto& id : globalQueryPlan->getSharedQueryPlan(sharedQueryId)->getQueryIds()) {
        queryCatalogService->updateQueryStatus(id, QueryState::FAILED, "Failed");
    }

    //respond to the calling service which is the shared query id to the query being undeployed
    responsePromise.set_value(std::make_shared<FailQueryResponse>(sharedQueryId));

    //no follow up requests
    return {};
    //todo #3727: catch exceptions for error handling
}
}// namespace NES::RequestProcessor::Experimental
