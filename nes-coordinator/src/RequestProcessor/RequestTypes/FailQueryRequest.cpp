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
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Exceptions/RuntimeException.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/DeploymentPhase.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <RequestProcessor/RequestTypes/FailQueryRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Services/PlacementAmendment/PlacementAmendmentHandler.hpp>
#include <Services/PlacementAmendment/PlacementAmendmentInstance.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/RequestType.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::RequestProcessor {

FailQueryRequest::FailQueryRequest(const SharedQueryId sharedQueryId,
                                   const DecomposedQueryId failedDecomposedPlanId,
                                   DecomposedQueryPlanVersion failedDecomposedPlanVersion,
                                   const std::string& failureReason,
                                   const uint8_t maxRetries,
                                   const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler)
    : AbstractUniRequest({ResourceType::GlobalQueryPlan,
                          ResourceType::QueryCatalogService,
                          ResourceType::Topology,
                          ResourceType::UdfCatalog,
                          ResourceType::SourceCatalog,
                          ResourceType::GlobalExecutionPlan,
                          ResourceType::CoordinatorConfiguration,
                          ResourceType::StatisticProbeHandler},
                         maxRetries),
      sharedQueryId(sharedQueryId), decomposedQueryId(failedDecomposedPlanId),
      decomposedQueryVersion(failedDecomposedPlanVersion), failureReason(failureReason),
      placementAmendmentHandler(placementAmendmentHandler) {}

FailQueryRequestPtr FailQueryRequest::create(SharedQueryId sharedQueryId,
                                             DecomposedQueryId failedDecomposedQueryId,
                                             DecomposedQueryPlanVersion failedDecomposedPlanVersion,
                                             const std::string& failureReason,
                                             uint8_t maxRetries,
                                             const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler) {
    return std::make_shared<FailQueryRequest>(sharedQueryId,
                                              failedDecomposedQueryId,
                                              failedDecomposedPlanVersion,
                                              failureReason,
                                              maxRetries,
                                              placementAmendmentHandler);
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
    queryCatalog = storageHandle->getQueryCatalogHandle(requestId);
    topology = storageHandle->getTopologyHandle(requestId);
    coordinatorConfiguration = storageHandle->getCoordinatorConfiguration(requestId);
    auto sourceCatalog = storageHandle->getSourceCatalogHandle(requestId);
    auto udfCatalog = storageHandle->getUDFCatalogHandle(requestId);
    typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    //todo 4255: allow requests to skip to the front of the line
    queryCatalog->checkAndMarkSharedQueryForFailure(sharedQueryId, decomposedQueryId, decomposedQueryVersion);

    // 1. Remove shared query plan from the global query plan and mark it as failed
    globalQueryPlan->removeQuery(UNSURE_CONVERSION_TODO_4761(sharedQueryId, QueryId), RequestType::FailQuery);

    // 2. Extract the shared query plan marked as failed
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);

    // 3. Perform placement amendment and deployment of updated shared query plan
    NES_DEBUG("Performing Operator placement amendment and deployment for shared query plan");
    auto deploymentPhase = DeploymentPhase::create(queryCatalog);
    const auto& amendmentInstance = Optimizer::PlacementAmendmentInstance::create(sharedQueryPlan,
                                                                                  globalExecutionPlan,
                                                                                  topology,
                                                                                  typeInferencePhase,
                                                                                  coordinatorConfiguration,
                                                                                  queryCatalog,
                                                                                  deploymentPhase);
    placementAmendmentHandler->enqueueRequest(amendmentInstance);

    // If amendment successfully happens then update the catalog and return the call
    if (amendmentInstance->getFuture().get()) {
        // 4. Mark the shared query plan as failed in the catalog
        queryCatalog->updateSharedQueryStatus(sharedQueryId, QueryState::FAILED, failureReason);
        // 5. respond to the calling service which is the shared query id to the query being undeployed
        responsePromise.set_value(std::make_shared<FailQueryResponse>(sharedQueryId));
        // 6. no follow up requests
        return {};
        //todo #3727: catch exceptions for error handling
    }
    NES_ERROR("Failed to process Failed query request. Queuing another fail query request for the request executor.")
    return {FailQueryRequest::create(sharedQueryId,
                                     decomposedQueryId,
                                     decomposedQueryVersion,
                                     failureReason,
                                     1,
                                     placementAmendmentHandler)};
}

}// namespace NES::RequestProcessor
