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
                                   NES::WorkerRPCClientPtr workerRpcClient,
                                   std::promise<FailQueryResponse> responsePromise)
    : AbstractRequest(requestId,
                      {ResourceType::GlobalQueryPlan,
                       ResourceType::QueryCatalogService,
                       ResourceType::Topology,
                       ResourceType::GlobalExecutionPlan},
                      maxRetries,
                      std::move(responsePromise)),
      queryId(queryId), querySubPlanId(failedSubPlanId), workerRpcClient(std::move(workerRpcClient)) {}

std::shared_ptr<FailQueryRequest> FailQueryRequest::create(RequestId requestId,
                                                           NES::QueryId queryId,
                                                           NES::QuerySubPlanId failedSubPlanId,
                                                           uint8_t maxRetries,
                                                           NES::WorkerRPCClientPtr workerRpcClient,
                                                           std::promise<FailQueryResponse> responsePromise) {
    return std::make_shared<FailQueryRequest>(requestId,
                                              queryId,
                                              failedSubPlanId,
                                              maxRetries,
                                              std::move(workerRpcClient),
                                              std::move(responsePromise));
}

FailQueryRequest::FailQueryRequest(NES::QueryId queryId,
                                   uint8_t maxRetries,
                                   NES::WorkerRPCClientPtr workerRpcClient)
    : FailQueryRequest(queryId, INVALID_QUERY_SUB_PLAN_ID, maxRetries, workerRpcClient) {}

void FailQueryRequest::preRollbackHandle(RequestExecutionException&, NES::StorageHandler&) {}

void FailQueryRequest::preRollbackHandle(const RequestExecutionException&, NES::StorageHandler&) {}

void FailQueryRequest::rollBack(const RequestExecutionException&, StorageHandler&) {}

    if (ex.instanceOf<QueryUndeploymentException>()) {
        auto undeploymentException = dynamic_cast<QueryUndeploymentException&>(ex);
        //todo: if non grpc exception occured during undeployment, we cannot do anything

        //todo: keep this as handling for failed async results and differentiate it from the other cases (by subclassing undeployment exception?)
        auto sharedQueryId = undeploymentException.getSharedQueryId();

        if (sharedQueryId.has_value()) {
            //undeploy queries
            undeployQueries(storageHandler, sharedQueryId.value());

            //update global query plan
            postUndeployment(sharedQueryId.value());
        }
    } else if (ex.instanceOf<QueryNotFoundException>()) {
        NES_WARNING2("Exception {}", ex.as<QueryUndeploymentException>()->what());
    } else if (ex.instanceOf<InvalidQueryStatusException>()) {
        //query is already marked for failure, failed or stopped
        NES_WARNING2("Exception {}", ex.as<InvalidQueryStatusException>()->what());
    }
    //todo: handle post undeployment exception
}

void FailQueryRequest::undeployQueries(SharedQueryId sharedQueryId) {//undeploy queries
    try {
    auto queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
        //this also removes the subplans from the global execution plan. does that mean we do not have to touch that separately?
        queryUndeploymentPhase->execute(queryId, SharedQueryPlanStatus::Failed);
    } catch (Exceptions::RuntimeException& e) {
        throw Exceptions::QueryUndeploymentException(sharedQueryId, "failed to undeploy query with id " + std::to_string(queryId));
    }
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

    //respond to the calling service which is the shared query id o the query being undeployed
    FailQueryResponse response{};
    response.sharedQueryId = sharedQueryId;
    responsePromise.set_value(response);

    queryCatalogService->checkAndMarkForFailure(sharedQueryId, querySubPlanId);

    //todo: what does this call actually do if we can still retrieve the sqp in the postUndeployment funciton
    //todo: is this tested?
    globalQueryPlan->removeQuery(queryId, RequestType::FailQuery);

    //undeploy queries
    undeployQueries(storageHandle, sharedQueryId);

    //update global query plan
    postUndeployment(sharedQueryId);

    //todo: cleanup
}

void NES::Experimental::FailQueryRequest::postUndeployment(SharedQueryId sharedQueryId) {
    auto sqp = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
    if (!sqp) {
        //todo throw PostUndeploymentException query not found
    }
    for (auto& id : sqp->getQueryIds()) {
        queryCatalogService->updateQueryStatus(id, QueryStatus::FAILED, "Failed");
    }

    globalQueryPlan->removeQuery(queryId, RequestType::FailQuery);
}
}// namespace NES::Experimental
