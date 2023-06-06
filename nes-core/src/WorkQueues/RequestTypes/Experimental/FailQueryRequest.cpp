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

void FailQueryRequest::postRollbackHandle(const RequestExecutionException& ex, NES::StorageHandler& storageHandler) {

    if (ex.instanceOf<Exceptions::QueryUndeploymentException>()) {
    //todo: move these cases to the back
    if (ex.instanceOf<Exceptions::QueryNotFoundException>()) {
        //todo: set flag that we do not want to retry this request?
        NES_WARNING2("Exception {}", ex.as<Exceptions::QueryUndeploymentException>()->what());
    } else if (ex.instanceOf<Exceptions::InvalidQueryStatusException>()) {
        //query is already marked for failure, failed or stopped
        //todo: set flag that we do not want to retry this request?
        NES_WARNING2("Exception {}", ex.as<Exceptions::InvalidQueryStatusException>()->what());
    } else if (ex.instanceOf<Exceptions::QueryUndeploymentException>()) {
        auto undeploymentException = dynamic_cast<Exceptions::QueryUndeploymentException&>(ex);
        //todo: laura suggests setting query to stopped for any exception not related to grpc

        //todo: keep this as handling for failed async results and differentiate it from the other cases (by subclassing undeployment exception?)
        auto sharedQueryId = undeploymentException.getSharedQueryId();

        if (sharedQueryId.has_value()) {
            //undeploy queries
            undeployQueries(storageHandler, sharedQueryId.value());

            //update global query plan
            postUndeployment(sharedQueryId.value());
        }
    }
    //todo: how to handle post undeploy exception
}

void FailQueryRequest::undeployQueries(SharedQueryId sharedQueryId) {//undeploy queries
    try {
    auto queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
        //this also removes the subplans from the global execution plan. does that mean we do not have to touch that separately?
        queryUndeploymentPhase->execute(queryId, SharedQueryPlanStatus::Failed);
    } catch (Exceptions::RuntimeException& e) {
        throw QueryUndeploymentException(sharedQueryId, "failed to undeploy query with id " + std::to_string(queryId));
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

    //todo: mark for failure without query subplan id if none was supplied. laura had a function for this?
    if (querySubPlanId == INVALID_QUERY_SUB_PLAN_ID) {
        //todo: remove wihtout sublplan id
    } else {
        queryCatalogService->checkAndMarkForFailure(sharedQueryId, querySubPlanId);
    }

    //todo: what does this call actually do if we can still retrieve the sqp in the postUndeployment funciton
    //todo: is this tested?
    globalQueryPlan->removeQuery(queryId, RequestType::FailQuery);

    //undeploy queries
    undeployQueries(storageHandle, sharedQueryId);

    //update global query plan
    postUndeployment(sharedQueryId);

    //todo: cleanup
}

//todo: rename to after undeployment
void NES::Experimental::FailQueryRequest::postUndeployment(SharedQueryId sharedQueryId) {
    auto sqp = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
    if (!sqp) {
        //todo throw PostUndeploymentException
    }
    for (auto& id : sqp->getQueryIds()) {
        queryCatalogService->updateQueryStatus(id, QueryStatus::FAILED, "Failed");
    }

    //todo: remove failed shared query plan from global query plan (how?)
}
}// namespace NES::Experimental
