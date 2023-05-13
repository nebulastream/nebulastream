#include <Util/RequestType.hpp>
#include <WorkQueues/RequestTypes/Experimental/FailQueryRequest.hpp>
#include <WorkQueues/StorageHandles/StorageHandler.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <WorkQueues/StorageHandles/StorageHandlerResourceType.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <utility>

namespace NES::Experimental {
FailQueryRequest::FailQueryRequest(NES::QueryId queryId, NES::QuerySubPlanId failedSubPlanId,
                                                      size_t maxRetries,
                                                      NES::WorkerRPCClientPtr  workerRpcClient) :
                                                                                     AbstractRequest(maxRetries),
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
        throw QueryNotFoundException("Could not find a query with the id " + std::to_string(queryId) + " in the global query plan");
    }

    queryCatalogService = storageHandle.getQueryCatalogHandle();

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
}
