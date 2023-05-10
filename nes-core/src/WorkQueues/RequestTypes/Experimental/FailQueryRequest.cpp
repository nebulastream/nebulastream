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
#include <utility>

namespace NES::Experimental {
//todo: do we expect to already have the query id and the failed query available here or should this request work with only the failed sub query id?
FailQueryRequest::FailQueryRequest(NES::QueryId queryId, NES::QuerySubPlanId failedSubPlanId,
                                                      size_t maxRetries,
                                                      NES::WorkerRPCClientPtr  workerRpcClient) :
                                                                                     AbstractRequest(maxRetries),
                                                                                     queryId(queryId), querySubPlanId(failedSubPlanId), workerRpcClient(std::move(workerRpcClient)) {}

void FailQueryRequest::preRollbackHandle(std::exception ex, NES::StorageHandler& storageHandler) {
    (void) ex;
    (void) storageHandler;
    //todo: if failed in stage checkAndMakrForFailure: do nothing
}

void FailQueryRequest::rollBack(std::exception& ex, StorageHandler& storageHandler) {
    (void) ex;
    (void) storageHandler;
    //todo: if failed in stage checkAndMakrForFailure: do nothing
}

void FailQueryRequest::postRollbackHandle(std::exception ex, NES::StorageHandler& storageHandler) {
    (void) ex;
    (void) storageHandler;
    //todo: if failed in stage checkAndMakrForFailure: do nothing
}

//todo: assign member variables here already or do it in execute function?
/*
void FailQueryRequest::preExecution(NES::StorageHandler& storageHandler) {
    this->storageHandle = storageHandle;
    storageHandle->preExecution(requiredResources);
}
 */

void FailQueryRequest::postExecution(NES::StorageHandler& storageHandler) {
    (void) storageHandler;
    //todo: reset member vars
    //todo: communicate with workers if necessary
}

void NES::Experimental::FailQueryRequest::executeRequestLogic(NES::StorageHandler& storageHandle) {
    globalQueryPlan = storageHandle.getGlobalQueryPlanHandle();
    auto sharedQueryPlanId = globalQueryPlan->getSharedQueryId(queryId);
    if (sharedQueryPlanId == INVALID_SHARED_QUERY_ID) {
        throw QueryNotFoundException("Could not find a query with the id " + std::to_string(queryId) + " in the global query plan");
    }

    queryCatalogService = storageHandle.getQueryCatalogHandle();

    //todo: why do we have to specify the query sub plan id here?
    if (!queryCatalogService->checkAndMarkForFailure(sharedQueryPlanId, querySubPlanId)) {
        /*todo: this should throw an invalid query status exception, but this has to be implemented inside the checkAndMarkForFailure function
         * because we do not know the actual type when we just get a bool returned
         */
        throw std::exception();
    }

    //todo: this can throw a runtime exception, what do we do in the error handling when we fail in this stage?
    globalQueryPlan->removeQuery(queryId, RequestType::Fail);
    globalExecutionPlan = storageHandle.getGlobalExecutionPlanHandle();
    topology = storageHandle.getTopologyHandle();
    auto queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);

    //todo: exception handling
    queryUndeploymentPhase->execute(queryId, SharedQueryPlanStatus::Failed);
    for (auto& queryId : globalQueryPlan->getSharedQueryPlan(sharedQueryPlanId)->getQueryIds()) {
        queryCatalogService->updateQueryStatus(queryId, QueryStatus::FAILED, "Failed");
    }
}
}
