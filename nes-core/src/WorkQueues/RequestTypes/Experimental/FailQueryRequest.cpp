#include <Util/RequestType.hpp>
#include <WorkQueues/RequestTypes/Experimental/FailQueryRequest.hpp>
#include <WorkQueues/StorageHandles/StorageHandler.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <WorkQueues/StorageHandles/StorageHandlerResourceType.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <utility>

namespace NES::Experimental {
//todo: do we expect to already have the sqp and the failed query available here or should this request work with only the failed sub query id?
//todo: global query plan seems to have a get shared query id function
FailQueryRequest::FailQueryRequest(NES::QueryId queryId, NES::QuerySubPlanId failedSubPlanId,
                                                      size_t maxRetries,
                                                      NES::WorkerRPCClientPtr  workerRpcClient) :
                                                                                     AbstractRequest(maxRetries),
                                                                                     queryId(queryId), workerRpcClient(std::move(workerRpcClient)) {}

void FailQueryRequest::preRollbackHandle(std::exception ex, NES::StorageHandler& storageHandler) {
    //todo: if failed in stage checkAndMakrForFailure: do nothing
}

void FailQueryRequest::rollBack(std::exception& ex, StorageHandler& storageHandle) {
    //todo: if failed in stage checkAndMakrForFailure: do nothing
}

void FailQueryRequest::postRollbackHandle(std::exception ex, NES::StorageHandler& storageHandler) {
    //todo: if failed in stage checkAndMakrForFailure: do nothing
}

//todo: assign member variables here already or do it in execute function?
/*
void FailQueryRequest::preExecution(NES::StorageHandler& storageHandler) {
    this->storageHandle = storageHandle;
    storageHandle->preExecution(requiredResources);
    //todo: assign resources to member variables
}
 */

void FailQueryRequest::postExecution(NES::StorageHandler& storageHandle) {
    //todo: reset member vars
    //todo: communicate with workers if necessary
}

void NES::Experimental::FailQueryRequest::executeRequestLogic(NES::StorageHandler& storageHandle) {
    //todo: this is what is done in the old code, but is this also what we want to keep doing?
    //globalQueryPlan->removeQuery(queryId, RequestType::Fail);

    globalQueryPlan = storageHandle.getGlobalQueryPlanHandle();
    auto sharedQueryPlanId = globalQueryPlan->getSharedQueryId(queryId);
    queryCatalogService = storageHandle.getQueryCatalogHandle();
    //todo: why do we have to specify the query sub plan id here?
    if (!queryCatalogService->checkAndMarkForFailure(sharedQueryPlanId, querySubPlanId)) {
        //todo: make custom exception for this request type
        //todo: add stage enum to exception type?
        throw std::exception();
    }

    //todo: using this call will probably not work, because the sqp is only marked for failure and not failed yet
    //todo: actually the query catalog service does not have any visibility of the actual sqps, so it cannot have changed their status
    //globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();

    globalQueryPlan->removeQuery(queryId, RequestType::Fail);
    globalExecutionPlan = storageHandle.getGlobalExecutionPlanHandle();
    auto queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);

    //todo: exception handling
    queryUndeploymentPhase->execute(queryId, SharedQueryPlanStatus::Failed);
    for (auto& queryId : globalQueryPlan->getSharedQueryPlan(sharedQueryPlanId)->getQueryIds()) {
        queryCatalogService->updateQueryStatus(queryId, QueryStatus::STOPPED, "Hard Stopped");
    }
}
}
