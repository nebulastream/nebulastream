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
//todo: do we expect to already have the query id and the failed query available here or should this request work with only the failed sub query id?
FailQueryRequest::FailQueryRequest(NES::QueryId queryId, NES::QuerySubPlanId failedSubPlanId,
                                                      size_t maxRetries,
                                                      NES::WorkerRPCClientPtr  workerRpcClient) :
                                                                                     AbstractRequest(maxRetries),
                                                                                     queryId(queryId), querySubPlanId(failedSubPlanId), workerRpcClient(std::move(workerRpcClient)) {}

void FailQueryRequest::preRollbackHandle(std::exception, NES::StorageHandler&) {}

void FailQueryRequest::rollBack(std::exception&, StorageHandler&) {}

void FailQueryRequest::postRollbackHandle(std::exception ex, NES::StorageHandler& storageHandler) {
    //todo: is this an efficient design?
    try {
        //todo: depending on where exactly the undeployment exception occured, we might have to repeat only parts of the undeployment phase
        //undeploy queries
        globalExecutionPlan = storageHandler.getGlobalExecutionPlanHandle();
        topology = storageHandler.getTopologyHandle();
        auto queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
        auto undeploymentException = dynamic_cast<QueryUndeploymentException&>(ex);
        queryUndeploymentPhase->execute(queryId, SharedQueryPlanStatus::Failed);

        for (auto& queryId : globalQueryPlan->getSharedQueryPlan(sharedQueryId)->getQueryIds()) {
            queryCatalogService->updateQueryStatus(queryId, QueryStatus::FAILED, "Failed");
        }

    } catch (std::bad_cast& e) {}
}

void FailQueryRequest::postExecution(NES::StorageHandler&) {}

void NES::Experimental::FailQueryRequest::executeRequestLogic(NES::StorageHandler& storageHandle) {
    globalQueryPlan = storageHandle.getGlobalQueryPlanHandle();
    sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
    if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
        throw QueryNotFoundException("Could not find a query with the id " + std::to_string(queryId) + " in the global query plan");
    }

    queryCatalogService = storageHandle.getQueryCatalogHandle();

    //todo: why do we have to specify the query sub plan id here?
    queryCatalogService->checkAndMarkForFailure(sharedQueryId, querySubPlanId);

    //todo: this can throw an exception on map access, what do we do in the error handling when we fail in this stage?
    globalQueryPlan->removeQuery(queryId, RequestType::Fail);

    //undeploy queries
    globalExecutionPlan = storageHandle.getGlobalExecutionPlanHandle();
    topology = storageHandle.getTopologyHandle();
    auto queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
    //todo: exception handling
    if (!queryUndeploymentPhase->execute(queryId, SharedQueryPlanStatus::Failed)) {
        throw QueryUndeploymentException("could not undeploy query " + std::to_string(queryId));
    }

    //update global query plan
    for (auto& id : globalQueryPlan->getSharedQueryPlan(sharedQueryId)->getQueryIds()) {
        queryCatalogService->updateQueryStatus(id, QueryStatus::FAILED, "Failed");
    }
}
}
