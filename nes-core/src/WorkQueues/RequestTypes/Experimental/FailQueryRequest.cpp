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
#include <Exceptions/FailQueryRequestExecutionException.h>
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

//todo: pass a CoordinatorRequestExecutionException here?
void FailQueryRequest::postRollbackHandle(std::exception ex, NES::StorageHandler& storageHandler) {
    try {
        //todo: depending on where exactly the undeployment exception occured, we might have to repeat only parts of the undeployment phase

        auto failQueryException = dynamic_cast<FailQueryRequestExecutionException&>(ex);
        switch ((FailQueryStage) failQueryException.getStage()) {
            case FailQueryStage::MARK_FOR_FAILURE: break;
            case FailQueryStage::UNDEPLOY: {
                //undeploy queries
                globalExecutionPlan = storageHandler.getGlobalExecutionPlanHandle();
                topology = storageHandler.getTopologyHandle();
                auto queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
                queryUndeploymentPhase->execute(queryId, SharedQueryPlanStatus::Failed);
                auto sharedQueryId = failQueryException.getSharedQueryid();

                for (auto& queryId : globalQueryPlan->getSharedQueryPlan(sharedQueryId)->getQueryIds()) {
                    queryCatalogService->updateQueryStatus(queryId, QueryStatus::FAILED, "Failed");
                }
            }
                break;
            case FailQueryStage::REMOVE: break;
        }

    } catch (std::bad_cast& e) {}
}

void FailQueryRequest::postExecution(NES::StorageHandler&) {}

void NES::Experimental::FailQueryRequest::executeRequestLogic(NES::StorageHandler& storageHandle) {
    globalQueryPlan = storageHandle.getGlobalQueryPlanHandle();
    auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
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
    try {
        if (!queryUndeploymentPhase->execute(queryId, SharedQueryPlanStatus::Failed)) {
            //throw QueryUndeploymentException("could not undeploy query " + std::to_string(queryId));
            throw FailQueryRequestExecutionException((uint8_t) FailQueryStage::UNDEPLOY, sharedQueryId);
        }
    } catch (std::exception &e) {
        throw FailQueryRequestExecutionException((uint8_t) FailQueryStage::UNDEPLOY, sharedQueryId);
    }

    //update global query plan
    for (auto& id : globalQueryPlan->getSharedQueryPlan(sharedQueryId)->getQueryIds()) {
        queryCatalogService->updateQueryStatus(id, QueryStatus::FAILED, "Failed");
    }
}
}
