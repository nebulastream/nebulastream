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

#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Util/RequestType.hpp>
#include <WorkQueues/RequestTypes/StopQueryRequest.hpp>
#include <string>
namespace NES {

//todo: remove method after Request Restructuring was successfully completed
StopQueryRequest::StopQueryRequest(QueryId queryId)
    : AbstractRequest(0), workerRpcClient(getWorkerRpcClient()), queryId(queryId), queryReconfiguration(false) {}
//todo: remove constructor after Request Restructuring was successfully completed
StopQueryRequestPtr StopQueryRequest::create(QueryId queryId) {
    return std::make_shared<StopQueryRequest>(StopQueryRequest(queryId));
}

StopQueryRequest::StopQueryRequest(QueryId queryId,
                                   size_t maxRetries,
                                   const WorkerRPCClientPtr& workerRpcClient,
                                   bool queryReconfiguration)
    : AbstractRequest(maxRetries), workerRpcClient(std::move(workerRpcClient)), queryId(queryId),
      queryReconfiguration(queryReconfiguration) {
    requiredResources = {
        StorageHandlerResourceType::QueryCatalogService,
        StorageHandlerResourceType::GlobalExecutionPlan,
        StorageHandlerResourceType::Topology,
        StorageHandlerResourceType::GlobalQueryPlan,
        StorageHandlerResourceType::UdfCatalog,
        StorageHandlerResourceType::SourceCatalog,
    };
}

StopQueryRequestPtr StopQueryRequest::create(QueryId queryId,
                                            size_t maxRetries,
                                            const WorkerRPCClientPtr& workerRpcClient,
                                            bool queryReconfiguration) {
    return std::make_shared<StopQueryRequest>(StopQueryRequest(queryId, maxRetries, workerRpcClient, queryReconfiguration));
}

void StopQueryRequest::preExecution(StorageHandler& storageHandler) {
    NES_TRACE2("Acquire locks.");
    storageHandler.acquireResources(requiredResources);
    globalExecutionPlan = storageHandler.getGlobalExecutionPlanHandle();
    topology = storageHandler.getTopologyHandle();
    queryCatalogService = storageHandler.getQueryCatalogServiceHandle();
    globalQueryPlan = storageHandler.getGlobalQueryPlanHandle();
    udfCatalog = storageHandler.getUDFCatalogHandle();
    sourceCatalog = storageHandler.getSourceCatalogHandle();
    NES_TRACE2("Locks acquired. Create Phases");
    typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, queryReconfiguration);
    queryDeploymentPhase = QueryDeploymentPhase::create(globalExecutionPlan, workerRpcClient, queryCatalogService);
    queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
    NES_TRACE2("Phases created.");
}

void StopQueryRequest::executeRequestLogic(StorageHandler& storageHandler) {
    NES_TRACE2("Start Stop Request logic.");
    try {
        //mark single query for hard stop
        auto markedForHardStopSuccessful = queryCatalogService->checkAndMarkForHardStop(queryId);
        if (!markedForHardStopSuccessful) {
            std::exception e = std::runtime_error("Failed to mark query for hard stop");
            throw e;
        }
        //remove single query from global query plan
        globalQueryPlan->removeQuery(queryId, RequestType::Stop);
        auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
        auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
        //undeploy SQP
        bool undeploymentSuccessful = queryUndeploymentPhase->execute(sharedQueryId, sharedQueryPlan->getStatus());
        if (!undeploymentSuccessful) {
            std::exception e = std::runtime_error("Failed to undeploy query");
            throw e;
        }
        if (SharedQueryPlanStatus::Stopped == sharedQueryPlan->getStatus()) {
            //Mark all contained queryIdAndCatalogEntryMapping as stopped
            for (auto& involvedQueryIds : sharedQueryPlan->getQueryIds()) {
                queryCatalogService->updateQueryStatus(involvedQueryIds, QueryStatus::STOPPED, "Hard Stopped");
            }
            //todo: #3728 remove shared query plan
        } else if (SharedQueryPlanStatus::Updated == sharedQueryPlan->getStatus()) {
            //3.3.2. Perform placement of updated shared query plan
            auto queryPlan = sharedQueryPlan->getQueryPlan();
            NES_DEBUG2("QueryProcessingService: Performing Operator placement for shared query plan");
            //todo: 3726 Where to get the placement strategy? Currently it is supplied with the run query request
            //todo: 3726 store placement strategy in shared query plan; do this in a separate issue
            //todo: 3726 add placement strategy to index for sqp identification
            //could we move it to the SQP? Since one SQP should have the same placement strategy, right?
            //otherwise maybe add it to an individual query plan?
            bool placementSuccessful = queryPlacementPhase->execute(PlacementStrategy::TopDown, sharedQueryPlan);
            if (!placementSuccessful) {
                throw QueryPlacementException(sharedQueryId,
                                              "QueryProcessingService: Failed to perform query placement for "
                                              "query plan with shared query id: "
                                                  + std::to_string(sharedQueryId));
            }

            //3.3.3. Perform deployment of re-placed shared query plan
            bool deploymentSuccessful = queryDeploymentPhase->execute(sharedQueryPlan);
            if (!deploymentSuccessful) {
                throw QueryDeploymentException(sharedQueryId,
                                               "QueryRequestProcessingService: Failed to deploy query with global query Id "
                                                   + std::to_string(sharedQueryId));
            }

            //Update the shared query plan as deployed
            sharedQueryPlan->setStatus(SharedQueryPlanStatus::Deployed);
        }

        //FIXME: This is a work-around for an edge case. To reproduce this:
        // 1. The query merging feature is enabled.
        // 2. A query from a shared query plan was removed but over all shared query plan is still serving other queryIdAndCatalogEntryMapping (Case 3.1).
        // Expected Result:
        //  - Query status of the removed query is marked as stopped.
        // Actual Result:
        //  - Query status of the removed query will not be set to stopped and the query will remain in MarkedForHardStop.
        queryCatalogService->updateQueryStatus(queryId, QueryStatus::STOPPED, "Hard Stopped");

    } catch (std::exception& e) {
        handleError(e, storageHandler);
    }
}

void StopQueryRequest::postExecution([[maybe_unused]] StorageHandler& storageHandler) {
    NES_TRACE2("Release locks.");
}

std::string StopQueryRequest::toString() { return "StopQueryRequest { QueryId: " + std::to_string(queryId) + "}"; }

/*todo: #3724 add error handling for:
 * 1. [16:06:55.369061] [E] [thread 107422] [QueryCatalogService.cpp:324] [addUpdatedQueryPlan] QueryCatalogService: Query Catalog does not contains the input queryId 0
 * 2. [16:49:07.569624] [E] [thread 109653] [RuntimeException.cpp:31] [RuntimeException] GlobalQueryPlan: Can not add query plan with invalid id. at /home/eleicha/Documents/DFKI/Code/nebulastream/nes-core/src/Plans/Global/Query/GlobalQueryPlan.cpp:33 addQueryPlan
*/
void StopQueryRequest::preRollbackHandle(std::exception ex,[[maybe_unused]] StorageHandler& storageHandle) {
    NES_TRACE2("Error: {}", ex.what());
}
void StopQueryRequest::postRollbackHandle(std::exception ex,[[maybe_unused]] StorageHandler& storageHandle) {
    NES_TRACE2("Error: {}", ex.what());
    //todo: #3635 call fail query request
}
void StopQueryRequest::rollBack(std::exception& ex,[[maybe_unused]] StorageHandler& storageHandle) {
    NES_TRACE2("Error: {}", ex.what());
    //todo: #3723 need to add instanceOf to errors to handle failures correctly
}
const WorkerRPCClientPtr& StopQueryRequest::getWorkerRpcClient() const { return workerRpcClient; }
const QueryId& StopQueryRequest::getQueryId() const { return queryId; }
}// namespace NES