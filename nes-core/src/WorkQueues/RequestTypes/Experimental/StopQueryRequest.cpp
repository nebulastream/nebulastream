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

#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Exceptions/InvalidQueryStateException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Exceptions/TypeInferenceException.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/RequestType.hpp>
#include <WorkQueues/RequestTypes/Experimental/FailQueryRequest.hpp>
#include <WorkQueues/RequestTypes/Experimental/StopQueryRequest.hpp>
#include <string>
#include <utility>

namespace NES {

namespace NES::Experimental {
StopQueryRequestExperimental::StopQueryRequestExperimental(const RequestId requestId,
                                                           const QueryId queryId,
                                                           const size_t maxRetries,
                                                           WorkerRPCClientPtr workerRpcClient,
                                                           Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                                           std::promise<StopQueryResponse> responsePromise)
    : AbstractRequest(requestId,
                      {
                          ResourceType::QueryCatalogService,
                          ResourceType::GlobalExecutionPlan,
                          ResourceType::Topology,
                          ResourceType::GlobalQueryPlan,
                          ResourceType::UdfCatalog,
                          ResourceType::SourceCatalog,
                      },
                      maxRetries,
                      std::move(responsePromise)),
      workerRpcClient(std::move(workerRpcClient)), queryId(queryId),
      coordinatorConfiguration(std::move(coordinatorConfiguration)) {}

StopQueryRequestPtr StopQueryRequestExperimental::create(const RequestId requestId,
                                                         const QueryId queryId,
                                                         const size_t maxRetries,
                                                         WorkerRPCClientPtr workerRpcClient,
                                                         Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                                         std::promise<StopQueryResponse> responsePromise) {
    return std::make_shared<StopQueryRequestExperimental>(requestId,
                                                          queryId,
                                                          maxRetries,
                                                          std::move(workerRpcClient),
                                                          std::move(coordinatorConfiguration),
                                                          std::move(responsePromise));
}

void StopQueryRequestExperimental::preExecution(StorageHandler& storageHandler) {
    NES_TRACE("Acquire Resources.");
    try {
        globalExecutionPlan = storageHandler.getGlobalExecutionPlanHandle(requestId);
        topology = storageHandler.getTopologyHandle(requestId);
        queryCatalogService = storageHandler.getQueryCatalogServiceHandle(requestId);
        globalQueryPlan = storageHandler.getGlobalQueryPlanHandle(requestId);
        udfCatalog = storageHandler.getUDFCatalogHandle(requestId);
        sourceCatalog = storageHandler.getSourceCatalogHandle(requestId);
        NES_TRACE("Locks acquired. Create Phases");
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        queryPlacementPhase =
            Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
        queryDeploymentPhase =
            QueryDeploymentPhase::create(globalExecutionPlan, workerRpcClient, queryCatalogService, coordinatorConfiguration);
        queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
        NES_TRACE("Phases created. Stop request initialized.");
    } catch (std::exception& e) {
        NES_TRACE("Failed to acquire resources.");
        //todo #3611: instead of matching on std::exception, implement a storae access handle excpetion which can be passed on
        RequestExecutionException executionException("Could not acquire resources");
        handleError(executionException, storageHandler);
    }
}

void StopQueryRequestExperimental::executeRequestLogic(StorageHandler& storageHandler) {
    NES_TRACE("Start Stop Request logic.");
    try {
        if (queryId == INVALID_SHARED_QUERY_ID) {
            throw QueryNotFoundException("Cannot stop query with invalid query id " + std::to_string(queryId)
                                         + ". Please enter a valid query id.");
        }
        //mark single query for hard stop
        auto markedForHardStopSuccessful = queryCatalogService->checkAndMarkForHardStop(queryId);
        if (!markedForHardStopSuccessful) {
            throw InvalidQueryStatusException({QueryStatus::OPTIMIZING,
                                               QueryStatus::REGISTERED,
                                               QueryStatus::DEPLOYED,
                                               QueryStatus::RUNNING,
                                               QueryStatus::RESTARTING},
                                              queryCatalogService->getEntryForQuery(queryId)->getQueryStatus());
        }
        //remove single query from global query plan
        globalQueryPlan->removeQuery(queryId, RequestType::StopQuery);
        auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
        if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
            throw QueryNotFoundException("Could not find a a valid shared query plan for query with id " + std::to_string(queryId)
                                         + " in the global query plan");
        }
        auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
        if (sharedQueryPlan == nullptr) {
            throw QueryNotFoundException("Could not find a a valid shared query plan for query with id " + std::to_string(queryId)
                                         + " in the global query plan");
        }
        //undeploy SQP
        queryUndeploymentPhase->execute(sharedQueryId, sharedQueryPlan->getStatus());
        if (SharedQueryPlanStatus::Stopped == sharedQueryPlan->getStatus()) {
            //Mark all contained queryIdAndCatalogEntryMapping as stopped
            for (auto& involvedQueryIds : sharedQueryPlan->getQueryIds()) {
                queryCatalogService->updateQueryStatus(involvedQueryIds, QueryState::STOPPED, "Hard Stopped");
            }
            globalQueryPlan->removeSharedQueryPlan(sharedQueryId);
        } else if (SharedQueryPlanStatus::Updated == sharedQueryPlan->getStatus()) {
            //3.3.2. Perform placement of updated shared query plan
            auto queryPlan = sharedQueryPlan->getQueryPlan();
            NES_DEBUG("QueryProcessingService: Performing Operator placement for shared query plan");
            bool placementSuccessful = queryPlacementPhase->execute(sharedQueryPlan);
            if (!placementSuccessful) {
                throw Exceptions::QueryPlacementException(sharedQueryId,
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

        //todo: #3742 FIXME: This is a work-around for an edge case. To reproduce this:
        // 1. The query merging feature is enabled.
        // 2. A query from a shared query plan was removed but over all shared query plan is still serving other queryIdAndCatalogEntryMapping (Case 3.1).
        // Expected Result:
        //  - Query status of the removed query is marked as stopped.
        // Actual Result:
        //  - Query status of the removed query will not be set to stopped and the query will remain in MarkedForHardStop.
        queryCatalogService->updateQueryStatus(queryId, QueryState::STOPPED, "Hard Stopped");

    } catch (RequestExecutionException& e) {
        handleError(e, storageHandler);
    }
}

void StopQueryRequestExperimental::postExecution([[maybe_unused]] StorageHandler& storageHandler) {
    NES_TRACE("Release locks.");
    storageHandler.releaseResources(requestId);
}

std::string StopQueryRequestExperimental::toString() { return "StopQueryRequest { QueryId: " + std::to_string(queryId) + "}"; }

void StopQueryRequestExperimental::preRollbackHandle(const RequestExecutionException& ex,
                                                     [[maybe_unused]] StorageHandler& storageHandle) {
    NES_TRACE("Error: {}", ex.what());
}

void StopQueryRequestExperimental::postRollbackHandle(const RequestExecutionException& ex,
                                                      [[maybe_unused]] StorageHandler& storageHandle) {
    NES_TRACE("Error: {}", ex.what());
    //todo: #3635 call fail query request
}

void StopQueryRequestExperimental::rollBack(const RequestExecutionException& ex, [[maybe_unused]] StorageHandler& storageHandle) {
    NES_TRACE("Error: {}", ex.what());
    //todo: when I would call fail query request, how would I release the allocated resources first?
    NES_TRACE("Error: {}", ex.what());
    if (ex.instanceOf<InvalidQueryStatusException>()) {
        //todo: currently, we only log the error. Should we change this behavior? I'm not even sure, if throwing an exception here is really the best way of handling this situation as it only occurs when the
        //query already failed, stopped, marked for stop etc.
        NES_ERROR("InvalidQueryStatusException: {}", ex.what());
    } else if (ex.instanceOf<QueryPlacementException>()) {
        //todo: should I call fail request here instead? What is the querySubplanId and how do I get it?
        NES_ERROR("QueryPlacementException: {}", ex.what());
        auto sharedQueryId = ex.as<QueryPlacementException>()->getSharedQueryId();
        queryUndeploymentPhase->execute(sharedQueryId, SharedQueryPlanStatus::Failed);
        auto sharedQueryMetaData = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
        for (auto currentQueryId : sharedQueryMetaData->getQueryIds()) {
            queryCatalogService->updateQueryStatus(currentQueryId, QueryStatus::FAILED, ex.what());
        }
    } else if (ex.instanceOf<QueryDeploymentException>()) {
        //todo: should I call fail request here instead? What is the querySubplanId and how do I get it?
        NES_ERROR("QueryDeploymentException: {}", ex.what());
        auto sharedQueryId = ex.as<QueryDeploymentException>()->getSharedQueryId();
        queryUndeploymentPhase->execute(sharedQueryId, SharedQueryPlanStatus::Failed);
        auto sharedQueryMetaData = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
        for (auto currentQueryId : sharedQueryMetaData->getQueryIds()) {
            queryCatalogService->updateQueryStatus(currentQueryId, QueryStatus::FAILED, ex.what());
        }
    } else if (ex.instanceOf<TypeInferenceException>()) {
        NES_ERROR("TypeInferenceException: {}", ex.what());
        auto currentQueryId = ex.as<TypeInferenceException>()->getQueryId();
        queryCatalogService->updateQueryStatus(currentQueryId, QueryStatus::FAILED, ex.what());
    } else if (ex.instanceOf<QueryUndeploymentException>()) {
        NES_ERROR("QueryUndeploymentException: {}", ex.what());
        //todo: call fail query request
    } else if (ex.instanceOf<QueryNotFoundException>()) {
        NES_ERROR("QueryNotFoundException: {}", ex.what());
    } else {
        NES_ERROR("Unknown exception: {}", ex.what());
    }
    //todo: #3723 need to add instanceOf to errors to handle failures correctly
}
}// namespace NES::Experimental