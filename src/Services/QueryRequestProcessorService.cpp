/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Exceptions/TypeInferenceException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Phases/QueryPlacementPhase.hpp>
#include <Phases/QueryPlacementRefinementPhase.hpp>
#include <Phases/QueryReconfigurationPhase.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryMetaData.hpp>
#include <Services/QueryRequestProcessorService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/QueryRequestQueue.hpp>
#include <exception>

namespace NES {

QueryRequestProcessorService::QueryRequestProcessorService(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                                           QueryCatalogPtr queryCatalog, GlobalQueryPlanPtr globalQueryPlan,
                                                           StreamCatalogPtr streamCatalog, WorkerRPCClientPtr workerRpcClient,
                                                           QueryRequestQueuePtr queryRequestQueue, bool enableQueryMerging)
    : queryProcessorStatusLock(), queryProcessorRunning(true), queryCatalog(queryCatalog), queryRequestQueue(queryRequestQueue),
      globalQueryPlan(globalQueryPlan) {

    NES_DEBUG("QueryRequestProcessorService()");
    typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    queryRewritePhase = QueryRewritePhase::create(streamCatalog);
    queryPlacementPhase = QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, streamCatalog);
    queryDeploymentPhase = QueryDeploymentPhase::create(globalExecutionPlan, workerRpcClient);
    queryReconfigurationPhase = QueryReconfigurationPhase::create(topology, globalExecutionPlan, workerRpcClient, streamCatalog);
    queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
    globalQueryPlanUpdatePhase =
        GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, enableQueryMerging);
}

QueryRequestProcessorService::~QueryRequestProcessorService() { NES_DEBUG("~QueryRequestProcessorService()"); }

void QueryRequestProcessorService::start() {

    try {
        while (isQueryProcessorRunning()) {
            NES_INFO("QueryRequestProcessorService: Waiting for new query request trigger");
            const std::vector<QueryCatalogEntry> queryRequests = queryRequestQueue->getNextBatch();
            NES_INFO("QueryProcessingService: Found " << queryRequests.size() << " query requests to process");
            if (queryRequests.empty()) {
                continue;
            }

            //FIXME: What to do if a query contains different placement strategy?
            std::string placementStrategy = queryRequests[0].getQueryPlacementStrategy();
            try {
                NES_INFO("QueryProcessingService: Calling GlobalQueryPlanUpdatePhase");
                globalQueryPlanUpdatePhase->execute(queryRequests);

                auto sharedQueryMetaDataToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
                for (auto sharedQueryMetaData : sharedQueryMetaDataToDeploy) {
                    SharedQueryId sharedQueryId = sharedQueryMetaData->getSharedQueryId();
                    NES_DEBUG("QueryProcessingService: Updating Query Plan with global query id : " << sharedQueryId);

                    if (sharedQueryMetaData->isEmpty() && !sharedQueryMetaData->isNew()) {
                            NES_DEBUG("QueryProcessingService: Undeploying Query Plan with global query id : " << sharedQueryId);
                            bool successful = queryUndeploymentPhase->execute(sharedQueryId);
                            if (!successful) {
                                throw QueryUndeploymentException("Unable to stop Global QueryId "
                                                                 + std::to_string(sharedQueryId));
                            }
                        } else if (sharedQueryMetaData->isNew()) {

                        auto queryPlan = sharedQueryMetaData->getQueryPlan();

                            NES_DEBUG(
                                "QueryProcessingService: Performing Query Operator placement for query with shared query id : "
                                << sharedQueryId);
                            std::string placementStrategy = queryCatalogEntry.getQueryPlacementStrategy();
                            bool placementSuccessful = queryPlacementPhase->execute(placementStrategy, queryPlan);
                            if (!placementSuccessful) {
                                throw QueryPlacementException("QueryProcessingService: Failed to perform query placement for "
                                                              "query plan with shared query id: "
                        NES_DEBUG("QueryProcessingService: Performing Query Operator placement for query with shared query id : "
                                  << sharedQueryId);

                        bool placementSuccessful = queryPlacementPhase->execute(placementStrategy, queryPlan);
                        if (!placementSuccessful) {
                            throw QueryPlacementException(sharedQueryId,
                                                          "QueryProcessingService: Failed to perform query placement for "
                                                          "query plan with shared query id: "
                                                              + std::to_string(sharedQueryId));
                            }
                            bool successful = queryDeploymentPhase->execute(sharedQueryId);
                            if (!successful) {
                                throw QueryDeploymentException(
                                    sharedQueryId,"QueryRequestProcessingService: Failed to deploy query with global query Id "
                                    + std::to_string(sharedQueryId));
                            }
                        } else {
                            auto queryPlan = sharedQueryMetaData->getQueryPlan();
                            bool successful = queryReconfigurationPhase->execute(queryPlan);
                            if (!successful) {
                                throw QueryDeploymentException(
                                    "QueryRequestProcessingService: Failed to deploy query with global query Id "
                                    + std::to_string(sharedQueryId));
                            }
                        }
                        //Mark the meta data as deployed
                        sharedQueryMetaData->markAsDeployed();
                    }

                for (auto queryRequest : queryRequests) {
                    auto queryId = queryRequest.getQueryId();
                    if (queryRequest.getQueryStatus() == QueryStatus::Registered) {
                        queryCatalog->markQueryAs(queryId, QueryStatus::Running);
                    } else {
                        queryCatalog->markQueryAs(queryId, QueryStatus::Stopped);
                    }
    }
                //FIXME: Proper error handling #1585
            } catch (QueryPlacementException& ex) {
                NES_ERROR("QueryRequestProcessingService QueryPlacementException: " << ex.what());
                auto sharedQueryId = ex.getSharedQueryId();
                queryUndeploymentPhase->execute(sharedQueryId);
                auto sharedQueryMetaData = globalQueryPlan->getSharedQueryMetaData(sharedQueryId);
                for (auto queryId : sharedQueryMetaData->getQueryIds()) {
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                }
            } catch (QueryDeploymentException& ex) {
                NES_ERROR("QueryRequestProcessingService QueryDeploymentException: " << ex.what());
                auto sharedQueryId = ex.getSharedQueryId();
                queryUndeploymentPhase->execute(sharedQueryId);
                auto sharedQueryMetaData = globalQueryPlan->getSharedQueryMetaData(sharedQueryId);
                for (auto queryId : sharedQueryMetaData->getQueryIds()) {
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                }
            } catch (TypeInferenceException& ex) {
                NES_ERROR("QueryRequestProcessingService TypeInferenceException: " << ex.what());
                auto queryId = ex.getQueryId();
                queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
            } catch (InvalidQueryStatusException& ex) {
                NES_ERROR("QueryRequestProcessingService InvalidQueryStatusException: " << ex.what());
            } catch (QueryNotFoundException& ex) {
                NES_ERROR("QueryRequestProcessingService QueryNotFoundException: " << ex.what());
            } catch (QueryUndeploymentException& ex) {
                NES_ERROR("QueryRequestProcessingService QueryUndeploymentException: " << ex.what());
            } catch (Exception& ex) {
                NES_FATAL_ERROR(
                    "QueryProcessingService: Received unexpected exception while scheduling the queries: " << ex.what());
                shutDown();
            }
        }
        NES_WARNING("QueryProcessingService: Terminated");
    } catch (std::exception& ex) {
        NES_FATAL_ERROR("QueryProcessingService: Received unexpected exception while scheduling the queries: " << ex.what());
        shutDown();
    }
}

bool QueryRequestProcessorService::isQueryProcessorRunning() {
    std::unique_lock<std::mutex> lock(queryProcessorStatusLock);
    return queryProcessorRunning;
}

void QueryRequestProcessorService::shutDown() {
    std::unique_lock<std::mutex> lock(queryProcessorStatusLock);
    this->queryProcessorRunning = false;
    queryRequestQueue->insertPoisonPill();
}

}// namespace NES