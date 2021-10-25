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
#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Exceptions/TypeInferenceException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Optimizer/Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryPlacementRefinementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Services/NESRequestProcessorService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/NESRequestQueue.hpp>
#include <WorkQueues/RequestTypes/NESRequest.hpp>
#include <WorkQueues/RequestTypes/RunQueryRequest.hpp>
#include <exception>
#include <utility>
#include <z3++.h>

namespace NES {

NESRequestProcessorService::NESRequestProcessorService(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                       const TopologyPtr& topology,
                                                       const QueryCatalogPtr& queryCatalog,
                                                       const GlobalQueryPlanPtr& globalQueryPlan,
                                                       const StreamCatalogPtr& streamCatalog,
                                                       const WorkerRPCClientPtr& workerRpcClient,
                                                       NESRequestQueuePtr queryRequestQueue,
                                                       Optimizer::QueryMergerRule queryMergerRule)
    : queryProcessorRunning(true), queryCatalog(queryCatalog), queryRequestQueue(std::move(queryRequestQueue)),
      globalQueryPlan(globalQueryPlan) {

    NES_DEBUG("QueryRequestProcessorService()");
    typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, streamCatalog, z3Context);
    queryDeploymentPhase = QueryDeploymentPhase::create(globalExecutionPlan, workerRpcClient);
    queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
    z3::config cfg;
    cfg.set("timeout", 50000);
    cfg.set("model", false);
    cfg.set("type_check", false);
    z3Context = std::make_shared<z3::context>(cfg);
    globalQueryPlanUpdatePhase =
        Optimizer::GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, z3Context, queryMergerRule);
}

NESRequestProcessorService::~NESRequestProcessorService() { NES_DEBUG("~QueryRequestProcessorService()"); }

void NESRequestProcessorService::start() {
    try {
        while (isQueryProcessorRunning()) {
            NES_DEBUG("QueryRequestProcessorService: Waiting for new query request trigger");
            auto nesRequests = queryRequestQueue->getNextBatch();
            NES_TRACE("QueryProcessingService: Found " << nesRequests.size() << " query requests to process");
            if (nesRequests.empty()) {
                continue;
            }

            //FIXME: What to do if a different requests contain different placement strategies within a batch?
            std::string placementStrategy = "BottomUp";
            if (nesRequests[0]->instanceOf<RunQueryRequest>()) {
                placementStrategy = nesRequests[0]->as<RunQueryRequest>()->getQueryPlacementStrategy();
            }

            try {
                NES_INFO("QueryProcessingService: Calling GlobalQueryPlanUpdatePhase");
                globalQueryPlanUpdatePhase->execute(nesRequests);
                auto sharedQueryPlanToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
                for (const auto& sharedQueryPlan : sharedQueryPlanToDeploy) {
                    SharedQueryId sharedQueryId = sharedQueryPlan->getSharedQueryId();
                    NES_DEBUG("QueryProcessingService: Updating Query Plan with global query id : " << sharedQueryId);

                    if (!sharedQueryPlan->isNew()) {
                        NES_DEBUG("QueryProcessingService: Undeploying Query Plan with global query id : " << sharedQueryId);
                        bool successful = queryUndeploymentPhase->execute(sharedQueryId);
                        if (!successful) {
                            throw QueryUndeploymentException("Unable to stop Global QueryId " + std::to_string(sharedQueryId));
                        }
                    }

                    if (!sharedQueryPlan->isEmpty()) {
                        auto queryPlan = sharedQueryPlan->getQueryPlan();
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
                                sharedQueryId,
                                "QueryRequestProcessingService: Failed to deploy query with global query Id "
                                    + std::to_string(sharedQueryId));
                        }
                    }
                    //Mark the meta data as deployed
                    sharedQueryPlan->markAsDeployed();
                    sharedQueryPlan->setAsOld();
                }

                for (const auto& queryRequest : nesRequests) {
                    auto queryId = queryRequest->getQueryId();
                    auto catalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
                    if (catalogEntry->getQueryStatus() == QueryStatus::Scheduling) {
                        queryCatalog->markQueryAs(queryId, QueryStatus::Running);
                    } else {
                        queryCatalog->markQueryAs(queryId, QueryStatus::Stopped);
                    }
                }
                //FIXME: Proper error handling #1585
            } catch (QueryPlacementException& ex) {
                NES_ERROR("QueryRequestProcessingService QueryPlacementException: " << ex.what());
                NES_ERROR("QueryRequestProcessingService QueryPlacementException: " << ex.what());
                auto sharedQueryId = ex.getSharedQueryId();
                queryUndeploymentPhase->execute(sharedQueryId);
                auto sharedQueryMetaData = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
                for (auto queryId : sharedQueryMetaData->getQueryIds()) {
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                }
            } catch (QueryDeploymentException& ex) {
                NES_ERROR("QueryRequestProcessingService QueryDeploymentException: " << ex.what());
                NES_ERROR("QueryRequestProcessingService QueryDeploymentException: " << ex.what());
                auto sharedQueryId = ex.getSharedQueryId();
                queryUndeploymentPhase->execute(sharedQueryId);
                auto sharedQueryMetaData = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
                for (auto queryId : sharedQueryMetaData->getQueryIds()) {
                    queryCatalog->setQueryFailureReason(queryId, ex.what());
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
            } catch (InvalidQueryException& ex) {
                NES_ERROR("QueryRequestProcessingService InvalidQueryException: " << ex.what());
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

bool NESRequestProcessorService::isQueryProcessorRunning() {
    std::unique_lock<std::mutex> lock(queryProcessorStatusLock);
    return queryProcessorRunning;
}

void NESRequestProcessorService::shutDown() {
    std::unique_lock<std::mutex> lock(queryProcessorStatusLock);
    if (queryProcessorRunning) {
        this->queryProcessorRunning = false;
        queryRequestQueue->insertPoisonPill();
    }
}

}// namespace NES