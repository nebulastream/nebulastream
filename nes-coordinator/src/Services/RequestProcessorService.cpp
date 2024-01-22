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

#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include <Catalogs/Exceptions/QueryNotFoundException.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Optimizer/Exceptions/QueryPlacementAdditionException.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/RequestTypes/QueryRequests/FailQueryRequest.hpp>
#include <Optimizer/RequestTypes/QueryRequests/StopQueryRequest.hpp>
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Services/RequestProcessorService.hpp>
#include <Util/Logger/Logger.hpp>
#include <WorkQueues/RequestQueue.hpp>
#include <exception>
#include <z3++.h>

namespace NES {

RequestProcessorService::RequestProcessorService(const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan,
                                                 const TopologyPtr& topology,
                                                 const QueryCatalogServicePtr& queryCatalogService,
                                                 const GlobalQueryPlanPtr& globalQueryPlan,
                                                 const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                 const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                                                 const RequestQueuePtr& queryRequestQueue,
                                                 const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration,
                                                 const z3::ContextPtr& z3Context)
    : queryProcessorRunning(true), queryCatalogService(queryCatalogService), queryRequestQueue(queryRequestQueue),
      globalQueryPlan(globalQueryPlan), globalExecutionPlan(globalExecutionPlan), udfCatalog(udfCatalog), z3Context(z3Context) {

    NES_DEBUG("QueryRequestProcessorService()");
    typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    queryReconfiguration = coordinatorConfiguration->enableQueryReconfiguration;
    globalQueryPlanUpdatePhase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                                               queryCatalogService,
                                                                               sourceCatalog,
                                                                               globalQueryPlan,
                                                                               z3Context,
                                                                               coordinatorConfiguration,
                                                                               udfCatalog,
                                                                               globalExecutionPlan);
    queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                   topology,
                                                                                   typeInferencePhase,
                                                                                   coordinatorConfiguration);
    queryDeploymentPhase = QueryDeploymentPhase::create(globalExecutionPlan, queryCatalogService, coordinatorConfiguration);
    queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan);
}

void RequestProcessorService::start() {
    try {
        while (isQueryProcessorRunning()) {
            NES_DEBUG("QueryRequestProcessorService: Waiting for new query request trigger");
            auto requests = queryRequestQueue->getNextBatch();
            NES_TRACE("QueryProcessingService: Found {} query requests to process", requests.size());
            if (requests.empty()) {
                continue;
            }

            try {

                NES_INFO("QueryProcessingService: Calling GlobalQueryPlanUpdatePhase");
                //1. Update global query plan by applying all requests
                globalQueryPlanUpdatePhase->execute(requests);

                //2. Fetch all shared query plans updated post applying the requests
                auto sharedQueryPlanToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

                //3. Iterate over shared query plans and take decision to a. deploy, b. undeploy, or c. redeploy
                for (const auto& sharedQueryPlan : sharedQueryPlanToDeploy) {

                    // Check if experimental feature for reconfiguring shared query plans without redeployment is disabled
                    if (!queryReconfiguration) {

                        //3.1. Fetch the shared query plan id
                        SharedQueryId sharedQueryId = sharedQueryPlan->getId();
                        NES_DEBUG("QueryProcessingService: Updating Query Plan with global query id : {}", sharedQueryId);

                        //3.2. If the shared query plan is newly created
                        if (SharedQueryPlanStatus::CREATED == sharedQueryPlan->getStatus()) {

                            NES_DEBUG("QueryProcessingService: Shared Query Plan is new.");

                            //3.2.1. Perform placement of new shared query plan
                            auto queryPlan = sharedQueryPlan->getQueryPlan();
                            NES_DEBUG("QueryProcessingService: Performing Operator placement for shared query plan");
                            auto deploymentContexts = queryPlacementAmendmentPhase->execute(sharedQueryPlan);

                            //3.2.2. Perform deployment of placed shared query plan
                            queryDeploymentPhase->execute(sharedQueryPlan);

                            //Update the shared query plan as deployed
                            sharedQueryPlan->setStatus(SharedQueryPlanStatus::DEPLOYED);

                            // 3.3. Check if the shared query plan was updated after addition or removal of operators
                        } else if (SharedQueryPlanStatus::UPDATED == sharedQueryPlan->getStatus()) {

                            NES_DEBUG("QueryProcessingService: Shared Query Plan is non empty and an older version is already "
                                      "running.");

                            //3.3.1. First undeploy the running shared query plan with the shared query plan id
                            queryUndeploymentPhase->execute(sharedQueryId, SharedQueryPlanStatus::UPDATED);

                            //3.3.2. Perform placement of updated shared query plan
                            auto queryPlan = sharedQueryPlan->getQueryPlan();
                            NES_DEBUG("QueryProcessingService: Performing Operator placement for shared query plan");
                            auto deploymentContexts = queryPlacementAmendmentPhase->execute(sharedQueryPlan);
                            //3.3.3. Perform deployment of re-placed shared query plan
                            queryDeploymentPhase->execute(sharedQueryPlan);
                            //Update the shared query plan as deployed
                            sharedQueryPlan->setStatus(SharedQueryPlanStatus::DEPLOYED);

                            // 3.4. Check if the shared query plan is empty and already running
                        } else if (SharedQueryPlanStatus::STOPPED == sharedQueryPlan->getStatus()
                                   || SharedQueryPlanStatus::FAILED == sharedQueryPlan->getStatus()) {

                            NES_DEBUG("QueryProcessingService: Shared Query Plan is empty and an older version is already "
                                      "running.");

                            //3.4.1. Undeploy the running shared query plan
                            queryUndeploymentPhase->execute(sharedQueryId, sharedQueryPlan->getStatus());

                            //3.4.2. Mark all contained queryIdAndCatalogEntryMapping as stopped
                            for (auto& queryId : sharedQueryPlan->getQueryIds()) {
                                queryCatalogService->updateQueryStatus(queryId, QueryState::STOPPED, "Hard Stopped");
                            }
                        }

                    } else {
                        //Yet another cool feature under development
                    }
                }

                //Remove all query plans that are either stopped or failed
                globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();

                //FIXME: #2496 This is a work-around for an edge case. To reproduce this:
                // 1. The query merging feature is enabled.
                // 2. A query from a shared query plan was removed but over all shared query plan is still serving other queryIdAndCatalogEntryMapping (Case 3.1).
                // Expected Result:
                //  - Query status of the removed query is marked as stopped.
                // Actual Result:
                //  - Query status of the removed query will not be set to stopped and the query will remain in MarkedForHardStop.
                for (const auto& queryRequest : requests) {
                    if (queryRequest->instanceOf<StopQueryRequest>()) {
                        auto stopQueryRequest = queryRequest->as<StopQueryRequest>();
                        auto queryId = stopQueryRequest->getQueryId();
                        queryCatalogService->updateQueryStatus(queryId, QueryState::STOPPED, "Hard Stopped");
                    } else if (queryRequest->instanceOf<FailQueryRequest>()) {
                        auto failQueryRequest = queryRequest->as<FailQueryRequest>();
                        auto sharedQueryPlanId = failQueryRequest->getSharedQueryId();
                        auto failureReason = failQueryRequest->getFailureReason();
                        auto queryIds = queryCatalogService->getQueryIdsForSharedQueryId(sharedQueryPlanId);
                        for (const auto& queryId : queryIds) {
                            queryCatalogService->updateQueryStatus(queryId, QueryState::FAILED, failureReason);
                        }
                    }
                }

                //FIXME: Proper error handling #1585
            } catch (Exceptions::QueryPlacementAdditionException& ex) {
                NES_ERROR("QueryRequestProcessingService: QueryPlacementException: {}", ex.what());
                auto sharedQueryId = ex.getQueryId();
                queryUndeploymentPhase->execute(sharedQueryId, SharedQueryPlanStatus::FAILED);
                auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
                for (auto queryId : sharedQueryPlan->getQueryIds()) {
                    queryCatalogService->updateQueryStatus(queryId, QueryState::FAILED, ex.what());
                }
            } catch (QueryDeploymentException& ex) {
                NES_ERROR("QueryRequestProcessingService: QueryDeploymentException: {}", ex.what());
                auto sharedQueryId = ex.getQueryId();
                queryUndeploymentPhase->execute(sharedQueryId, SharedQueryPlanStatus::FAILED);
                auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
                for (auto queryId : sharedQueryPlan->getQueryIds()) {
                    queryCatalogService->updateQueryStatus(queryId, QueryState::FAILED, ex.what());
                }
            } catch (TypeInferenceException& ex) {
                NES_ERROR("QueryRequestProcessingService: TypeInferenceException: {}", ex.what());
                auto queryId = ex.getQueryId();
                queryCatalogService->updateQueryStatus(queryId, QueryState::FAILED, ex.what());
            } catch (Exceptions::InvalidQueryStateException& ex) {
                NES_ERROR("QueryRequestProcessingService: InvalidQueryStateException: {}", ex.what());
            } catch (Exceptions::QueryNotFoundException& ex) {
                NES_ERROR("QueryRequestProcessingService: QueryNotFoundException: {}", ex.what());
            } catch (Exceptions::QueryUndeploymentException& ex) {
                NES_ERROR("QueryRequestProcessingService: QueryUndeploymentException: {}", ex.what());
            } catch (InvalidQueryException& ex) {
                NES_ERROR("QueryRequestProcessingService InvalidQueryException: {}", ex.what());
            } catch (std::exception& ex) {
                NES_FATAL_ERROR("QueryProcessingService: Received unexpected exception while scheduling the "
                                "queryIdAndCatalogEntryMapping: {}",
                                ex.what());
                shutDown();
            }
        }
        NES_WARNING("QueryProcessingService: Terminated");
    } catch (std::exception& ex) {
        NES_FATAL_ERROR("QueryProcessingService: Received unexpected exception while scheduling the queryId : {}", ex.what());
        shutDown();
    }
    shutDown();
}

bool RequestProcessorService::isQueryProcessorRunning() {
    std::unique_lock<std::mutex> lock(queryProcessorStatusLock);
    return queryProcessorRunning;
}

void RequestProcessorService::shutDown() {
    std::unique_lock<std::mutex> lock(queryProcessorStatusLock);
    NES_INFO("Request Processor Service is shutting down! No further requests can be processed!");
    if (queryProcessorRunning) {
        this->queryProcessorRunning = false;
        queryRequestQueue->insertPoisonPill();
    }
}

}// namespace NES