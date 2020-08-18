#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Deployer/QueryDeployer.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Phases/QueryPlacementPhase.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Phases/QueryPlacementRefinementPhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryRequestProcessorService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/QueryRequestQueue.hpp>

namespace NES {

QueryRequestProcessorService::QueryRequestProcessorService(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan,
                                                           QueryCatalogPtr queryCatalog, StreamCatalogPtr streamCatalog, WorkerRPCClientPtr workerRpcClient,
                                                           QueryDeployerPtr queryDeployer, QueryRequestQueuePtr queryRequestQueue)
    : queryProcessorStatusLock(), queryProcessorRunning(true), queryCatalog(queryCatalog), queryRequestQueue(queryRequestQueue) {

    NES_INFO("QueryProcessorService()");
    typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    queryRewritePhase = QueryRewritePhase::create();
    queryPlacementPhase = QueryPlacementPhase::create(globalExecutionPlan, nesTopologyPlan, typeInferencePhase, streamCatalog);
    queryDeploymentPhase = QueryDeploymentPhase::create(globalExecutionPlan, workerRpcClient, queryDeployer);
    queryUndeploymentPhase = QueryUndeploymentPhase::create(globalExecutionPlan, workerRpcClient);
    queryPlacementRefinementPhase = QueryPlacementRefinementPhase::create(globalExecutionPlan);
}

void QueryRequestProcessorService::start() {

    try {
        while (isQueryProcessorRunning()) {
            NES_INFO("QueryRequestProcessorService: Waiting for new query request trigger");
            const std::vector<QueryCatalogEntry> queryCatalogEntryBatch = queryRequestQueue->getNextBatch();
            NES_INFO("QueryProcessingService: Found " << queryCatalogEntryBatch.size() << " query requests to schedule");
            //process the queries using query-at-a-time model
            for (auto queryCatalogEntry : queryCatalogEntryBatch) {
                auto queryPlan = queryCatalogEntry.getQueryPlan();
                std::string queryId = queryPlan->getQueryId();
                try {
                    if (queryCatalogEntry.getQueryStatus() == QueryStatus::MarkedForStop) {
                        NES_INFO("QueryProcessingService: Request received for stopping the query " + queryId);
                        bool successful = queryUndeploymentPhase->execute(queryId);
                        if (!successful) {
                            throw QueryUndeploymentException("Unable to stop the query " + queryId);
                        }
                        queryCatalog->markQueryAs(queryId, QueryStatus::Stopped);
                    } else if (queryCatalogEntry.getQueryStatus() == QueryStatus::Registered) {
                        NES_INFO("QueryProcessingService: Request received for optimizing and deploying of the query " + queryId << " status=" << queryCatalogEntry.getQueryStatusAsString());
                        queryCatalog->markQueryAs(queryId, QueryStatus::Scheduling);
                        std::string placementStrategy = queryCatalogEntry.getQueryPlacementStrategy();
                        NES_DEBUG("QueryProcessingService: Performing Query rewrite phase for query: " + queryId);
                        queryPlan = queryRewritePhase->execute(queryPlan);
                        if (!queryPlan) {
                            throw Exception("QueryProcessingService: Failed during query rewrite phase for query: " + queryId);
                        }
                        NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " + queryId);
                        queryPlan = typeInferencePhase->execute(queryPlan);
                        if (!queryPlan) {
                            throw Exception("QueryProcessingService: Failed during Type inference phase for query: " + queryId);
                        }
                        NES_DEBUG("QueryProcessingService: Performing Query Operator placement for query: " + queryId);
                        bool placementSuccessful = queryPlacementPhase->execute(placementStrategy, queryPlan);
                        if (!placementSuccessful) {
                            throw QueryPlacementException("QueryProcessingService: Failed to perform query placement for query: " + queryId);
                        }
                        NES_DEBUG("QueryProcessingService: Performing Query Operator placement for query: " + queryId);
                        bool refinementSuccessful = queryPlacementRefinementPhase->execute(queryPlan->getQueryId());
                        if (!refinementSuccessful) {
                            throw QueryPlacementException("QueryProcessingService: Failed to perform query refinement for query: " + queryId);
                        }
                        bool successful = queryDeploymentPhase->execute(queryId);
                        if (!successful) {
                            throw QueryDeploymentException("QueryRequestProcessingService: Failed to deploy query with Id " + queryId);
                        }
                        queryCatalog->markQueryAs(queryId, QueryStatus::Running);
                    } else {
                        NES_ERROR("QueryProcessingService: Request received for query with status " << queryCatalogEntry.getQueryStatus() << " ");
                        throw InvalidQueryStatusException({QueryStatus::MarkedForStop, QueryStatus::Scheduling}, queryCatalogEntry.getQueryStatus());
                    }
                } catch (QueryPlacementException& ex) {
                    NES_ERROR("QueryRequestProcessingService: " << ex.what());
                    queryUndeploymentPhase->execute(queryId);
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                } catch (QueryDeploymentException& ex) {
                    NES_ERROR("QueryRequestProcessingService: " << ex.what());
                    queryUndeploymentPhase->execute(queryId);
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                } catch (InvalidQueryStatusException& ex) {
                    NES_ERROR("QueryRequestProcessingService: " << ex.what());
                    queryUndeploymentPhase->execute(queryId);
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                } catch (QueryNotFoundException& ex) {
                    NES_ERROR("QueryRequestProcessingService: " << ex.what());
                } catch (QueryUndeploymentException& ex) {
                    NES_ERROR("QueryRequestProcessingService: " << ex.what());
                } catch (Exception& ex) {
                    NES_ERROR("QueryRequestProcessingService: " << ex.what());
                    queryUndeploymentPhase->execute(queryId);
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                }
            }
        }
        NES_WARNING("QueryProcessingService: Terminated");
    } catch (...) {
        NES_FATAL_ERROR("QueryProcessingService: Received unexpected exception while scheduling the queries.");
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