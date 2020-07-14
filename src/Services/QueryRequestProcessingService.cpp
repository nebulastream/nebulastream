#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Deployer/QueryDeployer.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryPlacementPhase.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryRequestProcessingService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryRequestProcessingService::QueryRequestProcessingService(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan, QueryCatalogPtr queryCatalog, StreamCatalogPtr streamCatalog)
    : globalExecutionPlan(globalExecutionPlan), queryCatalog(queryCatalog) {

    NES_INFO("QueryProcessorService()");
    typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    queryRewritePhase = QueryRewritePhase::create();
    queryPlacementPhase = QueryPlacementPhase::create(globalExecutionPlan, nesTopologyPlan, typeInferencePhase, streamCatalog);
}

int QueryRequestProcessingService::operator()() {

    try {
        while (true) {
            const std::vector<QueryCatalogEntryPtr> queryCatalogEntryBatch = queryCatalog->getQueriesToSchedule();
            if (queryCatalogEntryBatch.empty()) {
                //process the queries using query-at-a-time model
                for (auto queryCatalogEntry : queryCatalogEntryBatch) {

                    auto queryPlan = queryCatalogEntry->getQueryPlan();
                    std::string queryId = queryPlan->getQueryId();
                    try {

                        if (queryCatalogEntry->getQueryStatus() == QueryStatus::MarkedForStop) {
                            NES_WARNING("QueryProcessingService: Request received for stopping the query " + queryId);
                            queryService->stopAndUndeployQuery(queryId);
                        } else if (queryCatalogEntry->getQueryStatus() == QueryStatus::Registered) {

                            queryCatalog->markQueryAs(queryId, QueryStatus::Scheduling);
                            std::string placementStrategy = queryCatalogEntry->getQueryPlacementStrategy();
                            queryPlan = queryRewritePhase->execute(queryPlan);
                            queryPlan = typeInferencePhase->execute(queryPlan);
                            queryPlacementPhase->execute(placementStrategy, queryPlan);
                            //Check if someone stopped the query meanwhile
                            if (queryCatalogEntry->getQueryStatus() == QueryStatus::MarkedForStop) {
                                NES_WARNING("QueryProcessingService: Found query with Id " + queryId + " stopped after performing the query placement.");
                                NES_WARNING("QueryProcessingService: Rolling back the placement for query with Id " + queryId);
                                queryService->stopAndUndeployQuery(queryId);
                                continue;
                            }

                            bool successful = queryService->deployAndStartQuery(queryId);
                            if (!successful) {
                                throw QueryDeploymentException("Failed to deploy query with Id " + queryId);
                            }
                        }

                    } catch (QueryPlacementException ex) {
                        //Rollback if failure happen while placing the query.
                        globalExecutionPlan->removeQuerySubPlans(queryId);
                        queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                    } catch (QueryDeploymentException ex) {
                        //Rollback if failure happen while placing the query.
                        globalExecutionPlan->removeQuerySubPlans(queryId);
                        queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                    } catch (Exception ex) {
                        globalExecutionPlan->removeQuerySubPlans(queryId);
                        queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                    }
                }
            } else {
                NES_INFO("QueryProcessingService: No query to schedule will try again in 5 seconds");
                sleep(5);
            }
        }
    } catch (...) {
        NES_FATAL_ERROR("QueryProcessingService: Received unexpected exception while scheduling the queries.");
        queryProcessorRunning = false;
    }
}

bool QueryRequestProcessingService::isQueryProcessorRunning() const {
    return queryProcessorRunning;
}

}// namespace NES