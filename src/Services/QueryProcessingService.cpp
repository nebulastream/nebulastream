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
#include <Services/QueryProcessingService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryProcessingService::QueryProcessingService(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan, QueryCatalogPtr queryCatalog,
                                               StreamCatalogPtr streamCatalog, QueryDeployerPtr queryDeployer, WorkerRPCClientPtr workerRPCClient)
    : globalExecutionPlan(globalExecutionPlan), queryCatalog(queryCatalog), queryDeployer(queryDeployer), workerRPCClient(workerRPCClient) {

    NES_INFO("QueryProcessorService()");
    typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    queryRewritePhase = QueryRewritePhase::create();
    queryPlacementPhase = QueryPlacementPhase::create(globalExecutionPlan, nesTopologyPlan, typeInferencePhase, streamCatalog);
}

int QueryProcessingService::operator()() {
    try {

        while (true) {
            const std::vector<QueryCatalogEntryPtr> queryCatalogEntryBatch = queryCatalog->getQueriesToSchedule();
            if (queryCatalogEntryBatch.empty()) {
                //process the queries using query-at-a-time model
                for (auto queryCatalogEntry : queryCatalogEntryBatch) {

                    auto queryPlan = queryCatalogEntry->getQueryPlan();
                    std::string queryId = queryPlan->getQueryId();
                    if (queryCatalogEntry->getQueryStatus() == QueryStatus::Stopped) {
                        NES_WARNING("QueryProcessingService: ");
                        continue;
                    }

                    std::string placementStrategy = queryCatalogEntry->getQueryPlacementStrategy();

                    try {
                        queryCatalog->markQueryAs(queryId, QueryStatus::Scheduling);
                        queryPlan = queryRewritePhase->execute(queryPlan);
                        queryPlan = typeInferencePhase->execute(queryPlan);
                        queryPlacementPhase->execute(placementStrategy, queryPlan);
                        //Check if someone stopped the query meanwhile
                        if (queryCatalogEntry->getQueryStatus() == QueryStatus::MarkedForStop) {
                            NES_WARNING("QueryProcessingService: Found query with Id " + queryId + " stopped after performing the query placement.");
                            NES_WARNING("QueryProcessingService: Rolling back the placement for query with Id " + queryId);
                            queryService->stopQuery(queryId);
                            globalExecutionPlan->removeQuerySubPlans(queryId);
                            continue;
                        }

                        bool successful = queryService->deployAndStartQuery(queryId);
                        if (!successful) {
                            throw QueryDeploymentException("Failed to deploy query with Id " + queryId);
                        }
                        queryCatalog->markQueryAs(queryId, QueryStatus::Running);
                    } catch (QueryPlacementException ex) {
                        //Rollback if failure happen while placing the query.
                        queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                    } catch (QueryDeploymentException ex) {
                        //Rollback if failure happen while placing the query.
                        queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                    } catch (Exception ex) {
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

bool QueryProcessingService::isQueryProcessorRunning() const {
    return queryProcessorRunning;
}

}// namespace NES