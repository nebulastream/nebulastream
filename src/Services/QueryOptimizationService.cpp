#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Phases/QueryPlacementPhase.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Services/QueryOptimizationService.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryOptimizationService::QueryOptimizationService(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan, QueryCatalogPtr queryCatalog,
                                                   StreamCatalogPtr streamCatalog) : queryCatalog(queryCatalog) {
    NES_INFO("QueryProcessorService()");
    typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    queryRewritePhase = QueryRewritePhase::create();
    queryPlacementPhase = QueryPlacementPhase::create(globalExecutionPlan, nesTopologyPlan, typeInferencePhase, streamCatalog);
}

int QueryOptimizationService::operator()() {

    while (true) {
        const std::vector<QueryCatalogEntryPtr> queryCatalogEntryBatch = queryCatalog->getQueriesToSchedule();
        if (queryCatalogEntryBatch.empty()) {
            //process the queries using a-query-at-a-time model
            for (auto queryCatalogEntry : queryCatalogEntryBatch) {

                try {
                    std::string placementStrategy = queryCatalogEntry->getQueryPlacementStrategy();
                    auto queryPlan = queryCatalogEntry->getQueryPlan();
                    queryPlan = queryRewritePhase->execute(queryPlan);
                    queryPlan = typeInferencePhase->execute(queryPlan);
                    queryPlacementPhase->execute(placementStrategy, queryPlan);
                } catch (QueryPlacementException ex) {
                    //Rollback if failure happen while placing the query.
                } catch (Exception ex) {
                }
            }
        }
    }
}

}// namespace NES