#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Services/QueryProcessorService.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryProcessorService::QueryProcessorService(QueryCatalogPtr queryCatalog, StreamCatalogPtr streamCatalog) : queryCatalog(queryCatalog) {
    NES_INFO("QueryProcessorService()");
    typeInferencePhase = TypeInferencePhase::create(streamCatalog);
}

int QueryProcessorService::operator()() {

    while (true) {
        const std::vector<QueryPlanPtr> queryBatch = queryCatalog->getQueriesToSchedule();
        if (queryBatch.empty()) {
            //process the queries using a-query-at-a-time model
            for (auto queryPlan : queryBatch) {
                queryPlan = typeInferencePhase->transform(queryPlan);
            }
        }
    }
}

}// namespace NES