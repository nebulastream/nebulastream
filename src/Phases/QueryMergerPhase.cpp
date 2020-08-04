#include <Exceptions/QueryMergerException.hpp>
#include <Phases/QueryMergerPhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryMergerPhase::QueryMergerPhase() {
    globalQueryPlan = GlobalQueryPlan::create();
}

QueryMergerPhasePtr QueryMergerPhase::create() {
    return std::make_shared<QueryMergerPhase>(QueryMergerPhase());
}

GlobalQueryPlanPtr QueryMergerPhase::execute(std::vector<QueryPlanPtr> batchOfQueries) {
    NES_INFO("QueryMergerPhase: Performing merger of a query batch of size " << batchOfQueries.size() << " into GlobalQueryPlan.");
    try {
        for (auto& queryPlan : batchOfQueries) {
            NES_TRACE("QueryMergerPhase: Merging query plan for the query: " << queryPlan->getQueryId());
            globalQueryPlan->addQueryPlan(queryPlan);
        }
        NES_DEBUG("QueryMergerPhase: Successfully merged batch of queries");
        return globalQueryPlan;
    } catch (std::exception ex) {
        NES_ERROR("QueryMergerPhase: Exception occurred while merging batch of queries: " << ex.what());
        throw QueryMergerException("QueryMergerPhase: Exception occurred while merging batch of queries");
    }
}

}// namespace NES
