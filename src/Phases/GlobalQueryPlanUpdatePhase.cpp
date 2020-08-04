#include <Catalogs/QueryCatalogEntry.hpp>
#include <Exceptions/QueryMergerException.hpp>
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

GlobalQueryPlanUpdatePhase::GlobalQueryPlanUpdatePhase() {
    globalQueryPlan = GlobalQueryPlan::create();
}

QueryMergerPhasePtr GlobalQueryPlanUpdatePhase::create() {
    return std::make_shared<GlobalQueryPlanUpdatePhase>(GlobalQueryPlanUpdatePhase());
}

GlobalQueryPlanPtr GlobalQueryPlanUpdatePhase::execute(const std::vector<QueryCatalogEntry> queryRequests) {
    NES_INFO("GlobalQueryPlanUpdatePhase: Updating global query plan using a query request batch of size " << queryRequests.size());
    try {
        for (auto queryRequest : queryRequests) {

            std::string queryId = queryRequest.getQueryId();
            if (queryRequest.getQueryStatus() == MarkedForStop) {
                NES_TRACE("GlobalQueryPlanUpdatePhase: Removing query plan for the query: " << queryId);
                globalQueryPlan->removeQuery(queryId);
            } else if (queryRequest.getQueryStatus() == Scheduling) {
                NES_TRACE("GlobalQueryPlanUpdatePhase: Adding query plan for the query: " << queryId);
                globalQueryPlan->addQueryPlan(queryRequest.getQueryPlan());
            } else {
                NES_ERROR("GlobalQueryPlanUpdatePhase: Received query request with unhandled status: " << queryRequest.getQueryStatusAsString());
                throw Exception("GlobalQueryPlanUpdatePhase: Received query request with unhandled status" + queryRequest.getQueryStatusAsString());
            }
        }
        NES_DEBUG("GlobalQueryPlanUpdatePhase: Successfully updated global query plan");
        return globalQueryPlan;
    } catch (std::exception& ex) {
        NES_ERROR("GlobalQueryPlanUpdatePhase: Exception occurred while updating global query plan with: " << ex.what());
        throw QueryMergerException("GlobalQueryPlanUpdatePhase: Exception occurred while updating Global Query Plan");
    }
}

}// namespace NES
