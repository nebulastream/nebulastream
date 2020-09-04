#include <Catalogs/QueryCatalogEntry.hpp>
#include <Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

GlobalQueryPlanUpdatePhase::GlobalQueryPlanUpdatePhase(GlobalQueryPlanPtr globalQueryPlan) : globalQueryPlan(globalQueryPlan) {}

GlobalQueryPlanUpdatePhasePtr GlobalQueryPlanUpdatePhase::create(GlobalQueryPlanPtr globalQueryPlan) {
    return std::make_shared<GlobalQueryPlanUpdatePhase>(GlobalQueryPlanUpdatePhase(globalQueryPlan));
}

GlobalQueryPlanPtr GlobalQueryPlanUpdatePhase::execute(const std::vector<QueryCatalogEntry> queryRequests) {
    NES_INFO("GlobalQueryPlanUpdatePhase: Updating global query plan using a query request batch of size " << queryRequests.size());
    try {
        for (auto queryRequest : queryRequests) {
            QueryId queryId = queryRequest.getQueryId();
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
        throw GlobalQueryPlanUpdateException("GlobalQueryPlanUpdatePhase: Exception occurred while updating Global Query Plan");
    }
}

}// namespace NES
