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
    NES_INFO("GlobalQueryPlanUpdatePhase: Updating global query plan using a query request batch of size "
             << queryRequests.size());
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
                NES_ERROR("GlobalQueryPlanUpdatePhase: Received query request with unhandled status: "
                          << queryRequest.getQueryStatusAsString());
                throw Exception("GlobalQueryPlanUpdatePhase: Received query request with unhandled status"
                                + queryRequest.getQueryStatusAsString());
            }
            globalQueryPlan->updateGlobalQueryMetaDataMap();
        }
        NES_DEBUG("GlobalQueryPlanUpdatePhase: Successfully updated global query plan");
        return globalQueryPlan;
    } catch (std::exception& ex) {
        NES_ERROR("GlobalQueryPlanUpdatePhase: Exception occurred while updating global query plan with: " << ex.what());
        throw GlobalQueryPlanUpdateException("GlobalQueryPlanUpdatePhase: Exception occurred while updating Global Query Plan");
    }
}

}// namespace NES
