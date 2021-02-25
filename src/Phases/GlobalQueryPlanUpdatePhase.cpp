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

#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/QueryCatalogEntry.hpp>
#include <Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Phases/QueryMergerPhase.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

GlobalQueryPlanUpdatePhase::GlobalQueryPlanUpdatePhase(QueryCatalogPtr queryCatalog, StreamCatalogPtr streamCatalog,
                                                       GlobalQueryPlanPtr globalQueryPlan, bool enableQueryMerging)
    : enableQueryMerging(enableQueryMerging), queryCatalog(queryCatalog), streamCatalog(streamCatalog),
      globalQueryPlan(globalQueryPlan) {
    queryMergerPhase = QueryMergerPhase::create();
    typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    queryRewritePhase = QueryRewritePhase::create(streamCatalog);
}

GlobalQueryPlanUpdatePhasePtr GlobalQueryPlanUpdatePhase::create(QueryCatalogPtr queryCatalog, StreamCatalogPtr streamCatalog,
                                                                 GlobalQueryPlanPtr globalQueryPlan, bool enableQueryMerging) {
    return std::make_shared<GlobalQueryPlanUpdatePhase>(
        GlobalQueryPlanUpdatePhase(queryCatalog, streamCatalog, globalQueryPlan, enableQueryMerging));
}

GlobalQueryPlanPtr GlobalQueryPlanUpdatePhase::execute(const std::vector<QueryCatalogEntry> queryRequests) {
    //FIXME: Proper error handling #1585
    try {
        for (auto queryRequest : queryRequests) {
            QueryId queryId = queryRequest.getQueryId();

            if (queryRequest.getQueryStatus() == QueryStatus::MarkedForStop) {
                NES_INFO("QueryProcessingService: Request received for stopping the query " << queryId);
                globalQueryPlan->removeQuery(queryId);
            } else if (queryRequest.getQueryStatus() == QueryStatus::Registered) {
                auto queryPlan = queryRequest.getQueryPlan();
                NES_INFO("QueryProcessingService: Request received for optimizing and deploying of the query "
                         << queryId << " status=" << queryRequest.getQueryStatusAsString());
                queryCatalog->markQueryAs(queryId, QueryStatus::Scheduling);

                NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " << queryId);
                queryPlan = typeInferencePhase->execute(queryPlan);

                NES_DEBUG("QueryProcessingService: Performing Query rewrite phase for query: " << queryId);
                queryPlan = queryRewritePhase->execute(queryPlan);
                if (!queryPlan) {
                    throw Exception("QueryProcessingService: Failed during query rewrite phase for query: "
                                    + std::to_string(queryId));
                }

                NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " << queryId);
                queryPlan = typeInferencePhase->execute(queryPlan);
                if (!queryPlan) {
                    throw Exception("QueryProcessingService: Failed during Type inference phase for query: "
                                    + std::to_string(queryId));
                }

                NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " << queryId);
                globalQueryPlan->addQueryPlan(queryPlan);
            } else {
                NES_ERROR("QueryProcessingService: Request received for query with status " << queryRequest.getQueryStatus());
                throw InvalidQueryStatusException({QueryStatus::MarkedForStop, QueryStatus::Scheduling},
                                                  queryRequest.getQueryStatus());
            }
        }

        if (enableQueryMerging) {
            NES_DEBUG("QueryProcessingService: Applying Query Merger Rules as Query Merging is enabled.");
            queryMergerPhase->execute(globalQueryPlan);
        }
        NES_DEBUG("GlobalQueryPlanUpdatePhase: Successfully updated global query plan");
        return globalQueryPlan;
    } catch (std::exception& ex) {
        NES_ERROR("GlobalQueryPlanUpdatePhase: Exception occurred while updating global query plan with: " << ex.what());
        throw GlobalQueryPlanUpdateException("GlobalQueryPlanUpdatePhase: Exception occurred while updating Global Query Plan");
    }
}

}// namespace NES
